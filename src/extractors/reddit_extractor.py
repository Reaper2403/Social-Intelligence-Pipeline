"""
Extractor: Reddit
-----------------
Encapsulates all Reddit API interaction and master-store management.

Responsibilities:
  - Authenticate with PRAW using env credentials.
  - Search configured subreddits / keywords.
  - Deduplicate posts against the master store.
  - Compute opportunity scores (post-level and comment-level).
  - Return newly scraped posts (does NOT write to disk — caller handles I/O).

Public API
----------
    extractor = RedditExtractor(config)
    new_posts, updated_ids = extractor.run(existing_posts_data)
"""

import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import praw
from dotenv import load_dotenv
from praw.exceptions import PRAWException

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Opportunity-score weight constants
# ---------------------------------------------------------------------------
W1_SCORE_VELOCITY  = 1.0
W2_COMMENT_VELOCITY = 1.5
W3_COMMENT_SCORE   = 1.0
W4_COMMENT_REPLIES = 2.0
AGE_SMOOTHING      = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_text(text: str | None) -> str:
    if text is None:
        return ""
    cleaned = re.sub(r"\s+", " ", text.strip())
    cleaned = cleaned.replace('"', '""')
    return cleaned


def _extract_images(submission) -> str:
    images = []
    if hasattr(submission, "url") and submission.url:
        if any(ext in submission.url.lower() for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
            images.append(submission.url)
    if getattr(submission, "is_gallery", False):
        meta = getattr(submission, "media_metadata", None)
        if meta:
            for item in meta.values():
                if "s" in item and "u" in item["s"]:
                    images.append(item["s"]["u"].replace("preview.redd.it", "i.redd.it"))
    return "; ".join(images)


def _calc_post_score(score: float, upvote_ratio: float, num_comments: int, age_hours: float) -> float:
    denom = age_hours + AGE_SMOOTHING
    if denom == 0:
        return 0.0
    return round(
        W1_SCORE_VELOCITY * (score * upvote_ratio) / denom
        + W2_COMMENT_VELOCITY * num_comments / denom,
        2,
    )


def _calc_comment_score(post_score: float, comment_score: int, num_replies: int, depth: int) -> float:
    depth_factor = 1 / (depth + 1)
    return round(
        post_score + (W3_COMMENT_SCORE * comment_score + W4_COMMENT_REPLIES * num_replies) * depth_factor,
        2,
    )


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class RedditExtractor:
    """
    Scrapes Reddit using the config dict and deduplicates against an
    existing posts dict (post_id → post record).

    Args:
        config: dict loaded from config.json via json_loader.load_config()
    """

    def __init__(self, config: dict):
        self.config = config
        self._reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT"),
        )
        logger.info("RedditExtractor initialised (PRAW authenticated).")

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self, existing_posts: dict) -> tuple[list, list]:
        """
        Execute the extraction.

        Args:
            existing_posts: dict mapping post_id → post record (from master store)

        Returns:
            (new_posts, updated_ids)
              new_posts   — list of brand-new post records (newly_added=True)
              updated_ids — list of post_ids whose keyword lists were extended
        """
        search_cfg  = self.config["search_settings"]
        api_cfg     = self.config["api_settings"]
        filter_cfg  = self.config["filter_settings"]
        export_cfg  = self.config["export_settings"]

        keywords        = search_cfg["keywords"]
        subreddits      = search_cfg.get("target_subreddits", [])
        limit_per_kw    = search_cfg["posts_per_keyword"]
        sort_method     = search_cfg["sort_method"].lower()
        time_filter     = search_cfg.get("time_filter", "week")
        min_score       = filter_cfg.get("min_score", 0)
        excl_nsfw       = filter_cfg.get("exclude_nsfw", False)
        excl_deleted    = filter_cfg.get("exclude_deleted_posts", True)
        incl_comments   = export_cfg.get("include_comments", True)
        max_comments    = export_cfg.get("max_comments_per_post", 10)
        incl_images     = export_cfg.get("include_images", True)
        excl_del_cmts   = filter_cfg.get("exclude_deleted_comments", True)
        rate_delay      = api_cfg.get("rate_limit_delay", 1.0)
        proc_delay      = api_cfg.get("post_processing_delay", 0.5)

        # ---- Collect posts across all keyword × subreddit combos ----------
        session_hits: dict[str, dict] = {}   # post_id → {submission, matching_keywords, subreddit}

        for keyword in keywords:
            logger.info(f"Keyword: '{keyword}'")
            for subreddit_name in (subreddits or ["all"]):
                try:
                    sub = self._reddit.subreddit(subreddit_name)
                    if sort_method == "top":
                        posts = sub.search(keyword, sort="top", time_filter=time_filter, limit=limit_per_kw)
                    elif sort_method == "hot":
                        posts = sub.search(keyword, sort="hot", limit=limit_per_kw)
                    elif sort_method == "new":
                        posts = sub.search(keyword, sort="new", limit=limit_per_kw)
                    elif sort_method == "controversial":
                        posts = sub.search(keyword, sort="controversial", time_filter=time_filter, limit=limit_per_kw)
                    else:
                        posts = sub.search(keyword, sort="top", time_filter=time_filter, limit=limit_per_kw)

                    for submission in posts:
                        try:
                            if excl_nsfw and submission.over_18:
                                continue
                            if submission.score < min_score:
                                continue
                            if excl_deleted and (submission.author is None or submission.selftext == "[deleted]"):
                                continue

                            pid = submission.id
                            if pid in session_hits:
                                session_hits[pid]["matching_keywords"].add(keyword)
                            else:
                                session_hits[pid] = {
                                    "submission": submission,
                                    "matching_keywords": {keyword},
                                    "subreddit_searched": subreddit_name,
                                }
                        except Exception as e:
                            logger.warning(f"Post error in r/{subreddit_name}: {e}")

                    time.sleep(rate_delay)
                except Exception as e:
                    err_str = str(e)
                    if "Redirect to /subreddits/search" in err_str:
                        logger.warning(
                            f"r/{subreddit_name} appears to be banned, quarantined, or deleted "
                            f"(Reddit redirected to subreddit search). Skipping. "
                            f"Remove it from target_subreddits in config.json to suppress this warning."
                        )
                    else:
                        logger.error(f"Search error in r/{subreddit_name}: {e}")

        logger.info(f"Unique session posts found: {len(session_hits)}")

        # ---- Deduplicate & process ----------------------------------------
        new_posts: list = []
        updated_ids: list = []

        for pid, info in session_hits.items():
            kws = list(info["matching_keywords"])

            if pid in existing_posts:
                # Extend keyword list in-place on the caller's dict
                old_kws = set(existing_posts[pid]["search_info"]["keywords"])
                existing_posts[pid]["search_info"]["keywords"] = list(old_kws | set(kws))
                updated_ids.append(pid)
                logger.info(f"  Updated keywords for existing post {pid}.")
                continue

            submission = info["submission"]
            try:
                now_utc = datetime.now(timezone.utc)
                created = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
                age_hours = round((now_utc - created).total_seconds() / 3600, 2)

                post_opp_score = _calc_post_score(
                    submission.score, submission.upvote_ratio,
                    submission.num_comments, age_hours
                )

                comments_data: list = []
                if incl_comments:
                    submission.comments.replace_more(limit=0)
                    top_comments = sorted(submission.comments, key=lambda x: x.score, reverse=True)[:max_comments]
                    processed_comment_ids: set = set()

                    for rank, comment in enumerate(top_comments, 1):
                        try:
                            if excl_del_cmts and (comment.author is None or comment.body == "[deleted]"):
                                continue
                            cid = comment.id
                            if cid in processed_comment_ids:
                                continue
                            processed_comment_ids.add(cid)

                            num_replies = len(comment.replies.list())
                            depth       = getattr(comment, "depth", 0)
                            cmt_score   = _calc_comment_score(post_opp_score, comment.score, num_replies, depth)

                            comments_data.append({
                                "id": cid,
                                "rank": rank,
                                "author": str(comment.author) if comment.author else "[deleted]",
                                "body": _clean_text(comment.body),
                                "score": comment.score,
                                "created_utc": datetime.fromtimestamp(comment.created_utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "num_replies": num_replies,
                                "depth": depth,
                                "opportunity_score_reply": cmt_score,
                            })
                        except Exception as e:
                            logger.warning(f"  Comment error: {e}")

                post_record = {
                    "search_info": {
                        "keywords": kws,
                        "subreddit_searched": info["subreddit_searched"],
                    },
                    "post_details": {
                        "id": pid,
                        "subreddit": submission.subreddit.display_name,
                        "title": _clean_text(submission.title),
                        "body": _clean_text(submission.selftext),
                        "author": str(submission.author) if submission.author else "[deleted]",
                        "score": submission.score,
                        "upvote_ratio": submission.upvote_ratio,
                        "num_comments": submission.num_comments,
                        "created_utc": datetime.fromtimestamp(submission.created_utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "url": submission.url,
                        "permalink": f"https://reddit.com{submission.permalink}",
                        "images": _extract_images(submission) if incl_images else "",
                        "is_video": submission.is_video,
                        "over_18": submission.over_18,
                        "age_hours": age_hours,
                        "opportunity_score_post": post_opp_score,
                    },
                    "top_comments": comments_data,
                    "newly_added": True,
                }

                new_posts.append(post_record)
                time.sleep(proc_delay)

            except Exception as e:
                logger.error(f"  Error processing post {pid}: {e}")

        logger.info(f"Extraction done — new: {len(new_posts)}, updated: {len(updated_ids)}")
        return new_posts, updated_ids
