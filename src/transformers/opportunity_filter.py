"""
Transformer: Opportunity Filter
---------------------------------
Reads the master Reddit data, computes per-score thresholds, and selects
only the top-N% of posts/comments that haven't yet been processed.

This is a pure transformer — it receives data and returns data.
All disk I/O is handled by the caller (via json_loader).

Public API
----------
    from src.transformers.opportunity_filter import OpportunityFilter

    opp_filter = OpportunityFilter(percentile=95)
    ai_input, new_ids = opp_filter.run(master_data, already_processed_ids)
"""

import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class OpportunityFilter:
    """
    Filters Reddit posts/comments to a high-value subset for AI analysis.

    Args:
        percentile: the score percentile used as the inclusion threshold (default 95)
    """

    def __init__(self, percentile: int = 95):
        if not 0 < percentile < 100:
            raise ValueError(f"percentile must be between 1 and 99, got {percentile}")
        self.percentile = percentile

    def run(
        self,
        master_data: dict,
        processed_ids: set,
    ) -> tuple[list[dict], set]:
        """
        Select new, high-value opportunities from the master data store.

        Args:
            master_data:   dict from json_loader.load_master_store()
            processed_ids: set of opportunity IDs already sent for AI analysis

        Returns:
            (ai_input, new_ids)
              ai_input  — list of opportunity dicts ready for the AI stage
              new_ids   — set of opportunity IDs included in this batch
        """
        all_posts = master_data.get("posts", [])
        if not all_posts:
            logger.warning("master_data contains no posts.")
            return [], set()

        # ---- Compute thresholds -----------------------------------------
        post_scores = [
            p["post_details"]["opportunity_score_post"]
            for p in all_posts
            if p.get("post_details", {}).get("opportunity_score_post") is not None
        ]
        comment_scores = [
            c["opportunity_score_reply"]
            for p in all_posts
            for c in p.get("top_comments", [])
            if c.get("opportunity_score_reply") is not None
        ]

        if not post_scores or not comment_scores:
            logger.error("No opportunity scores found in master data.")
            return [], set()

        post_threshold    = float(np.percentile(post_scores, self.percentile))
        comment_threshold = float(np.percentile(comment_scores, self.percentile))
        logger.info(
            f"Thresholds (top {100 - self.percentile}%) — "
            f"Post: {post_threshold:.2f}, Comment: {comment_threshold:.2f}"
        )

        # ---- Filter --------------------------------------------------------
        ai_input: list[dict] = []
        new_ids: set = set()
        skipped = 0

        for post in all_posts:
            details  = post.get("post_details", {})
            post_id  = details.get("id")

            # --- Post-level opportunity ---
            if post_id:
                opp_id = f"post_{post_id}"
                if opp_id in processed_ids:
                    skipped += 1
                else:
                    score = details.get("opportunity_score_post")
                    if score and score > post_threshold:
                        ai_input.append({
                            "opportunity_id":   opp_id,
                            "opportunity_type": "Reply to Post",
                            "post_title":       details.get("title", ""),
                            "post_body":        details.get("body", ""),
                        })
                        new_ids.add(opp_id)

            # --- Comment-level opportunities ---
            for comment in post.get("top_comments", []):
                cid = comment.get("id")
                if not cid:
                    continue
                opp_id = f"comment_{cid}"
                if opp_id in processed_ids:
                    skipped += 1
                    continue
                score = comment.get("opportunity_score_reply")
                if score and score > comment_threshold:
                    ai_input.append({
                        "opportunity_id":       opp_id,
                        "opportunity_type":     "Reply to Comment",
                        "post_title":           details.get("title", ""),
                        "post_body":            details.get("body", ""),
                        "target_comment_text":  comment.get("body", ""),
                    })
                    new_ids.add(opp_id)

        logger.info(
            f"Filter complete — new opportunities: {len(ai_input)}, "
            f"already processed (skipped): {skipped}"
        )
        return ai_input, new_ids
