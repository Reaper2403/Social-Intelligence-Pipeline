"""
src/loaders/db_loader.py
------------------------
Database Loader — persists every pipeline artefact to the relational DB.

This loader is the DB equivalent of json_loader.py. It is called by pipeline.py
after each stage and mirrors all data into the configured database.

Responsibilities
----------------
  upsert_posts()        → insert new posts; update keywords on duplicates
  upsert_comments()     → insert new / skip existing comments (by reddit_id)
  upsert_analyses()     → insert AI results; skip already-analyzed IDs
  mark_reported()       → flip analyses.reported = True for written reports
  start_run()           → create a PipelineRun audit record
  finish_run()          → update the run with completion stats

Design
------
  - All writes are upserts (INSERT OR IGNORE / ON CONFLICT DO UPDATE) so the
    loader is idempotent — re-running the pipeline never creates duplicates.
  - The caller (pipeline.py) owns the session lifecycle; this module provides
    stateless functions that accept an open session.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.db.models import Analysis, Comment, PipelineRun, Post

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_dt(dt_str: str | None) -> datetime | None:
    """Parse 'YYYY-MM-DD HH:MM:SS' strings produced by the extractor."""
    if not dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# PipelineRun
# ---------------------------------------------------------------------------

def start_run(session: Session) -> PipelineRun:
    """Insert a new PipelineRun row and return it (with id populated)."""
    run = PipelineRun(status="running")
    session.add(run)
    session.flush()   # populate run.id without committing
    logger.info(f"Pipeline run #{run.id} started.")
    return run


def finish_run(
    session: Session,
    run: PipelineRun,
    *,
    status: str = "complete",
    new_posts: int = 0,
    updated_posts: int = 0,
    opportunities_found: int = 0,
    analyses_completed: int = 0,
    reports_generated: int = 0,
) -> None:
    """Update the PipelineRun row with completion stats."""
    run.finished_at         = datetime.utcnow()
    run.status              = status
    run.new_posts           = new_posts
    run.updated_posts       = updated_posts
    run.opportunities_found = opportunities_found
    run.analyses_completed  = analyses_completed
    run.reports_generated   = reports_generated
    logger.info(f"Pipeline run #{run.id} → {status}.")


# ---------------------------------------------------------------------------
# Posts & Comments
# ---------------------------------------------------------------------------

def upsert_posts(session: Session, new_posts: list[dict], run_id: int | None = None) -> int:
    """
    Insert new posts; if a post already exists (same reddit_id), update its
    keywords and opportunity score only.

    Returns the number of rows actually inserted (not updated).
    """
    inserted = 0
    for post_record in new_posts:
        det = post_record["post_details"]
        reddit_id = det["id"]

        existing = session.scalar(select(Post).where(Post.reddit_id == reddit_id))

        if existing:
            # Merge keyword list only
            old_kws = set(existing.keywords.split(",")) if existing.keywords else set()
            new_kws = set(post_record["search_info"].get("keywords", []))
            existing.keywords = ",".join(sorted(old_kws | new_kws))
            existing.opportunity_score = det.get("opportunity_score_post", existing.opportunity_score)
            logger.debug(f"Post {reddit_id} already in DB — keywords updated.")
        else:
            post_row = Post(
                run_id=run_id,
                reddit_id=reddit_id,
                subreddit=det.get("subreddit", ""),
                subreddit_searched=post_record["search_info"].get("subreddit_searched", ""),
                keywords=",".join(post_record["search_info"].get("keywords", [])),
                title=det.get("title", ""),
                body=det.get("body", ""),
                author=det.get("author", ""),
                url=det.get("url", ""),
                permalink=det.get("permalink", ""),
                images=det.get("images", ""),
                is_video=det.get("is_video"),
                over_18=det.get("over_18"),
                score=det.get("score", 0),
                upvote_ratio=det.get("upvote_ratio", 0.0),
                num_comments=det.get("num_comments", 0),
                age_hours=det.get("age_hours", 0.0),
                opportunity_score=det.get("opportunity_score_post", 0.0),
                reddit_created_utc=_parse_dt(det.get("created_utc")),
            )
            session.add(post_row)
            session.flush()  # populate post_row.id for FK use below

            # Insert comments that belong to this post
            for cmt in post_record.get("top_comments", []):
                cmt_id = cmt.get("id")
                if not cmt_id:
                    continue
                exists_cmt = session.scalar(select(Comment).where(Comment.reddit_id == cmt_id))
                if not exists_cmt:
                    session.add(Comment(
                        post_id=post_row.id,
                        reddit_id=cmt_id,
                        author=cmt.get("author", ""),
                        body=cmt.get("body", ""),
                        rank=cmt.get("rank", 0),
                        score=cmt.get("score", 0),
                        num_replies=cmt.get("num_replies", 0),
                        depth=cmt.get("depth", 0),
                        opportunity_score=cmt.get("opportunity_score_reply", 0.0),
                        reddit_created_utc=_parse_dt(cmt.get("created_utc")),
                    ))

            inserted += 1

    logger.info(f"DB upsert: {inserted} posts inserted, {len(new_posts) - inserted} already existed.")
    return inserted


# ---------------------------------------------------------------------------
# AI Analyses
# ---------------------------------------------------------------------------

def upsert_analyses(session: Session, analyses: list[dict]) -> int:
    """
    Insert AI analysis results. Skips any opportunity_id already in the table.
    Returns the number of rows inserted.
    """
    inserted = 0
    for item in analyses:
        opp_id = item.get("opportunity_id")
        if not opp_id:
            continue

        existing = session.scalar(select(Analysis).where(Analysis.opportunity_id == opp_id))
        if existing:
            logger.debug(f"Analysis for {opp_id} already in DB — skipping.")
            continue

        # Derive target_reddit_id and type from opp_id (e.g. "post_abc123")
        parts = opp_id.split("_", 1)
        opp_type    = parts[0] if len(parts) == 2 else "unknown"
        target_rid  = parts[1] if len(parts) == 2 else opp_id

        session.add(Analysis(
            opportunity_id=opp_id,
            opportunity_type=opp_type,
            target_reddit_id=target_rid,
            status=item.get("status", ""),
            reason=item.get("reason"),
            conversation_theme=item.get("conversation_theme"),
            relevant_philosophy=item.get("relevant_philosophy"),
            strategic_direction=item.get("strategic_direction"),
            reported=False,
        ))
        inserted += 1

    logger.info(f"DB upsert: {inserted} analyses inserted.")
    return inserted


# ---------------------------------------------------------------------------
# Reported flag
# ---------------------------------------------------------------------------

def mark_reported(session: Session, reported_ids: set[str]) -> None:
    """
    Flip analyses.reported = True for every opportunity_id in reported_ids.
    Called after the ReportLoader generates Word documents.
    """
    if not reported_ids:
        return
    rows = session.scalars(
        select(Analysis).where(Analysis.opportunity_id.in_(reported_ids))
    ).all()
    for row in rows:
        row.reported = True
    logger.info(f"Marked {len(rows)} analyses as reported in DB.")
