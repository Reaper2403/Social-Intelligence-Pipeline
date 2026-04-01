"""
tests/test_phase2_db.py
-----------------------
Unit tests for Phase 2: Database Integration.

Uses an in-memory SQLite database so no files are written to disk.
No real Reddit API or Anthropic API calls are made.

Run with:
    cd "Reddit Projects"
    python -m pytest tests/test_phase2_db.py -v
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.models import Analysis, Base, Comment, PipelineRun, Post
from src.loaders import db_loader


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def in_memory_session():
    """Provide a fresh in-memory SQLite session for each test."""
    engine  = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)


def _make_post_record(reddit_id: str = "abc123", score: int = 100) -> dict:
    """Build a minimal post record matching the extractor's output schema."""
    return {
        "search_info": {
            "keywords": ["productivity", "burnout"],
            "subreddit_searched": "productivity",
        },
        "post_details": {
            "id": reddit_id,
            "subreddit": "productivity",
            "title": f"Test Post {reddit_id}",
            "body": "Post body text here.",
            "author": "test_user",
            "score": score,
            "upvote_ratio": 0.95,
            "num_comments": 42,
            "age_hours": 12.5,
            "opportunity_score_post": 88.5,
            "url": f"https://reddit.com/r/productivity/{reddit_id}",
            "permalink": f"https://reddit.com/r/productivity/comments/{reddit_id}",
            "images": "",
            "is_video": False,
            "over_18": False,
            "created_utc": "2024-01-15 10:00:00",
        },
        "top_comments": [
            {
                "id": f"c_{reddit_id}_1",
                "author": "commenter_a",
                "body": "Great insight!",
                "rank": 1,
                "score": 55,
                "num_replies": 3,
                "depth": 0,
                "opportunity_score_reply": 95.2,
                "created_utc": "2024-01-15 11:00:00",
            }
        ],
        "newly_added": True,
    }


def _make_analysis(opp_id: str, status: str = "Suitable") -> dict:
    return {
        "opportunity_id":      opp_id,
        "status":              status,
        "reason":              None if status == "Suitable" else "Off topic",
        "conversation_theme":  "Productivity" if status == "Suitable" else None,
        "relevant_philosophy": "Zero-Effort Value" if status == "Suitable" else None,
        "strategic_direction": "Engage with passive tracking angle" if status == "Suitable" else None,
    }


# ===========================================================================
# PipelineRun tests
# ===========================================================================
class TestPipelineRun:
    def test_start_run_creates_row(self, in_memory_session):
        run = db_loader.start_run(in_memory_session)
        in_memory_session.flush()
        assert run.id is not None
        assert run.status == "running"

    def test_finish_run_updates_status(self, in_memory_session):
        run = db_loader.start_run(in_memory_session)
        in_memory_session.flush()
        db_loader.finish_run(in_memory_session, run, status="complete", new_posts=5)
        assert run.status == "complete"
        assert run.new_posts == 5
        assert run.finished_at is not None

    def test_multiple_runs_are_independent(self, in_memory_session):
        run1 = db_loader.start_run(in_memory_session)
        in_memory_session.flush()
        run2 = db_loader.start_run(in_memory_session)
        in_memory_session.flush()
        assert run1.id != run2.id


# ===========================================================================
# Post upsert tests
# ===========================================================================
class TestUpsertPosts:
    def test_insert_new_post(self, in_memory_session):
        post = _make_post_record("p001")
        inserted = db_loader.upsert_posts(in_memory_session, [post])
        in_memory_session.commit()
        assert inserted == 1
        row = in_memory_session.scalar(select(Post).where(Post.reddit_id == "p001"))
        assert row is not None
        assert row.title == "Test Post p001"
        assert row.opportunity_score == 88.5

    def test_post_keywords_stored(self, in_memory_session):
        post = _make_post_record("p002")
        db_loader.upsert_posts(in_memory_session, [post])
        in_memory_session.commit()
        row = in_memory_session.scalar(select(Post).where(Post.reddit_id == "p002"))
        assert "productivity" in row.keywords
        assert "burnout" in row.keywords

    def test_duplicate_post_not_inserted_twice(self, in_memory_session):
        post = _make_post_record("p003")
        db_loader.upsert_posts(in_memory_session, [post])
        in_memory_session.commit()
        inserted2 = db_loader.upsert_posts(in_memory_session, [post])
        in_memory_session.commit()
        assert inserted2 == 0
        count = in_memory_session.query(Post).filter(Post.reddit_id == "p003").count()
        assert count == 1

    def test_duplicate_post_merges_keywords(self, in_memory_session):
        post1 = _make_post_record("p004")
        post1["search_info"]["keywords"] = ["productivity"]
        db_loader.upsert_posts(in_memory_session, [post1])
        in_memory_session.commit()

        post2 = _make_post_record("p004")
        post2["search_info"]["keywords"] = ["burnout"]
        db_loader.upsert_posts(in_memory_session, [post2])
        in_memory_session.commit()

        row = in_memory_session.scalar(select(Post).where(Post.reddit_id == "p004"))
        assert "productivity" in row.keywords
        assert "burnout" in row.keywords

    def test_comments_inserted_with_post(self, in_memory_session):
        post = _make_post_record("p005")
        db_loader.upsert_posts(in_memory_session, [post])
        in_memory_session.commit()

        post_row = in_memory_session.scalar(select(Post).where(Post.reddit_id == "p005"))
        assert len(post_row.comments) == 1
        assert post_row.comments[0].reddit_id == "c_p005_1"
        assert post_row.comments[0].opportunity_score == 95.2

    def test_multiple_posts_inserted(self, in_memory_session):
        posts = [_make_post_record(f"p{i:03d}") for i in range(5)]
        inserted = db_loader.upsert_posts(in_memory_session, posts)
        in_memory_session.commit()
        assert inserted == 5
        assert in_memory_session.query(Post).count() == 5

    def test_post_run_id_linked(self, in_memory_session):
        run = db_loader.start_run(in_memory_session)
        in_memory_session.flush()
        post = _make_post_record("p006")
        db_loader.upsert_posts(in_memory_session, [post], run_id=run.id)
        in_memory_session.commit()
        row = in_memory_session.scalar(select(Post).where(Post.reddit_id == "p006"))
        assert row.run_id == run.id


# ===========================================================================
# Analysis upsert tests
# ===========================================================================
class TestUpsertAnalyses:
    def test_insert_analysis(self, in_memory_session):
        analysis = _make_analysis("post_abc")
        inserted = db_loader.upsert_analyses(in_memory_session, [analysis])
        in_memory_session.commit()
        assert inserted == 1
        row = in_memory_session.scalar(
            select(Analysis).where(Analysis.opportunity_id == "post_abc")
        )
        assert row is not None
        assert row.status == "Suitable"
        assert row.opportunity_type == "post"
        assert row.target_reddit_id == "abc"

    def test_duplicate_analysis_not_inserted(self, in_memory_session):
        analysis = _make_analysis("post_dup")
        db_loader.upsert_analyses(in_memory_session, [analysis])
        in_memory_session.commit()
        inserted2 = db_loader.upsert_analyses(in_memory_session, [analysis])
        in_memory_session.commit()
        assert inserted2 == 0

    def test_unsuitable_analysis_stored(self, in_memory_session):
        analysis = _make_analysis("comment_xyz", status="Unsuitable")
        db_loader.upsert_analyses(in_memory_session, [analysis])
        in_memory_session.commit()
        row = in_memory_session.scalar(
            select(Analysis).where(Analysis.opportunity_id == "comment_xyz")
        )
        assert row.status == "Unsuitable"
        assert row.reason == "Off topic"

    def test_reported_defaults_to_false(self, in_memory_session):
        db_loader.upsert_analyses(in_memory_session, [_make_analysis("post_new")])
        in_memory_session.commit()
        row = in_memory_session.scalar(
            select(Analysis).where(Analysis.opportunity_id == "post_new")
        )
        assert row.reported is False


# ===========================================================================
# mark_reported tests
# ===========================================================================
class TestMarkReported:
    def test_mark_reported_flips_flag(self, in_memory_session):
        db_loader.upsert_analyses(in_memory_session, [_make_analysis("post_r1")])
        in_memory_session.commit()
        db_loader.mark_reported(in_memory_session, {"post_r1"})
        in_memory_session.commit()
        row = in_memory_session.scalar(
            select(Analysis).where(Analysis.opportunity_id == "post_r1")
        )
        assert row.reported is True

    def test_mark_reported_empty_set_is_noop(self, in_memory_session):
        # Should not raise
        db_loader.mark_reported(in_memory_session, set())
        in_memory_session.commit()

    def test_mark_reported_only_affects_specified_ids(self, in_memory_session):
        db_loader.upsert_analyses(in_memory_session, [
            _make_analysis("post_a"),
            _make_analysis("post_b"),
        ])
        in_memory_session.commit()
        db_loader.mark_reported(in_memory_session, {"post_a"})
        in_memory_session.commit()

        row_a = in_memory_session.scalar(select(Analysis).where(Analysis.opportunity_id == "post_a"))
        row_b = in_memory_session.scalar(select(Analysis).where(Analysis.opportunity_id == "post_b"))
        assert row_a.reported is True
        assert row_b.reported is False


# ===========================================================================
# Schema / model integrity tests
# ===========================================================================
class TestSchema:
    def test_all_tables_created(self, in_memory_session):
        from sqlalchemy import inspect
        inspector = inspect(in_memory_session.bind)
        tables = inspector.get_table_names()
        assert "runs"     in tables
        assert "posts"    in tables
        assert "comments" in tables
        assert "analyses" in tables

    def test_post_comment_cascade_delete(self, in_memory_session):
        post = _make_post_record("del001")
        db_loader.upsert_posts(in_memory_session, [post])
        in_memory_session.commit()

        post_row = in_memory_session.scalar(select(Post).where(Post.reddit_id == "del001"))
        in_memory_session.delete(post_row)
        in_memory_session.commit()

        # Comments should be deleted via cascade
        count = in_memory_session.query(Comment).filter(Comment.reddit_id == "c_del001_1").count()
        assert count == 0

    def test_datetime_parsing(self):
        assert db_loader._parse_dt("2024-01-15 10:00:00") == datetime(2024, 1, 15, 10, 0, 0)
        assert db_loader._parse_dt(None) is None
        assert db_loader._parse_dt("invalid") is None
