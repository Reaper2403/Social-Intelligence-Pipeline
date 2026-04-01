"""
src/db/models.py
----------------
SQLAlchemy ORM models for the Social Engineering Research Pipeline.

Tables
------
  runs          — one row per pipeline execution (metadata)
  posts         — one row per Reddit post
  comments      — one row per comment (FK → posts)
  analyses      — one row per AI analysis result (FK → posts or comments)

Design notes
------------
- Uses SQLAlchemy 2.x declarative style with type annotations.
- The engine is configured via DATABASE_URL in .env (defaults to local SQLite).
- Alembic is used for schema migrations (see alembic/).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# PipelineRun  — audit table, one row per pipeline invocation
# ---------------------------------------------------------------------------

class PipelineRun(Base):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="running")   # running | complete | failed
    new_posts: Mapped[int] = mapped_column(Integer, default=0)
    updated_posts: Mapped[int] = mapped_column(Integer, default=0)
    opportunities_found: Mapped[int] = mapped_column(Integer, default=0)
    analyses_completed: Mapped[int] = mapped_column(Integer, default=0)
    reports_generated: Mapped[int] = mapped_column(Integer, default=0)

    posts: Mapped[list[Post]] = relationship("Post", back_populates="run")


# ---------------------------------------------------------------------------
# Post
# ---------------------------------------------------------------------------

class Post(Base):
    __tablename__ = "posts"
    __table_args__ = (UniqueConstraint("reddit_id", name="uq_posts_reddit_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(ForeignKey("runs.id"), nullable=True)

    # Reddit identifiers
    reddit_id: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    subreddit: Mapped[str] = mapped_column(String(128))
    subreddit_searched: Mapped[str] = mapped_column(String(128))
    keywords: Mapped[str] = mapped_column(Text)          # comma-separated

    # Post content
    title: Mapped[str] = mapped_column(Text)
    body: Mapped[str] = mapped_column(Text, nullable=True)
    author: Mapped[str] = mapped_column(String(128))
    url: Mapped[str] = mapped_column(Text)
    permalink: Mapped[str] = mapped_column(Text)
    images: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_video: Mapped[bool | None] = mapped_column(nullable=True)
    over_18: Mapped[bool | None] = mapped_column(nullable=True)

    # Metrics
    score: Mapped[int] = mapped_column(Integer)
    upvote_ratio: Mapped[float] = mapped_column(Float)
    num_comments: Mapped[int] = mapped_column(Integer)
    age_hours: Mapped[float] = mapped_column(Float)
    opportunity_score: Mapped[float] = mapped_column(Float)

    # Timestamps
    reddit_created_utc: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    run: Mapped[PipelineRun | None] = relationship("PipelineRun", back_populates="posts")
    comments: Mapped[list[Comment]] = relationship(
        "Comment", back_populates="post", cascade="all, delete-orphan"
    )
    analysis: Mapped[Analysis | None] = relationship(
        "Analysis",
        primaryjoin="and_(Analysis.opportunity_type=='post', Analysis.target_reddit_id==Post.reddit_id)",
        foreign_keys="Analysis.target_reddit_id",
        uselist=False,
        viewonly=True,
    )


# ---------------------------------------------------------------------------
# Comment
# ---------------------------------------------------------------------------

class Comment(Base):
    __tablename__ = "comments"
    __table_args__ = (UniqueConstraint("reddit_id", name="uq_comments_reddit_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id"), nullable=False)

    reddit_id: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    author: Mapped[str] = mapped_column(String(128))
    body: Mapped[str] = mapped_column(Text)
    rank: Mapped[int] = mapped_column(Integer)
    score: Mapped[int] = mapped_column(Integer)
    num_replies: Mapped[int] = mapped_column(Integer)
    depth: Mapped[int] = mapped_column(Integer)
    opportunity_score: Mapped[float] = mapped_column(Float)
    reddit_created_utc: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    post: Mapped[Post] = relationship("Post", back_populates="comments")


# ---------------------------------------------------------------------------
# Analysis  — AI result for a post or comment opportunity
# ---------------------------------------------------------------------------

class Analysis(Base):
    __tablename__ = "analyses"
    __table_args__ = (
        UniqueConstraint("opportunity_id", name="uq_analyses_opportunity_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    opportunity_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    opportunity_type: Mapped[str] = mapped_column(String(32))     # "post" | "comment"
    target_reddit_id: Mapped[str] = mapped_column(String(16), index=True)

    status: Mapped[str] = mapped_column(String(16))               # Suitable | Unsuitable
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    conversation_theme: Mapped[str | None] = mapped_column(Text, nullable=True)
    relevant_philosophy: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategic_direction: Mapped[str | None] = mapped_column(Text, nullable=True)

    reported: Mapped[bool] = mapped_column(default=False)
    analysed_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
