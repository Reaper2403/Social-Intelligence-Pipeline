"""
src/db/session.py
-----------------
Database engine & session factory.

Reads DATABASE_URL from the environment (via .env).

Supported database URLs:
  SQLite  (default, zero-config):  sqlite:///./data/pipeline.db
  PostgreSQL:                       postgresql://user:pass@host:5432/dbname

Usage
-----
    from src.db.session import get_session, init_db

    init_db()          # create all tables if they don't exist (dev convenience)

    with get_session() as session:
        session.add(some_model)
        session.commit()
"""

import logging
import os
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from src.db.models import Base

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

# Default: SQLite file alongside the data/ folder so it's auto-discovered
_DEFAULT_DB_URL = (
    "sqlite:///"
    + str(Path(__file__).parent.parent.parent / "data" / "pipeline.db")
)

DATABASE_URL = os.getenv("DATABASE_URL", _DEFAULT_DB_URL)

# connect_args only used for SQLite (enables WAL mode for concurrent reads)
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    connect_args=_connect_args,
    echo=False,          # set True to log all SQL for debugging
)

# Enable WAL mode for SQLite — better concurrency when multiple processes read
if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def init_db() -> None:
    """
    Create all tables that don't yet exist.
    Safe to call on every startup — it's a no-op if tables already exist.
    For production schema changes, use Alembic migrations instead.
    """
    Base.metadata.create_all(bind=engine)
    logger.info(f"Database initialised → {DATABASE_URL}")


@contextmanager
def get_session() -> Session:
    """
    Provide a transactional scope around a series of operations.

    Usage::

        with get_session() as session:
            session.add(record)
            session.commit()
    """
    session: Session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
