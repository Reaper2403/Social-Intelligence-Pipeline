"""
pipeline.py — Main Pipeline Controller (Phase 2: Database Integration)
-----------------------------------------------------------------------
Orchestrates the four pipeline stages using dedicated ETL classes:

  Stage 1  Extract   →  RedditExtractor
  Stage 2  Transform →  OpportunityFilter
  Stage 3  Transform →  AIAnalyzer
  Stage 4  Load      →  ReportLoader

All data is dual-persisted:
  • JSON files  (data/)          — same as Phase 1, always on
  • SQLite / PostgreSQL database  — NEW in Phase 2, via db_loader

Switch from SQLite to PostgreSQL at any time by setting DATABASE_URL in .env:
  DATABASE_URL=postgresql://user:pass@host:5432/dbname

Usage
-----
    python pipeline.py              # full run (stages 1-4)
    python pipeline.py --start 2   # skip Reddit fetch
    python pipeline.py --start 3   # skip filter; use existing ai_input_minimal.json
    python pipeline.py --start 4   # skip AI; use existing ai_analysis_output.json
    python pipeline.py --yes        # skip Anthropic cost confirmation
    python pipeline.py --no-db      # disable DB writes (JSON only)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.session import get_session, init_db
from src.db import models  # noqa: F401 — ensure models are registered before init_db
from src.loaders import db_loader
from src.extractors.reddit_extractor import RedditExtractor
from src.loaders.json_loader import (
    append_processed_ids,
    append_reported_ids,
    load_config,
    load_json,
    load_master_store,
    load_processed_ids,
    load_reported_ids,
    save_json,
    save_master_store,
    save_session_export,
)
from src.loaders.report_loader import ReportLoader
from src.transformers.ai_analyzer import AIAnalyzer
from src.transformers.opportunity_filter import OpportunityFilter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR   = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"

# PIPELINE_CONFIG_DIR separates static read-only config files (config.json,
# system_prompt_final.txt) from writable output files (data/).
#
# • Locally:  defaults to data/  (same as before — no change needed)
# • In Docker: set to /app/config/ via docker-compose environment so that
#   static files are read from the image layer, NOT the mounted ./data volume.
#   This avoids macOS VirtioFS EDEADLK errors caused by reading from a volume
#   that is simultaneously being written to.
CONFIG_DIR = Path(os.environ.get("PIPELINE_CONFIG_DIR", str(DATA_DIR)))

SYSTEM_PROMPT_PATH = CONFIG_DIR / "system_prompt_final.txt"
AI_INPUT_PATH      = DATA_DIR   / "ai_input_minimal.json"
AI_OUTPUT_PATH     = DATA_DIR   / "ai_analysis_output.json"


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def stage1_extract(config: dict, run, session, use_db: bool) -> dict:
    """Fetch from Reddit → update master store (JSON + DB)."""
    logger.info("═" * 50)
    logger.info("STAGE 1 — Extract: Reddit Scraper")
    logger.info("═" * 50)

    master_data = load_master_store()
    posts_dict  = {p["post_details"]["id"]: p for p in master_data.get("posts", [])}

    extractor = RedditExtractor(config)
    new_posts, updated_ids = extractor.run(posts_dict)

    if new_posts:
        for p in new_posts:
            posts_dict[p["post_details"]["id"]] = p

        # JSON persistence
        save_master_store(posts_dict)
        save_session_export(new_posts, config)

        # DB persistence
        if use_db:
            inserted = db_loader.upsert_posts(session, new_posts, run_id=run.id if run else None)
            session.commit()
            if run:
                run.new_posts    = inserted
                run.updated_posts = len(updated_ids)
                session.commit()

        logger.info(f"Stage 1 done — {len(new_posts)} new posts, {len(updated_ids)} updated.")
    else:
        logger.info("Stage 1 done — no new posts found.")

    return posts_dict


def stage2_filter(posts_dict: dict | None, use_db: bool) -> list:
    """Filter master store for high-value opportunities."""
    logger.info("═" * 50)
    logger.info("STAGE 2 — Transform: Opportunity Filter")
    logger.info("═" * 50)

    if posts_dict is None:
        master_data = load_master_store()
    else:
        master_data = {"posts": list(posts_dict.values())}

    processed_ids = load_processed_ids()
    opp_filter    = OpportunityFilter(percentile=95)
    ai_input, new_ids = opp_filter.run(master_data, processed_ids)

    if ai_input:
        save_json(ai_input, AI_INPUT_PATH, compact=True)
        append_processed_ids(new_ids)
        logger.info(f"Stage 2 done — {len(ai_input)} opportunities written to {AI_INPUT_PATH.name}")
    else:
        logger.info("Stage 2 done — no new high-value opportunities.")

    return ai_input


def stage3_analyze(ai_input: list | None, run, session, use_db: bool, skip_confirm: bool = False) -> list:
    """Send opportunities to Claude AI → persist results (JSON + DB)."""
    logger.info("═" * 50)
    logger.info("STAGE 3 — Transform: AI Analysis")
    logger.info("═" * 50)

    if ai_input is None:
        ai_input = load_json(AI_INPUT_PATH) or []

    if not ai_input:
        logger.info("No opportunities to analyse. Skipping stage 3.")
        return []

    if not skip_confirm:
        print("\n" + "─" * 50)
        print("  !! PENDING: ANTHROPIC API CALL (costs money) !!")
        print("─" * 50)
        print(f"  Opportunities to analyse: {len(ai_input)}")
        confirm = input("  Type 'OK PASSED' to proceed: ").strip()
        if confirm != "OK PASSED":
            logger.info("Confirmation not received. Halting at stage 3.")
            sys.exit(0)

    analyzer = AIAnalyzer(system_prompt_path=SYSTEM_PROMPT_PATH)
    analyses  = analyzer.run(ai_input)

    if analyses:
        # JSON persistence
        save_json(analyses, AI_OUTPUT_PATH)

        # DB persistence
        if use_db:
            inserted = db_loader.upsert_analyses(session, analyses)
            session.commit()
            if run:
                run.opportunities_found = len(ai_input)
                run.analyses_completed  = inserted
                session.commit()

        logger.info(f"Stage 3 done — {len(analyses)} analyses saved.")
    else:
        logger.warning("Stage 3 — no analyses returned.")

    return analyses


def stage4_report(analyses: list | None, posts_dict: dict | None, run, session, use_db: bool) -> None:
    """Generate Word report documents → update reported flags in DB."""
    logger.info("═" * 50)
    logger.info("STAGE 4 — Load: Report Generator")
    logger.info("═" * 50)

    if analyses is None:
        analyses = load_json(AI_OUTPUT_PATH) or []
    if not analyses:
        logger.info("No analyses available. Skipping stage 4.")
        return

    if posts_dict is None:
        master_data = load_master_store()
    else:
        master_data = {"posts": list(posts_dict.values())}

    reported_ids   = load_reported_ids()
    loader         = ReportLoader(master_data, analyses, reported_ids)
    newly_reported = loader.generate()

    # JSON persistence
    append_reported_ids(newly_reported)

    # DB persistence
    if use_db and newly_reported:
        db_loader.mark_reported(session, newly_reported)
        session.commit()
        if run:
            run.reports_generated = len(newly_reported)
            session.commit()

    logger.info(f"Stage 4 done — {len(newly_reported)} new opportunities reported.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Social Engineering Research Pipeline — Phase 2 (Database Integration)"
    )
    parser.add_argument(
        "--start", type=int, choices=[1, 2, 3, 4], default=1,
        help="Stage to start from (1=fetch, 2=filter, 3=AI, 4=report). Default: 1"
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip Anthropic API cost confirmation prompt."
    )
    parser.add_argument(
        "--no-db", dest="no_db", action="store_true",
        help="Disable database writes (JSON output only)."
    )
    return parser.parse_args()


def main() -> None:
    args   = parse_args()
    start  = args.start
    use_db = not args.no_db

    logger.info("╔" + "═" * 48 + "╗")
    logger.info("║   SOCIAL ENGINEERING RESEARCH PIPELINE v4.0   ║")
    logger.info("╚" + "═" * 48 + "╝")

    config = load_config(CONFIG_DIR / "config.json")
    if config is None:
        logger.error("Cannot load config.json. Aborting.")
        sys.exit(1)

    # Initialise DB (creates tables if they don't exist)
    if use_db:
        init_db()

    posts_dict: dict | None = None
    ai_input:   list | None = None
    analyses:   list | None = None

    with get_session() as session:
        run = db_loader.start_run(session) if use_db else None
        if use_db:
            session.commit()

        try:
            if start <= 1:
                posts_dict = stage1_extract(config, run, session, use_db)

            if start <= 2:
                ai_input = stage2_filter(posts_dict, use_db)

            if start <= 3:
                analyses = stage3_analyze(ai_input, run, session, use_db, skip_confirm=args.yes)

            stage4_report(analyses, posts_dict, run, session, use_db)

            if use_db and run:
                db_loader.finish_run(session, run, status="complete")
                session.commit()

        except Exception as exc:
            logger.exception(f"Pipeline failed: {exc}")
            if use_db and run:
                db_loader.finish_run(session, run, status="failed")
                session.commit()
            sys.exit(1)

    logger.info("╔" + "═" * 48 + "╗")
    logger.info("║            PIPELINE RUN COMPLETE               ║")
    logger.info("╚" + "═" * 48 + "╝")


if __name__ == "__main__":
    main()
