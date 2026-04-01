"""
Loader: JSON / File I/O
-----------------------
Centralises all file-system I/O for the pipeline.

Responsibilities:
  - load_config()          → reads data/config.json
  - load_master_store()    → reads data/master_reddit_data.json
  - save_master_store()    → writes data/master_reddit_data.json
  - save_session_export()  → writes session-scoped JSON (new posts only)
  - load_processed_ids()   → reads data/processed_ids.log
  - append_processed_ids() → appends new IDs to data/processed_ids.log
  - load_reported_ids()    → reads data/reported_ids.log
  - append_reported_ids()  → appends new IDs to data/reported_ids.log
  - load_json()            → generic JSON loader
  - save_json()            → generic JSON saver
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Canonical data directory (two levels up from this file: src/loaders/ → project root → data/)
DATA_DIR = Path(__file__).parent.parent.parent / "data"


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | list | None:
    """
    Load any JSON file. Returns None on failure.
    Catches OSError in addition to the usual suspects — Docker Desktop on macOS
    can raise [Errno 35] EDEADLK when reading files through VirtioFS volume mounts.
    In that case we treat the file as unreadable and the caller gets None.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"File not found: {path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in {path}: {e}")
        return None
    except OSError as e:
        logger.warning(
            f"OS error reading {path} (errno {e.errno}: {e.strerror}). "
            f"This can happen with Docker Desktop volume mounts on macOS — "
            f"treating file as missing and starting fresh."
        )
        return None


def save_json(data: dict | list, path: Path, compact: bool = False) -> bool:
    """Save data as JSON. Returns True on success."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            if compact:
                json.dump(data, f, separators=(",", ":"), ensure_ascii=False)
            else:
                json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON → {path}")
        return True
    except Exception as e:
        logger.error(f"Could not save JSON to {path}: {e}")
        return False


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(config_path: Path | None = None) -> dict | None:
    """Load and validate the pipeline config. Defaults to data/config.json."""
    path = config_path or DATA_DIR / "config.json"
    config = load_json(path)
    if config is None:
        return None

    required = ["search_settings", "export_settings", "api_settings", "filter_settings"]
    missing = [k for k in required if k not in config]
    if missing:
        logger.error(f"config.json is missing required keys: {missing}")
        return None

    logger.info(f"Config loaded from {path}")
    return config


# ---------------------------------------------------------------------------
# Master Reddit data store
# ---------------------------------------------------------------------------

def load_master_store(path: Path | None = None) -> dict:
    """
    Load the master Reddit data file.
    Returns a dict with 'posts' list and 'master_data_info' keys.
    Returns an empty store structure if the file does not exist yet.
    """
    path = path or DATA_DIR / "master_reddit_data.json"
    data = load_json(path)
    if data is None:
        logger.info("No master store found. Starting fresh.")
        return {"master_data_info": {}, "posts": []}
    logger.info(f"Loaded master store: {len(data.get('posts', []))} posts from {path}")
    return data


def save_master_store(posts_data: dict, path: Path | None = None) -> bool:
    """
    Persist the master Reddit data store.

    Args:
        posts_data: dict mapping post_id → post record (as maintained by RedditExtractor)
        path:       optional override for the output file path
    """
    path = path or DATA_DIR / "master_reddit_data.json"
    payload = {
        "master_data_info": {
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_posts": len(posts_data),
            "script_version": "3.0-etl",
        },
        "posts": list(posts_data.values()),
    }
    return save_json(payload, path)


def save_session_export(new_posts: list, config: dict, path: Path | None = None) -> bool:
    """Save only the newly scraped posts from the current session."""
    if not new_posts:
        logger.info("No new posts to export for this session.")
        return True

    export_settings = config.get("export_settings", {})
    filename = export_settings.get("custom_filename", f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    output_path = path or DATA_DIR / filename

    payload = {
        "session_info": {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_new_posts": len(new_posts),
            "script_version": "3.0-etl",
        },
        "posts": new_posts,
    }
    return save_json(payload, output_path)


# ---------------------------------------------------------------------------
# ID tracking logs
# ---------------------------------------------------------------------------

def _load_id_log(path: Path) -> set:
    try:
        with open(path, "r", encoding="utf-8") as f:
            ids = {line.strip() for line in f if line.strip()}
        logger.info(f"Loaded {len(ids)} IDs from {path}")
        return ids
    except FileNotFoundError:
        logger.info(f"No ID log at {path}. Starting fresh.")
        return set()


def _append_id_log(path: Path, new_ids: set) -> None:
    if not new_ids:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for item_id in sorted(new_ids):
            f.write(f"{item_id}\n")
    logger.info(f"Appended {len(new_ids)} IDs to {path}")


def load_processed_ids(path: Path | None = None) -> set:
    """Load the set of opportunity IDs already sent for AI analysis."""
    return _load_id_log(path or DATA_DIR / "processed_ids.log")


def append_processed_ids(new_ids: set, path: Path | None = None) -> None:
    """Append newly processed opportunity IDs to the log."""
    _append_id_log(path or DATA_DIR / "processed_ids.log", new_ids)


def load_reported_ids(path: Path | None = None) -> set:
    """Load the set of opportunity IDs already written to reports."""
    return _load_id_log(path or DATA_DIR / "reported_ids.log")


def append_reported_ids(new_ids: set, path: Path | None = None) -> None:
    """Append newly reported opportunity IDs to the log."""
    _append_id_log(path or DATA_DIR / "reported_ids.log", new_ids)
