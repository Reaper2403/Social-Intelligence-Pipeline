# Social Intelligence Pipeline

A modular, containerised **ETL data engineering pipeline** that continuously mines Reddit for high-signal discussions, enriches them with LLM-powered analysis, and delivers structured engagement reports.

Built as a **Data Engineering portfolio project** demonstrating production-grade patterns: modular ETL architecture, relational database integration, schema-migrated persistence, containerised deployment, and a comprehensive automated test suite.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       pipeline.py (Orchestrator)                │
│                                                                 │
│  Stage 1 EXTRACT    Stage 2 TRANSFORM   Stage 3 TRANSFORM       │
│  ┌──────────────┐   ┌───────────────┐   ┌──────────────────┐   │
│  │Reddit        │──▶│Opportunity    │──▶│AI Analyzer       │   │
│  │Extractor     │   │Filter (P95)   │   │(Anthropic Claude)│   │
│  │(PRAW)        │   │               │   │                  │   │
│  └──────────────┘   └───────────────┘   └──────────────────┘   │
│                                                  │               │
│  Stage 4 LOAD                                    ▼               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Report Loader   │   JSON Loader   │   DB Loader        │   │
│  │  (.docx reports) │   (data/ backup)│   (PostgreSQL/SQLite│   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Dual persistence** — every pipeline run writes to both a relational database (source of truth) and JSON files (portable backup).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Reddit API | PRAW (Python Reddit API Wrapper) |
| AI Analysis | Anthropic Claude (tool_use / structured JSON output) |
| ORM | SQLAlchemy 2.x (declarative, type-annotated models) |
| Migrations | Alembic (auto-generated, version-controlled schema) |
| Database | PostgreSQL 16 (prod) · SQLite (local dev) |
| Containerisation | Docker + Docker Compose |
| Testing | pytest (90 tests, zero external API calls) |
| Report Generation | python-docx |

---

## Project Structure

```
.
├── pipeline.py                    # Orchestrator & CLI entry point
├── Dockerfile                     # python:3.11-slim image
├── docker-compose.yml             # postgres:16 + scraper services
├── docker/
│   └── entrypoint.sh              # DB wait → alembic migrate → run
├── alembic/                       # Schema migration history
│   └── versions/
│       └── *_initial_schema.py    # Tables: runs, posts, comments, analyses
├── src/
│   ├── extractors/
│   │   └── reddit_extractor.py    # PRAW client, scoring, deduplication
│   ├── transformers/
│   │   ├── opportunity_filter.py  # P95 score threshold, ID tracking
│   │   └── ai_analyzer.py        # Claude API, batching, tool_use schema
│   ├── loaders/
│   │   ├── json_loader.py         # All file I/O in one place
│   │   ├── db_loader.py           # Idempotent DB upserts
│   │   └── report_loader.py       # Word document generation
│   └── db/
│       ├── models.py              # SQLAlchemy ORM: PipelineRun, Post, Comment, Analysis
│       └── session.py             # Engine factory, get_session(), init_db()
├── tests/
│   ├── test_phase1_etl.py         # 25 ETL component tests
│   ├── test_phase2_db.py          # 20 database layer tests
│   └── test_phase3_docker.py      # 45 Docker config tests
├── data/
│   ├── config.json                # Search configuration (subreddits, keywords)
│   └── system_prompt_final.txt    # LLM analysis persona & instructions
└── requirements.txt
```

---

## Database Schema

```
runs          PipelineRun audit log — one row per execution
├── posts     Scraped Reddit posts (unique by reddit_id, upserted)
│   └── comments  Top comments per post (FK → posts, cascade delete)
└── analyses  Claude AI results (FK via reddit_id, reported flag)
```

All writes are **idempotent upserts** — re-running never creates duplicates. Keyword lists are merged on conflict.

---

## Quick Start

### Option A — Local (SQLite, no Docker)

**Prerequisites:** Python 3.11+, Reddit API credentials, Anthropic API key.

```bash
# 1. Clone and set up environment
git clone <repo-url>
cd social-intelligence-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env
# Edit .env with your API keys

# 3. Apply database schema
alembic upgrade head

# 4. Configure your search target
# Edit data/config.json — set subreddits, keywords, and system_prompt_final.txt persona

# 5. Run the full pipeline
python pipeline.py --yes

# Outputs:
#   data/pipeline.db              ← SQLite database
#   data/master_reddit_data.json  ← full JSON backup
#   reports/Report_Posts.docx     ← engagement report
#   reports/Report_Comments.docx  ← comment opportunities
```

### Option B — Docker (PostgreSQL, production-like)

**Prerequisites:** Docker Desktop.

```bash
# 1. Configure credentials
cp .env.example .env
# Edit .env with your API keys (DATABASE_URL is set automatically by docker-compose)

# 2. Configure your search target
# Edit data/config.json with target subreddits and keywords
# Edit data/system_prompt_final.txt with your analysis persona

# 3. Build and run everything with one command
docker compose up --build

# Container startup sequence:
#   ✔ PostgreSQL 16 starts and passes healthcheck
#   ✔ alembic upgrade head runs (schema created/updated)
#   ✔ pipeline.py runs all 4 stages non-interactively

# Outputs appear on your host machine:
#   data/master_reddit_data.json  ← full scraped data
#   reports/Report_Posts.docx     ← post opportunities
#   reports/Report_Comments.docx  ← comment opportunities

# Subsequent runs (deduplicates automatically):
docker compose up
```

---

## Configuration

### `data/config.json` — Search Parameters

```json
{
  "search_settings": {
    "keywords": ["burnout", "productivity", "time management"],
    "target_subreddits": ["productivity", "ADHD", "cscareerquestions"],
    "posts_per_keyword": 10,
    "sort_method": "top",
    "time_filter": "week"
  },
  "api_settings": {
    "rate_limit_delay": 1.5,
    "max_retries": 3
  },
  "filter_settings": {
    "min_score": 20,
    "exclude_nsfw": false
  }
}
```

> **Note:** When using Docker, rebuild the image after changing `config.json` or `system_prompt_final.txt`, as they are baked into the image layer: `docker compose up --build`

### `data/system_prompt_final.txt` — AI Persona

Defines the LLM's analysis framework. Edit this to change the strategic angle — the Claude API will classify each opportunity against the principles defined here and generate a `strategic_direction` for each suitable post.

---

## Running the Pipeline

### CLI Options

```bash
python pipeline.py [OPTIONS]

Options:
  --start {1,2,3,4}   Resume from a specific stage (default: 1)
                         1 = Extract (Reddit scrape)
                         2 = Transform (opportunity filter)
                         3 = Transform (AI analysis)
                         4 = Load (report generation)
  --yes               Skip the Anthropic API cost confirmation prompt
  --no-db             Disable database writes (JSON output only)
```

### Stage-by-Stage Resumption

```bash
# Re-run only AI analysis and report generation (skips Reddit scrape):
python pipeline.py --start 3 --yes

# Regenerate reports from existing analysis output:
python pipeline.py --start 4

# Docker equivalent:
docker compose run scraper --start 3 --yes
```

---

## Querying the Database

### PostgreSQL (Docker)

```bash
docker exec -it pipeline_db psql -U pipeline_user -d pipeline_db
```

```sql
-- Run history
SELECT id, status, started_at, new_posts, analyses_completed
FROM runs ORDER BY id DESC;

-- Top posts by opportunity score
SELECT reddit_id, subreddit, title, opportunity_score
FROM posts ORDER BY opportunity_score DESC LIMIT 10;

-- Suitable, unreported analyses
SELECT opportunity_id, conversation_theme, strategic_direction
FROM analyses
WHERE status = 'Suitable' AND reported = false;

-- Summary
SELECT
  (SELECT COUNT(*) FROM runs)     AS total_runs,
  (SELECT COUNT(*) FROM posts)    AS total_posts,
  (SELECT COUNT(*) FROM comments) AS total_comments,
  (SELECT COUNT(*) FROM analyses) AS total_analyses;
```

### SQLite (Local)

Connect with [DB Browser for SQLite](https://sqlitebrowser.org/) — open `data/pipeline.db`.

### Switch to External PostgreSQL

Set `DATABASE_URL` in `.env`:
```
DATABASE_URL=postgresql://user:password@host:5432/dbname
```
No code changes required — Alembic and SQLAlchemy handle the rest.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `REDDIT_CLIENT_ID` | ✅ | Reddit API app client ID |
| `REDDIT_CLIENT_SECRET` | ✅ | Reddit API app client secret |
| `REDDIT_USER_AGENT` | ✅ | PRAW user agent string |
| `ANTHROPIC_API_KEY` | ✅ | Claude API key |
| `DATABASE_URL` | ❌ | Database connection string (defaults to SQLite) |
| `PIPELINE_CONFIG_DIR` | ❌ | Path to static config files (set automatically in Docker) |

---

## Testing

```bash
# Full test suite — 90 tests, ~2 seconds, no external API calls
python -m pytest tests/ -v

# Run specific phase tests
python -m pytest tests/test_phase1_etl.py -v   # ETL component logic
python -m pytest tests/test_phase2_db.py -v    # Database layer
python -m pytest tests/test_phase3_docker.py -v # Docker config validation
```

Tests use:
- **In-memory SQLite** for all database tests (no file system side effects)
- **`unittest.mock`** to intercept all Anthropic API calls
- **`tmp_path`** for all file system tests

---

## Getting Reddit API Credentials

1. Go to [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps)
2. Click **"create another app..."**
3. Select **"script"** type
4. Set redirect URI to `http://localhost:8080`
5. Copy the **client ID** (under app name) and **client secret**

---

## Roadmap

| Phase | Status | Description |
|---|---|---|
| Phase 1 — Modular ETL | ✅ Complete | Extractor, Transformer, Loader classes + test suite |
| Phase 2 — Database Integration | ✅ Complete | SQLAlchemy ORM, Alembic migrations, dual persistence |
| Phase 3 — Containerisation | ✅ Complete | Dockerfile, docker-compose, entrypoint, healthchecks |
| Phase 4 — Orchestration | 🔜 Planned | Apache Airflow / Prefect DAG for scheduled runs |
| Phase 5 — Observability | 🔜 Planned | Structured logging, metrics, run dashboard |
| Phase 6 — API + UI | 🔜 Planned | FastAPI backend + Streamlit dashboard |
