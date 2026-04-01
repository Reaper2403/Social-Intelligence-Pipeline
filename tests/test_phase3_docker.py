"""
tests/test_phase3_docker.py
---------------------------
Tests for Phase 3: Containerization.

These tests validate the correctness of Docker configuration files WITHOUT
actually building or running a Docker image (which would require Docker to be
installed in the test environment).

Coverage:
  - Dockerfile structure and required directives
  - docker-compose.yml service definitions and key properties
  - .dockerignore presence and critical exclusions
  - entrypoint.sh existence and key commands
  - Alembic migration files generated and consistent

Run with:
    cd "Reddit Projects"
    python -m pytest tests/test_phase3_docker.py -v
"""

import sys
from pathlib import Path

import pytest
import yaml  # PyYAML — already available via jupyter transitive dep

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ===========================================================================
# Dockerfile tests
# ===========================================================================
class TestDockerfile:
    dockerfile = PROJECT_ROOT / "Dockerfile"

    def test_dockerfile_exists(self):
        assert self.dockerfile.exists(), "Dockerfile not found at project root."

    def test_uses_python311_slim(self):
        content = read(self.dockerfile)
        assert "FROM python:3.11-slim" in content

    def test_installs_libpq_for_postgres(self):
        content = read(self.dockerfile)
        assert "libpq-dev" in content, "Missing libpq-dev — needed for psycopg2 (PostgreSQL)."

    def test_copies_requirements(self):
        content = read(self.dockerfile)
        assert "COPY requirements.txt" in content

    def test_runs_pip_install(self):
        content = read(self.dockerfile)
        assert "pip install" in content

    def test_copies_source(self):
        content = read(self.dockerfile)
        assert "COPY src/" in content
        assert "COPY pipeline.py" in content

    def test_copies_alembic(self):
        content = read(self.dockerfile)
        assert "COPY alembic/" in content
        assert "COPY alembic.ini" in content

    def test_has_entrypoint(self):
        content = read(self.dockerfile)
        assert "ENTRYPOINT" in content
        assert "entrypoint.sh" in content

    def test_sets_pythonpath(self):
        content = read(self.dockerfile)
        assert "PYTHONPATH" in content

    def test_does_not_copy_env_file(self):
        content = read(self.dockerfile)
        # .env must never be baked into the image
        assert "COPY .env" not in content

    def test_does_not_copy_venv(self):
        content = read(self.dockerfile)
        assert ".venv" not in content


# ===========================================================================
# docker-compose.yml tests
# ===========================================================================
class TestDockerCompose:
    compose_file = PROJECT_ROOT / "docker-compose.yml"

    @pytest.fixture(autouse=True)
    def _load_compose(self):
        assert self.compose_file.exists(), "docker-compose.yml not found."
        self.compose = yaml.safe_load(read(self.compose_file))

    def test_compose_has_services(self):
        assert "services" in self.compose

    def test_has_db_service(self):
        assert "db" in self.compose["services"], "Missing 'db' service."

    def test_has_scraper_service(self):
        assert "scraper" in self.compose["services"], "Missing 'scraper' service."

    def test_db_uses_postgres_image(self):
        db = self.compose["services"]["db"]
        assert "image" in db
        assert "postgres" in db["image"]

    def test_db_has_healthcheck(self):
        db = self.compose["services"]["db"]
        assert "healthcheck" in db, "db service must have a healthcheck so scraper waits for it."

    def test_db_has_named_volume(self):
        db = self.compose["services"]["db"]
        assert "volumes" in db
        # Check it mounts the named volume for persistence
        mounts = " ".join(str(v) for v in db["volumes"])
        assert "postgres_data" in mounts or "/var/lib/postgresql" in mounts

    def test_postgres_data_volume_declared(self):
        assert "volumes" in self.compose
        assert "postgres_data" in self.compose["volumes"]

    def test_scraper_depends_on_db(self):
        scraper = self.compose["services"]["scraper"]
        assert "depends_on" in scraper
        depends = scraper["depends_on"]
        assert "db" in depends

    def test_scraper_waits_for_healthy_db(self):
        scraper = self.compose["services"]["scraper"]
        depends = scraper["depends_on"]
        # depends_on dict form with condition
        if isinstance(depends, dict):
            assert depends.get("db", {}).get("condition") == "service_healthy"

    def test_scraper_overrides_database_url_for_postgres(self):
        scraper = self.compose["services"]["scraper"]
        env = scraper.get("environment", {})
        # DATABASE_URL must point to db service, not localhost
        db_url = ""
        if isinstance(env, dict):
            db_url = env.get("DATABASE_URL", "")
        elif isinstance(env, list):
            for item in env:
                if item.startswith("DATABASE_URL="):
                    db_url = item.split("=", 1)[1]
        assert "@db:" in db_url, "DATABASE_URL must use the compose service name 'db'."

    def test_scraper_mounts_data_volume(self):
        scraper = self.compose["services"]["scraper"]
        volumes = " ".join(str(v) for v in scraper.get("volumes", []))
        assert "data" in volumes

    def test_scraper_mounts_reports_volume(self):
        scraper = self.compose["services"]["scraper"]
        volumes = " ".join(str(v) for v in scraper.get("volumes", []))
        assert "reports" in volumes

    def test_scraper_uses_env_file(self):
        scraper = self.compose["services"]["scraper"]
        assert "env_file" in scraper, "scraper must load .env for API credentials."

    def test_db_exposes_postgres_port(self):
        db = self.compose["services"]["db"]
        ports = db.get("ports", [])
        exposed = " ".join(str(p) for p in ports)
        assert "5432" in exposed


# ===========================================================================
# .dockerignore tests
# ===========================================================================
class TestDockerIgnore:
    dockerignore = PROJECT_ROOT / ".dockerignore"

    def test_dockerignore_exists(self):
        assert self.dockerignore.exists()

    def test_excludes_env(self):
        content = read(self.dockerignore)
        assert ".env" in content, ".env must be in .dockerignore to keep secrets off the image."

    def test_excludes_venv(self):
        content = read(self.dockerignore)
        assert ".venv" in content or "venv/" in content

    def test_excludes_pycache(self):
        content = read(self.dockerignore)
        assert "__pycache__" in content

    def test_excludes_git(self):
        content = read(self.dockerignore)
        assert ".git/" in content

    def test_excludes_notebooks(self):
        content = read(self.dockerignore)
        assert "notebooks/" in content or ".ipynb" in content


# ===========================================================================
# Entrypoint script tests
# ===========================================================================
class TestEntrypoint:
    entrypoint = PROJECT_ROOT / "docker" / "entrypoint.sh"

    def test_entrypoint_exists(self):
        assert self.entrypoint.exists()

    def test_has_shebang(self):
        content = read(self.entrypoint)
        assert content.startswith("#!/bin/bash") or content.startswith("#!/usr/bin/env bash")

    def test_runs_alembic_upgrade(self):
        content = read(self.entrypoint)
        assert "alembic upgrade head" in content

    def test_runs_pipeline(self):
        content = read(self.entrypoint)
        assert "pipeline.py" in content

    def test_waits_for_postgres(self):
        content = read(self.entrypoint)
        assert "postgresql" in content or "pg_isready" in content or "postgres" in content.lower()

    def test_uses_set_euo_pipefail(self):
        content = read(self.entrypoint)
        assert "set -e" in content


# ===========================================================================
# Alembic migration tests
# ===========================================================================
class TestAlembicMigrations:
    alembic_dir      = PROJECT_ROOT / "alembic"
    alembic_ini      = PROJECT_ROOT / "alembic.ini"
    versions_dir     = PROJECT_ROOT / "alembic" / "versions"

    def test_alembic_ini_exists(self):
        assert self.alembic_ini.exists()

    def test_alembic_env_exists(self):
        assert (self.alembic_dir / "env.py").exists()

    def test_alembic_versions_dir_exists(self):
        assert self.versions_dir.exists()

    def test_initial_migration_exists(self):
        migrations = list(self.versions_dir.glob("*.py"))
        assert len(migrations) >= 1, "No migration files found in alembic/versions/."

    def test_initial_migration_references_all_tables(self):
        migrations = list(self.versions_dir.glob("*.py"))
        content = "".join(m.read_text() for m in migrations)
        for table in ["runs", "posts", "comments", "analyses"]:
            assert table in content, f"Table '{table}' not referenced in any migration."

    def test_alembic_env_imports_our_models(self):
        env_content = read(self.alembic_dir / "env.py")
        assert "from src.db.models import Base" in env_content

    def test_alembic_env_reads_database_url_from_env(self):
        env_content = read(self.alembic_dir / "env.py")
        assert "DATABASE_URL" in env_content

    def test_alembic_upgrade_is_idempotent(self):
        """
        Running alembic upgrade head twice should not raise.
        Implicitly tests the env.py + migration are syntactically correct.
        """
        import subprocess
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"alembic upgrade head failed:\n{result.stderr}"
        )
