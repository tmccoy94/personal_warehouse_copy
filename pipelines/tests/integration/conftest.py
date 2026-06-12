"""Shared fixtures for integration tests.

Integration tests run full asset / loader code against the real warehouse
Postgres container (`docker compose up -d`), connecting from the host on
localhost:POSTGRES_PORT. They operate EXCLUSIVELY on the disposable `test`
schema:

  - The running containers are never switched or restarted — the active
    WAREHOUSE_SCHEMA (usually prod) keeps serving Dagster untouched.
  - The `test` schema is dropped and rebuilt from the real DDL
    (warehouse_schema.sql + warehouse_functions.sql) once per session, so
    DO NOT keep anything you care about in it.
  - The staging schema is never written to (the DDL's staging statements are
    all IF NOT EXISTS no-ops against the existing tables).
  - The .schema_intent runtime gate is satisfied by monkeypatching the env
    var and intent file inside this pytest process only.

If Postgres is unreachable (Docker not running), every integration test skips
with a clear reason — plain `uv run pytest` stays green without Docker.

Run only unit tests:        uv run pytest -m "not integration"
Run only integration tests: uv run pytest -m integration
"""
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import pipelines.loading
from pipelines.defs.assets.warehouse_schema.schema import _split_statements
from pipelines.defs.resources import WarehousePgResource
from pipelines.queries import load

TEST_SCHEMA = "test"

# pipelines/tests/integration/conftest.py -> repo root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _env_from_dotenv() -> dict[str, str]:
    """Parse the project-root .env file (KEY=VALUE lines, # comments)."""
    values: dict[str, str] = {}
    env_file = _PROJECT_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _connection_settings() -> dict[str, str]:
    """Connection settings for the warehouse Postgres, host-side.

    Real environment variables win; the project .env is the fallback. Mirrors
    how copy_schema.py connects (localhost + published POSTGRES_PORT).
    """
    dotenv = _env_from_dotenv()

    def get(key: str, default: str = "") -> str:
        return os.environ.get(key) or dotenv.get(key) or default

    return {
        "host": get("WAREHOUSE_TEST_HOST", "localhost"),
        "port": get("POSTGRES_PORT", "5432"),
        "database": get("POSTGRES_DB", "warehouse"),
        "user": get("POSTGRES_USER", "warehouse_admin"),
        "password": get("DAGSTER_POSTGRES_PASSWORD"),
    }


@pytest.fixture(scope="session")
def pg_settings() -> dict[str, str]:
    settings = _connection_settings()
    if not settings["password"]:
        pytest.skip(
            "DAGSTER_POSTGRES_PASSWORD not found in environment or .env — "
            "cannot connect to the warehouse Postgres"
        )
    return settings


@pytest.fixture(scope="session")
def engine(pg_settings):
    """Session engine pinned to the `test` schema via search_path."""
    # Hard guard: this suite drops its target schema. Never aim it anywhere else.
    assert TEST_SCHEMA not in {"dev", "prod", "staging", "public"}, (
        f"Integration tests must target a disposable schema, got {TEST_SCHEMA!r}"
    )
    eng = create_engine(
        f"postgresql+psycopg://{pg_settings['user']}:{pg_settings['password']}"
        f"@{pg_settings['host']}:{pg_settings['port']}/{pg_settings['database']}",
        connect_args={"options": f"-csearch_path={TEST_SCHEMA}"},
    )
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError:
        pytest.skip(
            "warehouse Postgres not reachable on "
            f"{pg_settings['host']}:{pg_settings['port']} — "
            "run `docker compose up -d` to enable integration tests"
        )
    yield eng
    eng.dispose()


def run_ddl(engine) -> None:
    """Execute the full warehouse DDL through the same code path as the
    warehouse_tables / warehouse_funcs assets."""
    with engine.connect() as conn:
        for stmt in _split_statements(load("warehouse_schema.sql")):
            conn.execute(text(stmt))
        for stmt in _split_statements(load("warehouse_functions.sql")):
            conn.execute(text(stmt))
        conn.commit()


@pytest.fixture(scope="session")
def test_schema(engine) -> str:
    """Fresh `test` schema built from the real DDL, once per session."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {TEST_SCHEMA}"))
        conn.commit()
    run_ddl(engine)
    return TEST_SCHEMA


@pytest.fixture(autouse=True)
def aligned_schema_intent(monkeypatch, tmp_path):
    """Point the .schema_intent runtime gate at `test` for this process only.

    audit_load() refuses to write when the intent file and WAREHOUSE_SCHEMA
    disagree. The real intent file (usually 'prod') is never touched — the
    module constant is monkeypatched to a temp file for each test.
    """
    monkeypatch.setenv("WAREHOUSE_SCHEMA", TEST_SCHEMA)
    intent_file = tmp_path / ".schema_intent"
    intent_file.write_text(TEST_SCHEMA)
    monkeypatch.setattr(pipelines.loading, "_SCHEMA_INTENT_FILE", intent_file)
    return intent_file


@pytest.fixture()
def warehouse_pg(pg_settings, test_schema) -> WarehousePgResource:
    """The real Dagster resource, whose get_engine() picks up the monkeypatched
    WAREHOUSE_SCHEMA=test."""
    return WarehousePgResource(
        host=pg_settings["host"],
        port=pg_settings["port"],
        database=pg_settings["database"],
        user=pg_settings["user"],
        password=pg_settings["password"],
    )


@pytest.fixture(scope="session")
def seeded_branches(engine, test_schema) -> dict[str, int]:
    """Minimal dim_branch rows for tests that resolve branch names to keys."""
    branches = [
        (1, "Main Branch", "MAIN"),
        (2, "North Branch", "NORTH"),
        (3, "East Branch", "EAST"),
        (4, "South Branch", "SOUTH"),
        (5, "OP Branch", "OP"),
    ]
    with engine.begin() as conn:
        for serial, name, abbreviation in branches:
            conn.execute(
                text(
                    "INSERT INTO dim_branch "
                    "  (source_branch_serial, branch_name, abbreviation) "
                    "VALUES (:serial, :name, :abbreviation) "
                    "ON CONFLICT (branch_name) DO NOTHING"
                ),
                {"serial": serial, "name": name, "abbreviation": abbreviation},
            )
        rows = conn.execute(
            text("SELECT branch_name, branch_key FROM dim_branch")
        ).all()
    return {row.branch_name: row.branch_key for row in rows}


def clear_audit_rows(engine, asset_name: str) -> None:
    """Remove load_audit rows for one asset.

    Directly-invoked assets all share Dagster's 'EPHEMERAL' run_id, so a
    second invocation in the same test would violate the (batch_id, asset_name)
    primary key without this.
    """
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM load_audit WHERE asset_name = :asset_name"),
            {"asset_name": asset_name},
        )
