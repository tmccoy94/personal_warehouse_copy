"""Shared helpers used by every warehouse load asset.

Covers three repeated patterns:
  - Fetching rows from DB2 via ibm_db
  - Resolving source serials to warehouse surrogate keys
  - Wrapping every load in a load_audit row via a context manager
"""
from __future__ import annotations

import ibm_db
import os
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from sqlalchemy import Engine, text

from pipelines.utils import last_day_of_month

# Intent file written by switch_schema.py on the host — readable from the container
# via the pipelines/src bind mount.
_SCHEMA_INTENT_FILE = Path(__file__).parent / ".schema_intent"


def _check_schema_intent() -> None:
    """Verify the running WAREHOUSE_SCHEMA matches what switch_schema.py last set.

    Raises RuntimeError if the intent file is missing or if the running env var
    does not match the intended schema. Called at the start of every audit_load()
    so no loader can write data when the schema is misaligned.
    """
    running = os.environ.get("WAREHOUSE_SCHEMA", "").strip()

    if not _SCHEMA_INTENT_FILE.exists():
        raise RuntimeError(
            "No schema intent file found — switch_schema.py has never been run "
            "or the file was deleted.\n"
            "Run switch_schema.py from the project root and select a schema before "
            "loading data."
        )

    intended = _SCHEMA_INTENT_FILE.read_text().strip()

    if intended != running:
        raise RuntimeError(
            f"Schema mismatch — cannot load data.\n\n"
            f"  switch_schema.py last set:  {intended}\n"
            f"  WAREHOUSE_SCHEMA running:   {running}\n\n"
            f"These must match. Run switch_schema.py and select '{running}' to "
            f"realign, or restart containers after updating .env to "
            f"WAREHOUSE_SCHEMA={intended}."
        )


# ── DB2 fetch ─────────────────────────────────────────────────────────────────

def db2_fetch_all(connection, sql: str, params: dict | None = None) -> list[dict]:
    """Execute a DB2 query and return all rows as a list of lowercase-key dicts.

    Pass params as a dict with keys matching the :name placeholders in the SQL.
    Substitution rules:
      date   → 'YYYY-MM-DD'  (quoted)
      str    → 'value'        (quoted, single-quotes escaped)
      None   → NULL
      int/float → bare value  (no quotes)

    With no params, exec_immediate is used directly (faster for static queries).
    """
    if params:
        for key, value in params.items():
            if isinstance(value, date):
                formatted = f"'{value.strftime('%Y-%m-%d')}'"
            elif isinstance(value, str):
                formatted = f"'{value.replace(chr(39), chr(39) + chr(39))}'"
            elif value is None:
                formatted = "NULL"
            else:
                formatted = str(value)
            sql = sql.replace(f":{key}", formatted)

    stmt = ibm_db.exec_immediate(connection, sql)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append({key.lower(): value for key, value in row.items()})
        row = ibm_db.fetch_assoc(stmt)
    return rows


def db2_fetch_chunked(
    connection,
    sql: str,
    params: dict | None = None,
    chunk_size: int = 5_000,
) -> Iterator[list[dict]]:
    """Execute a DB2 query and yield rows in chunks of chunk_size.

    Identical param substitution to db2_fetch_all but never loads the full
    result set into memory — each chunk is yielded and can be inserted into
    PostgreSQL before the next chunk is fetched. Use for large CORE tables
    (SHARE, ACCOUNT, TRANSACTION, MONETARY) to keep memory usage flat.
    """
    if params:
        for key, value in params.items():
            if isinstance(value, date):
                formatted = f"'{value.strftime('%Y-%m-%d')}'"
            elif isinstance(value, str):
                formatted = f"'{value.replace(chr(39), chr(39) + chr(39))}'"
            elif value is None:
                formatted = "NULL"
            else:
                formatted = str(value)
            sql = sql.replace(f":{key}", formatted)

    stmt = ibm_db.exec_immediate(connection, sql)
    chunk: list[dict] = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        chunk.append({key.lower(): value for key, value in row.items()})
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
        row = ibm_db.fetch_assoc(stmt)
    if chunk:
        yield chunk


# ── Surrogate key resolution ──────────────────────────────────────────────────

def resolve_branch_keys(engine: Engine, source_branch_serials: Iterable[int]) -> dict[int, int]:
    """Return {source_branch_serial: branch_key} for every serial in the input.

    Serials not found in dim_branch are omitted from the result — the caller
    must decide how to handle unresolved rows (raise, skip, assign sentinel).
    """
    serials = list(source_branch_serials)
    if not serials:
        return {}
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT source_branch_serial, branch_key "
                "FROM dim_branch "
                "WHERE source_branch_serial = ANY(:serials)"
            ),
            {"serials": serials},
        )
        return {int(row.source_branch_serial): int(row.branch_key) for row in result}


def resolve_employee_keys(engine: Engine, source_employee_serials: Iterable[int]) -> dict[int, int]:
    """Return {source_employee_serial: employee_key} for every serial in the input.

    Serials not found in dim_employee are omitted — caller handles the gap.
    """
    serials = list(source_employee_serials)
    if not serials:
        return {}
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT source_employee_serial, employee_key "
                "FROM dim_employee "
                "WHERE source_employee_serial = ANY(:serials)"
            ),
            {"serials": serials},
        )
        return {int(row.source_employee_serial): int(row.employee_key) for row in result}


def resolve_person_keys(engine: Engine, source_person_serials: Iterable[int]) -> dict[int, int]:
    """Return {source_person_serial: person_key} for every serial in the input.

    Looks up via person_source_identifier (source_system='keystone',
    source_entity_type='member') — never via person_identity directly, since
    person_identity no longer holds source columns.

    Serials not found are omitted — caller should call upsert_person_identity
    first when serials may not yet have warehouse keys.
    """
    serials = list(source_person_serials)
    if not serials:
        return {}
    identifiers = [str(s) for s in serials]
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT source_identifier, person_key "
                "FROM person_source_identifier "
                "WHERE source_system = 'keystone' "
                "  AND source_entity_type = 'member' "
                "  AND source_identifier = ANY(:identifiers)"
            ),
            {"identifiers": identifiers},
        )
        return {int(row.source_identifier): int(row.person_key) for row in result}


def upsert_person_identity(
    engine: Engine,
    source_person_serials: Iterable[int],
    source_system: str = "keystone",
    source_entity_type: str = "member",
) -> dict[int, int]:
    """Mint person_identity rows for any new serials and record the crosswalk.

    For each serial:
      1. If already in person_source_identifier: return the existing person_key.
      2. If new: INSERT into person_identity (getting a fresh person_key),
         then INSERT into person_source_identifier.

    Returns {source_person_serial: person_key} for all input serials.
    """
    serials = list(source_person_serials)
    if not serials:
        return {}

    identifiers = [str(s) for s in serials]

    with engine.begin() as conn:
        # Step 1: find already-mapped serials
        existing = conn.execute(
            text(
                "SELECT source_identifier, person_key "
                "FROM person_source_identifier "
                "WHERE source_system = :source_system "
                "  AND source_entity_type = :source_entity_type "
                "  AND source_identifier = ANY(:identifiers)"
            ),
            {
                "source_system": source_system,
                "source_entity_type": source_entity_type,
                "identifiers": identifiers,
            },
        )
        serial_to_key: dict[int, int] = {
            int(row.source_identifier): int(row.person_key) for row in existing
        }

        # Step 2: mint keys for new serials
        new_serials = [s for s in serials if s not in serial_to_key]
        for serial in new_serials:
            row = conn.execute(
                text(
                    "INSERT INTO person_identity (created_at, updated_at) "
                    "VALUES (NOW(), NOW()) RETURNING person_key"
                )
            ).one()
            person_key = int(row.person_key)
            serial_to_key[serial] = person_key

            conn.execute(
                text(
                    "INSERT INTO person_source_identifier "
                    "  (person_key, source_system, source_entity_type, source_identifier) "
                    "VALUES (:person_key, :source_system, :source_entity_type, :source_identifier) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "person_key": person_key,
                    "source_system": source_system,
                    "source_entity_type": source_entity_type,
                    "source_identifier": str(serial),
                },
            )

    return serial_to_key


# ── Staging table read ───────────────────────────────────────────────────────

def fetch_person_snapshot_stage(engine: Engine) -> list[dict]:
    """Return all rows from person_snapshot_stage as a list of lowercase-key dicts.

    Used by every asset that previously called _fetch_snapshot_rows() against DB2.
    Guaranteed populated by ks_person_snapshot_stage_table before any downstream
    partitioned assets run.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM staging.person_snapshot_stage"))
        return [dict(row._mapping) for row in result]


# ── Partition management ──────────────────────────────────────────────────────

def ensure_snapshot_partition(engine: Engine, table_name: str, snapshot_date: date) -> None:
    """Create the monthly partition for snapshot_date in a partitioned table if it doesn't exist.

    table_name is the bare name without schema (e.g. 'person_snapshot_daily').
    The partition covers the full calendar month containing snapshot_date.
    Must be called before any INSERT into the parent table for that date.
    """
    year = snapshot_date.year
    month = snapshot_date.month
    start = snapshot_date.replace(day=1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    partition_name = f"{table_name}_{year}_{month:02d}"
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {partition_name} "
            f"PARTITION OF {table_name} "
            f"FOR VALUES FROM ('{start}') TO ('{end}')"
        ))


# ── Staging freshness guard ───────────────────────────────────────────────────

def assert_staging_fresh(engine: Engine, table: str, partition_date: date) -> None:
    """Raise RuntimeError if a staging table was not loaded on or after partition_date.

    Called at the top of every daily snapshot asset before any writes.
    Prevents silently writing a partition with stale upstream data — e.g. when a
    staging loader was missing from the job and Dagster used yesterday's table contents.

    Uses loaded_at cast to America/Chicago date so nightly jobs running at 11pm
    compare correctly against the partition date.
    """
    with engine.connect() as conn:
        latest_date = conn.execute(
            text(
                f"SELECT MAX(loaded_at AT TIME ZONE 'America/Chicago')::date FROM {table}"
            )
        ).scalar()

    if latest_date is None:
        raise RuntimeError(
            f"Staging table '{table}' has never been loaded. "
            f"Run the upstream loader asset first."
        )

    if latest_date < partition_date:
        raise RuntimeError(
            f"Staging table '{table}' is stale — last loaded {latest_date}, "
            f"partition requires {partition_date}. "
            f"Ensure the upstream loader ran successfully before retrying."
        )


def assert_snapshot_db_matches_partition(conn, partition_date: date) -> None:
    """Verify the connected DB2 snapshot's CORE.ENV.POSTING_DATE matches the partition's EOM.

    For monthly assets, KeystoneDatabaseResource routes to a frozen snapshot DB based
    on the partition month (MONTH_END_1 / MONTH_END_2 / QUARTER_END). The snapshot DBs
    are refreshed monthly — until a given month's EOM has passed, the corresponding
    snapshot DB still holds the previous quarter's data.

    Raises if the connected DB's POSTING_DATE does not equal the partition's EOM.
    This catches two failure modes:

      1. Materialising a future partition (e.g. running June 2026 mid-June, when
         QUARTER_END still holds March 2026 data — the asset would otherwise write
         March data tagged with a June snapshot_date).
      2. Materialising an old partition after the snapshot DB has rolled forward
         (e.g. trying to load Feb 2026 from MONTH_END_2 after Aug 2026 refresh).
    """
    expected = last_day_of_month(partition_date)
    rows = db2_fetch_all(conn, "SELECT POSTING_DATE FROM CORE.ENV")
    if not rows:
        raise RuntimeError(
            "CORE.ENV returned zero rows — cannot verify snapshot DB POSTING_DATE."
        )
    actual = rows[0].get("posting_date")
    if isinstance(actual, str):
        actual = date.fromisoformat(actual)
    if actual != expected:
        raise RuntimeError(
            f"Snapshot DB POSTING_DATE = {actual}, but partition {partition_date} "
            f"expects month-end snapshot for {expected}. The frozen snapshot DB "
            f"does not currently hold data for this partition. Wait until after "
            f"{expected} before materialising, or pick a partition the snapshot DB "
            f"actually covers."
        )


# ── Audit context manager ─────────────────────────────────────────────────────

@contextmanager
def audit_load(
    engine: Engine,
    *,
    batch_id: str,
    asset_name: str,
    source_system: str | None,
):
    """Context manager that writes a load_audit row for every asset run.

    Usage:
        with audit_load(engine, batch_id=context.run_id,
                        asset_name="load_dim_employee",
                        source_system="keystone") as stats:
            # ... do the load ...
            stats["row_count"] = upserted_count

    On clean exit: status='success', row_count from stats dict.
    On exception: status='failed', notes=str(exception); exception re-raised.
    """
    _check_schema_intent()

    stats: dict[str, int | None] = {"row_count": None}
    started_at = datetime.now(tz=timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO load_audit "
                "  (batch_id, source_system, asset_name, started_at, status) "
                "VALUES (:batch_id, :source_system, :asset_name, :started_at, 'running')"
            ),
            {
                "batch_id": batch_id,
                "source_system": source_system,
                "asset_name": asset_name,
                "started_at": started_at,
            },
        )

    try:
        yield stats
        completed_at = datetime.now(tz=timezone.utc)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE load_audit "
                    "SET status = 'success', completed_at = :completed_at, row_count = :row_count "
                    "WHERE batch_id = :batch_id AND asset_name = :asset_name"
                ),
                {
                    "completed_at": completed_at,
                    "row_count": stats["row_count"],
                    "batch_id": batch_id,
                    "asset_name": asset_name,
                },
            )
    except Exception as exc:
        completed_at = datetime.now(tz=timezone.utc)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE load_audit "
                    "SET status = 'failed', completed_at = :completed_at, notes = :notes "
                    "WHERE batch_id = :batch_id AND asset_name = :asset_name"
                ),
                {
                    "completed_at": completed_at,
                    "notes": str(exc),
                    "batch_id": batch_id,
                    "asset_name": asset_name,
                },
            )
        raise
