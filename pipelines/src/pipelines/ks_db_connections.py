"""Keystone DB2 connection routing.

Auth credentials (host, port, user, password) come from the KeystoneDatabaseResource
injected by Dagster. The database name — which DB to connect to — is application
routing logic and lives here, not in Docker env vars.

Usage:
    from pipelines.ks_db_connections import LIVE, MONTH_END_2, MONTH_SNAPSHOT_MAP

    # In an asset — pass context so scheduled runs self-correct
    conn = keystone_database.get_connection(context=context)           # daily
    conn = keystone_database.get_connection(context=context, job_type="monthly")

    # Explicit DB — bypasses both override and schedule logic
    conn = keystone_database.get_connection(db_name=MONTH_END_2)
"""
import os
from datetime import date

import ibm_db

# ── Database name constants ───────────────────────────────────────────────────

LIVE        = "D0001LIV"   # Live production database — used for all nightly loads
MONTH_END_1 = "D0001T31"   # Frozen month-end snapshot: Jan / Apr / Jul / Oct
MONTH_END_2 = "D0001T32"   # Frozen month-end snapshot: Feb / May / Aug / Nov
QUARTER_END = "D0001T33"   # Frozen month-end snapshot: Mar / Jun / Sep / Dec

# Maps a month name (as returned by strftime('%B')) to its snapshot DB.
# Used by the monthly snapshot asset to connect to the prior month's frozen DB.
MONTH_SNAPSHOT_MAP: dict[str, str] = {
    "January":   MONTH_END_1,
    "February":  MONTH_END_2,
    "March":     QUARTER_END,
    "April":     MONTH_END_1,
    "May":       MONTH_END_2,
    "June":      QUARTER_END,
    "July":      MONTH_END_1,
    "August":    MONTH_END_2,
    "September": QUARTER_END,
    "October":   MONTH_END_1,
    "November":  MONTH_END_2,
    "December":  QUARTER_END,
}


# ── Schedule-aware DB resolution ─────────────────────────────────────────────

def resolve_db_for_scheduled_run(context, job_type: str = "daily") -> str | None:
    """Return the correct DB for this run, ignoring KEYSTONE_DB_OVERRIDE when appropriate.

    job_type:
        "daily"   — scheduled runs always connect to LIVE and ignore any override;
                    manual runs return None so the caller respects KEYSTONE_DB_OVERRIDE.
        "monthly" — always derives the correct frozen snapshot DB from context.partition_key
                    regardless of whether the run is scheduled or manual. Override is ignored
                    and a warning is emitted if it doesn't match.
    """
    override = os.environ.get("KEYSTONE_DB_OVERRIDE")

    if job_type == "daily":
        if "dagster/schedule_name" not in context.run.tags:
            return None  # Manual run — respect KEYSTONE_DB_OVERRIDE as normal
        if override:
            context.log.warning(
                f"Scheduled nightly run: KEYSTONE_DB_OVERRIDE={override!r} is set "
                f"but will be ignored — daily job always connects to live ({LIVE}). "
                f"Clear the override in .env to suppress this warning."
            )
        return LIVE

    if job_type == "monthly":
        partition_month = date.fromisoformat(context.partition_key).strftime("%B")
        correct_db = MONTH_SNAPSHOT_MAP[partition_month]
        if override and override != correct_db:
            context.log.warning(
                f"Monthly run (partition {context.partition_key}): "
                f"KEYSTONE_DB_OVERRIDE={override!r} is set but will be ignored — "
                f"{partition_month} always uses {correct_db}. "
                f"Clear the override in .env to suppress this warning."
            )
        return correct_db

    return None


# ── Connection factory ────────────────────────────────────────────────────────

def get_ks_connection(resource, db_name: str | None = None) -> object:
    """Open an ibm_db connection to any Keystone DB using shared auth credentials.

    Args:
        resource: A KeystoneDatabaseResource instance (provides host/port/user/password).
        db_name:  Explicit DB to connect to. When None, falls back to
                  KEYSTONE_DB_OVERRIDE env var, then LIVE.

    Returns:
        An active ibm_db connection. Caller is responsible for closing it.
    """
    if db_name is None:
        db_name = os.environ.get("KEYSTONE_DB_OVERRIDE") or LIVE
    conn_str = (
        f"DATABASE={db_name};"
        f"HOSTNAME={resource.host};"
        f"PORT={resource.port};"
        "PROTOCOL=TCPIP;"
        f"UID={resource.user};"
        f"PWD={resource.password};"
    )
    return ibm_db.connect(conn_str, "", "")
