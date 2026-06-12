import ibm_db
from datetime import date

import dagster
from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.branch import dim_branch_table
from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.loading import (
    assert_snapshot_db_matches_partition,
    assert_staging_fresh,
    audit_load,
    db2_fetch_all,
    ensure_snapshot_partition,
    fetch_person_snapshot_stage,
    upsert_person_identity,
)
from pipelines.queries import load
from pipelines.utils import compute_record_hash, last_day_of_month

_HASH_COLUMNS = ["first_name", "middle_name", "last_name", "birth_date", "gender"]


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "drop_only"},
    deps=[dim_branch_table],
)
def ks_person_snapshot_stage_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    conn = keystone_database.get_connection(context=context)
    try:
        rows = db2_fetch_all(conn, load("person_snapshot_daily.sql"))
    finally:
        ibm_db.close(conn)

    if not rows:
        raise Exception(
            "person_snapshot_daily.sql returned zero rows — expected at least some active members"
        )

    engine = warehouse_pg.get_engine()

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.person_snapshot_stage"))
            pg_conn.execute(
                text("""
                    INSERT INTO staging.person_snapshot_stage
                        (person_serial, first_name, middle_name, last_name, birth_date, age,
                         gender, status, cred_score, score_date, branch_serial, home_branch_serial,
                         account_eligibility, account_type)
                    VALUES
                        (:person_serial, :first_name, :middle_name, :last_name, :birth_date, :age,
                         :gender, :status, :cred_score, :score_date, :branch_serial, :home_branch_serial,
                         :account_eligibility, :account_type)
                """),
                [
                    {
                        "person_serial":       int(row["person_serial"]),
                        "first_name":          row.get("first_name"),
                        "middle_name":         row.get("middle_name"),
                        "last_name":           row.get("last_name"),
                        "birth_date":          row.get("birth_date"),
                        "age":                 row.get("age"),
                        "gender":              row.get("gender"),
                        "status":              row.get("status"),
                        "cred_score":          row.get("cred_score"),
                        "score_date":          row.get("score_date"),
                        "branch_serial":       int(row["branch_serial"]) if row.get("branch_serial") is not None else None,
                        "home_branch_serial":  int(row["home_branch_serial"]) if row.get("home_branch_serial") is not None else None,
                        "account_eligibility": row.get("account_eligibility"),
                        "account_type":        row.get("account_type"),
                    }
                    for row in rows
                ],
            )
        stats["row_count"] = len(rows)

    return {"status": "ok", "rows": len(rows)}


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "drop_only"},
    deps=[ks_person_snapshot_stage_table, "ks_person_snapshot_monthly_table"],
    partitions_def=DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1, timezone="America/Chicago"),
)
def ks_person_snapshot_daily_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    snapshot_date = date.fromisoformat(context.partition_key)

    if snapshot_date == last_day_of_month(snapshot_date):
        context.log.info(
            f"{snapshot_date} is the last day of the month — "
            "skipping daily load. The monthly pipeline handles this date."
        )
        return {"status": "skipped", "reason": "last day of month", "snapshot_date": str(snapshot_date)}

    engine = warehouse_pg.get_engine()

    # Guard: ensure the staging table was loaded today before writing this partition.
    assert_staging_fresh(engine, "staging.person_snapshot_stage", snapshot_date)

    # Guard: if this snapshot_date is already populated, skip rather than re-fetch.
    # The live DB has moved on — re-running a past partition would overwrite correct
    # historical data with today's live state. Returning success here clears a failed
    # partition status without touching the data.
    with engine.connect() as conn:
        already_loaded = conn.execute(
            text("SELECT 1 FROM person_snapshot_daily WHERE snapshot_date = :d LIMIT 1"),
            {"d": snapshot_date},
        ).scalar()
    if already_loaded:
        context.log.info(
            f"Snapshot data for {snapshot_date} already present — "
            "skipping re-fetch to preserve historical accuracy."
        )
        return {"status": "skipped", "reason": "already loaded", "snapshot_date": str(snapshot_date)}

    rows = fetch_person_snapshot_stage(engine)

    if not rows:
        raise Exception(
            f"person_snapshot_stage is empty for partition {snapshot_date} "
            "— run ks_person_snapshot_stage_table first"
        )

    source_person_serials = [int(row["person_serial"]) for row in rows]
    person_key_map = upsert_person_identity(engine, source_person_serials)

    missing_keys = [s for s in source_person_serials if s not in person_key_map]
    if missing_keys:
        raise Exception(
            f"upsert_person_identity failed to produce person_key for "
            f"{len(missing_keys)} serial(s) — first few: {missing_keys[:5]}"
        )

    ensure_snapshot_partition(engine, "person_snapshot_daily", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text(
                    "DELETE FROM person_snapshot_daily "
                    "WHERE snapshot_date = :snapshot_date"
                ),
                {"snapshot_date": snapshot_date},
            )

            insert_rows = []
            for row in rows:
                source_person_serial = int(row["person_serial"])
                person_key = person_key_map[source_person_serial]
                record_hash = compute_record_hash(
                    {
                        "first_name":  row.get("first_name"),
                        "middle_name": row.get("middle_name"),
                        "last_name":   row.get("last_name"),
                        "birth_date":  row.get("birth_date"),
                        "gender":      row.get("gender"),
                    },
                    _HASH_COLUMNS,
                )
                insert_rows.append(
                    {
                        "person_key":           person_key,
                        "snapshot_date":        snapshot_date,
                        "source_person_serial": source_person_serial,
                        "first_name":           row.get("first_name"),
                        "middle_name":          row.get("middle_name"),
                        "last_name":            row.get("last_name"),
                        "birth_date":           row.get("birth_date"),
                        "gender":               row.get("gender"),
                        "record_hash":          record_hash,
                        "batch_id":             context.run_id,
                    }
                )

            pg_conn.execute(
                text("""
                    INSERT INTO person_snapshot_daily
                        (person_key, snapshot_date, source_system, source_person_serial,
                         first_name, middle_name, last_name, birth_date, gender,
                         record_hash, batch_id)
                    VALUES
                        (:person_key, :snapshot_date, 'keystone', :source_person_serial,
                         :first_name, :middle_name, :last_name, :birth_date, :gender,
                         :record_hash, :batch_id)
                """),
                insert_rows,
            )

        stats["row_count"] = len(insert_rows)

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": len(insert_rows)}


@asset(
    group_name="ks_monthly_loaders",
    tags={"recovery": "drop_only"},
    deps=[dim_branch_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_person_monthly_snapshot_stage_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    # Partition_key is the first day of the month being loaded (e.g. '2026-05-01').
    # The monthly KeystoneDatabaseResource routes to the correct frozen snapshot DB
    # (MONTH_END_1 / MONTH_END_2 / QUARTER_END) via MONTH_SNAPSHOT_MAP[partition_month_name].
    partition_date = date.fromisoformat(context.partition_key)
    month_name = partition_date.strftime('%B')

    conn = keystone_database.get_connection(context=context, job_type="monthly")
    context.log.info(f"Loading {month_name} {partition_date.year} data via monthly snapshot DB")

    try:
        # Fail loudly if the routed snapshot DB doesn't actually hold this month yet
        # (e.g. trying to materialise June 2026 in mid-June, before QUARTER_END refreshes).
        assert_snapshot_db_matches_partition(conn, partition_date)
        rows = db2_fetch_all(conn, load("person_snapshot_daily.sql"))
    finally:
        ibm_db.close(conn)

    if not rows:
        raise Exception(
            f"Snapshot DB returned zero rows for {month_name} {partition_date.year} — "
            "expected at least some active members"
        )

    engine = warehouse_pg.get_engine()

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.person_snapshot_stage"))
            pg_conn.execute(
                text("""
                    INSERT INTO staging.person_snapshot_stage
                        (person_serial, first_name, middle_name, last_name, birth_date, age,
                         gender, status, cred_score, score_date, branch_serial, home_branch_serial,
                         product_count, products, account_eligibility, account_type)
                    VALUES
                        (:person_serial, :first_name, :middle_name, :last_name, :birth_date, :age,
                         :gender, :status, :cred_score, :score_date, :branch_serial, :home_branch_serial,
                         :product_count, :products, :account_eligibility, :account_type)
                """),
                [
                    {
                        "person_serial":       int(row["person_serial"]),
                        "first_name":          row.get("first_name"),
                        "middle_name":         row.get("middle_name"),
                        "last_name":           row.get("last_name"),
                        "birth_date":          row.get("birth_date"),
                        "age":                 row.get("age"),
                        "gender":              row.get("gender"),
                        "status":              row.get("status"),
                        "cred_score":          row.get("cred_score"),
                        "score_date":          row.get("score_date"),
                        "branch_serial":       int(row["branch_serial"]) if row.get("branch_serial") is not None else None,
                        "home_branch_serial":  int(row["home_branch_serial"]) if row.get("home_branch_serial") is not None else None,
                        "product_count":       row.get("product_count"),
                        "products":            row.get("products"),
                        "account_eligibility": row.get("account_eligibility"),
                        "account_type":        row.get("account_type"),
                    }
                    for row in rows
                ],
            )
        stats["row_count"] = len(rows)

    return {
        "status": "ok",
        "partition": str(partition_date),
        "month": f"{month_name} {partition_date.year}",
        "rows": len(rows),
    }


@asset(
    group_name="ks_monthly_loaders",
    tags={"recovery": "drop_only"},
    deps=[ks_person_monthly_snapshot_stage_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_person_snapshot_monthly_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    # snapshot_date = last calendar day of the partition month.
    # Re-runs are idempotent — the writes below DELETE + INSERT for this snapshot_date.
    partition_date = date.fromisoformat(context.partition_key)
    snapshot_date = last_day_of_month(partition_date)
    engine = warehouse_pg.get_engine()

    rows = fetch_person_snapshot_stage(engine)
    if not rows:
        raise Exception(
            f"Staging is empty — run ks_person_monthly_snapshot_stage_table first"
        )

    source_person_serials = [int(row["person_serial"]) for row in rows]
    person_key_map = upsert_person_identity(engine, source_person_serials)

    # Build insert rows once — used for both person_snapshot_daily and person_snapshot_monthly
    insert_rows = []
    for row in rows:
        source_person_serial = int(row["person_serial"])
        person_key = person_key_map[source_person_serial]
        record_hash = compute_record_hash(
            {
                "first_name":  row.get("first_name"),
                "middle_name": row.get("middle_name"),
                "last_name":   row.get("last_name"),
                "birth_date":  row.get("birth_date"),
                "gender":      row.get("gender"),
            },
            _HASH_COLUMNS,
        )
        insert_rows.append({
            "person_key":           person_key,
            "snapshot_date":        snapshot_date,
            "source_person_serial": source_person_serial,
            "first_name":           row.get("first_name"),
            "middle_name":          row.get("middle_name"),
            "last_name":            row.get("last_name"),
            "birth_date":           row.get("birth_date"),
            "gender":               row.get("gender"),
            "record_hash":          record_hash,
            "batch_id":             context.run_id,
        })

    ensure_snapshot_partition(engine, "person_snapshot_daily",   snapshot_date)
    ensure_snapshot_partition(engine, "person_snapshot_monthly", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            for table in ("person_snapshot_daily", "person_snapshot_monthly"):
                pg_conn.execute(
                    text(f"DELETE FROM {table} WHERE snapshot_date = :d"),
                    {"d": snapshot_date},
                )
                pg_conn.execute(
                    text(f"""
                        INSERT INTO {table}
                            (person_key, snapshot_date, source_system, source_person_serial,
                             first_name, middle_name, last_name, birth_date, gender,
                             record_hash, batch_id)
                        VALUES
                            (:person_key, :snapshot_date, 'keystone', :source_person_serial,
                             :first_name, :middle_name, :last_name, :birth_date, :gender,
                             :record_hash, :batch_id)
                    """),
                    insert_rows,
                )

        stats["row_count"] = len(insert_rows)

    return {
        "status": "ok",
        "snapshot_date": str(snapshot_date),
        "rows": len(insert_rows),
    }
