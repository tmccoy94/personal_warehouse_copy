from datetime import date

import dagster
from dagster import DailyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.person_snapshots import ks_person_snapshot_daily_table
from pipelines.defs.assets.warehouse_loaders.staging_tables_ks import (
    ks_staging_home_branch_table,
    ks_staging_person_score_table,
    ks_staging_share_table,
    ks_staging_account_table,
)
from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.loading import (
    assert_staging_fresh,
    audit_load,
    db2_fetch_all,
    ensure_snapshot_partition,
    resolve_branch_keys,
    upsert_person_identity,
)
from pipelines.queries import load


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "derived"},
    deps=[ks_person_snapshot_daily_table, ks_staging_share_table, ks_staging_home_branch_table],
    partitions_def=DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1, timezone="America/Chicago"),
)
def ks_member_branch_snapshot_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    snapshot_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    assert_staging_fresh(engine, "staging.ks_share", snapshot_date)
    assert_staging_fresh(engine, "staging.ks_home_branch", snapshot_date)

    # Collect all branch serials from raw tables for key resolution
    with engine.connect() as conn:
        branch_serials_result = conn.execute(text("""
            SELECT DISTINCT branch_serial FROM staging.ks_share
            WHERE branch_serial IS NOT NULL
            UNION
            SELECT DISTINCT branch_serial FROM staging.ks_home_branch
            WHERE branch_serial IS NOT NULL
        """))
        all_branch_serials = {int(row.branch_serial) for row in branch_serials_result}

    branch_key_map = resolve_branch_keys(engine, all_branch_serials)

    ensure_snapshot_partition(engine, "ks_member_branch_snapshot", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text("DELETE FROM ks_member_branch_snapshot WHERE snapshot_date = :d"),
                {"d": snapshot_date},
            )

            # Opening branch: exact same logic as the validated member_count_by_branch
            # QA query — OldestGeneralShare CTE with full Trust/Vendor/Deceased exclusions.
            # Only members (open General Share, not vendor, not deceased) appear here.
            # Home branch: from staging.ks_home_branch
            rows_result = pg_conn.execute(text("""
                WITH OldestGeneralShare AS (
                    SELECT DISTINCT ON (share.person_serial)
                        share.person_serial,
                        share.branch_serial
                    FROM staging.ks_share AS share
                    WHERE share.share_id        = '0001'
                      AND share.close_date      IS NULL
                      AND share.charge_off_date IS NULL
                    ORDER BY share.person_serial, share.share_serial ASC
                )
                SELECT
                    psi.person_key,
                    gs.person_serial        AS source_person_serial,
                    gs.branch_serial        AS opening_branch_serial,
                    home.branch_serial      AS home_branch_serial
                FROM OldestGeneralShare AS gs
                JOIN staging.ks_person AS person ON person.person_serial = gs.person_serial
                JOIN person_source_identifier AS psi ON
                    psi.source_system      = 'keystone' AND
                    psi.source_entity_type = 'member'  AND
                    psi.source_identifier  = gs.person_serial::TEXT
                LEFT JOIN staging.ks_home_branch AS home ON
                    home.person_serial = gs.person_serial
                WHERE gs.person_serial <> 0
                  AND (
                      EXISTS (
                          SELECT 1 FROM staging.ks_account AS trust_acct
                          WHERE trust_acct.primary_person_serial = gs.person_serial
                            AND trust_acct.close_date IS NULL
                            AND TRIM(trust_acct.type_description) = 'Trust'
                      )
                      OR (
                          COALESCE(person.person_type, '') <> 'Vendor'
                          AND NOT EXISTS (
                              SELECT 1 FROM staging.ks_ac_note AS note
                              WHERE note.person_serial = gs.person_serial
                                AND note.note_type     = 'Deceased Member'
                                AND (note.expiration_date IS NULL
                                     OR note.expiration_date > (SELECT posting_date FROM staging.ks_env))
                          )
                      )
                  )
            """))

            insert_rows = []
            for row in rows_result:
                insert_rows.append({
                    "person_key":           row.person_key,
                    "snapshot_date":        snapshot_date,
                    "source_person_serial": row.source_person_serial,
                    "branch_key":           branch_key_map.get(int(row.opening_branch_serial)) if row.opening_branch_serial else None,
                    "home_branch_key":      branch_key_map.get(int(row.home_branch_serial)) if row.home_branch_serial else None,
                })

            if insert_rows:
                pg_conn.execute(
                    text("""
                        INSERT INTO ks_member_branch_snapshot
                            (person_key, snapshot_date, source_system, source_person_serial,
                             branch_key, home_branch_key)
                        VALUES
                            (:person_key, :snapshot_date, 'keystone', :source_person_serial,
                             :branch_key, :home_branch_key)
                    """),
                    insert_rows,
                )

        stats["row_count"] = len(insert_rows)

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": len(insert_rows)}


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "derived"},
    deps=[ks_person_snapshot_daily_table, ks_staging_share_table],
    partitions_def=DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1, timezone="America/Chicago"),
)
def ks_products_snapshot_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    snapshot_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    assert_staging_fresh(engine, "staging.ks_share", snapshot_date)

    ensure_snapshot_partition(engine, "ks_products_snapshot", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text("DELETE FROM ks_products_snapshot WHERE snapshot_date = :d"),
                {"d": snapshot_date},
            )

            # Derive share-based products from staging.ks_share using open/close dates.
            # A share is active on snapshot_date if close_date and charge_off_date are
            # either null or fall on/after snapshot_date.
            # Loan products are loaded separately through fct_loans — not duplicated here.
            result = pg_conn.execute(text("""
                INSERT INTO ks_products_snapshot
                    (person_key, snapshot_date, source_system, source_person_serial,
                     product_name, product_count)
                SELECT
                    psi.person_key,
                    :snapshot_date,
                    'keystone',
                    share.person_serial,
                    share.share_type,
                    COUNT(*) AS product_count
                FROM staging.ks_share AS share
                JOIN person_source_identifier AS psi ON
                    psi.source_system      = 'keystone' AND
                    psi.source_entity_type = 'member'  AND
                    psi.source_identifier  = share.person_serial::TEXT
                WHERE
                    share.share_type IS NOT NULL AND
                    (share.close_date      IS NULL OR share.close_date      >= :snapshot_date) AND
                    (share.charge_off_date IS NULL OR share.charge_off_date >= :snapshot_date)
                GROUP BY
                    psi.person_key,
                    share.person_serial,
                    share.share_type
            """), {"snapshot_date": snapshot_date})

        stats["row_count"] = result.rowcount

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": result.rowcount}


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "derived"},
    deps=[ks_person_snapshot_daily_table, ks_staging_account_table],
    partitions_def=DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1, timezone="America/Chicago"),
)
def ks_relationships_snapshot_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    snapshot_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    assert_staging_fresh(engine, "staging.ks_account", snapshot_date)

    ensure_snapshot_partition(engine, "ks_relationships_snapshot", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text("DELETE FROM ks_relationships_snapshot WHERE snapshot_date = :d"),
                {"d": snapshot_date},
            )

            # Derive from staging.ks_account — one row per person using their primary account.
            # Status: 'Platinum' if any account has a Platinum relationship_description.
            result = pg_conn.execute(text("""
                INSERT INTO ks_relationships_snapshot
                    (person_key, snapshot_date, source_system, source_person_serial,
                     status, account_eligibility, account_type)
                SELECT
                    psi.person_key,
                    :snapshot_date,
                    'keystone',
                    account.primary_person_serial,
                    MAX(CASE WHEN account.relationship_description LIKE '%Platinum%' THEN 'Platinum' END),
                    MAX(account.eligibility_description),
                    MAX(account.type_description)
                FROM staging.ks_account AS account
                JOIN person_source_identifier AS psi ON
                    psi.source_system      = 'keystone' AND
                    psi.source_entity_type = 'member'  AND
                    psi.source_identifier  = account.primary_person_serial::TEXT
                WHERE
                    account.close_date IS NULL
                GROUP BY
                    psi.person_key,
                    account.primary_person_serial
            """), {"snapshot_date": snapshot_date})

        stats["row_count"] = result.rowcount

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": result.rowcount}


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "derived"},
    deps=[ks_person_snapshot_daily_table, ks_staging_person_score_table],
    partitions_def=DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1, timezone="America/Chicago"),
)
def ks_credit_score_snapshot_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    snapshot_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    assert_staging_fresh(engine, "staging.ks_person_score", snapshot_date)

    ensure_snapshot_partition(engine, "ks_credit_score_snapshot", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text("DELETE FROM ks_credit_score_snapshot WHERE snapshot_date = :d"),
                {"d": snapshot_date},
            )

            result = pg_conn.execute(text("""
                INSERT INTO ks_credit_score_snapshot
                    (person_key, snapshot_date, source_system, source_person_serial,
                     credit_score, credit_score_date)
                SELECT
                    psi.person_key,
                    :snapshot_date,
                    'keystone',
                    score.person_serial,
                    score.credit_score,
                    score.score_date
                FROM staging.ks_person_score AS score
                JOIN person_source_identifier AS psi ON
                    psi.source_system      = 'keystone' AND
                    psi.source_entity_type = 'member'  AND
                    psi.source_identifier  = score.person_serial::TEXT
            """), {"snapshot_date": snapshot_date})

        stats["row_count"] = result.rowcount

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": result.rowcount}


@asset(
    group_name="ks_daily_loaders",
    tags={"recovery": "live_date_parameterized"},
    deps=[warehouse_schema_ready],
    partitions_def=DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1, timezone="America/Chicago"),
)
def ks_person_contact_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    snapshot_date = date.fromisoformat(context.partition_key)

    conn = keystone_database.get_connection(context=context)
    try:
        address_rows = db2_fetch_all(conn, load("person_address.sql"), {"posting_date": snapshot_date})
        contact_rows = db2_fetch_all(conn, load("person_contact_details.sql"))
    finally:
        import ibm_db
        ibm_db.close(conn)

    engine = warehouse_pg.get_engine()

    all_person_serials = (
        {int(row["source_identifier"]) for row in address_rows}
        | {int(row["source_identifier"]) for row in contact_rows}
    )
    person_key_map = upsert_person_identity(engine, list(all_person_serials))

    rows_to_insert = []

    for row in address_rows:
        source_identifier = row["source_identifier"]
        person_key = person_key_map.get(int(source_identifier))
        if person_key is None:
            raise Exception(
                f"upsert_person_identity failed to produce person_key for "
                f"source_identifier={source_identifier}"
            )
        address_parts = [
            p for p in [
                row.get("street") or "",
                row.get("ad2") or "",
                row.get("city") or "",
                row.get("state") or "",
                row.get("zip") or "",
            ] if p.strip()
        ]
        address_value = ", ".join(address_parts) if address_parts else None
        if address_value:
            is_expired = row.get("is_expired")
            rows_to_insert.append({
                "person_key":          person_key,
                "snapshot_date":       snapshot_date,
                "source_system":       "keystone",
                "source_identifier":   source_identifier,
                "contact_type":        row.get("category"),
                "contact_value":       address_value,
                "is_primary":          row.get("is_primary"),
                "is_active":           (not is_expired) if is_expired is not None else None,
                "last_verified_date":  row.get("address_effective_date"),
                "valid_to":            row.get("expiration_date"),
            })

    for row in contact_rows:
        source_identifier = row["source_identifier"]
        person_key = person_key_map.get(int(source_identifier))
        if person_key is None:
            raise Exception(
                f"upsert_person_identity failed to produce person_key for "
                f"source_identifier={source_identifier}"
            )
        contact_value = row.get("value")
        if not contact_value:
            continue

        bad_contact = row.get("bad_contact")
        is_active = (bad_contact != "Y") if bad_contact is not None else None
        is_primary = row.get("is_primary")

        rows_to_insert.append({
            "person_key":          person_key,
            "snapshot_date":       snapshot_date,
            "source_system":       "keystone",
            "source_identifier":   source_identifier,
            "contact_type":        row.get("type"),
            "contact_value":       contact_value,
            "is_primary":          is_primary,
            "is_active":           is_active,
            "last_verified_date":  row.get("last_verification_date"),
            "valid_to":            row.get("expiration_date"),
        })

    ensure_snapshot_partition(engine, "person_contact", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text(
                    "DELETE FROM person_contact "
                    "WHERE snapshot_date = :snapshot_date AND source_system = 'keystone'"
                ),
                {"snapshot_date": snapshot_date},
            )

            if rows_to_insert:
                pg_conn.execute(
                    text("""
                        INSERT INTO person_contact
                            (person_key, snapshot_date, source_system, source_identifier,
                             contact_type, contact_value,
                             is_primary, is_active, last_verified_date, valid_to)
                        VALUES
                            (:person_key, :snapshot_date, :source_system, :source_identifier,
                             :contact_type, :contact_value,
                             :is_primary, :is_active, :last_verified_date, :valid_to)
                    """),
                    rows_to_insert,
                )

        stats["row_count"] = len(rows_to_insert)

    return {
        "status": "ok",
        "snapshot_date": str(snapshot_date),
        "rows": len(rows_to_insert),
    }
