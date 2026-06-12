"""
ks_daily_repair / ks_monthly_repair — manually-triggered assets to clean up bad job runs.

A "bad run" is one where the nightly job connected to the wrong database
(e.g. KEYSTONE_DB_OVERRIDE was still set to a snapshot DB), causing the
daily partition to contain stale or incorrect data.

Recovery behaviour by tag:
    staging_mirror         — self-heal: next nightly run reloads staging via
                             truncate/reload. No partition data in warehouse.
                             Nothing to drop or recover here.

    drop_only              — unrecoverable: person biographic snapshots are
                             point-in-time captures that cannot be reconstructed
                             from today's live DB for a past date. Drop and log.

    derived                — recoverable: re-derive from current staging tables.
                             Staging is always truncate/reload so the next nightly
                             load will have fresh live data; re-running the derive
                             SQL produces the correct warehouse snapshot.

    live_date_parameterized — recoverable: re-run the DB2 source query with an
                              explicit posting_date = bad_date so the effective/
                              expiration filters treat that date as "today".

Usage: in Dagster UI, select ks_daily_repair_table, choose the bad partition
date, and materialise. Do NOT run this on a schedule.
"""
from datetime import date, timedelta

import dagster
import ibm_db
from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.ks_db_connections import LIVE
from pipelines.loading import (
    assert_snapshot_db_matches_partition,
    audit_load,
    db2_fetch_all,
    ensure_snapshot_partition,
    resolve_branch_keys,
    upsert_person_identity,
)
from pipelines.queries import load
from pipelines.utils import last_day_of_month


# Warehouse tables with daily partitions that need to be dropped on a bad run.
_DROP_ONLY_TABLES   = ["person_snapshot_daily"]
_DERIVED_TABLES     = [
    "ks_member_branch_snapshot",
    "ks_products_snapshot",
    "ks_relationships_snapshot",
    "ks_credit_score_snapshot",
]
_CONTACT_TABLE = "person_contact"


@asset(
    group_name="repair_ops",
    tags={"recovery": "repair_trigger"},
    partitions_def=DailyPartitionsDefinition(
        start_date="2026-06-01",
        end_offset=1,
        timezone="America/Chicago",
    ),
)
def ks_daily_repair(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    """Drop bad daily partition data and recover what is possible from live DB."""
    bad_date = date.fromisoformat(context.partition_key)
    engine   = warehouse_pg.get_engine()
    batch_id = f"repair-{bad_date}"

    context.log.info(f"Starting repair for bad partition: {bad_date}")

    # ── Step 1: Drop all bad partition data ──────────────────────────────────

    with engine.begin() as pg_conn:
        for table in _DROP_ONLY_TABLES + _DERIVED_TABLES:
            pg_conn.execute(
                text(f"DELETE FROM {table} WHERE snapshot_date = :d"),
                {"d": bad_date},
            )
            context.log.info(f"Dropped {table} partition for {bad_date}")

        pg_conn.execute(
            text("DELETE FROM person_contact WHERE snapshot_date = :d AND source_system = 'keystone'"),
            {"d": bad_date},
        )
        context.log.info(f"Dropped person_contact partition for {bad_date}")

    # ── Step 2: Log drop_only (unrecoverable) ────────────────────────────────

    for table in _DROP_ONLY_TABLES:
        context.log.warning(
            f"[drop_only] {table} for {bad_date} was dropped and will NOT be recovered. "
            f"Person biographic snapshots are point-in-time and cannot be reconstructed "
            f"from today's live DB for a past date. A one-day gap in this table is acceptable."
        )

    # ── Step 3: Recover derived assets from current staging ──────────────────
    # Staging tables are always truncate/reload so they contain the most recent
    # live data. Re-deriving from them is the best possible recovery for a past date.

    context.log.info("Re-deriving assets from current staging tables...")

    # ks_member_branch_snapshot
    with engine.connect() as conn:
        branch_serials_result = conn.execute(text("""
            SELECT DISTINCT branch_serial FROM staging.ks_share WHERE branch_serial IS NOT NULL
            UNION
            SELECT DISTINCT branch_serial FROM staging.ks_home_branch WHERE branch_serial IS NOT NULL
        """))
        all_branch_serials = {int(row.branch_serial) for row in branch_serials_result}

    branch_key_map = resolve_branch_keys(engine, all_branch_serials)
    ensure_snapshot_partition(engine, "ks_member_branch_snapshot", bad_date)

    with engine.begin() as pg_conn:
        rows_result = pg_conn.execute(text("""
            WITH OldestGeneralShare AS (
                SELECT DISTINCT ON (share.person_serial)
                    share.person_serial, share.branch_serial
                FROM staging.ks_share AS share
                WHERE share.share_id = '0001' AND share.close_date IS NULL
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
                psi.source_system = 'keystone' AND psi.source_entity_type = 'member'
                AND psi.source_identifier = gs.person_serial::TEXT
            LEFT JOIN staging.ks_home_branch AS home ON home.person_serial = gs.person_serial
            WHERE gs.person_serial <> 0
              AND (
                  EXISTS (SELECT 1 FROM staging.ks_account AS trust
                          WHERE trust.primary_person_serial = gs.person_serial
                            AND trust.close_date IS NULL
                            AND TRIM(trust.type_description) = 'Trust')
                  OR (
                      COALESCE(person.person_type, '') <> 'Vendor'
                      AND NOT EXISTS (SELECT 1 FROM staging.ks_ac_note AS note
                                      WHERE note.person_serial = gs.person_serial
                                        AND note.note_type = 'Deceased Member'
                                        AND (note.expiration_date IS NULL
                                             OR note.expiration_date >
                                                (SELECT posting_date FROM staging.ks_env)))
                  )
              )
        """))
        branch_rows = [
            {
                "person_key":           row.person_key,
                "snapshot_date":        bad_date,
                "source_person_serial": row.source_person_serial,
                "branch_key":           branch_key_map.get(int(row.opening_branch_serial)) if row.opening_branch_serial else None,
                "home_branch_key":      branch_key_map.get(int(row.home_branch_serial)) if row.home_branch_serial else None,
            }
            for row in rows_result
        ]
        if branch_rows:
            pg_conn.execute(text("""
                INSERT INTO ks_member_branch_snapshot
                    (person_key, snapshot_date, source_system, source_person_serial,
                     branch_key, home_branch_key)
                VALUES
                    (:person_key, :snapshot_date, 'keystone', :source_person_serial,
                     :branch_key, :home_branch_key)
            """), branch_rows)
    context.log.info(f"[derived] Recovered ks_member_branch_snapshot: {len(branch_rows)} rows")

    # ks_products_snapshot, ks_relationships_snapshot, ks_credit_score_snapshot
    # — re-run their INSERT logic directly using current staging
    ensure_snapshot_partition(engine, "ks_products_snapshot", bad_date)
    ensure_snapshot_partition(engine, "ks_relationships_snapshot", bad_date)
    ensure_snapshot_partition(engine, "ks_credit_score_snapshot", bad_date)

    with engine.begin() as pg_conn:
        r = pg_conn.execute(text("""
            INSERT INTO ks_products_snapshot
                (person_key, snapshot_date, source_system, source_person_serial,
                 product_name, product_count)
            SELECT psi.person_key, :bad_date, 'keystone', share.person_serial,
                   share.share_type, COUNT(*) AS product_count
            FROM staging.ks_share AS share
            JOIN person_source_identifier AS psi ON
                psi.source_system = 'keystone' AND psi.source_entity_type = 'member'
                AND psi.source_identifier = share.person_serial::TEXT
            WHERE share.share_type IS NOT NULL
              AND (share.close_date IS NULL OR share.close_date >= :bad_date)
              AND (share.charge_off_date IS NULL OR share.charge_off_date >= :bad_date)
            GROUP BY psi.person_key, share.person_serial, share.share_type
        """), {"bad_date": bad_date})
        context.log.info(f"[derived] Recovered ks_products_snapshot: {r.rowcount} rows")

        r = pg_conn.execute(text("""
            INSERT INTO ks_relationships_snapshot
                (person_key, snapshot_date, source_system, source_person_serial,
                 status, account_eligibility, account_type)
            SELECT psi.person_key, :bad_date, 'keystone', account.primary_person_serial,
                   MAX(CASE WHEN account.relationship_description LIKE '%Platinum%' THEN 'Platinum' END),
                   MAX(account.eligibility_description),
                   MAX(account.type_description)
            FROM staging.ks_account AS account
            JOIN person_source_identifier AS psi ON
                psi.source_system = 'keystone' AND psi.source_entity_type = 'member'
                AND psi.source_identifier = account.primary_person_serial::TEXT
            WHERE account.close_date IS NULL
            GROUP BY psi.person_key, account.primary_person_serial
        """), {"bad_date": bad_date})
        context.log.info(f"[derived] Recovered ks_relationships_snapshot: {r.rowcount} rows")

        r = pg_conn.execute(text("""
            INSERT INTO ks_credit_score_snapshot
                (person_key, snapshot_date, source_system, source_person_serial,
                 credit_score, credit_score_date)
            SELECT psi.person_key, :bad_date, 'keystone', score.person_serial,
                   score.credit_score, score.score_date
            FROM staging.ks_person_score AS score
            JOIN person_source_identifier AS psi ON
                psi.source_system = 'keystone' AND psi.source_entity_type = 'member'
                AND psi.source_identifier = score.person_serial::TEXT
        """), {"bad_date": bad_date})
        context.log.info(f"[derived] Recovered ks_credit_score_snapshot: {r.rowcount} rows")

    # ── Step 4: Recover live_date_parameterized (person_contact) ─────────────
    # Pass bad_date as posting_date so effective/expiration filters treat it as
    # "today", recovering the contact records that were valid on that date.

    context.log.info(f"Recovering person_contact for {bad_date} from live DB...")
    conn = keystone_database.get_connection(db_name=LIVE)
    try:
        address_rows = db2_fetch_all(conn, load("person_address.sql"), {"posting_date": bad_date})
        contact_rows = db2_fetch_all(conn, load("person_contact_details.sql"))
    finally:
        ibm_db.close(conn)

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
            continue
        address_parts = [p for p in [
            row.get("street") or "", row.get("ad2") or "",
            row.get("city") or "", row.get("state") or "", row.get("zip") or "",
        ] if p.strip()]
        address_value = ", ".join(address_parts) if address_parts else None
        if address_value:
            is_expired = row.get("is_expired")
            rows_to_insert.append({
                "person_key": person_key, "snapshot_date": bad_date,
                "source_system": "keystone", "source_identifier": source_identifier,
                "contact_type": row.get("category"), "contact_value": address_value,
                "is_primary": row.get("is_primary"),
                "is_active": (not is_expired) if is_expired is not None else None,
                "last_verified_date": row.get("address_effective_date"),
                "valid_to": row.get("expiration_date"),
            })

    for row in contact_rows:
        source_identifier = row["source_identifier"]
        person_key = person_key_map.get(int(source_identifier))
        if person_key is None or not row.get("value"):
            continue
        bad_contact = row.get("bad_contact")
        rows_to_insert.append({
            "person_key": person_key, "snapshot_date": bad_date,
            "source_system": "keystone", "source_identifier": source_identifier,
            "contact_type": row.get("type"), "contact_value": row.get("value"),
            "is_primary": row.get("is_primary"),
            "is_active": (bad_contact != "Y") if bad_contact is not None else None,
            "last_verified_date": row.get("last_verification_date"),
            "valid_to": row.get("expiration_date"),
        })

    ensure_snapshot_partition(engine, "person_contact", bad_date)

    with engine.begin() as pg_conn:
        if rows_to_insert:
            pg_conn.execute(text("""
                INSERT INTO person_contact
                    (person_key, snapshot_date, source_system, source_identifier,
                     contact_type, contact_value, is_primary, is_active,
                     last_verified_date, valid_to)
                VALUES
                    (:person_key, :snapshot_date, :source_system, :source_identifier,
                     :contact_type, :contact_value, :is_primary, :is_active,
                     :last_verified_date, :valid_to)
            """), rows_to_insert)
    context.log.info(
        f"[live_date_parameterized] Recovered person_contact: {len(rows_to_insert)} rows "
        f"(addresses + contacts as-of {bad_date})"
    )

    context.log.info(f"Repair complete for {bad_date}.")
    return {
        "status": "ok",
        "bad_date": str(bad_date),
        "drop_only_dropped": _DROP_ONLY_TABLES,
        "derived_recovered": _DERIVED_TABLES,
        "contact_recovered": len(rows_to_insert),
    }


# ---------------------------------------------------------------------------
# Monthly repair
# ---------------------------------------------------------------------------

@asset(
    group_name="repair_ops",
    tags={"recovery": "repair_trigger"},
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_monthly_repair(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    """Drop a bad monthly partition and fully reload from the correct snapshot DB.

    Unlike daily, monthly data is fully recoverable — the snapshot DBs
    (T31 / T32 / T33) preserve data for up to 3 months.  This asset connects
    to the correct snapshot DB for the partition month, drops the bad rows,
    and reloads all monthly warehouse tables.

    Usage: in Dagster UI, select ks_monthly_repair, choose the bad partition
    month (first day of that month), and materialise.
    """
    from pipelines.defs.assets.warehouse_loaders.fct_members import (
        assign_branch_with_fallback,
    )
    from pipelines.ks_db_connections import MONTH_SNAPSHOT_MAP
    from pipelines.utils import month_id_from_date

    partition_date = date.fromisoformat(context.partition_key)
    snapshot_date  = last_day_of_month(partition_date)
    month_id       = month_id_from_date(partition_date)
    month_name     = (snapshot_date).strftime("%B")
    correct_db     = MONTH_SNAPSHOT_MAP[month_name]
    engine         = warehouse_pg.get_engine()

    context.log.info(
        f"Monthly repair for {snapshot_date} — using snapshot DB '{correct_db}' ({month_name})"
    )

    # ── Step 1: Drop bad partition data ──────────────────────────────────────

    _monthly_warehouse_tables = [
        ("person_snapshot_monthly",         "snapshot_date"),
        ("fct_home_branch_members_summary", "snapshot_date"),
        ("fct_members_summary",             "snapshot_date"),
    ]

    with engine.begin() as pg_conn:
        for table, date_col in _monthly_warehouse_tables:
            pg_conn.execute(
                text(f"DELETE FROM {table} WHERE {date_col} = :d"),
                {"d": snapshot_date},
            )
            context.log.info(f"Dropped {table} for {snapshot_date}")
        pg_conn.execute(text("DELETE FROM fct_members_added WHERE month_id = :m"), {"m": month_id})
        pg_conn.execute(text("DELETE FROM fct_members_lost  WHERE month_id = :m"), {"m": month_id})
        context.log.info(f"Dropped fct_members_added + fct_members_lost for month_id {month_id}")

    # ── Step 2: Reload from correct snapshot DB ───────────────────────────────

    conn = keystone_database.get_connection(db_name=correct_db)
    try:
        # Fail loudly if correct_db doesn't actually hold this partition's month-end
        # (prevents writing future-month snapshot_date rows containing stale data —
        # the bug that produced the bad 2026-06-30 person_snapshot_monthly load).
        assert_snapshot_db_matches_partition(conn, partition_date)

        # Person monthly snapshot stage
        snapshot_rows = db2_fetch_all(conn, load("person_snapshot_daily.sql"))

        # Home branch totals + opened
        hb_totals_rows = db2_fetch_all(
            conn, load("home_branch_zips.sql"), {"as_of_date": snapshot_date}
        )
        hb_opened_rows = db2_fetch_all(
            conn, load("home_branch_opened.sql"),
            {"begin_date": partition_date, "end_date": snapshot_date},
        )

        # Members added / lost
        added_rows = db2_fetch_all(
            conn, load("members_added.sql"),
            {"start_date": partition_date, "end_date": snapshot_date},
        )
        lost_rows = db2_fetch_all(
            conn, load("members_lost.sql"),
            {"start_date": partition_date, "end_date": snapshot_date},
        )
    finally:
        ibm_db.close(conn)

    context.log.info(
        f"DB2 ({correct_db}): {len(snapshot_rows)} person rows, "
        f"{len(hb_totals_rows)} hb totals, {len(hb_opened_rows)} hb opened, "
        f"{len(added_rows)} members added, {len(lost_rows)} members lost"
    )

    # ── Step 3: Write person_snapshot_monthly ────────────────────────────────

    from pipelines.loading import upsert_person_identity, resolve_person_keys, ensure_snapshot_partition

    all_serials = [int(r["person_serial"]) for r in snapshot_rows]
    person_key_map = upsert_person_identity(engine, all_serials)

    ensure_snapshot_partition(engine, "person_snapshot_monthly", snapshot_date)

    monthly_rows = []
    for row in snapshot_rows:
        serial = int(row["person_serial"])
        pk = person_key_map.get(serial)
        if pk is None:
            continue
        monthly_rows.append({
            "person_key": pk, "snapshot_date": snapshot_date,
            "source_system": "keystone", "source_person_serial": serial,
            "first_name": row.get("first_name"), "middle_name": row.get("middle_name"),
            "last_name": row.get("last_name"), "birth_date": row.get("birth_date"),
            "gender": row.get("gender"), "record_hash": None, "batch_id": f"repair-{snapshot_date}",
        })

    with engine.begin() as pg_conn:
        if monthly_rows:
            pg_conn.execute(text("""
                INSERT INTO person_snapshot_monthly
                    (person_key, snapshot_date, source_system, source_person_serial,
                     first_name, middle_name, last_name, birth_date, gender,
                     record_hash, batch_id)
                VALUES
                    (:person_key, :snapshot_date, :source_system, :source_person_serial,
                     :first_name, :middle_name, :last_name, :birth_date, :gender,
                     :record_hash, :batch_id)
            """), monthly_rows)
    context.log.info(f"Wrote {len(monthly_rows)} rows to person_snapshot_monthly")

    # ── Step 4: Write fct_members_added + fct_members_lost ───────────────────

    al_serials = [int(r["person_serial"]) for r in added_rows + lost_rows]
    pk_map = resolve_person_keys(engine, al_serials) if al_serials else {}

    added_insert = [
        {
            "source_person_serial": int(row["person_serial"]),
            "person_key": pk_map.get(int(row["person_serial"])),
            "month_id": month_id,
            "event_date": row.get("first_acct_open_date") or partition_date,
            "agency": row.get("agency"), "account_category": row.get("category"),
            "returning_member": row.get("returning_member") == "YES",
            "first_gensh_opened": row.get("first_acct_open_date"),
        }
        for row in added_rows
    ]
    lost_insert = [
        {
            "source_person_serial": int(row["person_serial"]),
            "person_key": pk_map.get(int(row["person_serial"])),
            "month_id": month_id,
            "event_date": row.get("last_gensh_closed") or snapshot_date,
            "close_reason": row.get("ac_close_reason"),
            "membership_duration_yrs": row.get("membership_duration"),
            "first_gensh_opened": row.get("first_gensh_opened"),
        }
        for row in lost_rows
    ]

    with engine.begin() as pg_conn:
        for row in added_insert:
            pg_conn.execute(text("""
                INSERT INTO fct_members_added
                    (source_system, month_id, source_person_serial, person_key,
                     event_date, agency, account_category, returning_member, first_gensh_opened)
                VALUES
                    ('keystone', :month_id, :source_person_serial, :person_key,
                     :event_date, :agency, :account_category, :returning_member, :first_gensh_opened)
                ON CONFLICT (source_system, source_person_serial, month_id) DO NOTHING
            """), row)
        for row in lost_insert:
            pg_conn.execute(text("""
                INSERT INTO fct_members_lost
                    (source_system, month_id, source_person_serial, person_key,
                     event_date, close_reason, membership_duration_yrs, first_gensh_opened)
                VALUES
                    ('keystone', :month_id, :source_person_serial, :person_key,
                     :event_date, :close_reason, :membership_duration_yrs, :first_gensh_opened)
                ON CONFLICT (source_system, source_person_serial, month_id) DO NOTHING
            """), row)
    context.log.info(f"Wrote {len(added_insert)} added + {len(lost_insert)} lost rows")

    # ── Step 5: Write fct_home_branch_members_summary ────────────────────────

    with engine.connect() as pg_conn:
        branch_rows_result = pg_conn.execute(
            text(
                "SELECT branch_name, branch_key FROM dim_branch "
                "WHERE source_system IS NOT NULL OR branch_name = 'Unassigned'"
            )
        ).all()
        branch_key_by_name = {r.branch_name: r.branch_key for r in branch_rows_result}

    for row in hb_totals_rows:
        row["assigned_branch"] = assign_branch_with_fallback(row.get("zip"), row.get("home_branch"))
    for row in hb_opened_rows:
        row["assigned_branch"] = assign_branch_with_fallback(row.get("zip"), row.get("home_branch"))

    branch_totals: dict[str, int] = {}
    branch_added_hb: dict[str, int] = {}
    for row in hb_totals_rows:
        b = row["assigned_branch"]
        branch_totals[b] = branch_totals.get(b, 0) + 1
    for row in hb_opened_rows:
        b = row["assigned_branch"]
        branch_added_hb[b] = branch_added_hb.get(b, 0) + 1

    hb_insert_rows = [
        {"snapshot_date": snapshot_date, "branch_key": branch_key_by_name[bn],
         "total_members": branch_totals.get(bn, 0), "members_added": branch_added_hb.get(bn, 0)}
        for bn in branch_key_by_name
        if bn not in ("CU Total", "LSI Branch - Online Applications")
    ]

    with engine.begin() as pg_conn:
        pg_conn.execute(text("""
            INSERT INTO fct_home_branch_members_summary
                (snapshot_date, branch_key, total_members, members_added)
            VALUES (:snapshot_date, :branch_key, :total_members, :members_added)
        """), hb_insert_rows)
    context.log.info(f"Wrote {len(hb_insert_rows)} rows to fct_home_branch_members_summary")

    # ── Step 6: fct_members_summary ───────────────────────────────────────────

    from pipelines.defs.assets.warehouse_loaders.fct_members import (
        _compute_fct_members_summary,
    )
    from pipelines.utils import month_id_from_date as _mid

    _month_id    = _mid(partition_date)
    summary_rows = _compute_fct_members_summary(engine, snapshot_date, _month_id)
    context.log.info(f"fct_members_summary: {summary_rows} row")

    context.log.info(f"Monthly repair complete for {snapshot_date}.")
    return {
        "status": "ok",
        "snapshot_date": str(snapshot_date),
        "snapshot_db": correct_db,
        "person_monthly_rows": len(monthly_rows),
        "added_rows": len(added_insert),
        "lost_rows": len(lost_insert),
        "hb_branch_rows": len(hb_insert_rows),
        "summary_rows": summary_rows,
        "branch_summary_rows": branch_summary,
    }
