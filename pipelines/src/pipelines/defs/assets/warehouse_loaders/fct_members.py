from datetime import date

import dagster
from dagster import MonthlyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.branch import CORE_TO_WAREHOUSE_BRANCH_NAME, dim_branch_table
from pipelines.defs.assets.warehouse_loaders.person_snapshots import ks_person_snapshot_monthly_table
from pipelines.defs.assets.warehouse_loaders.staging_tables_ks_monthly import (
    ks_staging_ac_note_monthly_table,
    ks_staging_account_monthly_table,
    ks_staging_login_monthly_table,
    ks_staging_loan_monthly_table,
    ks_staging_monetary_monthly_table,
    ks_staging_person_monthly_table,
    ks_staging_share_monthly_table,
    ks_staging_transaction_monthly_table,
    ks_staging_env_monthly_table,
)
from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready

from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.loading import assert_snapshot_db_matches_partition, audit_load, db2_fetch_all, resolve_person_keys
from pipelines.queries import load
from pipelines.utils import last_day_of_month, month_id_from_date


# ---------------------------------------------------------------------------
# ZIP-based branch territory assignment
# Copied verbatim from the exploratory notebook preserved at
# "qa/homebranch script check/home_branch_member_count.ipynb".
# TO_RESOLVE: verify these ZIP lists match current branch territory assignments if
# branches expand or territory boundaries change.
# ---------------------------------------------------------------------------

_SOUTH_BRANCH_ZIPS = {
    64012, 64019, 64030, 64034, 64078, 64063, 64131, 64132, 64134, 64137, 64138, 64139,
    64145, 64146, 64147, 64149, 64170, 64197, 64701, 64720, 64724, 64725, 64726, 64730,
    64733, 64735, 64739, 64740, 64742, 64743, 64746, 64747, 64761, 64763, 64765, 64770,
    64779, 64780, 64783, 64788, 65323, 65338, 65360,
}

_OVERLAND_PARK_BRANCH_ZIPS = {
    66013, 66051, 66061, 66062, 66063, 66083, 66085, 66201, 66202, 66203, 66204,
    66206, 66207, 66208, 66209, 66210, 66211, 66212, 66213, 66214, 66215, 66216, 66217,
    66218, 66219, 66220, 66221, 66223, 66224, 66225, 66226, 66227, 66250, 66251, 66282,
    66285, 66266,
}

_NORTH_BRANCH_ZIPS = {
    64144, 64120, 64188, 64162, 64117, 64161, 64168, 64150, 64119, 64118, 64151, 64158,
    64069, 64156, 64157, 64155, 64154, 64068, 64166, 64165, 64072, 64167, 64092, 64164,
    64153, 64195, 64190, 64163, 64073, 64028, 64089, 64060, 64077, 64024, 64079, 64492,
    64018, 64444, 64017, 64062, 64098, 64439, 64086, 64465, 64085, 64454, 64477, 64448,
    64671, 64493, 64440, 64401, 64035, 64484, 64490, 64507, 64650, 64504, 64443, 64637,
    64503, 64620, 64429, 64474, 64501, 64502, 64508, 64506, 64430, 64668, 64649, 64644,
    64505, 64625, 64436, 64422, 64689, 64459, 64497, 64469, 64656, 64633, 64421, 64485,
    64625, 64494, 64620, 64622, 64664, 64640, 64638, 64654, 64682, 64686, 64480, 64463,
    64473, 64449, 64670, 64639, 64623, 64427, 64647, 64601, 64643, 64657, 64451, 64457,
    64648, 64636, 64438, 64423, 64688, 64433,
}

_MAIN_BRANCH_ZIPS = {
    64101, 64102, 64105, 64106, 64107, 64108, 64109, 64110, 64111, 64112, 64113, 64114,
    64116, 64121, 64123, 64124, 64125, 64126, 64127, 64128, 64129, 64130, 64141, 64148,
    64171, 64179, 64180, 64184, 64187, 64191, 64196, 64198, 64899, 64999,
}

_EAST_BRANCH_ZIPS = {
    64001, 64002, 64011, 64013, 64014, 64015, 64016, 64020, 64021, 64022, 64029, 64036,
    64037, 64040, 64050, 64051, 64052, 64053, 64054, 64055, 64056, 64057, 64058, 64061,
    64063, 64064, 64065, 64066, 64067, 64070, 64071, 64074, 64075, 64076, 64080, 64081,
    64082, 64086, 64088, 64090, 64093, 64096, 64097, 64136, 65236, 65286, 65301, 65302,
    65305, 65321, 65327, 65332, 65333, 65334, 65335, 65336, 65337, 65338, 65339, 65340,
    65344, 65351,
}


def assign_branch_with_fallback(zip_code: str | None, home_branch_name: str | None) -> str:
    """Return the warehouse branch name for a member based on ZIP code, falling back to CU_HOME_BRANCH.

    Priority:
      1. ZIP matches a known branch territory → assign to that branch
      2. ZIP is absent or unmatched → use home_branch_name (normalized to warehouse name)
      3. Neither → 'Unassigned'

    'Unassigned' is preserved in the fct table for traceability. Presentation views
    (e.g. v_home_branch_members_monthly) roll Unassigned counts into Main for BI.
    """
    if zip_code is not None:
        try:
            zip_int = int(str(zip_code).strip()[:5])
            if zip_int in _SOUTH_BRANCH_ZIPS:
                return "South Branch"
            if zip_int in _OVERLAND_PARK_BRANCH_ZIPS:
                return "OP Branch"
            if zip_int in _NORTH_BRANCH_ZIPS:
                return "North Branch"
            if zip_int in _EAST_BRANCH_ZIPS:
                return "East Branch"
            if zip_int in _MAIN_BRANCH_ZIPS:
                return "Main Branch"
        except (ValueError, TypeError):
            pass

    if home_branch_name:
        return CORE_TO_WAREHOUSE_BRANCH_NAME.get(home_branch_name, home_branch_name)

    return "Unassigned"


# ---------------------------------------------------------------------------
# Tier 4: members added / lost
# ---------------------------------------------------------------------------

@asset(
    group_name="ks_monthly_loaders",
    deps=[warehouse_schema_ready],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_members_added_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    """Members added in the partition month. event_date = first account open date."""
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date   = last_day_of_month(partition_date)
    month_id   = month_id_from_date(partition_date)

    conn = keystone_database.get_connection(context=context, job_type="monthly")
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        added_rows = db2_fetch_all(
            conn, load("members_added.sql"),
            {"start_date": start_date, "end_date": end_date},
        )
    finally:
        import ibm_db
        ibm_db.close(conn)

    engine = warehouse_pg.get_engine()
    serials = [int(r["person_serial"]) for r in added_rows]
    person_key_map = resolve_person_keys(engine, serials) if serials else {}

    insert_rows = [
        {
            "source_person_serial": int(row["person_serial"]),
            "person_key":           person_key_map.get(int(row["person_serial"])),
            "month_id":             month_id,
            "event_date":           row.get("first_acct_open_date") or start_date,
            "agency":               row.get("agency"),
            "account_category":     row.get("category"),
            "returning_member":     row.get("returning_member") == "YES",
            "first_gensh_opened":   row.get("first_acct_open_date"),
        }
        for row in added_rows
    ]

    upserted = 0
    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="keystone") as stats:
        with engine.begin() as pg_conn:
            for row in insert_rows:
                result = pg_conn.execute(text("""
                    INSERT INTO fct_members_added
                        (source_system, month_id, source_person_serial, person_key,
                         event_date, agency, account_category, returning_member, first_gensh_opened)
                    VALUES
                        ('keystone', :month_id, :source_person_serial, :person_key,
                         :event_date, :agency, :account_category, :returning_member, :first_gensh_opened)
                    ON CONFLICT (source_system, source_person_serial, month_id) DO NOTHING
                """), row)
                upserted += result.rowcount
        stats["row_count"] = upserted

    return {"status": "ok", "month_id": month_id, "added": len(added_rows), "upserted": upserted}


@asset(
    group_name="ks_monthly_loaders",
    deps=[warehouse_schema_ready],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_members_lost_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    """Members lost in the partition month. event_date = last General Share close date."""
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date   = last_day_of_month(partition_date)
    month_id   = month_id_from_date(partition_date)

    conn = keystone_database.get_connection(context=context, job_type="monthly")
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        lost_rows = db2_fetch_all(
            conn, load("members_lost.sql"),
            {"start_date": start_date, "end_date": end_date},
        )
    finally:
        import ibm_db
        ibm_db.close(conn)

    engine = warehouse_pg.get_engine()
    serials = [int(r["person_serial"]) for r in lost_rows]
    person_key_map = resolve_person_keys(engine, serials) if serials else {}

    insert_rows = [
        {
            "source_person_serial":    int(row["person_serial"]),
            "person_key":              person_key_map.get(int(row["person_serial"])),
            "month_id":                month_id,
            # event_date = last General Share close date (when membership ended)
            "event_date":              row.get("last_gensh_closed") or end_date,
            "close_reason":            row.get("ac_close_reason"),
            "membership_duration_yrs": row.get("membership_duration"),
            "first_gensh_opened":      row.get("first_gensh_opened"),
        }
        for row in lost_rows
    ]

    upserted = 0
    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="keystone") as stats:
        with engine.begin() as pg_conn:
            for row in insert_rows:
                result = pg_conn.execute(text("""
                    INSERT INTO fct_members_lost
                        (source_system, month_id, source_person_serial, person_key,
                         event_date, close_reason, membership_duration_yrs, first_gensh_opened)
                    VALUES
                        ('keystone', :month_id, :source_person_serial, :person_key,
                         :event_date, :close_reason, :membership_duration_yrs, :first_gensh_opened)
                    ON CONFLICT (source_system, source_person_serial, month_id) DO NOTHING
                """), row)
                upserted += result.rowcount
        stats["row_count"] = upserted

    return {"status": "ok", "month_id": month_id, "lost": len(lost_rows), "upserted": upserted}


# ---------------------------------------------------------------------------
# Tier 6a: home branch members summary (ZIP-territory logic)
# ---------------------------------------------------------------------------

@asset(
    group_name="ks_monthly_loaders",
    deps=[dim_branch_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_home_branch_members_summary_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date = last_day_of_month(partition_date)
    snapshot_date = end_date

    conn = keystone_database.get_connection(context=context, job_type="monthly")
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        totals_rows = db2_fetch_all(
            conn,
            load("home_branch_zips.sql"),
            {"as_of_date": snapshot_date},
        )
        added_rows = db2_fetch_all(
            conn,
            load("home_branch_opened.sql"),
            {"begin_date": start_date, "end_date": end_date},
        )
    finally:
        import ibm_db
        ibm_db.close(conn)

    for row in totals_rows:
        row["assigned_branch"] = assign_branch_with_fallback(
            row.get("zip"), row.get("home_branch")
        )

    for row in added_rows:
        row["assigned_branch"] = assign_branch_with_fallback(
            row.get("zip"), row.get("home_branch")
        )

    engine = warehouse_pg.get_engine()

    with engine.connect() as pg_conn:
        branch_rows = pg_conn.execute(
            text(
                "SELECT branch_name, branch_key FROM dim_branch "
                "WHERE source_system IS NOT NULL OR branch_name = 'Unassigned'"
            )
        ).all()
        branch_key_by_name = {row.branch_name: row.branch_key for row in branch_rows}

    required_names = {"South Branch", "OP Branch", "North Branch", "East Branch", "Main Branch", "Unassigned"}
    missing = required_names - set(branch_key_by_name.keys())
    if missing:
        raise Exception(
            f"dim_branch is missing entries for: {missing}. "
            "Materialize dim_branch_table first."
        )

    branch_totals: dict[str, int] = {}
    for row in totals_rows:
        branch = row["assigned_branch"]
        branch_totals[branch] = branch_totals.get(branch, 0) + 1

    branch_added: dict[str, int] = {}
    for row in added_rows:
        branch = row["assigned_branch"]
        branch_added[branch] = branch_added.get(branch, 0) + 1

    # Fail loudly if assign_branch_with_fallback produced a name with no matching
    # dim_branch row — otherwise those members get silently dropped on lookup.
    unmapped = (set(branch_totals) | set(branch_added)) - set(branch_key_by_name)
    if unmapped:
        raise Exception(
            f"assign_branch_with_fallback returned branch names not in dim_branch: {sorted(unmapped)}. "
            "Add the CORE→warehouse mapping in _CORE_TO_WAREHOUSE_BRANCH_NAME or create the dim_branch entry."
        )

    # Write rows for the 5 real branches + Unassigned. CU Total and LSI roll-ups
    # are computed elsewhere; presentation views fold Unassigned into Main for BI.
    insert_rows = []
    for branch_name, branch_key in branch_key_by_name.items():
        if branch_name in ("CU Total", "LSI Branch - Online Applications"):
            continue
        insert_rows.append(
            {
                "snapshot_date": snapshot_date,
                "branch_key":    branch_key,
                "total_members": branch_totals.get(branch_name, 0),
                "members_added": branch_added.get(branch_name, 0),
            }
        )

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text(
                    "DELETE FROM fct_home_branch_members_summary "
                    "WHERE snapshot_date = :snapshot_date"
                ),
                {"snapshot_date": snapshot_date},
            )
            pg_conn.execute(
                text("""
                    INSERT INTO fct_home_branch_members_summary
                        (snapshot_date, branch_key, total_members, members_added)
                    VALUES
                        (:snapshot_date, :branch_key, :total_members, :members_added)
                """),
                insert_rows,
            )

        stats["row_count"] = len(insert_rows)

    return {
        "status": "ok",
        "snapshot_date": str(snapshot_date),
        "rows": len(insert_rows),
    }


# ---------------------------------------------------------------------------
# Tier 6b: CU-total members summary
# ---------------------------------------------------------------------------
# Shared computation helpers — called by both normal assets and repair_ops
# ---------------------------------------------------------------------------

def _compute_fct_members_summary(engine, snapshot_date: date, month_id: int) -> int:
    """DELETE + INSERT fct_members_summary for one snapshot_date. Returns row count."""
    with engine.begin() as pg_conn:
        pg_conn.execute(
            text("DELETE FROM fct_members_summary WHERE snapshot_date = :d"),
            {"d": snapshot_date},
        )
        result = pg_conn.execute(text("""
                WITH member_counts AS (
                    -- Opening-branch member counts derived from staging.ks_share_monthly,
                    -- a point-in-time mirror of CORE.SHARE loaded from the partition's
                    -- frozen snapshot DB (MONTH_END_1/2 or QUARTER_END). Matches the
                    -- validated logic in qa/queries/warehouse/member_count_by_branch.sql.
                    -- All staging.ks_*_monthly tables in this query are loaded by the
                    -- ks_monthly_loaders staging assets ahead of this asset.
                    SELECT
                        COUNT(*)                                                  AS total_members,
                        COUNT(*) FILTER (WHERE branch.branch_name = 'OP Branch')  AS ks_members
                    FROM (
                        SELECT DISTINCT ON (share.person_serial)
                            share.person_serial,
                            share.branch_serial
                        FROM staging.ks_share_monthly AS share
                        WHERE share.share_id        = '0001'
                          AND share.close_date      IS NULL
                          AND share.charge_off_date IS NULL
                        ORDER BY share.person_serial, share.share_serial ASC
                    ) AS oldest_share
                    JOIN staging.ks_person_monthly AS person ON person.person_serial = oldest_share.person_serial
                    LEFT JOIN dim_branch AS branch
                        ON branch.source_system         = 'keystone'
                       AND branch.source_branch_serial = oldest_share.branch_serial
                    WHERE oldest_share.person_serial <> 0
                      AND (
                          EXISTS (
                              SELECT 1 FROM staging.ks_account_monthly AS trust_acct
                              WHERE trust_acct.primary_person_serial = oldest_share.person_serial
                                AND trust_acct.close_date IS NULL
                                AND TRIM(trust_acct.type_description) = 'Trust'
                          )
                          OR (
                              COALESCE(person.person_type, '') <> 'Vendor'
                              AND NOT EXISTS (
                                  SELECT 1 FROM staging.ks_ac_note_monthly AS note
                                  WHERE note.person_serial = oldest_share.person_serial
                                    AND note.note_type     = 'Deceased Member'
                                    AND (note.expiration_date IS NULL
                                         OR note.expiration_date >
                                            (SELECT posting_date FROM staging.ks_env_monthly))
                              )
                          )
                      )
                ),
                events AS (
                    SELECT
                        (SELECT COUNT(*) FROM fct_members_added WHERE month_id = :month_id) AS members_added,
                        (SELECT COUNT(*) FROM fct_members_lost  WHERE month_id = :month_id) AS members_lost
                ),
                hb AS (
                    -- hb_mo includes Unassigned: the v_home_branch_members_monthly view
                    -- folds Unassigned into Main Branch (which is MO), so the hb_mo column
                    -- here must include them too to match the verification CSV.
                    SELECT
                        COALESCE(SUM(hbms.total_members) FILTER (
                            WHERE branch.branch_name = 'OP Branch'
                        ), 0) AS hb_ks,
                        COALESCE(SUM(hbms.total_members) FILTER (
                            WHERE branch.branch_name NOT IN (
                                'CU Total', 'OP Branch',
                                'LSI Branch - Online Applications'
                            )
                        ), 0) AS hb_mo
                    FROM fct_home_branch_members_summary AS hbms
                    JOIN dim_branch AS branch ON branch.branch_key = hbms.branch_key
                    WHERE hbms.snapshot_date = :snapshot_date
                ),
                prior AS (
                    SELECT total_members, ks_members, mo_members, active_members,
                           members_18_50, hb_ks_members, hb_mo_members, non_kcpd_members
                    FROM fct_members_summary
                    WHERE snapshot_date = (
                        SELECT MAX(snapshot_date) FROM fct_members_summary
                        WHERE snapshot_date < :snapshot_date
                    )
                )
                INSERT INTO fct_members_summary (
                    snapshot_date,
                    total_members, ks_members, mo_members,
                    active_members, members_18_50,
                    hb_ks_members, hb_mo_members,
                    members_lost, members_added, non_kcpd_members,
                    net_new_members, net_new_ks_members, net_new_mo_members,
                    net_new_active_members, net_new_members_18_50,
                    net_new_hb_mo, net_new_hb_ks, net_new_non_kcpd_members
                )
                SELECT
                    :snapshot_date,
                    mc.total_members,
                    mc.ks_members,
                    mc.total_members - mc.ks_members                          AS mo_members,

                    -- 3+ services count: exact validated QA logic from staging
                    (WITH ProductCounts AS (
                        SELECT person_serial, COUNT(product) AS service_count
                        FROM (
                            SELECT share.person_serial, share.share_type AS product
                            FROM staging.ks_share_monthly AS share
                            WHERE share.close_date IS NULL AND share.charge_off_date IS NULL
                              AND share.share_type IS NOT NULL
                            UNION ALL
                            SELECT loan.person_serial,
                                   COALESCE(loan.loan_type_description, 'Unknown Loan') AS product
                            FROM staging.ks_loan_monthly AS loan
                            WHERE loan.close_date IS NULL AND loan.charge_off_date IS NULL
                            UNION ALL
                            SELECT DISTINCT login.person_serial, 'Online Banking' AS product
                            FROM staging.ks_login_monthly AS login
                            WHERE login.channel = 'Access_Softek'
                            UNION ALL
                            SELECT DISTINCT s2.person_serial, 'Direct Deposit' AS product
                            FROM staging.ks_share_monthly AS s2
                            WHERE s2.close_date IS NULL AND s2.charge_off_date IS NULL
                              AND s2.ach_deposit_last_amount >= 500.00
                              AND s2.ach_deposit_last_date >
                                  (SELECT posting_date FROM staging.ks_env_monthly) - INTERVAL '30 days'
                            UNION ALL
                            SELECT DISTINCT txn.person_serial, 'Bill Payment' AS product
                            FROM staging.ks_transaction_monthly AS txn
                            JOIN staging.ks_monetary_monthly AS monetary
                                ON monetary.transaction_serial = txn.transaction_serial
                            WHERE monetary.source = 'B'
                              AND txn.posting_date >
                                  (SELECT posting_date FROM staging.ks_env_monthly) - INTERVAL '30 days'
                        ) AS products WHERE product IS NOT NULL
                        GROUP BY person_serial
                    ),
                    GeneralShareHolders AS (
                        SELECT DISTINCT ON (gs.person_serial) gs.person_serial
                        FROM staging.ks_share_monthly AS gs
                        WHERE gs.share_id = '0001'
                          AND gs.close_date IS NULL AND gs.charge_off_date IS NULL
                        ORDER BY gs.person_serial, gs.share_serial ASC
                    )
                    SELECT COUNT(*)
                    FROM GeneralShareHolders AS mb2
                    JOIN staging.ks_person_monthly AS p2 ON p2.person_serial = mb2.person_serial
                    LEFT JOIN ProductCounts AS pc ON pc.person_serial = mb2.person_serial
                    WHERE mb2.person_serial <> 0
                      AND COALESCE(pc.service_count, 0) >= 3
                      AND COALESCE(p2.person_type, '') <> 'Vendor'
                      AND NOT EXISTS (
                          SELECT 1 FROM staging.ks_ac_note_monthly AS note
                          WHERE note.person_serial = mb2.person_serial
                            AND note.note_type     = 'Deceased Member'
                            AND (note.expiration_date IS NULL
                                 OR note.expiration_date >
                                    (SELECT posting_date FROM staging.ks_env_monthly))
                      )
                    )                                                         AS active_members,

                    -- 18-50 count: exact validated QA logic from staging
                    (SELECT COUNT(DISTINCT share.person_serial)
                     FROM staging.ks_share_monthly AS share
                     JOIN staging.ks_person_monthly AS person
                         ON person.person_serial = share.person_serial
                     WHERE share.share_id        = '0001'
                       AND share.close_date      IS NULL
                       AND share.charge_off_date IS NULL
                       AND share.person_serial  <> 0
                       AND (
                           EXISTS (
                               SELECT 1 FROM staging.ks_account_monthly AS trust_acct
                               WHERE trust_acct.primary_person_serial = share.person_serial
                                 AND trust_acct.close_date IS NULL
                                 AND TRIM(trust_acct.type_description) = 'Trust'
                           )
                           OR (
                               COALESCE(person.person_type, '') <> 'Vendor'
                               AND NOT EXISTS (
                                   SELECT 1 FROM staging.ks_ac_note_monthly AS note
                                   WHERE note.person_serial = share.person_serial
                                     AND note.note_type     = 'Deceased Member'
                                     AND (note.expiration_date IS NULL
                                          OR note.expiration_date >
                                             (SELECT posting_date FROM staging.ks_env_monthly))
                               )
                               AND NOT EXISTS (
                                   SELECT 1 FROM staging.ks_account_monthly AS excl_acct
                                   WHERE excl_acct.primary_person_serial = share.person_serial
                                     AND excl_acct.close_date IS NULL
                                     AND TRIM(excl_acct.type_description)
                                         IN ('Reserves', '1-Choose One', 'Trust')
                               )
                               AND person.birth_date <=
                                   (SELECT posting_date FROM staging.ks_env_monthly) - INTERVAL '18 years'
                               AND person.birth_date >
                                   (SELECT posting_date FROM staging.ks_env_monthly) - INTERVAL '51 years'
                           )
                       )
                    )                                                         AS members_18_50,

                    hb.hb_ks                                                  AS hb_ks_members,
                    hb.hb_mo                                                  AS hb_mo_members,
                    ev.members_lost,
                    ev.members_added,

                    -- Non-KCPD: TO_RESOLVE — this column is meant to count NEW members
                    -- (additions this month) whose eligibility is not 'KCPD (Missouri)',
                    -- not the total. Current implementation still reads from
                    -- ks_member_branch_snapshot for snapshot_date and returns 0 — left
                    -- in place until the field is reworked against fct_members_added.
                    (SELECT COUNT(DISTINCT pb2.person_key)
                     FROM ks_member_branch_snapshot AS pb2
                     LEFT JOIN ks_relationships_snapshot AS rel
                         ON rel.person_key    = pb2.person_key
                        AND rel.snapshot_date = (
                            SELECT MAX(snapshot_date) FROM ks_relationships_snapshot
                        )
                     WHERE pb2.snapshot_date = :snapshot_date
                       AND COALESCE(rel.account_eligibility, '') <> 'KCPD (Missouri)'
                    )                                                         AS non_kcpd_members,

                    -- Net-change columns: all computed as current minus prior month.
                    -- NULL when no prior row exists (first month ever loaded).
                    -- Note: net_new_members is headcount delta (not added minus lost —
                    -- those differ when returning members are counted as events).
                    mc.total_members
                        - COALESCE(p.total_members,  mc.total_members)        AS net_new_members,
                    mc.ks_members
                        - COALESCE(p.ks_members,     mc.ks_members)           AS net_new_ks_members,
                    (mc.total_members - mc.ks_members)
                        - COALESCE(p.mo_members,
                                   mc.total_members - mc.ks_members)          AS net_new_mo_members,
                    NULL                                                       AS net_new_active_members,
                    NULL                                                       AS net_new_members_18_50,
                    hb.hb_mo - COALESCE(p.hb_mo_members, hb.hb_mo)           AS net_new_hb_mo,
                    hb.hb_ks - COALESCE(p.hb_ks_members, hb.hb_ks)          AS net_new_hb_ks,
                    NULL                                                       AS net_new_non_kcpd_members

                FROM member_counts mc
                CROSS JOIN events ev
                CROSS JOIN hb
                LEFT JOIN prior p ON TRUE
            """), {"snapshot_date": snapshot_date, "month_id": month_id})

        # Patch net_new_active_members and net_new_members_18_50 — these are inline
        # subqueries in the INSERT so they can't self-reference for the delta.
        pg_conn.execute(text("""
            UPDATE fct_members_summary AS cur
            SET
                net_new_active_members = cur.active_members - COALESCE(
                    (SELECT p.active_members FROM fct_members_summary AS p
                     WHERE p.snapshot_date = (
                         SELECT MAX(pp.snapshot_date) FROM fct_members_summary AS pp
                         WHERE pp.snapshot_date < cur.snapshot_date
                     )),
                    cur.active_members
                ),
                net_new_members_18_50 = cur.members_18_50 - COALESCE(
                    (SELECT p.members_18_50 FROM fct_members_summary AS p
                     WHERE p.snapshot_date = (
                         SELECT MAX(pp.snapshot_date) FROM fct_members_summary AS pp
                         WHERE pp.snapshot_date < cur.snapshot_date
                     )),
                    cur.members_18_50
                )
            WHERE cur.snapshot_date = :snapshot_date
        """), {"snapshot_date": snapshot_date})

    return result.rowcount


# ---------------------------------------------------------------------------
# Tier 6b: CU-total members summary
# ---------------------------------------------------------------------------

@asset(
    group_name="ks_monthly_loaders",
    deps=[
        ks_person_snapshot_monthly_table,
        ks_fct_members_added_table,
        ks_fct_members_lost_table,
        ks_fct_home_branch_members_summary_table,
        # Monthly staging mirrors — _compute_fct_members_summary reads from these
        # instead of the daily staging tables so totals are point-in-time accurate
        # against the partition's frozen snapshot DB.
        ks_staging_share_monthly_table,
        ks_staging_person_monthly_table,
        ks_staging_account_monthly_table,
        ks_staging_ac_note_monthly_table,
        ks_staging_loan_monthly_table,
        ks_staging_login_monthly_table,
        ks_staging_transaction_monthly_table,
        ks_staging_monetary_monthly_table,
        ks_staging_env_monthly_table,
    ],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_members_summary_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    snapshot_date  = last_day_of_month(partition_date)
    month_id       = month_id_from_date(partition_date)
    engine         = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system=None) as stats:
        stats["row_count"] = _compute_fct_members_summary(engine, snapshot_date, month_id)

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": stats["row_count"]}


