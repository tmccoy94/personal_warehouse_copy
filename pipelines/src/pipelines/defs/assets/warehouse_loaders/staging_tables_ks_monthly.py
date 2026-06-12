"""Monthly point-in-time CORE staging mirrors.

Mirrors of staging.ks_* tables loaded from the frozen MONTH_END_x snapshot DB instead
of LIVE. The monthly fact assets (ks_fct_members_summary, ks_fct_loan_applications,
ks_fct_loan_requests, ks_fct_loans) read from these instead of the daily tables so
their numbers are point-in-time accurate against the snapshot month-end.

Each asset is MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"). The connection
routes to the correct snapshot DB via job_type="monthly" (see ks_db_connections
.resolve_db_for_scheduled_run). The assert_snapshot_db_matches_partition guard at
the start of each asset fails loudly if the snapshot DB's POSTING_DATE doesn't equal
the partition's EOM — preventing the silent-wrong-data bug that was producing bogus
6/30 rows previously.

Two tables (ks_transaction_monthly, ks_monetary_monthly) load by COPYING from the
daily 90-day rolling staging tables filtered to the partition's 30-day window. Pulling
CORE.MONETARY from DB2 is a ~20-min operation; copying from already-loaded daily
staging is milliseconds. The loader fails loudly if the daily window doesn't fully
cover the required range.
"""
from datetime import date, timedelta

import dagster
import ibm_db
from dagster import MonthlyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.staging_tables_ks import (
    ks_staging_monetary_table,
    ks_staging_transaction_table,
)
from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.loading import assert_snapshot_db_matches_partition, db2_fetch_chunked
from pipelines.queries import load
from pipelines.utils import last_day_of_month

_PARTITIONS = MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago")
_CHUNK_SIZE_DEFAULT  = 5_000
_CHUNK_SIZE_LARGE    = 5_000
_TAG = {"recovery": "staging_mirror"}


# ── ENV posting date mirror (load first — the assert guard reads it) ──────────

@asset(group_name="ks_monthly_loaders", deps=[warehouse_schema_ready],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_env_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        rows = []
        stmt = ibm_db.exec_immediate(conn, load("staging_ks_env.sql"))
        row = ibm_db.fetch_assoc(stmt)
        while row:
            rows.append({k.lower(): v for k, v in row.items()})
            row = ibm_db.fetch_assoc(stmt)
        if rows:
            with engine.begin() as pg_conn:
                pg_conn.execute(text("TRUNCATE staging.ks_env_monthly"))
                pg_conn.execute(
                    text("INSERT INTO staging.ks_env_monthly (posting_date) VALUES (:posting_date)"),
                    rows,
                )
            context.log.info(f"Loaded ENV.POSTING_DATE = {rows[0]['posting_date']} into ks_env_monthly")
            return {"status": "ok", "posting_date": str(rows[0]["posting_date"])}
    finally:
        ibm_db.close(conn)
    return {"status": "ok", "rows": 0}


# ── Reference / dimension-ish source tables ───────────────────────────────────

@asset(group_name="ks_monthly_loaders", deps=[ks_staging_env_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_person_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_person_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_person.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_person_monthly
                            (person_serial, first_name, middle_name, last_name, birth_date, gender, person_type)
                        VALUES
                            (:person_serial, :first_name, :middle_name, :last_name, :birth_date, :gender, :person_type)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_person_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_env_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_account_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_account_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_account.sql"), chunk_size=_CHUNK_SIZE_LARGE):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_account_monthly
                            (account_serial, primary_person_serial, open_date, close_date,
                             dormancy_restriction, relationship_serial, eligibility_serial,
                             type_serial, eligibility_description, type_description,
                             relationship_description, close_reason_description)
                        VALUES
                            (:account_serial, :primary_person_serial, :open_date, :close_date,
                             :dormancy_restriction, :relationship_serial, :eligibility_serial,
                             :type_serial, :eligibility_description, :type_description,
                             :relationship_description, :close_reason_description)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_account_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders",
       deps=[ks_staging_env_monthly_table, ks_staging_account_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_share_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_share_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_share.sql"), chunk_size=_CHUNK_SIZE_LARGE):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_share_monthly
                            (share_serial, person_serial, account_serial, share_id, type_serial,
                             share_type, branch_serial, open_date, close_date, charge_off_date,
                             ach_deposit_last_amount, ach_deposit_last_date)
                        VALUES
                            (:share_serial, :person_serial, :account_serial, :share_id, :type_serial,
                             :share_type, :branch_serial, :open_date, :close_date, :charge_off_date,
                             :ach_deposit_last_amount, :ach_deposit_last_date)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_share_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_env_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_ac_note_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_ac_note_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_ac_note.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_ac_note_monthly
                            (ac_note_serial, person_serial, note_type, expiration_date)
                        VALUES
                            (:ac_note_serial, :person_serial, :note_type, :expiration_date)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_ac_note_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_env_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_login_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_login_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_login.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_login_monthly
                            (login_serial, person_serial, channel)
                        VALUES
                            (:login_serial, :person_serial, :channel)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_login_monthly")
    return {"status": "ok", "rows": total}


# ── Transaction / monetary: copy from daily staging instead of DB2 pull ──────

@asset(group_name="ks_monthly_loaders",
       deps=[ks_staging_env_monthly_table, ks_staging_transaction_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_transaction_monthly_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Load transactions in the partition's 30-day window by copying from daily staging.

    The daily ks_staging_transaction_table maintains a 90-day rolling window from
    LIVE. For a monthly partition we need transactions in the 30 days ending at the
    partition's EOM, which is fully contained in that 90-day window as long as the
    partition is recent enough. Fails loudly if the daily window doesn't cover the
    needed range — fall back to a fresh DB2 pull at that point.
    """
    partition_date = date.fromisoformat(context.partition_key)
    snapshot_date = last_day_of_month(partition_date)
    window_start = snapshot_date - timedelta(days=30)
    engine = warehouse_pg.get_engine()

    with engine.connect() as pg_conn:
        daily_min = pg_conn.execute(text("SELECT MIN(posting_date) FROM staging.ks_transaction")).scalar()
    if daily_min is None or daily_min > window_start:
        raise RuntimeError(
            f"Daily staging.ks_transaction earliest date = {daily_min} does not cover "
            f"required window start {window_start} (snapshot_date {snapshot_date}, 30-day window). "
            f"Run the daily ks_staging_transaction_table for an earlier window, or fall back "
            f"to a fresh DB2 pull against the snapshot DB."
        )

    with engine.begin() as pg_conn:
        pg_conn.execute(text("TRUNCATE staging.ks_transaction_monthly"))
        result = pg_conn.execute(
            text("""
                INSERT INTO staging.ks_transaction_monthly
                    (transaction_serial, person_serial, posting_date, loaded_at)
                SELECT transaction_serial, person_serial, posting_date, NOW()
                FROM staging.ks_transaction
                WHERE posting_date >  :window_start
                  AND posting_date <= :snapshot_date
            """),
            {"window_start": window_start, "snapshot_date": snapshot_date},
        )
    context.log.info(
        f"Copied {result.rowcount:,} rows into staging.ks_transaction_monthly "
        f"({window_start} < posting_date <= {snapshot_date})"
    )
    return {"status": "ok", "rows": result.rowcount}


@asset(group_name="ks_monthly_loaders",
       deps=[ks_staging_env_monthly_table, ks_staging_monetary_table,
             ks_staging_transaction_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_monetary_monthly_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Load monetary records for the partition window by copying from daily staging."""
    partition_date = date.fromisoformat(context.partition_key)
    snapshot_date = last_day_of_month(partition_date)
    window_start = snapshot_date - timedelta(days=30)
    engine = warehouse_pg.get_engine()

    with engine.connect() as pg_conn:
        daily_min = pg_conn.execute(text("SELECT MIN(posting_date) FROM staging.ks_monetary")).scalar()
    if daily_min is None or daily_min > window_start:
        raise RuntimeError(
            f"Daily staging.ks_monetary earliest date = {daily_min} does not cover "
            f"required window start {window_start} (snapshot_date {snapshot_date})."
        )

    with engine.begin() as pg_conn:
        pg_conn.execute(text("TRUNCATE staging.ks_monetary_monthly"))
        result = pg_conn.execute(
            text("""
                INSERT INTO staging.ks_monetary_monthly
                    (monetary_serial, transaction_serial, posting_date, category, source,
                     target_category, target_serial, principal, interest, new_balance, loaded_at)
                SELECT monetary_serial, transaction_serial, posting_date, category, source,
                       target_category, target_serial, principal, interest, new_balance, NOW()
                FROM staging.ks_monetary
                WHERE posting_date >  :window_start
                  AND posting_date <= :snapshot_date
            """),
            {"window_start": window_start, "snapshot_date": snapshot_date},
        )
    context.log.info(
        f"Copied {result.rowcount:,} rows into staging.ks_monetary_monthly "
        f"({window_start} < posting_date <= {snapshot_date})"
    )
    return {"status": "ok", "rows": result.rowcount}


# ── Loan source tables ────────────────────────────────────────────────────────

@asset(group_name="ks_monthly_loaders",
       deps=[ks_staging_env_monthly_table, ks_staging_account_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_loan_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_loan_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_loan.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_loan_monthly
                            (loan_serial, account_serial, person_serial,
                             open_date, close_date, charge_off_date, maturity_date,
                             branch_serial, loan_type_description, balance, interest_rate,
                             payment_count_dq_under_30, payment_count_dq_30_to_59,
                             payment_count_dq_60_to_89, payment_count_dq_90_to_119,
                             payment_count_dq_120_and_up)
                        VALUES
                            (:loan_serial, :account_serial, :person_serial,
                             :open_date, :close_date, :charge_off_date, :maturity_date,
                             :branch_serial, :loan_type_description, :balance, :interest_rate,
                             :payment_count_dq_under_30, :payment_count_dq_30_to_59,
                             :payment_count_dq_60_to_89, :payment_count_dq_90_to_119,
                             :payment_count_dq_120_and_up)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_loan_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_env_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_app_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_app_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_app.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_app_monthly
                            (app_serial, application_number, application_date,
                             app_type, withdrawn_indicator, processor_user_serial)
                        VALUES
                            (:app_serial, :application_number, :application_date,
                             :app_type, :withdrawn_indicator, :processor_user_serial)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_app_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_app_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_loan_request_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_loan_request_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_loan_request.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_loan_request_monthly
                            (loan_request_serial, app_serial, type_serial, loan_type,
                             branch_serial, credit_limit, credit_score,
                             processor_user_serial, approved_loan_serial)
                        VALUES
                            (:loan_request_serial, :app_serial, :type_serial, :loan_type,
                             :branch_serial, :credit_limit, :credit_score,
                             :processor_user_serial, :approved_loan_serial)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_loan_request_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_app_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_applicant_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_applicant_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_applicant.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_applicant_monthly
                            (applicant_serial, app_serial, person_serial, ordinal)
                        VALUES
                            (:applicant_serial, :app_serial, :person_serial, :ordinal)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_applicant_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_applicant_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_applicant_score_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_applicant_score_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_applicant_score.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_applicant_score_monthly
                            (applicant_score_serial, applicant_serial, score, score_date)
                        VALUES
                            (:applicant_score_serial, :applicant_serial, :score, :score_date)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_applicant_score_monthly")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_monthly_loaders", deps=[ks_staging_loan_request_monthly_table],
       tags=_TAG, partitions_def=_PARTITIONS)
def ks_staging_decision_monthly_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context, job_type="monthly")
    total = 0
    try:
        assert_snapshot_db_matches_partition(conn, partition_date)
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_decision_monthly"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_decision.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_decision_monthly
                            (decision_serial, loan_request_serial, type_serial,
                             decision_date, decision_maker_user_serial,
                             decision_category_code, decision_category, decision_description)
                        VALUES
                            (:decision_serial, :loan_request_serial, :type_serial,
                             :decision_date, :decision_maker_user_serial,
                             :decision_category_code, :decision_category, :decision_description)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_decision_monthly")
    return {"status": "ok", "rows": total}
