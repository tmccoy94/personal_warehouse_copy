import ibm_db
from datetime import date, timedelta

import dagster
from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.loading import db2_fetch_chunked
from pipelines.queries import load

_CHUNK_SIZE_DEFAULT  = 5_000
_CHUNK_SIZE_LARGE    = 5_000
_CHUNK_SIZE_ROLLING  = 2_000   # transaction/monetary can be very wide rows


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_person_mirror_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_person"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_person.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_person
                            (person_serial, first_name, middle_name, last_name, birth_date, gender, person_type)
                        VALUES
                            (:person_serial, :first_name, :middle_name, :last_name, :birth_date, :gender, :person_type)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_person")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_account_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_account"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_account.sql"), chunk_size=_CHUNK_SIZE_LARGE):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_account
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
    context.log.info(f"Loaded {total:,} rows into staging.ks_account")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready, ks_staging_account_table], tags={"recovery": "staging_mirror"})
def ks_staging_share_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_share"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_share.sql"), chunk_size=_CHUNK_SIZE_LARGE):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_share
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
    context.log.info(f"Loaded {total:,} rows into staging.ks_share")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_home_branch_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_home_branch"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_home_branch.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_home_branch (person_serial, branch_serial)
                        VALUES (:person_serial, :branch_serial)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_home_branch")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_person_score_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_person_score"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_person_score.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_person_score (person_serial, credit_score, score_date)
                        VALUES (:person_serial, :credit_score, :score_date)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_person_score")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_ac_note_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_ac_note"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_ac_note.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_ac_note (ac_note_serial, person_serial, note_type, expiration_date)
                        VALUES (:ac_note_serial, :person_serial, :note_type, :expiration_date)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_ac_note")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_login_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_login"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_login.sql"), chunk_size=_CHUNK_SIZE_DEFAULT):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_login (login_serial, person_serial, channel)
                        VALUES (:login_serial, :person_serial, :channel)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_login")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_transaction_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    retention_cutoff = date.today() - timedelta(days=90)

    # Incremental: fetch from the day after last loaded posting_date.
    # Falls back to 90-day full pull on first run when table is empty.
    with engine.connect() as pg_conn:
        last_date = pg_conn.execute(
            text("SELECT MAX(posting_date) FROM staging.ks_transaction")
        ).scalar()

    if last_date is None:
        fetch_from = retention_cutoff
        context.log.info(f"Initial load — fetching 90 days from {fetch_from}")
    else:
        fetch_from = last_date + timedelta(days=1)
        context.log.info(f"Incremental load — fetching from {fetch_from}")

    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            for chunk in db2_fetch_chunked(
                conn,
                load("staging_ks_transaction.sql"),
                params={"cutoff_date": fetch_from},
                chunk_size=_CHUNK_SIZE_ROLLING,
            ):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_transaction
                            (transaction_serial, person_serial, posting_date)
                        VALUES
                            (:transaction_serial, :person_serial, :posting_date)
                        ON CONFLICT (transaction_serial) DO NOTHING
                    """),
                    chunk,
                )
                total += len(chunk)
            # Trim records outside the 90-day retention window
            deleted = pg_conn.execute(
                text("DELETE FROM staging.ks_transaction WHERE posting_date < :cutoff"),
                {"cutoff": retention_cutoff},
            ).rowcount
            context.log.info(f"Trimmed {deleted:,} rows older than {retention_cutoff}")
    finally:
        ibm_db.close(conn)
    context.log.info(f"Appended {total:,} rows into staging.ks_transaction")
    return {"status": "ok", "rows_added": total, "fetch_from": str(fetch_from)}


@asset(group_name="ks_daily_loaders", deps=[ks_staging_transaction_table], tags={"recovery": "staging_mirror"})
def ks_staging_monetary_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    retention_cutoff = date.today() - timedelta(days=90)

    with engine.connect() as pg_conn:
        last_date = pg_conn.execute(
            text("SELECT MAX(posting_date) FROM staging.ks_monetary")
        ).scalar()

    if last_date is None:
        fetch_from = retention_cutoff
        context.log.info(f"Initial load — fetching 90 days from {fetch_from}")
    else:
        fetch_from = last_date + timedelta(days=1)
        context.log.info(f"Incremental load — fetching from {fetch_from}")

    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            for chunk in db2_fetch_chunked(
                conn,
                load("staging_ks_monetary.sql"),
                params={"cutoff_date": fetch_from},
                chunk_size=_CHUNK_SIZE_ROLLING,
            ):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_monetary
                            (monetary_serial, transaction_serial, posting_date, category,
                             source, target_category, target_serial,
                             principal, interest, new_balance)
                        VALUES
                            (:monetary_serial, :transaction_serial, :posting_date, :category,
                             :source, :target_category, :target_serial,
                             :principal, :interest, :new_balance)
                        ON CONFLICT (monetary_serial) DO NOTHING
                    """),
                    chunk,
                )
                total += len(chunk)
            deleted = pg_conn.execute(
                text("DELETE FROM staging.ks_monetary WHERE posting_date < :cutoff"),
                {"cutoff": retention_cutoff},
            ).rowcount
            context.log.info(f"Trimmed {deleted:,} rows older than {retention_cutoff}")
    finally:
        ibm_db.close(conn)
    context.log.info(f"Appended {total:,} rows into staging.ks_monetary")
    return {"status": "ok", "rows_added": total, "fetch_from": str(fetch_from)}


# ── ENV posting date mirror ───────────────────────────────────────────────────


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_env_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    try:
        rows = []
        stmt = ibm_db.exec_immediate(conn, load("staging_ks_env.sql"))
        row = ibm_db.fetch_assoc(stmt)
        while row:
            rows.append({k.lower(): v for k, v in row.items()})
            row = ibm_db.fetch_assoc(stmt)
        if rows:
            with engine.begin() as pg_conn:
                pg_conn.execute(text("TRUNCATE staging.ks_env"))
                pg_conn.execute(
                    text("INSERT INTO staging.ks_env (posting_date) VALUES (:posting_date)"),
                    rows,
                )
            posting_date = rows[0]["posting_date"]
            context.log.info(f"Loaded ENV.POSTING_DATE = {posting_date}")
            return {"status": "ok", "posting_date": str(posting_date)}
    finally:
        ibm_db.close(conn)
    return {"status": "ok", "rows": 0}


# ── Loan raw mirrors ──────────────────────────────────────────────────────────


@asset(group_name="ks_daily_loaders", deps=[ks_staging_account_table], tags={"recovery": "staging_mirror"})
def ks_staging_loan_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_loan"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_loan.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_loan
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
    context.log.info(f"Loaded {total:,} rows into staging.ks_loan")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready], tags={"recovery": "staging_mirror"})
def ks_staging_app_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_app"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_app.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_app
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
    context.log.info(f"Loaded {total:,} rows into staging.ks_app")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[ks_staging_app_table], tags={"recovery": "staging_mirror"})
def ks_staging_loan_request_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_loan_request"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_loan_request.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_loan_request
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
    context.log.info(f"Loaded {total:,} rows into staging.ks_loan_request")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[ks_staging_app_table], tags={"recovery": "staging_mirror"})
def ks_staging_applicant_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_applicant"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_applicant.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_applicant
                            (applicant_serial, app_serial, person_serial, ordinal)
                        VALUES
                            (:applicant_serial, :app_serial, :person_serial, :ordinal)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_applicant")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[ks_staging_applicant_table], tags={"recovery": "staging_mirror"})
def ks_staging_applicant_score_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_applicant_score"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_applicant_score.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_applicant_score
                            (applicant_score_serial, applicant_serial, score, score_date)
                        VALUES
                            (:applicant_score_serial, :applicant_serial, :score, :score_date)
                    """),
                    chunk,
                )
                total += len(chunk)
    finally:
        ibm_db.close(conn)
    context.log.info(f"Loaded {total:,} rows into staging.ks_applicant_score")
    return {"status": "ok", "rows": total}


@asset(group_name="ks_daily_loaders", deps=[ks_staging_loan_request_table], tags={"recovery": "staging_mirror"})
def ks_staging_decision_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
    total = 0
    try:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("TRUNCATE staging.ks_decision"))
            for chunk in db2_fetch_chunked(conn, load("staging_ks_decision.sql")):
                pg_conn.execute(
                    text("""
                        INSERT INTO staging.ks_decision
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
    context.log.info(f"Loaded {total:,} rows into staging.ks_decision")
    return {"status": "ok", "rows": total}
