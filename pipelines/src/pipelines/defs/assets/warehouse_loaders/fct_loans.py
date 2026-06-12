from datetime import date

import dagster
from dagster import MonthlyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.branch import dim_branch_table
from pipelines.defs.assets.warehouse_loaders.employee import dim_employee_table
from pipelines.defs.assets.warehouse_loaders.staging_tables_ks import (
    ks_staging_loan_table,
)
from pipelines.defs.assets.warehouse_loaders.staging_tables_ks_monthly import (
    ks_staging_app_monthly_table,
    ks_staging_applicant_monthly_table,
    ks_staging_applicant_score_monthly_table,
    ks_staging_decision_monthly_table,
    ks_staging_loan_monthly_table,
    ks_staging_loan_request_monthly_table,
    ks_staging_person_monthly_table,
)
from pipelines.defs.resources import WarehousePgResource
from pipelines.loading import audit_load, ensure_snapshot_partition
from pipelines.utils import last_day_of_month


@asset(
    group_name="ks_monthly_loaders",
    deps=[ks_staging_app_monthly_table, ks_staging_applicant_monthly_table,
          ks_staging_applicant_score_monthly_table, ks_staging_loan_request_monthly_table,
          ks_staging_person_monthly_table, dim_branch_table, dim_employee_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_loan_applications_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date = last_day_of_month(partition_date)
    engine = warehouse_pg.get_engine()

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            result = pg_conn.execute(text("""
                WITH
                PrimaryApplicant AS (
                    SELECT
                        applicant.app_serial,
                        applicant.person_serial,
                        applicant.applicant_serial,
                        ROW_NUMBER() OVER (
                            PARTITION BY applicant.app_serial ORDER BY applicant.ordinal ASC
                        ) AS rank
                    FROM staging.ks_applicant_monthly AS applicant
                ),
                CoApplicant AS (
                    SELECT
                        applicant.app_serial,
                        applicant.person_serial,
                        applicant.applicant_serial,
                        ROW_NUMBER() OVER (
                            PARTITION BY applicant.app_serial ORDER BY applicant.ordinal ASC
                        ) AS rank
                    FROM staging.ks_applicant_monthly AS applicant
                ),
                LatestApplicantScore AS (
                    SELECT
                        score.applicant_serial,
                        score.score,
                        ROW_NUMBER() OVER (
                            PARTITION BY score.applicant_serial ORDER BY score.score_date DESC
                        ) AS rn
                    FROM staging.ks_applicant_score_monthly AS score
                ),
                AppBranch AS (
                    SELECT
                        lr.app_serial,
                        lr.branch_serial,
                        ROW_NUMBER() OVER (
                            PARTITION BY lr.app_serial ORDER BY lr.loan_request_serial ASC
                        ) AS rn
                    FROM staging.ks_loan_request_monthly AS lr
                )
                INSERT INTO fct_loan_applications AS loan_app
                    (source_system, source_app_serial, application_number, application_date,
                     app_type, withdrawn_status,
                     source_primary_person_serial, primary_person_key, primary_credit_score,
                     source_co_applicant_person_serial, co_applicant_person_key,
                     co_applicant_name, co_applicant_credit_score,
                     app_processor_key, branch_key)
                SELECT
                    'keystone',
                    app.app_serial,
                    app.application_number,
                    app.application_date,
                    app.app_type,
                    CASE app.withdrawn_indicator
                        WHEN 'A' THEN 'Withdrawn'
                        WHEN 'D' THEN 'Duplicate'
                        ELSE NULL
                    END,
                    pa.person_serial,
                    psi_primary.person_key,
                    primary_score.score,
                    ca.person_serial,
                    psi_co.person_key,
                    COALESCE(co_person.first_name, '') || ' ' || COALESCE(co_person.last_name, ''),
                    co_score.score,
                    emp_processor.employee_key,
                    branch.branch_key
                FROM staging.ks_app_monthly AS app
                LEFT JOIN PrimaryApplicant AS pa ON
                    pa.app_serial = app.app_serial AND pa.rank = 1
                LEFT JOIN LatestApplicantScore AS primary_score ON
                    primary_score.applicant_serial = pa.applicant_serial AND primary_score.rn = 1
                LEFT JOIN CoApplicant AS ca ON
                    ca.app_serial = app.app_serial AND ca.rank = 2
                LEFT JOIN LatestApplicantScore AS co_score ON
                    co_score.applicant_serial = ca.applicant_serial AND co_score.rn = 1
                LEFT JOIN staging.ks_person_monthly AS co_person ON
                    co_person.person_serial = ca.person_serial
                LEFT JOIN AppBranch AS ab ON
                    ab.app_serial = app.app_serial AND ab.rn = 1
                LEFT JOIN person_source_identifier AS psi_primary ON
                    psi_primary.source_system      = 'keystone' AND
                    psi_primary.source_entity_type = 'member'   AND
                    psi_primary.source_identifier  = pa.person_serial::TEXT
                LEFT JOIN person_source_identifier AS psi_co ON
                    psi_co.source_system      = 'keystone' AND
                    psi_co.source_entity_type = 'member'   AND
                    psi_co.source_identifier  = ca.person_serial::TEXT
                LEFT JOIN dim_employee AS emp_processor ON
                    emp_processor.source_system           = 'keystone' AND
                    emp_processor.source_employee_serial  = app.processor_user_serial
                LEFT JOIN dim_branch AS branch ON
                    branch.source_system          = 'keystone' AND
                    branch.source_branch_serial   = ab.branch_serial
                WHERE app.app_type LIKE '%New Loan App%'
                  AND app.application_date BETWEEN :start_date AND :end_date
                ON CONFLICT (source_system, source_app_serial) DO UPDATE SET
                    application_number                = EXCLUDED.application_number,
                    application_date                  = EXCLUDED.application_date,
                    app_type                          = EXCLUDED.app_type,
                    withdrawn_status                  = EXCLUDED.withdrawn_status,
                    source_primary_person_serial      = EXCLUDED.source_primary_person_serial,
                    primary_person_key                = EXCLUDED.primary_person_key,
                    primary_credit_score              = EXCLUDED.primary_credit_score,
                    source_co_applicant_person_serial = EXCLUDED.source_co_applicant_person_serial,
                    co_applicant_person_key           = EXCLUDED.co_applicant_person_key,
                    co_applicant_name                 = EXCLUDED.co_applicant_name,
                    co_applicant_credit_score         = EXCLUDED.co_applicant_credit_score,
                    app_processor_key                 = EXCLUDED.app_processor_key,
                    branch_key                        = EXCLUDED.branch_key
                WHERE (
                    loan_app.application_number                IS DISTINCT FROM EXCLUDED.application_number                OR
                    loan_app.app_type                          IS DISTINCT FROM EXCLUDED.app_type                          OR
                    loan_app.withdrawn_status                  IS DISTINCT FROM EXCLUDED.withdrawn_status                  OR
                    loan_app.primary_person_key                IS DISTINCT FROM EXCLUDED.primary_person_key                OR
                    loan_app.primary_credit_score              IS DISTINCT FROM EXCLUDED.primary_credit_score              OR
                    loan_app.co_applicant_person_key           IS DISTINCT FROM EXCLUDED.co_applicant_person_key           OR
                    loan_app.co_applicant_name                 IS DISTINCT FROM EXCLUDED.co_applicant_name                 OR
                    loan_app.co_applicant_credit_score         IS DISTINCT FROM EXCLUDED.co_applicant_credit_score         OR
                    loan_app.app_processor_key                 IS DISTINCT FROM EXCLUDED.app_processor_key                 OR
                    loan_app.branch_key                        IS DISTINCT FROM EXCLUDED.branch_key
                )
            """), {"start_date": start_date, "end_date": end_date})

        stats["row_count"] = result.rowcount

    return {"status": "ok", "partition": str(partition_date), "rows": result.rowcount}


@asset(
    group_name="ks_monthly_loaders",
    deps=[ks_fct_loan_applications_table, ks_staging_loan_request_monthly_table,
          ks_staging_decision_monthly_table, ks_staging_loan_monthly_table,
          ks_staging_app_monthly_table, dim_branch_table, dim_employee_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_loan_requests_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date = last_day_of_month(partition_date)
    engine = warehouse_pg.get_engine()

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            result = pg_conn.execute(text("""
                WITH LatestDecision AS (
                    SELECT
                        decision.loan_request_serial,
                        decision.decision_serial,
                        decision.decision_date,
                        decision.decision_maker_user_serial,
                        decision.decision_category,
                        decision.decision_description,
                        ROW_NUMBER() OVER (
                            PARTITION BY decision.loan_request_serial
                            ORDER BY decision.decision_date DESC, decision.decision_serial DESC
                        ) AS rn
                    FROM staging.ks_decision_monthly AS decision
                )
                INSERT INTO fct_loan_requests AS loan_req
                    (source_loan_request_serial, application_key, application_number,
                     loan_type, requested_amount, loan_request_credit_score,
                     branch_key, decision_category, decision_description, decision_date,
                     decision_maker_key, processor_key, source_approved_loan_serial,
                     loan_open_date, is_payday_alt)
                SELECT
                    lr.loan_request_serial,
                    app_fct.application_key,
                    app.application_number,
                    lr.loan_type,
                    lr.credit_limit,
                    lr.credit_score,
                    branch.branch_key,
                    ld.decision_category,
                    ld.decision_description,
                    ld.decision_date,
                    emp_decision.employee_key,
                    emp_processor.employee_key,
                    lr.approved_loan_serial,
                    loan.open_date,
                    CASE WHEN lr.loan_type = 'Payday Alternative Loan' THEN TRUE ELSE FALSE END
                FROM staging.ks_loan_request_monthly AS lr
                JOIN staging.ks_app_monthly AS app ON app.app_serial = lr.app_serial
                JOIN fct_loan_applications AS app_fct ON
                    app_fct.source_system     = 'keystone' AND
                    app_fct.source_app_serial = lr.app_serial
                LEFT JOIN LatestDecision AS ld ON
                    ld.loan_request_serial = lr.loan_request_serial AND ld.rn = 1
                LEFT JOIN staging.ks_loan_monthly AS loan ON
                    loan.loan_serial = lr.approved_loan_serial
                LEFT JOIN dim_branch AS branch ON
                    branch.source_system        = 'keystone' AND
                    branch.source_branch_serial = lr.branch_serial
                LEFT JOIN dim_employee AS emp_decision ON
                    emp_decision.source_system          = 'keystone' AND
                    emp_decision.source_employee_serial = ld.decision_maker_user_serial
                LEFT JOIN dim_employee AS emp_processor ON
                    emp_processor.source_system          = 'keystone' AND
                    emp_processor.source_employee_serial = lr.processor_user_serial
                WHERE app.app_type LIKE '%New Loan App%'
                  AND app.application_date BETWEEN :start_date AND :end_date
                ON CONFLICT (source_system, source_loan_request_serial) DO UPDATE SET
                    application_key             = EXCLUDED.application_key,
                    application_number          = EXCLUDED.application_number,
                    loan_type                   = EXCLUDED.loan_type,
                    requested_amount            = EXCLUDED.requested_amount,
                    loan_request_credit_score   = EXCLUDED.loan_request_credit_score,
                    branch_key                  = EXCLUDED.branch_key,
                    decision_category           = EXCLUDED.decision_category,
                    decision_description        = EXCLUDED.decision_description,
                    decision_date               = EXCLUDED.decision_date,
                    decision_maker_key          = EXCLUDED.decision_maker_key,
                    processor_key               = EXCLUDED.processor_key,
                    source_approved_loan_serial = EXCLUDED.source_approved_loan_serial,
                    loan_open_date              = EXCLUDED.loan_open_date,
                    is_payday_alt               = EXCLUDED.is_payday_alt
                WHERE (
                    loan_req.application_key             IS DISTINCT FROM EXCLUDED.application_key             OR
                    loan_req.loan_type                   IS DISTINCT FROM EXCLUDED.loan_type                   OR
                    loan_req.requested_amount            IS DISTINCT FROM EXCLUDED.requested_amount            OR
                    loan_req.loan_request_credit_score   IS DISTINCT FROM EXCLUDED.loan_request_credit_score   OR
                    loan_req.branch_key                  IS DISTINCT FROM EXCLUDED.branch_key                  OR
                    loan_req.decision_category           IS DISTINCT FROM EXCLUDED.decision_category           OR
                    loan_req.decision_description        IS DISTINCT FROM EXCLUDED.decision_description        OR
                    loan_req.decision_date               IS DISTINCT FROM EXCLUDED.decision_date               OR
                    loan_req.decision_maker_key          IS DISTINCT FROM EXCLUDED.decision_maker_key          OR
                    loan_req.processor_key               IS DISTINCT FROM EXCLUDED.processor_key               OR
                    loan_req.source_approved_loan_serial IS DISTINCT FROM EXCLUDED.source_approved_loan_serial OR
                    loan_req.loan_open_date              IS DISTINCT FROM EXCLUDED.loan_open_date              OR
                    loan_req.is_payday_alt               IS DISTINCT FROM EXCLUDED.is_payday_alt
                )
            """), {"start_date": start_date, "end_date": end_date})

        stats["row_count"] = result.rowcount

    return {"status": "ok", "partition": str(partition_date), "rows": result.rowcount}


@asset(
    group_name="ks_monthly_loaders",
    deps=[ks_fct_loan_requests_table, ks_staging_loan_monthly_table,
          ks_staging_loan_request_monthly_table, dim_branch_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def ks_fct_loans_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    snapshot_date = last_day_of_month(partition_date)
    start_date = partition_date
    engine = warehouse_pg.get_engine()

    ensure_snapshot_partition(engine, "fct_loans", snapshot_date)

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(
                text("DELETE FROM fct_loans WHERE snapshot_date = :d"),
                {"d": snapshot_date},
            )

            # Each loan is joined to the single loan_request that was approved for it.
            # DISTINCT ON deduplicated by loan_serial in case multiple requests share
            # the same approved_loan_serial.
            result = pg_conn.execute(text("""
                WITH LoanWithRequest AS (
                    SELECT DISTINCT ON (loan.loan_serial)
                        loan.loan_serial,
                        loan.person_serial,
                        loan.open_date,
                        loan.close_date,
                        loan.maturity_date,
                        loan.balance,
                        loan.interest_rate,
                        (loan.payment_count_dq_under_30 + loan.payment_count_dq_30_to_59 +
                         loan.payment_count_dq_60_to_89 + loan.payment_count_dq_90_to_119 +
                         loan.payment_count_dq_120_and_up) > 0   AS is_delinquent,
                        lr.loan_request_serial,
                        lr.loan_type,
                        lr.credit_limit                           AS original_amount,
                        loan.branch_serial   -- from CORE.LOAN (fully populated), not LOAN_REQUEST
                    FROM staging.ks_loan_monthly AS loan
                    LEFT JOIN staging.ks_loan_request_monthly AS lr ON lr.approved_loan_serial = loan.loan_serial
                    WHERE loan.open_date <= :snapshot_date
                      AND (loan.close_date IS NULL OR loan.close_date >= :start_date)
                    ORDER BY loan.loan_serial, lr.loan_request_serial
                )
                INSERT INTO fct_loans
                    (snapshot_date, source_system, source_loan_serial, source_loan_request_serial,
                     source_person_serial, person_key, branch_key,
                     loan_type, loan_status, original_amount, current_balance,
                     open_date, close_date, maturity_date, interest_rate, is_delinquent)
                SELECT
                    :snapshot_date,
                    'keystone',
                    lwr.loan_serial,
                    lwr.loan_request_serial,
                    lwr.person_serial,
                    psi.person_key,
                    branch.branch_key,
                    lwr.loan_type,
                    CASE WHEN lwr.close_date IS NOT NULL THEN 'closed' ELSE 'open' END,
                    lwr.original_amount,
                    lwr.balance,
                    lwr.open_date,
                    lwr.close_date,
                    lwr.maturity_date,
                    lwr.interest_rate,
                    lwr.is_delinquent
                FROM LoanWithRequest AS lwr
                LEFT JOIN person_source_identifier AS psi ON
                    psi.source_system      = 'keystone' AND
                    psi.source_entity_type = 'member'   AND
                    psi.source_identifier  = lwr.person_serial::TEXT
                LEFT JOIN dim_branch AS branch ON
                    branch.source_system        = 'keystone' AND
                    branch.source_branch_serial = lwr.branch_serial
            """), {"snapshot_date": snapshot_date, "start_date": start_date})

        stats["row_count"] = result.rowcount

    return {"status": "ok", "snapshot_date": str(snapshot_date), "rows": result.rowcount}


@asset(
    group_name="ks_daily_loaders",
    deps=[ks_staging_loan_table, dim_branch_table],
)
def ks_fct_loans_by_branch_summary_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Loans opened per branch per month, split payday / non-payday + total.

    Derived entirely from staging.ks_loan by LOAN.OPEN_DATE month (EOM = snapshot_date).
    loan_count = payday + non_payday (every loan is one or the other). This is the
    authoritative source for this table — it supersedes the CSV backfill, which is
    no longer used to populate it. Truncate/reload each run from 2020-01-01 forward.

    Only emits rows for months that have ended (EOM <= staging.ks_env posting date).
    Without this filter, running the asset mid-month would write an incomplete row
    for the current month containing only loans opened so far — silently wrong data.
    """
    engine = warehouse_pg.get_engine()

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            pg_conn.execute(text("DELETE FROM fct_loans_by_branch_summary"))
            result = pg_conn.execute(text("""
                INSERT INTO fct_loans_by_branch_summary
                    (snapshot_date, branch_key, loan_count, payday_loan_count, non_payday_loan_count)
                SELECT
                    (date_trunc('month', loan.open_date) + INTERVAL '1 month - 1 day')::date AS snapshot_date,
                    branch.branch_key,
                    COUNT(*) AS loan_count,
                    COUNT(*) FILTER (
                        WHERE loan.loan_type_description = 'Payday Alternative Loan'
                    ) AS payday_loan_count,
                    COUNT(*) FILTER (
                        WHERE loan.loan_type_description IS DISTINCT FROM 'Payday Alternative Loan'
                    ) AS non_payday_loan_count
                FROM staging.ks_loan AS loan
                JOIN dim_branch AS branch ON
                    branch.source_system        = 'keystone' AND
                    branch.source_branch_serial = loan.branch_serial
                WHERE loan.open_date >= DATE '2020-01-01'
                  -- Only emit months whose EOM has been reached. Anything later is
                  -- a partial / in-progress month and would write incomplete data.
                  AND (date_trunc('month', loan.open_date) + INTERVAL '1 month - 1 day')::date
                      <= (SELECT posting_date FROM staging.ks_env)
                GROUP BY 1, 2
            """))

        stats["row_count"] = result.rowcount

    return {"status": "ok", "rows": result.rowcount}
