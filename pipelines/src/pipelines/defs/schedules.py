from datetime import timedelta

import dagster
from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
)

from pipelines.defs.assets.jasperserver_loaders.teller import (
    fct_teller_activity_table,
    fct_teller_out_of_balance_table,
)
from pipelines.defs.assets.warehouse_loaders.fct_members import (
    ks_fct_members_added_table,
    ks_fct_members_lost_table,
    ks_fct_members_summary_table,
    ks_fct_home_branch_members_summary_table,
)
from pipelines.defs.assets.warehouse_loaders.fct_loans import (
    ks_fct_loans_by_branch_summary_table,
)
from pipelines.defs.assets.warehouse_loaders.branch import dim_branch_table
from pipelines.defs.assets.warehouse_loaders.employee import dim_employee_table
from pipelines.defs.assets.warehouse_loaders.ks_supplementals import (
    ks_credit_score_snapshot_table,
    ks_member_branch_snapshot_table,
    ks_person_contact_table,
    ks_products_snapshot_table,
    ks_relationships_snapshot_table,
)
from pipelines.defs.assets.warehouse_loaders.person_snapshots import (
    ks_person_monthly_snapshot_stage_table,
    ks_person_snapshot_daily_table,
    ks_person_snapshot_monthly_table,
    ks_person_snapshot_stage_table,
)
from pipelines.defs.assets.warehouse_loaders.staging_tables_ks import (
    ks_staging_ac_note_table,
    ks_staging_account_table,
    ks_staging_env_table,
    ks_staging_app_table,
    ks_staging_applicant_score_table,
    ks_staging_applicant_table,
    ks_staging_decision_table,
    ks_staging_home_branch_table,
    ks_staging_loan_request_table,
    ks_staging_loan_table,
    ks_staging_login_table,
    ks_staging_monetary_table,
    ks_staging_person_score_table,
    ks_person_mirror_table,
    ks_staging_share_table,
    ks_staging_transaction_table,
)
from pipelines.defs.assets.warehouse_loaders.staging_tables_ks_monthly import (
    ks_staging_ac_note_monthly_table,
    ks_staging_account_monthly_table,
    ks_staging_app_monthly_table,
    ks_staging_applicant_monthly_table,
    ks_staging_applicant_score_monthly_table,
    ks_staging_decision_monthly_table,
    ks_staging_env_monthly_table,
    ks_staging_loan_monthly_table,
    ks_staging_loan_request_monthly_table,
    ks_staging_login_monthly_table,
    ks_staging_monetary_monthly_table,
    ks_staging_person_monthly_table,
    ks_staging_share_monthly_table,
    ks_staging_transaction_monthly_table,
)


# ── Jobs ──────────────────────────────────────────────────────────────────────

# Everything that runs every night:
#   raw CORE mirrors → dimension refreshes → daily partitioned snapshots
# Dagster executes in dep order automatically (raw before snapshots).
# The schedule passes today's date as the partition key; non-partitioned
# assets (raw tables, dims) simply ignore it.
ks_daily_job = define_asset_job(
    name="ks_daily_job",
    selection=AssetSelection.assets(
        # Raw CORE mirrors
        ks_person_mirror_table,
        ks_staging_ac_note_table,
        ks_staging_env_table,
        ks_staging_account_table,
        ks_staging_share_table,
        ks_staging_home_branch_table,
        ks_staging_person_score_table,
        ks_staging_login_table,
        ks_staging_transaction_table,
        ks_staging_monetary_table,
        ks_staging_loan_table,
        ks_staging_app_table,
        ks_staging_loan_request_table,
        ks_staging_applicant_table,
        ks_staging_applicant_score_table,
        ks_staging_decision_table,
        # Dimension refreshes
        dim_branch_table,
        dim_employee_table,
        # Daily partitioned snapshots (depend on staging tables above)
        ks_person_snapshot_stage_table,
        ks_person_snapshot_daily_table,
        ks_member_branch_snapshot_table,
        ks_products_snapshot_table,
        ks_relationships_snapshot_table,
        ks_credit_score_snapshot_table,
        ks_person_contact_table,
        # Loan rollups derived from staging
        ks_fct_loans_by_branch_summary_table,
    ),
)

# Everything that runs on the 1st of each month:
#   monthly person snapshot (uses prior-month Keystone snapshot DB)
#   JasperServer teller reports for the prior month
# The schedule passes the first day of the prior month as the partition key.
# All assets in the job are MonthlyPartitionsDefinition and consume the partition key.
ks_monthly_job = define_asset_job(
    name="ks_monthly_job",
    selection=AssetSelection.assets(
        # Monthly CORE staging mirrors (load first — point-in-time snapshot DB).
        # ks_env_monthly is the gate that every other monthly staging asset depends on.
        ks_staging_env_monthly_table,
        ks_staging_person_monthly_table,
        ks_staging_account_monthly_table,
        ks_staging_share_monthly_table,
        ks_staging_ac_note_monthly_table,
        ks_staging_login_monthly_table,
        ks_staging_transaction_monthly_table,
        ks_staging_monetary_monthly_table,
        ks_staging_loan_monthly_table,
        ks_staging_app_monthly_table,
        ks_staging_loan_request_monthly_table,
        ks_staging_applicant_monthly_table,
        ks_staging_applicant_score_monthly_table,
        ks_staging_decision_monthly_table,
        # Monthly person snapshot (uses staging.person_snapshot_stage which is
        # truncate+reloaded by ks_person_monthly_snapshot_stage_table)
        ks_person_monthly_snapshot_stage_table,
        ks_person_snapshot_monthly_table,
        # JasperServer reports
        fct_teller_activity_table,
        fct_teller_out_of_balance_table,
        # Monthly facts (depend on monthly staging above)
        ks_fct_members_added_table,
        ks_fct_members_lost_table,
        ks_fct_home_branch_members_summary_table,
        ks_fct_members_summary_table,
    ),
)

# HubSpot full pull — independent of Keystone availability.
hubspot_nightly_job = define_asset_job(
    name="hubspot_nightly_job",
    selection=AssetSelection.groups("hubspot_loaders"),
)


# ── Schedules ─────────────────────────────────────────────────────────────────

@dagster.schedule(
    cron_schedule="0 23 * * *",
    job=ks_daily_job,
    execution_timezone="America/Chicago",
)
def nightly_ks_schedule(context: ScheduleEvaluationContext):
    """Runs ks_daily_job at 11pm Central, passing today's date as partition key."""
    partition_key = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(run_key=partition_key, partition_key=partition_key)


@dagster.schedule(
    cron_schedule="0 1 1 * *",
    job=ks_monthly_job,
    execution_timezone="America/Chicago",
)
def monthly_ks_schedule(context: ScheduleEvaluationContext):
    """Runs ks_monthly_job at 1am on the 1st, passing the first day of the prior month."""
    first_of_this_month = context.scheduled_execution_time.replace(day=1)
    first_of_prev_month = (first_of_this_month - timedelta(days=1)).replace(day=1)
    partition_key = first_of_prev_month.strftime("%Y-%m-%d")
    return RunRequest(run_key=partition_key, partition_key=partition_key)


@dagster.schedule(
    cron_schedule="0 1 * * *",
    job=hubspot_nightly_job,
    execution_timezone="America/Chicago",
)
def nightly_hubspot_schedule(context: ScheduleEvaluationContext):
    return RunRequest(run_key=context.scheduled_execution_time.strftime("%Y-%m-%d"))


# ── Registration ──────────────────────────────────────────────────────────────

from pipelines.defs.notifications import job_failure_email_sensor


@dagster.definitions
def schedule_defs():
    return dagster.Definitions(
        jobs=[
            ks_daily_job,
            ks_monthly_job,
            hubspot_nightly_job,
        ],
        schedules=[
            nightly_ks_schedule,
            monthly_ks_schedule,
            nightly_hubspot_schedule,
        ],
        sensors=[
            job_failure_email_sensor,
        ],
    )
