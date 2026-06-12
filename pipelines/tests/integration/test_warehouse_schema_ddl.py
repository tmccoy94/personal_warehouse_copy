"""Integration tests for the warehouse DDL (warehouse_schema.sql).

The test_schema fixture already executed the full DDL into a fresh `test`
schema — these tests assert the result is complete and internally consistent.
This is the guard against the column-renamed-but-view-stale class of bug:
every view is created and selected, so a stale column reference fails here
instead of in prod.
"""
import pytest
from sqlalchemy import text

from .conftest import run_ddl

pytestmark = pytest.mark.integration


# Core warehouse objects that must exist after a clean DDL run. Not exhaustive —
# a missing table here means the DDL file is broken, not that the list is stale.
EXPECTED_TABLES = {
    "dim_branch",
    "dim_date",
    "dim_employee",
    "person_identity",
    "person_source_identifier",
    "person_contact",
    "person_snapshot_daily",
    "person_snapshot_monthly",
    "load_audit",
    "fct_members_summary",
    "fct_home_branch_members_summary",
    "fct_members_added",
    "fct_members_lost",
    "fct_loans",
    "fct_loans_by_branch_summary",
    "fct_loan_portfolio_summary",
    "fct_financial_ratios_summary",
    "fct_nps_summary",
    "fct_branch_profitability_summary",
    "fct_atm_by_machine_summary",
    "fct_digital_channels_transactions",
    "fct_teller_activity",
}

EXPECTED_VIEWS = {
    "v_members_monthly",
    "v_home_branch_members_monthly",
    "v_members_lost",
    "v_cc_loans_growth_ytd_monthly",
    "v_loans_by_branch_lsi_in_main",
    "v_financial_ratios_monthly",
    "v_atm_network_monthly",
    "v_nps_summary",
}


def test_expected_tables_exist(engine, test_schema):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_type = 'BASE TABLE'"
            ),
            {"schema": test_schema},
        ).all()
    created = {row.table_name for row in rows}
    missing = EXPECTED_TABLES - created
    assert not missing, f"DDL did not create expected tables: {sorted(missing)}"


def test_expected_views_exist_and_select_cleanly(engine, test_schema):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema = :schema"
            ),
            {"schema": test_schema},
        ).all()
        created = {row.table_name for row in rows}
        missing = EXPECTED_VIEWS - created
        assert not missing, f"DDL did not create expected views: {sorted(missing)}"

        # Selecting proves every column reference in the view body resolves.
        for view in sorted(created):
            conn.execute(text(f"SELECT * FROM {view} LIMIT 0"))


def test_v_cc_loans_growth_ytd_monthly_exposes_renamed_columns(engine, test_schema):
    """Locks in the loan_growth_ytd_pct / share_growth_ytd_pct rename — the view
    must read from the renamed fact columns and expose its BI-facing names."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :schema "
                "  AND table_name = 'v_cc_loans_growth_ytd_monthly'"
            ),
            {"schema": test_schema},
        ).all()
    columns = {row.column_name for row in rows}
    assert columns == {
        "snapshot_date",
        "credit_cards_growth_ytd_pct",
        "loans_growth_ytd_pct",
        "credit_cards_total_balance",
        "loans_total_balance",
    }


def test_fct_loan_portfolio_summary_has_renamed_columns_only(engine, test_schema):
    """The dropped duplicate and the pre-rename column names must not come back."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :schema "
                "  AND table_name = 'fct_loan_portfolio_summary'"
            ),
            {"schema": test_schema},
        ).all()
    columns = {row.column_name for row in rows}
    assert {"loan_growth_ytd_pct", "share_growth_ytd_pct",
            "loan_income_monthly", "share_income_monthly",
            "cc_income_monthly"} <= columns
    stale = {"loan_ytd_difference_pct", "share_ytd_difference_pct",
             "loan_mom_difference", "share_mom_difference", "cc_mom_difference"}
    assert not (stale & columns), f"Stale pre-rename columns present: {stale & columns}"


def test_ddl_is_idempotent(engine, test_schema):
    """Re-running the full DDL against an existing schema must be a clean no-op
    (everything is IF NOT EXISTS / CREATE OR REPLACE)."""
    run_ddl(engine)
