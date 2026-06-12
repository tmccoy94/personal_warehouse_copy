"""End-to-end integration tests for the manual_entries_table asset.

Runs the real asset function against the test schema with a synthetic CSV
covering every input format the month-end person produces by copy-paste:
display percentages, $ amounts with thousands separators, parenthesized
negatives, accounting blanks (' -   '), and Excel's M/D/YYYY rewrite of the
__meta__ snapshot_date cell. Then verifies idempotency and the COALESCE
cell-level preservation contract.
"""
from datetime import date
from decimal import Decimal

import dagster
import pytest
from sqlalchemy import text

from pipelines.defs.assets.file_loaders.csv_loads import manual_entries_table

from .conftest import clear_audit_rows

pytestmark = pytest.mark.integration

SNAPSHOT = date(2030, 5, 31)

FULL_CSV = """\
warehouse_table,warehouse_column,row_key,value,source_location,source_notes
__meta__,snapshot_date,,5/31/2030,,Excel-style date the asset must parse
fct_financial_ratios_summary,net_worth_pct,,12.53%,,
fct_financial_ratios_summary,atm_member_uses,," 2,042 ",,
fct_financial_ratios_summary,atm_network_ytd_net_income,,"$4,934 ",,
fct_nps_summary,nps_score,,72.4,,
fct_loan_portfolio_summary,loan_total_balance,,"129,065,561 ",,
fct_loan_portfolio_summary,loan_growth_ytd_pct,,0.74%,,
fct_loan_portfolio_summary,share_growth_ytd_pct,,0.016179608,,
fct_loan_portfolio_summary,cc_income_monthly,,-145379,,
fct_loan_portfolio_summary,cc_total_balance,,"(297.00)",,
fct_loan_portfolio_summary,atm_fraud_mtd,, -   ,,
fct_branch_profitability_summary,net_income,Main Branch,"$234,853.29 ",,
fct_atm_by_machine_summary,transactions,HQ,554,,
fct_digital_enrollments,enrolled_count,home_banking," 13,095.00 ",,
fct_digital_channels_transactions,bill_pay_transactions,,1234,,
"""

# Second pass: changes ONE cell. Everything loaded by FULL_CSV must survive.
SINGLE_CELL_CSV = """\
warehouse_table,warehouse_column,row_key,value,source_location,source_notes
__meta__,snapshot_date,,2030-05-31,,ISO format this time
fct_financial_ratios_summary,roa_pct,,0.98%,,
"""

BAD_DATE_CSV = """\
warehouse_table,warehouse_column,row_key,value,source_location,source_notes
__meta__,snapshot_date,,31/5/2030,,day-first format is not supported
fct_nps_summary,nps_score,,50,,
"""


def _run_asset(monkeypatch, warehouse_pg, engine, csv_text: str, tmp_path):
    csv_path = tmp_path / "manual_entries.csv"
    csv_path.write_text(csv_text, encoding="utf-8")
    monkeypatch.setenv("MANUAL_ENTRIES_PATH", str(csv_path))
    # Direct invocations share Dagster's 'EPHEMERAL' run_id — clear prior audit
    # rows so the (batch_id, asset_name) PK doesn't collide across runs.
    clear_audit_rows(engine, "manual_entries_table")
    return manual_entries_table(dagster.build_asset_context(), warehouse_pg)


def _scalar(engine, sql: str):
    with engine.connect() as conn:
        return conn.execute(text(sql), {"snapshot_date": SNAPSHOT}).scalar()


@pytest.fixture()
def loaded(monkeypatch, warehouse_pg, engine, seeded_branches, tmp_path):
    """Run the asset once with the full synthetic CSV."""
    result = _run_asset(monkeypatch, warehouse_pg, engine, FULL_CSV, tmp_path)
    return result


def test_meta_date_in_excel_format_is_parsed(engine, loaded):
    assert loaded["snapshot_date"] == str(SNAPSHOT)
    count = _scalar(
        engine,
        "SELECT COUNT(*) FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    )
    assert count == 1


def test_percent_values_stored_as_decimals(engine, loaded):
    assert _scalar(
        engine,
        "SELECT net_worth_pct FROM fct_financial_ratios_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("0.1253")
    assert _scalar(
        engine,
        "SELECT loan_growth_ytd_pct FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("0.0074")
    # Already-decimal input passes through _parse_numeric, NUMERIC(8,4) rounds it.
    assert _scalar(
        engine,
        "SELECT share_growth_ytd_pct FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("0.0162")


def test_accounting_formats_are_parsed(engine, loaded):
    # " 2,042 " — thousands separator + padding
    assert _scalar(
        engine,
        "SELECT atm_member_uses FROM fct_financial_ratios_summary WHERE snapshot_date = :snapshot_date",
    ) == 2042
    # "$4,934 " — currency
    assert _scalar(
        engine,
        "SELECT atm_network_ytd_net_income FROM fct_financial_ratios_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("4934")
    # "129,065,561 "
    assert _scalar(
        engine,
        "SELECT loan_total_balance FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("129065561")
    # "(297.00)" — parenthesized negative
    assert _scalar(
        engine,
        "SELECT cc_total_balance FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("-297.00")
    # plain negative
    assert _scalar(
        engine,
        "SELECT cc_income_monthly FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("-145379")
    # " -   " — accounting blank parses to NULL, never 0
    assert _scalar(
        engine,
        "SELECT atm_fraud_mtd FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) is None


def test_multi_row_tables_resolve_row_keys(engine, loaded, seeded_branches):
    with engine.connect() as conn:
        net_income = conn.execute(
            text(
                "SELECT net_income FROM fct_branch_profitability_summary "
                "WHERE snapshot_date = :snapshot_date AND branch_key = :branch_key"
            ),
            {"snapshot_date": SNAPSHOT, "branch_key": seeded_branches["Main Branch"]},
        ).scalar()
        transactions = conn.execute(
            text(
                "SELECT transactions FROM fct_atm_by_machine_summary "
                "WHERE snapshot_date = :snapshot_date AND atm_name = 'HQ'"
            ),
            {"snapshot_date": SNAPSHOT},
        ).scalar()
    assert net_income == Decimal("234853.29")
    assert transactions == 554


def test_rerun_is_idempotent(monkeypatch, warehouse_pg, engine, loaded, tmp_path):
    _run_asset(monkeypatch, warehouse_pg, engine, FULL_CSV, tmp_path)

    for table in ("fct_financial_ratios_summary", "fct_nps_summary",
                  "fct_loan_portfolio_summary"):
        count = _scalar(
            engine,
            f"SELECT COUNT(*) FROM {table} WHERE snapshot_date = :snapshot_date",
        )
        assert count == 1, f"{table} accumulated rows on re-run"


def test_single_cell_update_preserves_other_columns(
    monkeypatch, warehouse_pg, engine, loaded, tmp_path
):
    """The COALESCE contract: a CSV carrying one cell updates that cell and
    leaves every previously-loaded value untouched."""
    _run_asset(monkeypatch, warehouse_pg, engine, SINGLE_CELL_CSV, tmp_path)

    assert _scalar(
        engine,
        "SELECT roa_pct FROM fct_financial_ratios_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("0.0098")
    # Loaded by the first run, absent from the second CSV — must survive.
    assert _scalar(
        engine,
        "SELECT net_worth_pct FROM fct_financial_ratios_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("0.1253")
    assert _scalar(
        engine,
        "SELECT loan_total_balance FROM fct_loan_portfolio_summary WHERE snapshot_date = :snapshot_date",
    ) == Decimal("129065561")


def test_unrecognized_meta_date_fails_loudly(
    monkeypatch, warehouse_pg, engine, seeded_branches, tmp_path
):
    with pytest.raises(Exception, match="not a recognized date"):
        _run_asset(monkeypatch, warehouse_pg, engine, BAD_DATE_CSV, tmp_path)


def test_unknown_branch_name_fails_loudly(
    monkeypatch, warehouse_pg, engine, seeded_branches, tmp_path
):
    bad_branch_csv = (
        "warehouse_table,warehouse_column,row_key,value,source_location,source_notes\n"
        "__meta__,snapshot_date,,2030-05-31,,\n"
        "fct_branch_profitability_summary,net_income,Overland Park Branch,100,,\n"
    )
    with pytest.raises(Exception, match="Unknown branch 'Overland Park Branch'"):
        _run_asset(monkeypatch, warehouse_pg, engine, bad_branch_csv, tmp_path)


def test_home_banking_routes_to_digital_enrollments(engine, loaded):
    """Decision 47: the hand-entered home banking total lands as a service row
    in fct_digital_enrollments, and the channels summary (which no longer has
    a home_banking_users column) gets only its transaction counts."""
    with engine.connect() as conn:
        enrolled = conn.execute(
            text(
                "SELECT enrolled_count FROM fct_digital_enrollments "
                "WHERE snapshot_date = :snapshot_date AND service = 'home_banking'"
            ),
            {"snapshot_date": SNAPSHOT},
        ).scalar()
        bill_pay_txns = conn.execute(
            text(
                "SELECT bill_pay_transactions FROM fct_digital_channels_transactions "
                "WHERE snapshot_date = :snapshot_date"
            ),
            {"snapshot_date": SNAPSHOT},
        ).scalar()
    assert enrolled == 13095
    assert bill_pay_txns == 1234


def test_unknown_service_fails_loudly(
    monkeypatch, warehouse_pg, engine, seeded_branches, tmp_path
):
    bad_service_csv = (
        "warehouse_table,warehouse_column,row_key,value,source_location,source_notes\n"
        "__meta__,snapshot_date,,2030-05-31,,\n"
        "fct_digital_enrollments,enrolled_count,online_banking,5000,,\n"
    )
    with pytest.raises(Exception, match="Unknown service 'online_banking'"):
        _run_asset(monkeypatch, warehouse_pg, engine, bad_service_csv, tmp_path)
