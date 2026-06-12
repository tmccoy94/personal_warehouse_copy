import os
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import dagster
import pandas as pd
from dagster import MonthlyPartitionsDefinition, asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import WarehousePgResource
from pipelines.loading import audit_load
from pipelines.utils import last_day_of_month


# ── Path helpers ──────────────────────────────────────────────────────────────

def _csv_drop_path(table_name: str) -> Path:
    base = Path(os.environ.get("CSV_DROP_PATH", "/data/csv_drop"))
    return base / table_name


def _find_csv(folder: Path) -> Path:
    """Return the first CSV file found in folder, raise if none."""
    files = sorted(folder.glob("*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No CSV file found in {folder}. Drop a file there and re-run."
        )
    return files[0]


# ── Value parsers ─────────────────────────────────────────────────────────────

def _parse_decimal(value: object) -> Decimal | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_currency(value: object) -> Decimal | None:
    """Parse accounting-style currency strings: '$93,763.54 ' and '($297.00)' → Decimal."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace("$", "").replace(",", "").strip()
    try:
        result = Decimal(s)
        return -result if negative else result
    except InvalidOperation:
        return None


def _parse_pct(value: object) -> Decimal | None:
    """Parse a display percentage ('12.53%' or '12.53') → Decimal(0.1253).

    Matches the warehouse convention of storing percentages as decimals.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().rstrip("%")
    if not s:
        return None
    try:
        return Decimal(s) / 100
    except InvalidOperation:
        return None


def _parse_numeric(value: object) -> Decimal | None:
    """Parse a plain or copy-pasted accounting-style number.

    Tries a bare Decimal first; if that fails (e.g. thousands separators,
    '$', or parenthesized negatives), falls back to _parse_currency.
    """
    result = _parse_decimal(value)
    if result is not None:
        return result
    return _parse_currency(value)


def _parse_snapshot_date(value: str) -> date:
    """Parse the __meta__ snapshot_date cell.

    Excel rewrites this cell to the locale date format (e.g. '5/31/2026') on
    every save, so accept ISO (YYYY-MM-DD) plus the common spreadsheet
    slash/dash formats rather than requiring ISO only.
    """
    s = value.strip()
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {s!r}")


def _str_or_none(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s if s else None




def _clean_rating(value: object) -> str | None:
    """Return the survey rating exactly as given in the CSV ('Excellent!', 'Poor'),
    only trimming surrounding whitespace. The survey has just these two options.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    cleaned = str(value).strip()
    return cleaned or None


# ── Assets ────────────────────────────────────────────────────────────────────

@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def manual_entries_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Load manually entered monthly metrics from manual_entries.csv.

    The CSV is long-format with a `__meta__` row at the top that carries the
    target snapshot_date for this run — edit that single value to retarget the
    asset at a different month. Asset is unpartitioned because the date lives
    in the file, not in a Dagster partition selector.

    CSV layout:
        warehouse_table,warehouse_column,row_key,value,source_location,source_notes
        __meta__,snapshot_date,,2026-05-31,,Set to the EOM this run targets
        fct_financial_ratios_summary,net_worth_pct,,0.1263,...
        fct_nps_summary,nps_score,,72.4,...
        fct_branch_profitability_summary,net_income,Main Branch,123456.78,...
        fct_atm_by_machine_summary,transactions,HQ,1234,...

    Single-row tables (PK = snapshot_date alone) leave `row_key` blank.
    Multi-row tables (`fct_branch_profitability_summary`,
    `fct_atm_by_machine_summary`, `fct_digital_enrollments`) use `row_key` to
    identify which row of that table the value applies to — branch name
    (matched against dim_branch) for profitability, atm_name (used directly)
    for the ATM table, service name (home_banking / bill_pay / rdc) for
    digital enrollments.

    Values are written as decimal (e.g. 0.1263 for 12.63% — view-level ×100
    transformation handles display). Rows with empty `value` are skipped.
    Blank cells preserve existing values via COALESCE on ON CONFLICT DO UPDATE.
    """
    entries_path = Path(
        os.environ.get("MANUAL_ENTRIES_PATH", "/data/manual_entries/manual_entries.csv")
    )
    df = pd.read_csv(entries_path, dtype=str)

    # Pull snapshot_date from the __meta__ row — fail loudly if missing/invalid.
    meta = df[(df["warehouse_table"] == "__meta__") & (df["warehouse_column"] == "snapshot_date")]
    if meta.empty or meta.iloc[0]["value"] is None or not str(meta.iloc[0]["value"]).strip():
        raise Exception(
            "manual_entries.csv is missing a `__meta__,snapshot_date,,YYYY-MM-DD,,` row. "
            "Add one and re-run. This is the single source of truth for the date this load writes."
        )
    try:
        snapshot_date = _parse_snapshot_date(str(meta.iloc[0]["value"]))
    except ValueError as e:
        raise Exception(
            f"manual_entries.csv __meta__,snapshot_date value is not a recognized date "
            f"(expected YYYY-MM-DD or M/D/YYYY): {meta.iloc[0]['value']!r}"
        ) from e
    context.log.info(f"manual_entries.csv snapshot_date = {snapshot_date}")

    # Real data rows: drop the __meta__ row, drop empties.
    df = df[df["warehouse_table"] != "__meta__"]
    df = df[df["value"].notna() & (df["value"].str.strip() != "")]

    # Split rows by whether they carry a row_key (multi-row tables) or not (single-row tables).
    has_row_key = df["row_key"].notna() & (df["row_key"].str.strip() != "")
    single_row_df = df[~has_row_key]
    multi_row_df  = df[has_row_key]

    # Pivot single-row tables into {warehouse_table: {column: value_str}}
    by_table: dict[str, dict[str, str]] = {}
    for _, row in single_row_df.iterrows():
        table = row["warehouse_table"]
        by_table.setdefault(table, {})[row["warehouse_column"]] = row["value"]

    # For multi-row tables, group by (warehouse_table, row_key) -> {column: value_str}.
    # Currently only one column per multi-row table is in use (net_income, transactions),
    # but this shape supports future columns without changing the asset.
    by_table_row: dict[tuple[str, str], dict[str, str]] = {}
    for _, row in multi_row_df.iterrows():
        key = (row["warehouse_table"], str(row["row_key"]).strip())
        by_table_row.setdefault(key, {})[row["warehouse_column"]] = row["value"]

    engine = warehouse_pg.get_engine()
    tables_written = 0

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="manual",
    ) as stats:
        with engine.begin() as pg_conn:

            if "fct_financial_ratios_summary" in by_table:
                cols = by_table["fct_financial_ratios_summary"]
                # COALESCE keeps existing values when the CSV cell is blank — only
                # filled-in columns get overwritten. Lets the user edit one cell
                # without nuking everything else on the row.
                pg_conn.execute(
                    text("""
                        INSERT INTO fct_financial_ratios_summary AS ratios
                            (snapshot_date, net_worth_pct, roa_pct, op_expense_pct,
                             loan_to_share_pct, loan_delinquency_pct, loan_charge_off_ratio,
                             loan_growth_pct, atm_member_uses, atm_foreign_uses,
                             atm_network_ytd_net_income)
                        VALUES
                            (:snapshot_date, :net_worth_pct, :roa_pct, :op_expense_pct,
                             :loan_to_share_pct, :loan_delinquency_pct, :loan_charge_off_ratio,
                             :loan_growth_pct, :atm_member_uses, :atm_foreign_uses,
                             :atm_network_ytd_net_income)
                        ON CONFLICT (snapshot_date) DO UPDATE SET
                            net_worth_pct              = COALESCE(EXCLUDED.net_worth_pct,              ratios.net_worth_pct),
                            roa_pct                    = COALESCE(EXCLUDED.roa_pct,                    ratios.roa_pct),
                            op_expense_pct             = COALESCE(EXCLUDED.op_expense_pct,             ratios.op_expense_pct),
                            loan_to_share_pct          = COALESCE(EXCLUDED.loan_to_share_pct,          ratios.loan_to_share_pct),
                            loan_delinquency_pct       = COALESCE(EXCLUDED.loan_delinquency_pct,       ratios.loan_delinquency_pct),
                            loan_charge_off_ratio      = COALESCE(EXCLUDED.loan_charge_off_ratio,      ratios.loan_charge_off_ratio),
                            loan_growth_pct            = COALESCE(EXCLUDED.loan_growth_pct,            ratios.loan_growth_pct),
                            atm_member_uses            = COALESCE(EXCLUDED.atm_member_uses,            ratios.atm_member_uses),
                            atm_foreign_uses           = COALESCE(EXCLUDED.atm_foreign_uses,           ratios.atm_foreign_uses),
                            atm_network_ytd_net_income = COALESCE(EXCLUDED.atm_network_ytd_net_income, ratios.atm_network_ytd_net_income)
                    """),
                    {
                        "snapshot_date":              snapshot_date,
                        "net_worth_pct":              _parse_pct(cols.get("net_worth_pct")),
                        "roa_pct":                    _parse_pct(cols.get("roa_pct")),
                        "op_expense_pct":             _parse_pct(cols.get("op_expense_pct")),
                        "loan_to_share_pct":          _parse_pct(cols.get("loan_to_share_pct")),
                        "loan_delinquency_pct":       _parse_pct(cols.get("loan_delinquency_pct")),
                        "loan_charge_off_ratio":      _parse_pct(cols.get("loan_charge_off_ratio")),
                        "loan_growth_pct":            _parse_pct(cols.get("loan_growth_pct")),
                        "atm_member_uses":            _parse_numeric(cols.get("atm_member_uses")),
                        "atm_foreign_uses":           _parse_numeric(cols.get("atm_foreign_uses")),
                        "atm_network_ytd_net_income": _parse_numeric(cols.get("atm_network_ytd_net_income")),
                    },
                )
                tables_written += 1

            if "fct_nps_summary" in by_table:
                cols = by_table["fct_nps_summary"]
                pg_conn.execute(
                    text("""
                        INSERT INTO fct_nps_summary AS nps (snapshot_date, nps_score)
                        VALUES (:snapshot_date, :nps_score)
                        ON CONFLICT (snapshot_date) DO UPDATE SET
                            nps_score = COALESCE(EXCLUDED.nps_score, nps.nps_score)
                    """),
                    {
                        "snapshot_date": snapshot_date,
                        "nps_score":     _parse_numeric(cols.get("nps_score")),
                    },
                )
                tables_written += 1

            if "fct_loan_portfolio_summary" in by_table:
                cols = by_table["fct_loan_portfolio_summary"]
                pg_conn.execute(
                    text("""
                        INSERT INTO fct_loan_portfolio_summary AS portfolio
                            (snapshot_date, loan_total_balance, loan_income_monthly,
                             loan_growth_ytd_pct, share_total_balance, share_income_monthly,
                             share_growth_ytd_pct, cc_total_balance, cc_income_monthly,
                             cc_growth_ytd_pct, loan_charge_offs,
                             loan_charge_off_recoveries, cp_charge_offs, cp_charge_off_recoveries,
                             atm_fraud_mtd, check_loss_fraud_mtd, cc_fraud_mtd, debit_fraud_mtd)
                        VALUES
                            (:snapshot_date, :loan_total_balance, :loan_income_monthly,
                             :loan_growth_ytd_pct, :share_total_balance, :share_income_monthly,
                             :share_growth_ytd_pct, :cc_total_balance, :cc_income_monthly,
                             :cc_growth_ytd_pct, :loan_charge_offs,
                             :loan_charge_off_recoveries, :cp_charge_offs, :cp_charge_off_recoveries,
                             :atm_fraud_mtd, :check_loss_fraud_mtd, :cc_fraud_mtd, :debit_fraud_mtd)
                        ON CONFLICT (snapshot_date) DO UPDATE SET
                            loan_total_balance          = COALESCE(EXCLUDED.loan_total_balance,         portfolio.loan_total_balance),
                            loan_income_monthly         = COALESCE(EXCLUDED.loan_income_monthly,        portfolio.loan_income_monthly),
                            loan_growth_ytd_pct         = COALESCE(EXCLUDED.loan_growth_ytd_pct,        portfolio.loan_growth_ytd_pct),
                            share_total_balance         = COALESCE(EXCLUDED.share_total_balance,        portfolio.share_total_balance),
                            share_income_monthly        = COALESCE(EXCLUDED.share_income_monthly,       portfolio.share_income_monthly),
                            share_growth_ytd_pct        = COALESCE(EXCLUDED.share_growth_ytd_pct,       portfolio.share_growth_ytd_pct),
                            cc_total_balance            = COALESCE(EXCLUDED.cc_total_balance,           portfolio.cc_total_balance),
                            cc_income_monthly           = COALESCE(EXCLUDED.cc_income_monthly,          portfolio.cc_income_monthly),
                            cc_growth_ytd_pct           = COALESCE(EXCLUDED.cc_growth_ytd_pct,          portfolio.cc_growth_ytd_pct),
                            loan_charge_offs            = COALESCE(EXCLUDED.loan_charge_offs,           portfolio.loan_charge_offs),
                            loan_charge_off_recoveries  = COALESCE(EXCLUDED.loan_charge_off_recoveries, portfolio.loan_charge_off_recoveries),
                            cp_charge_offs              = COALESCE(EXCLUDED.cp_charge_offs,             portfolio.cp_charge_offs),
                            cp_charge_off_recoveries    = COALESCE(EXCLUDED.cp_charge_off_recoveries,   portfolio.cp_charge_off_recoveries),
                            atm_fraud_mtd               = COALESCE(EXCLUDED.atm_fraud_mtd,              portfolio.atm_fraud_mtd),
                            check_loss_fraud_mtd        = COALESCE(EXCLUDED.check_loss_fraud_mtd,       portfolio.check_loss_fraud_mtd),
                            cc_fraud_mtd                = COALESCE(EXCLUDED.cc_fraud_mtd,               portfolio.cc_fraud_mtd),
                            debit_fraud_mtd             = COALESCE(EXCLUDED.debit_fraud_mtd,            portfolio.debit_fraud_mtd)
                    """),
                    {
                        "snapshot_date":              snapshot_date,
                        "loan_total_balance":         _parse_numeric(cols.get("loan_total_balance")),
                        "loan_income_monthly":        _parse_numeric(cols.get("loan_income_monthly")),
                        "loan_growth_ytd_pct":        _parse_pct(cols.get("loan_growth_ytd_pct")),
                        "share_total_balance":        _parse_numeric(cols.get("share_total_balance")),
                        "share_income_monthly":       _parse_numeric(cols.get("share_income_monthly")),
                        "share_growth_ytd_pct":       _parse_numeric(cols.get("share_growth_ytd_pct")),
                        "cc_total_balance":           _parse_numeric(cols.get("cc_total_balance")),
                        "cc_income_monthly":          _parse_numeric(cols.get("cc_income_monthly")),
                        "cc_growth_ytd_pct":          _parse_numeric(cols.get("cc_growth_ytd_pct")),
                        "loan_charge_offs":           _parse_numeric(cols.get("loan_charge_offs")),
                        "loan_charge_off_recoveries": _parse_numeric(cols.get("loan_charge_off_recoveries")),
                        "cp_charge_offs":             _parse_numeric(cols.get("cp_charge_offs")),
                        "cp_charge_off_recoveries":   _parse_numeric(cols.get("cp_charge_off_recoveries")),
                        "atm_fraud_mtd":              _parse_numeric(cols.get("atm_fraud_mtd")),
                        "check_loss_fraud_mtd":       _parse_numeric(cols.get("check_loss_fraud_mtd")),
                        "cc_fraud_mtd":               _parse_numeric(cols.get("cc_fraud_mtd")),
                        "debit_fraud_mtd":            _parse_numeric(cols.get("debit_fraud_mtd")),
                    },
                )
                tables_written += 1

            # Both columns of fct_digital_channels_transactions are owned by manual
            # entries. The home banking total used to live here too — it now
            # routes to fct_digital_enrollments as a service row (Decision 47).
            if "fct_digital_channels_transactions" in by_table:
                cols = by_table["fct_digital_channels_transactions"]
                pg_conn.execute(
                    text("""
                        INSERT INTO fct_digital_channels_transactions AS channels
                            (snapshot_date, bill_pay_transactions, mobile_online_transactions)
                        VALUES
                            (:snapshot_date, :bill_pay_transactions, :mobile_online_transactions)
                        ON CONFLICT (snapshot_date) DO UPDATE SET
                            bill_pay_transactions      = COALESCE(EXCLUDED.bill_pay_transactions,      channels.bill_pay_transactions),
                            mobile_online_transactions = COALESCE(EXCLUDED.mobile_online_transactions, channels.mobile_online_transactions)
                    """),
                    {
                        "snapshot_date":             snapshot_date,
                        "bill_pay_transactions":     _parse_numeric(cols.get("bill_pay_transactions")),
                        "mobile_online_transactions": _parse_numeric(cols.get("mobile_online_transactions")),
                    },
                )
                tables_written += 1

            # ── Multi-row tables ─────────────────────────────────────────────
            # fct_branch_profitability_summary — one row per branch per month.
            # row_key holds the branch_name; we resolve it to branch_key via dim_branch.
            branch_entries = {
                row_key: cols for (table, row_key), cols in by_table_row.items()
                if table == "fct_branch_profitability_summary"
            }
            if branch_entries:
                branch_lookup_rows = pg_conn.execute(
                    text("SELECT branch_name, branch_key FROM dim_branch WHERE source_system IS NOT NULL")
                ).all()
                branch_key_by_name = {r.branch_name: r.branch_key for r in branch_lookup_rows}

                for branch_name, cols in branch_entries.items():
                    branch_key = branch_key_by_name.get(branch_name)
                    if branch_key is None:
                        raise Exception(
                            f"Unknown branch '{branch_name}' in manual_entries.csv "
                            f"fct_branch_profitability_summary row. Valid branch names: "
                            f"{sorted(branch_key_by_name.keys())}"
                        )
                    pg_conn.execute(
                        text("""
                            INSERT INTO fct_branch_profitability_summary AS profit
                                (snapshot_date, branch_key, net_income)
                            VALUES
                                (:snapshot_date, :branch_key, :net_income)
                            ON CONFLICT (snapshot_date, branch_key) DO UPDATE SET
                                net_income = COALESCE(EXCLUDED.net_income, profit.net_income)
                        """),
                        {
                            "snapshot_date": snapshot_date,
                            "branch_key":    branch_key,
                            "net_income":    _parse_numeric(cols.get("net_income")),
                        },
                    )
                    tables_written += 1

            # fct_atm_by_machine_summary — one row per ATM per month.
            # row_key holds the atm_name directly (it's the PK component).
            atm_entries = {
                row_key: cols for (table, row_key), cols in by_table_row.items()
                if table == "fct_atm_by_machine_summary"
            }
            for atm_name, cols in atm_entries.items():
                pg_conn.execute(
                    text("""
                        INSERT INTO fct_atm_by_machine_summary AS atm
                            (snapshot_date, atm_name, transactions)
                        VALUES
                            (:snapshot_date, :atm_name, :transactions)
                        ON CONFLICT (snapshot_date, atm_name) DO UPDATE SET
                            transactions = COALESCE(EXCLUDED.transactions, atm.transactions)
                    """),
                    {
                        "snapshot_date": snapshot_date,
                        "atm_name":      atm_name,
                        "transactions":  _parse_numeric(cols.get("transactions")),
                    },
                )
                tables_written += 1

            # fct_digital_enrollments — one row per digital service per month.
            # row_key holds the service name (PK component). The home_banking
            # total is hand-entered here (Decision 47); bill_pay and rdc rows
            # normally come from the CSV drop asset but can be corrected here.
            VALID_SERVICES = {"home_banking", "bill_pay", "rdc"}
            enrollment_entries = {
                row_key: cols for (table, row_key), cols in by_table_row.items()
                if table == "fct_digital_enrollments"
            }
            for service, cols in enrollment_entries.items():
                if service not in VALID_SERVICES:
                    raise Exception(
                        f"Unknown service '{service}' in manual_entries.csv "
                        f"fct_digital_enrollments row. Valid services: "
                        f"{sorted(VALID_SERVICES)}"
                    )
                pg_conn.execute(
                    text("""
                        INSERT INTO fct_digital_enrollments AS enrollments
                            (snapshot_date, service, enrolled_count)
                        VALUES
                            (:snapshot_date, :service, :enrolled_count)
                        ON CONFLICT (snapshot_date, service) DO UPDATE SET
                            enrolled_count = COALESCE(EXCLUDED.enrolled_count, enrollments.enrolled_count)
                    """),
                    {
                        "snapshot_date":  snapshot_date,
                        "service":        service,
                        "enrolled_count": _parse_numeric(cols.get("enrolled_count")),
                    },
                )
                tables_written += 1

        stats["row_count"] = tables_written

    return {
        "status": "ok",
        "snapshot_date": str(snapshot_date),
        "tables_updated": tables_written,
    }


@asset(
    group_name="file_loaders",
    deps=[warehouse_schema_ready],
    partitions_def=MonthlyPartitionsDefinition(start_date="2025-01-01", timezone="America/Chicago"),
)
def fct_digital_enrollments_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Count active enrolled users per digital service from monthly CSV exports.

    Expected subfolder layout under CSV_DROP_PATH/fct_digital_enrollments/:
      bill_pay/        — bill pay platform export (4 header/metadata rows to skip)
      rdc/             — remote deposit capture export (clean header, no skip)

    Online banking moved to manual_entries.csv (Decision 44) since the platform
    only exposes the enrolled total, not a row-level export — easier to type the
    number than parse a CSV that doesn't exist. That total lands in this same
    table as a home_banking service row via manual_entries_table (Decision 47);
    the DELETE below is narrowed to bill_pay/rdc so this asset never touches it.
    """
    snapshot_date = last_day_of_month(date.fromisoformat(context.partition_key))
    base = _csv_drop_path("fct_digital_enrollments")

    bp_file = _find_csv(base / "bill_pay")
    df_bp = pd.read_csv(bp_file, skiprows=4)
    bp_enrolled = len(df_bp)
    bp_active = int((df_bp["Active"] == "Yes").sum())

    rdc_file = _find_csv(base / "rdc")
    df_rdc = pd.read_csv(rdc_file)
    rdc_enrolled = len(df_rdc)
    rdc_active = int(df_rdc["Enabled"].isin([True, "True"]).sum())

    enrollment_rows = [
        {"service": "bill_pay", "enrolled_count": bp_enrolled, "active_count": bp_active},
        {"service": "rdc",      "enrolled_count": rdc_enrolled, "active_count": rdc_active},
    ]

    engine = warehouse_pg.get_engine()

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="csv_drop",
    ) as stats:
        with engine.begin() as pg_conn:
            # Only delete the two services this asset owns — leaves any historical
            # home_banking rows untouched (loaded by the old asset before the move).
            pg_conn.execute(
                text(
                    "DELETE FROM fct_digital_enrollments "
                    "WHERE snapshot_date = :snapshot_date "
                    "  AND service IN ('bill_pay', 'rdc')"
                ),
                {"snapshot_date": snapshot_date},
            )
            pg_conn.execute(
                text("""
                    INSERT INTO fct_digital_enrollments
                        (snapshot_date, service, enrolled_count, active_count)
                    VALUES
                        (:snapshot_date, :service, :enrolled_count, :active_count)
                """),
                [{"snapshot_date": snapshot_date, **row} for row in enrollment_rows],
            )

        stats["row_count"] = len(enrollment_rows)

    return {
        "status": "ok",
        "snapshot_date": str(snapshot_date),
        "bill_pay_active": bp_active,
        "rdc_active": rdc_active,
    }


@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def fct_survey_responses_table(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Upsert member survey responses from all CSVs in the survey drop folder.

    Accumulates across multiple files — re-running is safe (upsert on engagement_id).
    Rating is stored as its text label (e.g. 'Excellent', 'Poor'), cleaned of
    trailing punctuation/whitespace.
    """
    survey_folder = _csv_drop_path("fct_survey_responses")
    csv_files = sorted(survey_folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {survey_folder}. Drop a file there and re-run."
        )

    frames = [pd.read_csv(f, dtype=str, encoding="utf-8-sig") for f in csv_files]
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Engagement ID"])

    engine = warehouse_pg.get_engine()
    upserted = 0

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="csv_drop",
    ) as stats:
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                engagement_id = _str_or_none(row.get("Engagement ID"))
                if not engagement_id:
                    continue

                date_raw = row.get("Date")
                try:
                    response_date = (
                        pd.to_datetime(date_raw).date()
                        if pd.notna(date_raw)
                        else None
                    )
                except Exception:
                    response_date = None

                result = pg_conn.execute(
                    text("""
                        INSERT INTO fct_survey_responses AS survey
                            (engagement_id, response_date, queue_flow, channel,
                             survey_format, rating, additional_feedback,
                             member_name, email, phone)
                        VALUES
                            (:engagement_id, :response_date, :queue_flow, :channel,
                             :survey_format, :rating, :additional_feedback,
                             :member_name, :email, :phone)
                        ON CONFLICT (engagement_id) DO UPDATE SET
                            response_date       = EXCLUDED.response_date,
                            queue_flow          = EXCLUDED.queue_flow,
                            channel             = EXCLUDED.channel,
                            survey_format       = EXCLUDED.survey_format,
                            rating              = EXCLUDED.rating,
                            additional_feedback = EXCLUDED.additional_feedback,
                            member_name         = EXCLUDED.member_name,
                            email               = EXCLUDED.email,
                            phone               = EXCLUDED.phone
                        WHERE (
                            survey.response_date       IS DISTINCT FROM EXCLUDED.response_date       OR
                            survey.queue_flow          IS DISTINCT FROM EXCLUDED.queue_flow          OR
                            survey.channel             IS DISTINCT FROM EXCLUDED.channel             OR
                            survey.survey_format       IS DISTINCT FROM EXCLUDED.survey_format       OR
                            survey.rating              IS DISTINCT FROM EXCLUDED.rating              OR
                            survey.additional_feedback IS DISTINCT FROM EXCLUDED.additional_feedback OR
                            survey.member_name         IS DISTINCT FROM EXCLUDED.member_name         OR
                            survey.email               IS DISTINCT FROM EXCLUDED.email               OR
                            survey.phone               IS DISTINCT FROM EXCLUDED.phone
                        )
                    """),
                    {
                        "engagement_id":       engagement_id,
                        "response_date":       response_date,
                        "queue_flow":          _str_or_none(row.get("Queue/Flow")),
                        "channel":             _str_or_none(row.get("Channel")),
                        "survey_format":       _str_or_none(row.get("Survey Format")),
                        "rating":              _clean_rating(row.get("Rating")),
                        "additional_feedback": _str_or_none(row.get("Additional Feedback")),
                        "member_name":         _str_or_none(row.get("Name")),
                        "email":               _str_or_none(row.get("Email Address")),
                        "phone":               _str_or_none(row.get("Phone Number")),
                    },
                )
                upserted += result.rowcount

        stats["row_count"] = upserted

    return {"status": "ok", "responses_upserted": upserted}


# Note: fct_atm_by_machine_summary and fct_branch_profitability_summary used to
# have their own drop-folder assets here, but those tables are now populated via
# manual_entries.csv (row_key column carries the atm_name / branch_name). See
# Decisions 42 + 43 in docs/decisions_log.md. The data/csv_drop/<table>/
# folders for them are no longer monitored.
