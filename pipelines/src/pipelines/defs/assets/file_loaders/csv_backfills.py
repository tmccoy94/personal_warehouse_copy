"""One-time historical backfill assets.

Each asset reads every CSV from its subfolder under data/csv_backfill/<table>/,
parses dates, and upserts the full history into the warehouse in one pass.
These are unpartitioned and idempotent — safe to re-run if a file is updated.

Drop the source CSVs into the corresponding folder, then materialize the asset.
CSV_BACKFILL_PATH env var defaults to /data/csv_backfill inside the container.

TO_RESOLVE column names are marked below — inspect the actual CSV header row
and update the mapping before running each asset for the first time.
"""
import os
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import dagster
import pandas as pd
from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.branch import CORE_TO_WAREHOUSE_BRANCH_NAME, dim_branch_table
from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import WarehousePgResource
from pipelines.loading import audit_load
from pipelines.utils import last_day_of_month


# ── Path helpers ──────────────────────────────────────────────────────────────

def _backfill_path(table_name: str) -> Path:
    base = Path(os.environ.get("CSV_BACKFILL_PATH", "/data/csv_backfill"))
    return base / table_name


def _load_all_csvs(folder: Path, **read_kwargs) -> pd.DataFrame:
    """Concatenate every CSV in folder into one DataFrame.
    Uses utf-8-sig encoding by default to strip the UTF-8 BOM (ï»¿) that
    Excel-exported CSVs often include as the first character of the file.
    """
    files = sorted(folder.glob("*.csv")) + sorted(folder.glob("*.CSV"))
    if not files:
        raise FileNotFoundError(
            f"No CSV files found in {folder}. "
            "Drop the source files there and re-run."
        )
    read_kwargs.setdefault("encoding", "utf-8-sig")
    frames = [pd.read_csv(f, **read_kwargs) for f in files]
    df = pd.concat(frames, ignore_index=True)
    return df


def _parse_decimal(value: object) -> Decimal | None:
    """Parse a plain numeric string, returning None for blanks and ' - ' placeholders."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s == "-":
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_currency(value: object) -> Decimal | None:
    """Parse '$93,763.54' or '(297.00)' → Decimal."""
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
    """Parse display percentage '12.51%' or '12.51' → Decimal(0.1251)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().rstrip("%")
    try:
        return Decimal(s) / 100
    except InvalidOperation:
        return None


def _eom(d: object) -> date | None:
    """Parse a date value and return the last day of that month."""
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return None
    try:
        parsed = pd.to_datetime(str(d))
        return last_day_of_month(parsed.date())
    except Exception:
        return None


# ── Assets ────────────────────────────────────────────────────────────────────

@asset(group_name="file_loaders", deps=[dim_branch_table])
def fct_members_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_members_summary from TotalMembersStatsMonthly.csv.

    All rows in this CSV are CU-level totals — they go on the 'CU Total'
    sentinel branch row. Also writes the Home Branch MO/KS CU totals into
    fct_home_branch_members_summary on the 'CU Total' row for completeness.

    CSV columns confirmed:
        Month, Total Members, Kansas Members, Missouri Members,
        Active Members, Members 18-50, Home Branch MO, Home Branch KS,
        Net Additional Total Members, Net Additional Kansas Members,
        Net Additional Missouri Members, Net Additional Active Members,
        Net Additional Members 18-50, Net HB_MO, Net HB_KS,
        Closed Accounts, New Accounts
    """
    folder = _backfill_path("fct_members_summary")
    df = _load_all_csvs(folder)
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    engine = warehouse_pg.get_engine()

    with engine.connect() as conn:
        cu_total = conn.execute(
            text("SELECT branch_key FROM dim_branch WHERE branch_name = 'CU Total'")
        ).fetchone()
    if cu_total is None:
        raise Exception("CU Total sentinel row not found in dim_branch — run warehouse_branch_sentinels first")
    cu_total_key = cu_total.branch_key

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get("Month"))
                if snapshot_date is None:
                    continue

                # fct_members_summary — CU Total row
                result = pg_conn.execute(text("""
                    INSERT INTO fct_members_summary
                        (snapshot_date, branch_key, total_members, active_members,
                         net_new_members, members_18_50, ks_members, mo_members)
                    VALUES
                        (:snapshot_date, :branch_key, :total_members, :active_members,
                         :net_new_members, :members_18_50, :ks_members, :mo_members)
                    ON CONFLICT (snapshot_date, branch_key) DO UPDATE SET
                        total_members   = EXCLUDED.total_members,
                        active_members  = EXCLUDED.active_members,
                        net_new_members = EXCLUDED.net_new_members,
                        members_18_50   = EXCLUDED.members_18_50,
                        ks_members      = EXCLUDED.ks_members,
                        mo_members      = EXCLUDED.mo_members
                """), {
                    "snapshot_date":  snapshot_date,
                    "branch_key":     cu_total_key,
                    "total_members":  row.get("Total Members"),
                    "active_members": row.get("Active Members"),
                    "net_new_members":row.get("Net Additional Total Members"),
                    "members_18_50":  row.get("Members 18-50"),
                    "ks_members":     row.get("Kansas Members"),
                    "mo_members":     row.get("Missouri Members"),
                })
                upserted += result.rowcount

                # fct_home_branch_members_summary — CU Total row
                # Home Branch MO/KS columns are blank before Jan 2025 — skip if null
                hb_mo = row.get("Home Branch MO")
                hb_ks = row.get("Home Branch KS")
                if pd.notna(hb_mo) or pd.notna(hb_ks):
                    hb_total = (int(hb_mo) if pd.notna(hb_mo) else 0) + (int(hb_ks) if pd.notna(hb_ks) else 0)
                    pg_conn.execute(text("""
                        INSERT INTO fct_home_branch_members_summary
                            (snapshot_date, branch_key, total_members, ks_members, mo_members)
                        VALUES
                            (:snapshot_date, :branch_key, :total_members, :ks_members, :mo_members)
                        ON CONFLICT (snapshot_date, branch_key) DO UPDATE SET
                            total_members = EXCLUDED.total_members,
                            ks_members    = EXCLUDED.ks_members,
                            mo_members    = EXCLUDED.mo_members
                    """), {
                        "snapshot_date": snapshot_date,
                        "branch_key":    cu_total_key,
                        "total_members": hb_total,
                        "ks_members":    int(hb_ks) if pd.notna(hb_ks) else None,
                        "mo_members":    int(hb_mo) if pd.notna(hb_mo) else None,
                    })

        stats["row_count"] = upserted

    return {"status": "ok", "rows_upserted": upserted}


@asset(group_name="file_loaders", deps=[dim_branch_table])
def fct_home_branch_members_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_home_branch_members_summary from New Home Branch Members MO-MO.csv.
    Backfill starts Jan 2025.

    CSV columns confirmed:
        Month, Branch, Members Added, Total Members

    Branch name values: "East Branch", "Main Branch", "North Branch",
    "OP Branch", "South Branch" — matched against dim_branch.branch_name.

    Date column may be last-of-month (1/31/2025) or first-of-month (8/1/2025)
    — both are converted to EOM via _eom().
    """
    folder = _backfill_path("fct_home_branch_members_summary")
    df = _load_all_csvs(folder)
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    engine = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        skipped = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get("Month"))
                if snapshot_date is None:
                    continue

                branch_name = str(row.get("Branch", "")).strip()
                # Normalize CORE name → warehouse name (e.g. Overland Park Branch → OP Branch)
                # so CSVs using the CORE label don't silently skip.
                branch_name = CORE_TO_WAREHOUSE_BRANCH_NAME.get(branch_name, branch_name)
                branch_result = pg_conn.execute(
                    text("SELECT branch_key FROM dim_branch WHERE branch_name = :n"),
                    {"n": branch_name},
                ).fetchone()
                if branch_result is None:
                    context.log.warning(f"No dim_branch row for '{branch_name}' — skipping")
                    skipped += 1
                    continue

                result = pg_conn.execute(text("""
                    INSERT INTO fct_home_branch_members_summary
                        (snapshot_date, branch_key, total_members, members_added)
                    VALUES
                        (:snapshot_date, :branch_key, :total_members, :members_added)
                    ON CONFLICT (snapshot_date, branch_key) DO UPDATE SET
                        total_members = EXCLUDED.total_members,
                        members_added = EXCLUDED.members_added
                """), {
                    "snapshot_date": snapshot_date,
                    "branch_key":    branch_result.branch_key,
                    "total_members": int(row["Total Members"]) if pd.notna(row.get("Total Members")) else None,
                    "members_added": int(row["Members Added"]) if pd.notna(row.get("Members Added")) else None,
                })
                upserted += result.rowcount

        stats["row_count"] = upserted

    context.log.info(f"Upserted {upserted} rows, skipped {skipped} unknown branches")
    return {"status": "ok", "rows_upserted": upserted, "skipped": skipped}


@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def fct_financial_ratios_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_financial_ratios_summary.

    Each CSV is read individually and upserted by filename mapping.
    This avoids column-name conflicts when DataFrames from different files
    are merged. Each file contributes its one or two columns; COALESCE
    preserves existing values when a file doesn't cover a column.

    Files: Net-Worth.csv, ROA.csv, Operating_Expense.csv, Loan_to_Share.csv,
           Loan_Delinquency.csv, Loan_Charge_Off_Ratio.csv, Loan_Growth.csv,
           Prior_ATM_Usage.csv, ATM_Network_Profitability.csv
    """
    folder = _backfill_path("fct_financial_ratios_summary")

    # Map each filename stem to a function that extracts {warehouse_col: value}
    # from a single CSV row. Value column is always index 1 (or 1+2 for ATM usage).
    def _upsert_col(pg_conn, snapshot_date, col_name, value):
        pg_conn.execute(text(f"""
            INSERT INTO fct_financial_ratios_summary (snapshot_date, {col_name})
            VALUES (:snapshot_date, :value)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                {col_name} = COALESCE(EXCLUDED.{col_name}, fct_financial_ratios_summary.{col_name})
        """), {"snapshot_date": snapshot_date, "value": value})

    engine = warehouse_pg.get_engine()
    total = 0

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        with engine.begin() as pg_conn:
            for csv_file in sorted(folder.glob("*.csv")) + sorted(folder.glob("*.CSV")):
                stem = csv_file.stem  # e.g. "Net-Worth"
                df = pd.read_csv(csv_file, encoding="utf-8-sig")
                context.log.info(f"{stem}: {len(df)} rows, columns={list(df.columns)}")

                for _, row in df.iterrows():
                    snapshot_date = _eom(row.iloc[0])
                    if snapshot_date is None:
                        continue

                    val = row.iloc[1] if pd.notna(row.iloc[1]) else None

                    if stem == "Net-Worth":
                        _upsert_col(pg_conn, snapshot_date, "net_worth_pct", _parse_pct(val))
                    elif stem == "ROA":
                        _upsert_col(pg_conn, snapshot_date, "roa_pct", _parse_pct(val))
                    elif stem == "Operating_Expense":
                        _upsert_col(pg_conn, snapshot_date, "op_expense_pct", _parse_pct(val))
                    elif stem == "Loan_to_Share":
                        _upsert_col(pg_conn, snapshot_date, "loan_to_share_pct", _parse_pct(val))
                    elif stem == "Loan_Delinquency":
                        _upsert_col(pg_conn, snapshot_date, "loan_delinquency_pct", _parse_pct(val))
                    elif stem == "Loan_Charge_Off_Ratio":
                        _upsert_col(pg_conn, snapshot_date, "loan_charge_off_ratio", _parse_pct(val))
                    elif stem == "Loan_Growth":
                        _upsert_col(pg_conn, snapshot_date, "loan_growth_pct", _parse_pct(val))
                    elif stem == "ATM_Network_Profitability":
                        _upsert_col(pg_conn, snapshot_date, "atm_network_ytd_net_income", _parse_currency(val))
                    elif stem == "Prior_ATM_Usage":
                        # Two value columns: index 1 = Foreign, index 2 = Members
                        foreign = int(row.iloc[1]) if pd.notna(row.iloc[1]) else None
                        members = int(row.iloc[2]) if pd.notna(row.iloc[2]) else None
                        pg_conn.execute(text("""
                            INSERT INTO fct_financial_ratios_summary
                                (snapshot_date, atm_foreign_uses, atm_member_uses)
                            VALUES (:snapshot_date, :foreign, :members)
                            ON CONFLICT (snapshot_date) DO UPDATE SET
                                atm_foreign_uses = COALESCE(EXCLUDED.atm_foreign_uses, fct_financial_ratios_summary.atm_foreign_uses),
                                atm_member_uses  = COALESCE(EXCLUDED.atm_member_uses,  fct_financial_ratios_summary.atm_member_uses)
                        """), {"snapshot_date": snapshot_date, "foreign": foreign, "members": members})
                    else:
                        context.log.warning(f"Unrecognised file '{stem}' — skipping")
                        continue
                    total += 1

        stats["row_count"] = total

    return {"status": "ok", "rows_processed": total}


@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def fct_nps_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_nps_summary from NPS Score.csv.

    Drop NPS Score.csv into data/csv_backfill/fct_nps_summary/.
    CSV is sparse — only a few data points per year (survey-driven, not monthly).
    Confirmed CSV header columns: Month, Score.
    """
    folder = _backfill_path("fct_nps_summary")
    df = _load_all_csvs(folder)
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    DATE_COL  = "Month"
    SCORE_COL = "Score"

    engine = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get(DATE_COL))
                if snapshot_date is None:
                    continue
                score_val = row.get(SCORE_COL)
                if pd.isna(score_val):
                    continue
                result = pg_conn.execute(text("""
                    INSERT INTO fct_nps_summary (snapshot_date, nps_score)
                    VALUES (:snapshot_date, :nps_score)
                    ON CONFLICT (snapshot_date) DO UPDATE SET
                        nps_score = EXCLUDED.nps_score
                """), {
                    "snapshot_date": snapshot_date,
                    "nps_score":     _parse_currency(score_val),
                })
                upserted += result.rowcount
        stats["row_count"] = upserted

    return {"status": "ok", "rows_upserted": upserted}


@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def fct_loan_portfolio_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_loan_portfolio_summary.
    Drop all four CSVs into data/csv_backfill/fct_loan_portfolio_summary/:
      CFO_Dashboard.csv, LoanCreditCardTotals.csv,
      MarketingDBData.csv, _Fraud_Charge_Off_Long.csv

    All four are concatenated. Each row contributes only the columns it has;
    NaN columns leave existing warehouse values intact via COALESCE ON CONFLICT.

    Confirmed CSV column names:
      CFO_Dashboard:       Month, Loan, Loan Difference, Loan YTD Difference,
                           Share, Share Difference, Share YTD Difference
                           ("Loan Difference" / "Share Difference" are monthly
                           income, not period-over-period balance differences,
                           and load into loan_income_monthly / share_income_monthly)
      LoanCreditCardTotals:Month, Credit Card Total Balance, Loans Total Balance,
                           Net Credit Card Balance Diff, Net Loans Total Balance Diff
                           ("Net Credit Card Balance Diff" is monthly income,
                           loads into cc_income_monthly)
      MarketingDBData:     Month, CreditCards
                           ("Loans" is no longer consumed -- duplicate of
                           CFO_Dashboard's "Loan YTD Difference", which is the
                           more complete source for loan_growth_ytd_pct)
      _Fraud_Charge_Off_Long: Month, Loan Charge Offs, Loan Charge Off Recoveries,
                           Courtesy Pay Charge Offs, Courtsey Pay Charge Off Recoveries,
                           ATM Fraud MTD, Check Loss Fraud MTD, Credit Card Fraud MTD,
                           Debit Card Fruad MTD  (note typos in CSV headers)

    Fraud values are plain decimals (no $). Currency values use ($x.xx) for negatives.
    """
    folder = _backfill_path("fct_loan_portfolio_summary")
    df = _load_all_csvs(folder)
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    engine = warehouse_pg.get_engine()

    # All four CSVs share "Month" as the date column.
    DATE_COL        = "Month"
    # CFO_Dashboard.csv
    LOAN_BAL_COL    = "Loan"
    LOAN_INCOME_COL = "Loan Difference"
    LOAN_YTD_COL    = "Loan YTD Difference"
    SHARE_BAL_COL   = "Share"
    SHARE_INCOME_COL = "Share Difference"
    SHARE_YTD_COL   = "Share YTD Difference"
    # LoanCreditCardTotals.csv (also has Loans Total Balance / Net Loans — COALESCE handles overlap)
    CC_BAL_COL      = "Credit Card Total Balance"
    CC_INCOME_COL   = "Net Credit Card Balance Diff"
    # MarketingDBData.csv
    CC_GROWTH_COL   = "CreditCards"
    # _Fraud_Charge_Off_Long.csv (plain decimal values, note CSV typos in headers)
    LOAN_CO_COL     = "Loan Charge Offs"
    LOAN_CO_REC_COL = "Loan Charge Off Recoveries"
    CP_CO_COL       = "Courtesy Pay Charge Offs"
    CP_CO_REC_COL   = "Courtsey Pay Charge Off Recoveries"   # typo in CSV: "Courtsey"
    ATM_FRAUD_COL   = "ATM Fraud MTD"
    CHECK_FRAUD_COL = "Check Loss Fraud MTD"
    CC_FRAUD_COL    = "Credit Card Fraud MTD"
    DEBIT_FRAUD_COL = "Debit Card Fruad MTD"                 # typo in CSV: "Fruad"

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get(DATE_COL))
                if snapshot_date is None:
                    continue
                result = pg_conn.execute(text("""
                    INSERT INTO fct_loan_portfolio_summary
                        (snapshot_date, loan_total_balance, loan_income_monthly, loan_growth_ytd_pct,
                         share_total_balance, share_income_monthly, share_growth_ytd_pct,
                         cc_total_balance, cc_income_monthly, cc_growth_ytd_pct,
                         loan_charge_offs, loan_charge_off_recoveries, cp_charge_offs,
                         cp_charge_off_recoveries, atm_fraud_mtd, check_loss_fraud_mtd,
                         cc_fraud_mtd, debit_fraud_mtd)
                    VALUES
                        (:snapshot_date, :loan_total_balance, :loan_income_monthly, :loan_growth_ytd_pct,
                         :share_total_balance, :share_income_monthly, :share_growth_ytd_pct,
                         :cc_total_balance, :cc_income_monthly, :cc_growth_ytd_pct,
                         :loan_charge_offs, :loan_charge_off_recoveries, :cp_charge_offs,
                         :cp_charge_off_recoveries, :atm_fraud_mtd, :check_loss_fraud_mtd,
                         :cc_fraud_mtd, :debit_fraud_mtd)
                    ON CONFLICT (snapshot_date) DO UPDATE SET
                        loan_total_balance          = COALESCE(EXCLUDED.loan_total_balance,         fct_loan_portfolio_summary.loan_total_balance),
                        loan_income_monthly         = COALESCE(EXCLUDED.loan_income_monthly,        fct_loan_portfolio_summary.loan_income_monthly),
                        loan_growth_ytd_pct         = COALESCE(EXCLUDED.loan_growth_ytd_pct,        fct_loan_portfolio_summary.loan_growth_ytd_pct),
                        share_total_balance         = COALESCE(EXCLUDED.share_total_balance,        fct_loan_portfolio_summary.share_total_balance),
                        share_income_monthly        = COALESCE(EXCLUDED.share_income_monthly,       fct_loan_portfolio_summary.share_income_monthly),
                        share_growth_ytd_pct        = COALESCE(EXCLUDED.share_growth_ytd_pct,       fct_loan_portfolio_summary.share_growth_ytd_pct),
                        cc_total_balance            = COALESCE(EXCLUDED.cc_total_balance,           fct_loan_portfolio_summary.cc_total_balance),
                        cc_income_monthly           = COALESCE(EXCLUDED.cc_income_monthly,          fct_loan_portfolio_summary.cc_income_monthly),
                        cc_growth_ytd_pct           = COALESCE(EXCLUDED.cc_growth_ytd_pct,          fct_loan_portfolio_summary.cc_growth_ytd_pct),
                        loan_charge_offs            = COALESCE(EXCLUDED.loan_charge_offs,           fct_loan_portfolio_summary.loan_charge_offs),
                        loan_charge_off_recoveries  = COALESCE(EXCLUDED.loan_charge_off_recoveries, fct_loan_portfolio_summary.loan_charge_off_recoveries),
                        cp_charge_offs              = COALESCE(EXCLUDED.cp_charge_offs,             fct_loan_portfolio_summary.cp_charge_offs),
                        cp_charge_off_recoveries    = COALESCE(EXCLUDED.cp_charge_off_recoveries,   fct_loan_portfolio_summary.cp_charge_off_recoveries),
                        atm_fraud_mtd               = COALESCE(EXCLUDED.atm_fraud_mtd,              fct_loan_portfolio_summary.atm_fraud_mtd),
                        check_loss_fraud_mtd        = COALESCE(EXCLUDED.check_loss_fraud_mtd,       fct_loan_portfolio_summary.check_loss_fraud_mtd),
                        cc_fraud_mtd                = COALESCE(EXCLUDED.cc_fraud_mtd,               fct_loan_portfolio_summary.cc_fraud_mtd),
                        debit_fraud_mtd             = COALESCE(EXCLUDED.debit_fraud_mtd,            fct_loan_portfolio_summary.debit_fraud_mtd)
                """), {
                    "snapshot_date":            snapshot_date,
                    "loan_total_balance":        _parse_currency(row.get(LOAN_BAL_COL)),
                    "loan_income_monthly":       _parse_currency(row.get(LOAN_INCOME_COL)),
                    "loan_growth_ytd_pct":       _parse_pct(row.get(LOAN_YTD_COL)),
                    "share_total_balance":       _parse_currency(row.get(SHARE_BAL_COL)),
                    "share_income_monthly":      _parse_currency(row.get(SHARE_INCOME_COL)),
                    "share_growth_ytd_pct":      _parse_pct(row.get(SHARE_YTD_COL)),
                    "cc_total_balance":          _parse_currency(row.get(CC_BAL_COL)),
                    "cc_income_monthly":         _parse_currency(row.get(CC_INCOME_COL)),
                    "cc_growth_ytd_pct":         _parse_pct(row.get(CC_GROWTH_COL)),
                    "loan_charge_offs":          _parse_decimal(row.get(LOAN_CO_COL)),
                    "loan_charge_off_recoveries":_parse_decimal(row.get(LOAN_CO_REC_COL)),
                    "cp_charge_offs":            _parse_decimal(row.get(CP_CO_COL)),
                    "cp_charge_off_recoveries":  _parse_decimal(row.get(CP_CO_REC_COL)),
                    "atm_fraud_mtd":             _parse_decimal(row.get(ATM_FRAUD_COL)),
                    "check_loss_fraud_mtd":      _parse_decimal(row.get(CHECK_FRAUD_COL)),
                    "cc_fraud_mtd":              _parse_decimal(row.get(CC_FRAUD_COL)),
                    "debit_fraud_mtd":           _parse_decimal(row.get(DEBIT_FRAUD_COL)),
                })
                upserted += result.rowcount
        stats["row_count"] = upserted

    return {"status": "ok", "rows_upserted": upserted}


@asset(group_name="file_loaders", deps=[dim_branch_table])
def fct_branch_profitability_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_branch_profitability_summary from Branch_Profitability.csv.

    CSV columns confirmed: Month, Branch, Net Income
    Branch names: Main, North, East, South, Joco — mapped to dim_branch abbreviations.
    Net Income values are currency strings ($93,763.54 / ($297.00)).
    NOTE: 5/2021 Main shows $10,087,231 which looks like a data entry error — verify before loading.
    """
    folder = _backfill_path("fct_branch_profitability_summary")
    df = _load_all_csvs(folder)
    df.columns = df.columns.str.strip()
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    BRANCH_MAP = {
        "Main":  "MAIN",
        "North": "NORTH",
        "South": "SOUTH",
        "East":  "EAST",
        "Joco":  "OP",
    }

    engine = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        skipped = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get("Month"))
                if snapshot_date is None:
                    continue
                branch_raw = str(row.get("Branch", "")).strip()
                abbreviation = BRANCH_MAP.get(branch_raw)
                if abbreviation is None:
                    context.log.warning(f"Unknown branch '{branch_raw}' — skipping")
                    skipped += 1
                    continue
                branch_result = pg_conn.execute(
                    text("SELECT branch_key FROM dim_branch WHERE abbreviation = :a"),
                    {"a": abbreviation},
                ).fetchone()
                if branch_result is None:
                    context.log.warning(f"No dim_branch row for '{abbreviation}' — skipping")
                    skipped += 1
                    continue
                result = pg_conn.execute(text("""
                    INSERT INTO fct_branch_profitability_summary (snapshot_date, branch_key, net_income)
                    VALUES (:snapshot_date, :branch_key, :net_income)
                    ON CONFLICT (snapshot_date, branch_key) DO UPDATE SET
                        net_income = EXCLUDED.net_income
                """), {
                    "snapshot_date": snapshot_date,
                    "branch_key":    branch_result.branch_key,
                    "net_income":    _parse_currency(row.get("Net Income")),
                })
                upserted += result.rowcount
        stats["row_count"] = upserted

    context.log.info(f"Upserted {upserted} rows, skipped {skipped}")
    return {"status": "ok", "rows_upserted": upserted, "skipped": skipped}


@asset(group_name="file_loaders", deps=[dim_branch_table])
def fct_loans_by_branch_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_loans_by_branch_summary from Loans_Per_Branch_Per_Month.csv.

    CSV columns confirmed: Month, Branch  (trailing space in header), Loans
    Branch values: Main, North, South, East, Joco/JOCO — mapped to dim_branch abbreviations.
    """
    folder = _backfill_path("fct_loans_by_branch_summary")
    # Strip trailing whitespace from column headers (CSV has "Branch " with a trailing space)
    df = _load_all_csvs(folder)
    df.columns = df.columns.str.strip()
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    # Map short CSV branch names to dim_branch abbreviations
    BRANCH_MAP = {
        "Main":  "MAIN",
        "North": "NORTH",
        "South": "SOUTH",
        "East":  "EAST",
        "Joco":  "OP",
        "JOCO":  "OP",
    }

    engine = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get("Month"))
                if snapshot_date is None:
                    continue
                branch_raw = str(row.get("Branch", "")).strip()
                abbreviation = BRANCH_MAP.get(branch_raw)
                if abbreviation is None:
                    context.log.warning(f"Unknown branch '{branch_raw}' — skipping")
                    continue
                branch_result = pg_conn.execute(
                    text("SELECT branch_key FROM dim_branch WHERE abbreviation = :a"),
                    {"a": abbreviation},
                ).fetchone()
                if branch_result is None:
                    context.log.warning(f"No dim_branch row for abbreviation '{abbreviation}' — skipping")
                    continue
                result = pg_conn.execute(text("""
                    INSERT INTO fct_loans_by_branch_summary (snapshot_date, branch_key, loan_count)
                    VALUES (:snapshot_date, :branch_key, :loan_count)
                    ON CONFLICT (snapshot_date, branch_key) DO UPDATE SET
                        loan_count = EXCLUDED.loan_count
                """), {
                    "snapshot_date": snapshot_date,
                    "branch_key":    branch_result.branch_key,
                    "loan_count":    int(row.get("Loans")) if pd.notna(row.get("Loans")) else None,
                })
                upserted += result.rowcount
        stats["row_count"] = upserted

    return {"status": "ok", "rows_upserted": upserted}


@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def fct_atm_by_machine_summary_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_atm_by_machine_summary from Number_of_Transactions_by_ATM.csv.

    CSV columns confirmed: Month, ATM, Transactions
    One row per ATM per month. ATM names are short codes (Main Br, North Br,
    CPD, East Br, MPD, HQ, SPD, EPD, Joco, Worth) stored as-is.
    """
    folder = _backfill_path("fct_atm_by_machine_summary")
    df = _load_all_csvs(folder)
    context.log.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    engine = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                snapshot_date = _eom(row.get("Month"))
                if snapshot_date is None:
                    continue
                atm_name = str(row.get("ATM", "")).strip()
                if not atm_name:
                    continue
                transactions = row.get("Transactions")
                result = pg_conn.execute(text("""
                    INSERT INTO fct_atm_by_machine_summary (snapshot_date, atm_name, transactions)
                    VALUES (:snapshot_date, :atm_name, :transactions)
                    ON CONFLICT (snapshot_date, atm_name) DO UPDATE SET
                        transactions = EXCLUDED.transactions
                """), {
                    "snapshot_date": snapshot_date,
                    "atm_name":      atm_name,
                    "transactions":  int(transactions) if pd.notna(transactions) else None,
                })
                upserted += result.rowcount
        stats["row_count"] = upserted

    return {"status": "ok", "rows_upserted": upserted}


@asset(group_name="file_loaders", deps=[warehouse_schema_ready])
def fct_digital_enrollments_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_digital_enrollments from the month-per-row enrollment history file.

    Expected file: a single CSV in data/csv_backfill/fct_digital_enrollments/
    Format: header row, one row per month, one column per service:
        Month,Online Banking User Count,Bill Pay User Count,RDC Count
        Jan-20,8603,3093,3159
        ...

    Month labels are parsed from the Month column (Mon-YY / Mon-YYYY /
    YYYY-MM-DD all accepted) and keyed to EOM — fails loudly on anything
    unparseable rather than guessing. The original wide-format backfill
    generated dates positionally from an assumed Jan-2020 start, which is how
    the first load mis-anchored the whole series; explicit labels prevent that.
    Numbers may be comma-formatted (13,030) — stripped automatically. Blank
    rows and Excel's trailing empty columns are ignored.
    """
    folder = _backfill_path("fct_digital_enrollments")
    files = sorted(folder.glob("*.csv")) + sorted(folder.glob("*.tsv")) + sorted(folder.glob("*.txt"))
    if not files:
        raise FileNotFoundError(f"No file found in {folder}")

    df = pd.read_csv(files[0], encoding="utf-8-sig", dtype=str)
    # Excel pads saved CSVs with empty trailing columns — drop anything unnamed.
    df = df.loc[:, [c for c in df.columns if not c.startswith("Unnamed")]]
    context.log.info(f"Loaded {len(df)} month rows, columns: {list(df.columns)}")

    MONTH_COL = "Month"
    SERVICE_COLUMNS = {
        "Online Banking User Count": "home_banking",
        "Bill Pay User Count":       "bill_pay",
        "RDC Count":                 "rdc",
    }

    missing = {MONTH_COL, *SERVICE_COLUMNS} - set(df.columns)
    if missing:
        raise Exception(
            f"Backfill file {files[0].name} is missing expected columns: "
            f"{sorted(missing)}. Found: {list(df.columns)}"
        )

    def parse_month(label: str) -> date:
        for fmt in ("%b-%y", "%b-%Y", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return last_day_of_month(datetime.strptime(label, fmt).date())
            except ValueError:
                continue
        raise Exception(
            f"Unparseable Month label {label!r} in {files[0].name} — "
            f"expected formats like 'Jan-20', 'Jan-2020', or '2020-01-31'."
        )

    engine = warehouse_pg.get_engine()

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                month_label = str(row[MONTH_COL]).strip()
                if not month_label or month_label.lower() == "nan":
                    continue  # trailing blank row
                snapshot_date = parse_month(month_label)

                for column_name, service in SERVICE_COLUMNS.items():
                    raw_val = str(row[column_name]).strip()
                    if not raw_val or raw_val.lower() == "nan":
                        continue

                    enrolled = int(raw_val.replace(",", "").replace(" ", ""))

                    result = pg_conn.execute(text("""
                        INSERT INTO fct_digital_enrollments
                            (snapshot_date, service, enrolled_count)
                        VALUES (:snapshot_date, :service, :enrolled_count)
                        ON CONFLICT (snapshot_date, service) DO UPDATE SET
                            enrolled_count = EXCLUDED.enrolled_count
                    """), {
                        "snapshot_date":  snapshot_date,
                        "service":        service,
                        "enrolled_count": enrolled,
                    })
                    upserted += result.rowcount

        stats["row_count"] = upserted

    context.log.info(f"Upserted {upserted} service-month rows")
    return {"status": "ok", "rows_upserted": upserted}


@asset(group_name="file_loaders", deps=[dim_branch_table])
def fct_teller_activity_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_teller_activity from TellerActivity.csv.

    CSV columns confirmed: Date, User, Transactions
    The CSV contains duplicate rows (same teller/date with same or different counts)
    from multiple report exports being combined. Rows are summed by (Date, User)
    before inserting so each teller gets one row per day with the total count.
    """
    from pipelines.defs.assets.jasperserver_loaders.teller import (
        _resolve_employee_keys_by_teller_user,
    )

    folder = _backfill_path("fct_teller_activity")
    df = _load_all_csvs(folder)
    context.log.info(f"Loaded {len(df)} raw rows, columns: {list(df.columns)}")

    # Convert and drop invalid rows before aggregating
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "User"])
    df["Transactions"] = pd.to_numeric(df["Transactions"], errors="coerce").fillna(0).astype(int)

    # Sum duplicate (date, user) combinations from multiple report exports
    df = (
        df.groupby(["Date", "User"], as_index=False)["Transactions"]
        .sum()
    )
    context.log.info(f"After dedup/sum: {len(df)} unique (date, teller) rows")

    engine = warehouse_pg.get_engine()
    teller_users = df["User"].unique().tolist()
    employee_key_map = _resolve_employee_keys_by_teller_user(engine, teller_users)

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        upserted = 0
        skipped = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                teller_user = row["User"]
                employee_key = employee_key_map.get(teller_user)
                if employee_key is None:
                    skipped += 1
                    continue
                result = pg_conn.execute(text("""
                    INSERT INTO fct_teller_activity (activity_date, employee_key, branch_key, transaction_count)
                    VALUES (:activity_date, :employee_key, NULL, :transaction_count)
                    ON CONFLICT (activity_date, employee_key) DO UPDATE SET
                        transaction_count = EXCLUDED.transaction_count
                """), {
                    "activity_date":     row["Date"].date(),
                    "employee_key":      employee_key,
                    "transaction_count": int(row["Transactions"]),
                })
                upserted += result.rowcount
        stats["row_count"] = upserted

    context.log.info(f"Upserted {upserted}, skipped {skipped} unknown users")
    return {"status": "ok", "rows_upserted": upserted, "skipped": skipped}


@asset(group_name="file_loaders", deps=[dim_branch_table])
def fct_teller_out_of_balance_backfill(
    context: dagster.AssetExecutionContext,
    warehouse_pg: WarehousePgResource,
):
    """Backfill fct_teller_out_of_balance from Teller_Transactions_out_of_Balance.csv.

    CSV columns confirmed (has header row): Name, Transaction Date, Prev Cash, New Cash
    Cash values are currency strings: $0.00, ($10.00), $6,000.00, etc.
    Rows where Name starts with 'Totals' (summary rows) are filtered out.
    Rows with a blank Transaction Date are also skipped.
    """
    from pipelines.defs.assets.jasperserver_loaders.teller import (
        _resolve_employee_keys_by_teller_user,
    )

    folder = _backfill_path("fct_teller_out_of_balance")
    df = _load_all_csvs(folder)   # has real headers: Name, Transaction Date, Prev Cash, New Cash
    context.log.info(f"Loaded {len(df)} raw rows, columns: {list(df.columns)}")

    # Drop totals rows and blank-date rows
    df = df[~df["Name"].astype(str).str.strip().str.startswith("Totals")]
    df = df.dropna(subset=["Name", "Transaction Date"])
    df = df[df["Transaction Date"].astype(str).str.strip() != ""]
    context.log.info(f"After filtering: {len(df)} rows")

    engine = warehouse_pg.get_engine()
    teller_users = df["Name"].dropna().unique().tolist()
    employee_key_map = _resolve_employee_keys_by_teller_user(engine, teller_users)

    with audit_load(engine, batch_id=context.run_id,
                    asset_name=context.asset_key.path[-1], source_system="csv") as stats:
        inserted = 0
        skipped = 0
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                teller_user = str(row["Name"]).strip()
                employee_key = employee_key_map.get(teller_user)
                if employee_key is None:
                    skipped += 1
                    continue
                try:
                    transaction_date = pd.to_datetime(str(row["Transaction Date"])).date()
                except Exception:
                    continue
                if transaction_date is None:
                    continue
                pg_conn.execute(text("""
                    INSERT INTO fct_teller_out_of_balance
                        (employee_key, transaction_date, prev_cash, new_cash)
                    VALUES (:employee_key, :transaction_date, :prev_cash, :new_cash)
                """), {
                    "employee_key":     employee_key,
                    "transaction_date": transaction_date,
                    "prev_cash":        float(_parse_currency(row["Prev Cash"])) if pd.notna(row.get("Prev Cash")) else None,
                    "new_cash":         float(_parse_currency(row["New Cash"])) if pd.notna(row.get("New Cash")) else None,
                })
                inserted += 1
        stats["row_count"] = inserted

    context.log.info(f"Inserted {inserted}, skipped {skipped} unknown users")
    return {"status": "ok", "rows_inserted": inserted, "skipped": skipped}
