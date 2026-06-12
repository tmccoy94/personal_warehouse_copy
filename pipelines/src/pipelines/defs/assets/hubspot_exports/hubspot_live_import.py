"""Produce the HubSpot "live import" CSV from Keystone product data + a HubSpot export.

This replaces the manual notebook (combine_keystone_hubspot_info.ipynb): it pulls the
current product/contact picture from Keystone (DB2), joins it against a dropped HubSpot
"all contacts" CRM export on email + name, collapses Keystone product names into HubSpot
categories, and writes a dated CSV ready to re-import into HubSpot.

This asset does NOT touch the warehouse — its only output is a file on disk. Observability
is via Dagster logs + the returned MaterializeResult metadata (no load_audit row).

The transform mirrors the original notebook exactly so the output is byte-for-byte
reproducible from the same inputs (see tests/test_hubspot_live_import.py).
"""
import os
from datetime import date, datetime
from pathlib import Path

import dagster
import ibm_db
import pandas as pd
from dagster import asset

from pipelines.defs.resources import KeystoneDatabaseResource
from pipelines.loading import db2_fetch_all
from pipelines.queries import load
from pipelines.reference import load_product_category_map

# Columns the Keystone pull query returns (uppercased to match the legacy export CSV).
_KEYSTONE_QUERY = "keystone_products_for_hubspot.sql"

# Final HubSpot import column order.
_IMPORT_COLUMNS = [
    "First Name",
    "Last Name",
    "Email",
    "Phone Number",
    "Birthdate",
    "Number Of Products",
    "Products",
]


# ── Path helpers ────────────────────────────────────────────────────────────────

def _crm_export_dir() -> Path:
    base = Path(os.environ.get("CSV_DROP_PATH", "/data/csv_drop"))
    return base / "hubspot_live_import"


def _output_dir() -> Path:
    base = Path(os.environ.get("CSV_OUTPUT_PATH", "/data/csv_output"))
    return base / "hubspot_live_import"


def _find_csv(folder: Path) -> Path:
    """Return the first CSV file in folder (alphabetical), raise if none."""
    files = sorted(folder.glob("*.csv")) + sorted(folder.glob("*.CSV"))
    if not files:
        raise FileNotFoundError(
            f"No CSV found in {folder}. Drop the HubSpot CRM 'all contacts' export there and re-run."
        )
    return files[0]


# ── Pure transform helpers (unit-tested) ─────────────────────────────────────────

def build_product_categories(products_value: object, category_map: dict[str, str]) -> str:
    """Collapse a comma-separated Keystone PRODUCTS string into "; "-joined categories.

    Unmapped product names are skipped. Categories are de-duplicated, preserving first-
    seen order. Returns "" for blank/NaN input.
    """
    if products_value is None or (isinstance(products_value, float) and pd.isna(products_value)):
        return ""
    text = str(products_value).strip()
    if not text:
        return ""

    seen: set[str] = set()
    categories: list[str] = []
    for product in (p.strip() for p in text.split(",")):
        if not product:
            continue
        category = category_map.get(product)
        if category and category not in seen:
            seen.add(category)
            categories.append(category)
    return "; ".join(categories)


def find_unmapped_products(products_series: "pd.Series", category_map: dict[str, str]) -> set[str]:
    """Return the set of distinct Keystone product names not present in the category map."""
    unmapped: set[str] = set()
    for value in products_series.dropna():
        for product in (p.strip() for p in str(value).split(",")):
            if product and product not in category_map:
                unmapped.add(product)
    return unmapped


def build_live_import(
    keystone_df: pd.DataFrame,
    hubspot_df: pd.DataFrame,
    category_map: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join Keystone product data to the HubSpot export and build the import frame.

    Mirrors combine_keystone_hubspot_info.ipynb step for step. Returns
    (import_df, dropped_duplicates_df). `keystone_df` must use the uppercase column
    names from the pull query; `hubspot_df` must have whitespace-stripped headers
    (First Name, Last Name, Email, Phone Number, Birthdate, ...).
    """
    keystone = keystone_df.copy()
    hubspot = hubspot_df.copy()

    # Category column from the raw Keystone product list.
    keystone["PRODUCT_CATEGORY"] = keystone["PRODUCTS"].apply(
        lambda v: build_product_categories(v, category_map)
    )

    # Normalize match keys.
    keystone["ALL_PAST_EMAILS"] = keystone["ALL_PAST_EMAILS"].fillna("").str.lower()
    hubspot["Email"] = hubspot["Email"].fillna("").str.lower()
    keystone["FIRST_NAME_CLEAN"] = keystone["FIRST_NAME"].fillna("").str.strip().str.lower()
    keystone["LAST_NAME_CLEAN"] = keystone["LAST_NAME"].fillna("").str.strip().str.lower()
    hubspot["FIRST_NAME_CLEAN"] = hubspot["First Name"].fillna("").str.strip().str.lower()
    hubspot["LAST_NAME_CLEAN"] = hubspot["Last Name"].fillna("").str.strip().str.lower()

    # Explode all-past-emails so each historical email gets its own match attempt.
    keystone["EMAIL_LIST"] = keystone["ALL_PAST_EMAILS"].apply(
        lambda x: [e.strip() for e in x.split(",") if e.strip()]
    )
    keystone_exploded = keystone.explode("EMAIL_LIST")

    merged = keystone_exploded.merge(
        hubspot,
        left_on=["EMAIL_LIST", "FIRST_NAME_CLEAN", "LAST_NAME_CLEAN"],
        right_on=["Email", "FIRST_NAME_CLEAN", "LAST_NAME_CLEAN"],
        how="inner",
        suffixes=("_keystone", "_hubspot"),
    )

    # One row per Keystone person.
    merged = merged.drop_duplicates(subset=["PERSON_SERIAL"]).reset_index(drop=True)

    # Phone: prefer the Keystone number, fall back to the HubSpot number.
    merged["Phone Number"] = (
        merged["PHONE"].replace("", pd.NA).fillna(merged["Phone Number"])
    )

    # Duplicate-email cleanup: when the same HubSpot email matched more than once, drop the
    # rows whose Keystone birth date disagrees with HubSpot's (likely a wrong-person match).
    dup_mask = merged["Email"].duplicated(keep=False)
    dropped_duplicates = merged[dup_mask].copy()

    birthdate_hubspot = pd.to_datetime(merged.loc[dup_mask, "Birthdate"], errors="coerce")
    birthdate_keystone = pd.to_datetime(merged.loc[dup_mask, "BIRTH_DATE"], errors="coerce")
    bad_rows = merged.loc[dup_mask].index[birthdate_hubspot != birthdate_keystone]
    merged = merged.drop(index=bad_rows)

    import_df = pd.DataFrame(
        {
            "First Name": merged["First Name"],
            "Last Name": merged["Last Name"],
            "Email": merged["Email"],
            "Phone Number": merged["Phone Number"],
            "Birthdate": merged["BIRTH_DATE"],
            "Number Of Products": merged["PRODUCT_COUNT"],
            "Products": merged["PRODUCT_CATEGORY"],
        }
    ).reset_index(drop=True)[_IMPORT_COLUMNS]

    return import_df, dropped_duplicates


# ── Keystone pull → DataFrame ─────────────────────────────────────────────────────

def _format_birthdate(value: object) -> object:
    """Render a DB2 date/datetime as M/D/YYYY to match the legacy export + HubSpot."""
    if value is None or value == "":
        return value
    if isinstance(value, (datetime, date)):
        return f"{value.month}/{value.day}/{value.year}"
    # Already a string (e.g. ISO date) — try to normalize, else pass through verbatim.
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return value
    return f"{parsed.month}/{parsed.day}/{parsed.year}"


def _keystone_pull_to_df(rows: list[dict]) -> pd.DataFrame:
    """Turn db2_fetch_all() rows (lowercase keys) into a frame with the legacy uppercase columns."""
    df = pd.DataFrame(rows)
    df.columns = [c.upper() for c in df.columns]
    if "BIRTH_DATE" in df.columns:
        df["BIRTH_DATE"] = df["BIRTH_DATE"].apply(_format_birthdate)
    return df


# ── Asset ─────────────────────────────────────────────────────────────────────────

@asset(group_name="hubspot_exports")
def hubspot_live_import_csv(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
) -> dagster.MaterializeResult:
    """Build the dated HubSpot live-import CSV from a live Keystone pull + a dropped CRM export."""
    category_map = load_product_category_map()

    # 1. Live Keystone pull.
    conn = keystone_database.get_connection(context=context)
    try:
        rows = db2_fetch_all(conn, load(_KEYSTONE_QUERY))
    finally:
        ibm_db.close(conn)
    keystone_df = _keystone_pull_to_df(rows)
    context.log.info(f"Keystone pull: {len(keystone_df):,} people with products.")

    # 2. Dropped HubSpot CRM export.
    export_path = _find_csv(_crm_export_dir())
    hubspot_df = pd.read_csv(export_path, dtype=str, encoding="utf-8-sig")
    hubspot_df.columns = hubspot_df.columns.str.strip()  # the export ships a "Birthdate " header
    context.log.info(f"HubSpot export: {len(hubspot_df):,} contacts from {export_path.name}.")

    # 3. Transform.
    import_df, dropped_duplicates = build_live_import(keystone_df, hubspot_df, category_map)

    unmapped = find_unmapped_products(keystone_df["PRODUCTS"], category_map)
    if unmapped:
        context.log.warning(
            f"{len(unmapped)} Keystone product name(s) not in product_to_category.csv "
            f"(silently excluded from categories): {sorted(unmapped)}"
        )

    match_rate = (len(import_df) / len(keystone_df) * 100) if len(keystone_df) else 0.0
    context.log.info(
        f"Matched {len(import_df):,} contacts ({match_rate:.1f}% of Keystone); "
        f"dropped {len(dropped_duplicates):,} duplicate-email rows."
    )

    # 4. Write outputs (dated; same-day re-run overwrites).
    out_dir = _output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = date.today().isoformat()
    import_path = out_dir / f"hubspot_live_import_{stamp}.csv"
    error_path = out_dir / f"error_check_duplicates_{stamp}.csv"
    import_df.to_csv(import_path, index=False)
    dropped_duplicates.to_csv(error_path, index=False)
    context.log.info(f"Wrote {import_path} and {error_path}.")

    return dagster.MaterializeResult(
        metadata={
            "keystone_people": len(keystone_df),
            "hubspot_contacts": len(hubspot_df),
            "matched_contacts": len(import_df),
            "match_rate_pct": round(match_rate, 1),
            "dropped_duplicate_rows": len(dropped_duplicates),
            "unmapped_products": len(unmapped),
            "output_path": str(import_path),
            "error_check_path": str(error_path),
        }
    )
