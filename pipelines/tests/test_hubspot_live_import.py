"""Tests for the hubspot_live_import_csv transform.

The golden test reproduces the real March output from the three source CSVs that ship
in the repo's hubspot_contact_update/ folder; it skips if those fixtures are absent.
The unit tests cover the category mapping and the join/dedup edge cases.
"""
from pathlib import Path

import pandas as pd
import pytest

from pipelines.defs.assets.hubspot_exports.hubspot_live_import import (
    build_live_import,
    build_product_categories,
    find_unmapped_products,
)
from pipelines.reference import load_product_category_map

_FIXTURE_DIR = Path(__file__).parents[2] / "hubspot_contact_update"
_KEYSTONE_CSV = _FIXTURE_DIR / "keystone_products_for_hubspot_pull_3-30-26.csv"
_HUBSPOT_CSV = _FIXTURE_DIR / "hubspot-crm-exports-all-contacts-live-2026-03-31.csv"
_EXPECTED_CSV = _FIXTURE_DIR / "hubspot_live_import.csv"


# ── build_product_categories ──────────────────────────────────────────────────

_MAP = {
    "General Share": "Other",
    "Additional Share": "Other",
    "Power Checking": "Checking",
    "Online Banking": "Online Banking",
    "Platinum MasterCard": "Credit Card",
}


def test_categories_map_and_order_preserved():
    assert build_product_categories(
        "General Share, Power Checking, Online Banking", _MAP
    ) == "Other; Checking; Online Banking"


def test_categories_deduped_preserving_first_seen_order():
    # Two products both map to "Other"; Power Checking comes first → Checking leads.
    assert build_product_categories("Power Checking, General Share, Additional Share", _MAP) == "Checking; Other"


def test_categories_unmapped_products_skipped():
    assert build_product_categories("General Share, Mystery Product", _MAP) == "Other"


def test_categories_blank_and_nan():
    assert build_product_categories("", _MAP) == ""
    assert build_product_categories(float("nan"), _MAP) == ""
    assert build_product_categories(None, _MAP) == ""


def test_find_unmapped_products():
    series = pd.Series(["General Share, Mystery Product", "Power Checking", None])
    assert find_unmapped_products(series, _MAP) == {"Mystery Product"}


# ── build_live_import edge cases ────────────────────────────────────────────────

def _keystone_row(serial, first, last, emails, bday, phone, products="General Share", count=1):
    return {
        "PERSON_SERIAL": serial,
        "FIRST_NAME": first,
        "LAST_NAME": last,
        "ALL_PAST_EMAILS": emails,
        "BIRTH_DATE": bday,
        "PHONE": phone,
        "PRODUCT_COUNT": count,
        "PRODUCTS": products,
    }


def test_phone_falls_back_to_hubspot_when_keystone_blank():
    keystone = pd.DataFrame([_keystone_row(1, "Jane", "Roe", "jane@x.com", "1/1/1990", "")])
    hubspot = pd.DataFrame(
        [{"First Name": "Jane", "Last Name": "Roe", "Email": "jane@x.com",
          "Phone Number": "(555)111-2222", "Birthdate": "1/1/1990"}]
    )
    import_df, dropped = build_live_import(keystone, hubspot, _MAP)
    assert len(import_df) == 1
    assert import_df.iloc[0]["Phone Number"] == "(555)111-2222"
    assert import_df.iloc[0]["Products"] == "Other"
    assert dropped.empty


def test_keystone_phone_preferred_over_hubspot():
    keystone = pd.DataFrame([_keystone_row(1, "Jane", "Roe", "jane@x.com", "1/1/1990", "(816)999-0000")])
    hubspot = pd.DataFrame(
        [{"First Name": "Jane", "Last Name": "Roe", "Email": "jane@x.com",
          "Phone Number": "(555)111-2222", "Birthdate": "1/1/1990"}]
    )
    import_df, _ = build_live_import(keystone, hubspot, _MAP)
    assert import_df.iloc[0]["Phone Number"] == "(816)999-0000"


def test_duplicate_email_with_mismatched_birthdate_is_dropped():
    # One HubSpot contact matches two same-named Keystone people; only the birthdate match survives.
    keystone = pd.DataFrame([
        _keystone_row(1, "John", "Doe", "shared@x.com", "1/1/1990", "(816)111-1111"),
        _keystone_row(2, "John", "Doe", "shared@x.com", "2/2/1960", "(816)222-2222"),
    ])
    hubspot = pd.DataFrame(
        [{"First Name": "John", "Last Name": "Doe", "Email": "shared@x.com",
          "Phone Number": "(555)000-0000", "Birthdate": "1/1/1990"}]
    )
    import_df, dropped = build_live_import(keystone, hubspot, _MAP)
    assert len(import_df) == 1
    assert import_df.iloc[0]["Birthdate"] == "1/1/1990"
    assert len(dropped) == 2  # both duplicate-email rows are captured for review


def test_email_match_uses_past_emails_when_current_differs():
    # HubSpot has an old email; Keystone lists it among ALL_PAST_EMAILS.
    keystone = pd.DataFrame(
        [_keystone_row(1, "Amy", "Lee", "new@x.com, old@x.com", "3/3/1980", "(816)333-3333")]
    )
    hubspot = pd.DataFrame(
        [{"First Name": "Amy", "Last Name": "Lee", "Email": "old@x.com",
          "Phone Number": "(555)000-0000", "Birthdate": "3/3/1980"}]
    )
    import_df, _ = build_live_import(keystone, hubspot, _MAP)
    assert len(import_df) == 1
    assert import_df.iloc[0]["Email"] == "old@x.com"


# ── Golden reproduction against the real March files ────────────────────────────

@pytest.mark.skipif(
    not (_KEYSTONE_CSV.exists() and _HUBSPOT_CSV.exists() and _EXPECTED_CSV.exists()),
    reason="hubspot_contact_update source CSVs not present in this checkout",
)
def test_golden_reproduction_matches_march_output():
    keystone = pd.read_csv(_KEYSTONE_CSV, dtype=str, encoding="utf-8-sig")
    hubspot = pd.read_csv(_HUBSPOT_CSV, dtype=str, encoding="utf-8-sig")
    hubspot.columns = hubspot.columns.str.strip()
    category_map = load_product_category_map()

    import_df, _ = build_live_import(keystone, hubspot, category_map)
    expected = pd.read_csv(_EXPECTED_CSV, dtype=str, encoding="utf-8-sig")

    # Same number of matched contacts as the hand-built file.
    assert len(import_df) == len(expected) == 9043

    sort_cols = list(expected.columns)
    got = import_df.sort_values(sort_cols).reset_index(drop=True)
    want = expected.sort_values(sort_cols).reset_index(drop=True)
    pd.testing.assert_frame_equal(got, want, check_like=True)
