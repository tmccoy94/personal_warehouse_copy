from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from pipelines.utils import (
    age_group,
    calc_age,
    compute_record_hash,
    last_day_of_month,
    last_day_of_prior_month,
    month_id_from_date,
)


# ── calc_age ──────────────────────────────────────────────────────────────────

def to_series(*dates):
    return pd.to_datetime(pd.Series(list(dates)))


def test_calc_age_birthday_already_passed():
    result = calc_age(to_series(date(1990, 1, 1)), as_of=date(2026, 6, 15))
    assert result[0] == 36


def test_calc_age_birthday_not_yet_occurred():
    result = calc_age(to_series(date(1990, 12, 31)), as_of=date(2026, 6, 15))
    assert result[0] == 35


def test_calc_age_birthday_is_today_counts_as_passed():
    result = calc_age(to_series(date(1990, 6, 15)), as_of=date(2026, 6, 15))
    assert result[0] == 36


def test_calc_age_multiple_people():
    result = calc_age(
        to_series(date(1990, 1, 1), date(1990, 12, 31)),
        as_of=date(2026, 6, 15),
    )
    assert result[0] == 36
    assert result[1] == 35


def test_calc_age_null_birth_date_returns_na():
    result = calc_age(pd.to_datetime(pd.Series([None])), as_of=date(2026, 6, 15))
    assert pd.isna(result[0])


def test_calc_age_returns_int16_dtype():
    result = calc_age(to_series(date(1990, 1, 1)), as_of=date(2026, 6, 15))
    assert result.dtype == "Int16"


# ── age_group ─────────────────────────────────────────────────────────────────

def ages(*values):
    return pd.Series(list(values), dtype="Int16")


@pytest.mark.parametrize("age,expected_bucket", [
    (0,  "<18"),
    (17, "<18"),
    (18, "18-24"),
    (24, "18-24"),
    (25, "25-34"),
    (34, "25-34"),
    (35, "35-44"),
    (44, "35-44"),
    (45, "45-54"),
    (54, "45-54"),
    (55, "55-64"),
    (64, "55-64"),
    (65, "65-74"),
    (74, "65-74"),
    (75, "75+"),
    (99, "75+"),
])
def test_age_group_bucket_boundaries(age, expected_bucket):
    result = age_group(ages(age))
    assert result[0] == expected_bucket


def test_age_group_null_age_returns_nan():
    result = age_group(pd.Series([pd.NA], dtype="Int16"))
    assert pd.isna(result[0])


def test_age_group_mixed_with_null():
    result = age_group(ages(30, pd.NA, 65))
    assert result[0] == "25-34"
    assert pd.isna(result[1])
    assert result[2] == "65-74"


def test_calc_age_then_age_group_pipeline():
    birth_dates = to_series(date(2010, 1, 1), date(1995, 7, 4), date(1945, 3, 20))
    ages_result = calc_age(birth_dates, as_of=date(2026, 6, 15))
    groups = age_group(ages_result)
    assert groups[0] == "<18"
    assert groups[1] == "25-34"
    assert groups[2] == "75+"


# ── last_day_of_month ─────────────────────────────────────────────────────────

def test_last_day_of_month_january():
    assert last_day_of_month(date(2026, 1, 1)) == date(2026, 1, 31)

def test_last_day_of_month_february_non_leap():
    assert last_day_of_month(date(2025, 2, 1)) == date(2025, 2, 28)

def test_last_day_of_month_february_leap():
    assert last_day_of_month(date(2024, 2, 1)) == date(2024, 2, 29)

def test_last_day_of_month_december():
    assert last_day_of_month(date(2025, 12, 15)) == date(2025, 12, 31)

def test_last_day_of_month_april():
    assert last_day_of_month(date(2026, 4, 10)) == date(2026, 4, 30)


# ── last_day_of_prior_month ───────────────────────────────────────────────────

def test_last_day_of_prior_month_mid_month():
    assert last_day_of_prior_month(date(2026, 5, 9)) == date(2026, 4, 30)

def test_last_day_of_prior_month_first_of_month():
    assert last_day_of_prior_month(date(2026, 3, 1)) == date(2026, 2, 28)

def test_last_day_of_prior_month_crosses_year():
    assert last_day_of_prior_month(date(2026, 1, 15)) == date(2025, 12, 31)


# ── month_id_from_date ────────────────────────────────────────────────────────

def test_month_id_from_date():
    assert month_id_from_date(date(2026, 5, 31)) == 202605

def test_month_id_from_date_january():
    assert month_id_from_date(date(2025, 1, 1)) == 202501


# ── compute_record_hash ───────────────────────────────────────────────────────

HASH_COLUMNS = ["first_name", "last_name", "birth_date", "gender"]

GOLDEN_ROW = {
    "first_name": "Jane",
    "last_name": "Doe",
    "birth_date": date(1985, 3, 22),
    "gender": "F",
}

def test_compute_record_hash_is_deterministic():
    h1 = compute_record_hash(GOLDEN_ROW, HASH_COLUMNS)
    h2 = compute_record_hash(GOLDEN_ROW, HASH_COLUMNS)
    assert h1 == h2

def test_compute_record_hash_differs_on_data_change():
    changed = {**GOLDEN_ROW, "last_name": "Smith"}
    assert compute_record_hash(GOLDEN_ROW, HASH_COLUMNS) != compute_record_hash(changed, HASH_COLUMNS)

def test_compute_record_hash_column_order_does_not_matter():
    reversed_columns = list(reversed(HASH_COLUMNS))
    assert compute_record_hash(GOLDEN_ROW, HASH_COLUMNS) == compute_record_hash(GOLDEN_ROW, reversed_columns)

def test_compute_record_hash_none_is_empty_string():
    row_with_null = {**GOLDEN_ROW, "gender": None}
    row_with_empty = {**GOLDEN_ROW, "gender": ""}
    assert compute_record_hash(row_with_null, HASH_COLUMNS) == compute_record_hash(row_with_empty, HASH_COLUMNS)

def test_compute_record_hash_string_canonicalized():
    row_upper = {**GOLDEN_ROW, "first_name": "  JANE  "}
    assert compute_record_hash(row_upper, HASH_COLUMNS) == compute_record_hash(GOLDEN_ROW, HASH_COLUMNS)

def test_compute_record_hash_date_formatted():
    h = compute_record_hash({"birth_date": date(1985, 3, 22)}, ["birth_date"])
    assert len(h) == 64  # SHA-256 hex digest

def test_compute_record_hash_float_rounded():
    row_a = {"rate": 0.12509}  # rounds to 0.1251
    row_b = {"rate": 0.12514}  # also rounds to 0.1251
    assert compute_record_hash(row_a, ["rate"]) == compute_record_hash(row_b, ["rate"])

def test_compute_record_hash_decimal_matches_float():
    row_decimal = {"rate": Decimal("0.1251")}
    row_float = {"rate": 0.1251}
    assert compute_record_hash(row_decimal, ["rate"]) == compute_record_hash(row_float, ["rate"])

def test_compute_record_hash_list_order_insensitive():
    row_a = {"products": ["savings", "checking", "auto loan"]}
    row_b = {"products": ["auto loan", "savings", "checking"]}
    assert compute_record_hash(row_a, ["products"]) == compute_record_hash(row_b, ["products"])

def test_compute_record_hash_comma_separated_string_order_insensitive():
    row_a = {"products": "savings, checking, auto loan"}
    row_b = {"products": "auto loan, savings, checking"}
    assert compute_record_hash(row_a, ["products"]) == compute_record_hash(row_b, ["products"])

def test_compute_record_hash_datetime_utc_normalized():
    dt_naive = datetime(2026, 5, 9, 10, 30, 0)
    dt_utc = datetime(2026, 5, 9, 10, 30, 0, tzinfo=timezone.utc)
    assert compute_record_hash({"ts": dt_naive}, ["ts"]) == compute_record_hash({"ts": dt_utc}, ["ts"])

def test_compute_record_hash_missing_column_treated_as_none():
    row_no_key = {"first_name": "Jane"}
    row_explicit_none = {"first_name": "Jane", "middle_name": None}
    assert compute_record_hash(row_no_key, ["first_name", "middle_name"]) == \
        compute_record_hash(row_explicit_none, ["first_name", "middle_name"])
