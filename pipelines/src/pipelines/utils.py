from __future__ import annotations

import calendar
import hashlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd


def calc_age(birth_dates: pd.Series[date], as_of: date = None) -> pd.Series:
    """Return age in whole years as of as_of date (defaults to today).

    Matches the birthday-aware logic in person_service_details_query.sql:
    subtract 1 if the birthday has not yet occurred in the as_of year.
    """
    if as_of is None:
        as_of = date.today()
    ref = pd.Timestamp(as_of)
    years = ref.year - birth_dates.dt.year
    birthday_passed = (birth_dates.dt.month < ref.month) | (
        (birth_dates.dt.month == ref.month) & (birth_dates.dt.day <= ref.day)
    )
    return (years - (~birthday_passed).astype(int)).astype("Int16")


def last_day_of_month(any_date: date) -> date:
    last_day = calendar.monthrange(any_date.year, any_date.month)[1]
    return date(any_date.year, any_date.month, last_day)


def last_day_of_prior_month(today: date | None = None) -> date:
    if today is None:
        today = date.today()
    return today.replace(day=1) - timedelta(days=1)


def month_id_from_date(d: date) -> int:
    """Return YYYYMM integer from a date — used as a GROUP BY convenience key on dim_date."""
    return d.year * 100 + d.month


def _canonicalize_value(value: object) -> str:
    """Stable string representation of a single value for use in record_hash computation."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (float, Decimal)):
        return str(round(float(value), 4))
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        utc = value.astimezone(timezone.utc)
        return utc.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    if isinstance(value, date):  # date must follow datetime (datetime subclasses date)
        return value.strftime("%Y-%m-%d")
    if isinstance(value, list):
        parts = sorted(_canonicalize_value(item) for item in value)
        return ", ".join(parts)
    if isinstance(value, str):
        stripped = value.strip().lower()
        # comma-separated text (e.g. products lists) is treated as order-insensitive
        if ", " in stripped:
            parts = sorted(part.strip() for part in stripped.split(", "))
            return ", ".join(parts)
        return stripped
    return str(value).strip().lower()


def compute_record_hash(row: dict, columns: list[str]) -> str:
    """SHA-256 fingerprint of the specified columns in row, stable across runs.

    Columns are sorted before hashing so column order in the caller does not matter.
    Values are canonicalized before hashing per the rules in _canonicalize_value.
    """
    canonical_parts = [
        f"{col}={_canonicalize_value(row.get(col))}"
        for col in sorted(columns)
    ]
    payload = "|".join(canonical_parts)
    return hashlib.sha256(payload.encode()).hexdigest()


def age_group(ages: pd.Series[int]) -> pd.Series:
    """Map integer ages to standard reporting buckets.

    Buckets: <18 / 18-24 / 25-34 / 35-44 / 45-54 / 55-64 / 65-74 / 75+
    Returns a nullable string Series; NaN where age is null.
    """
    bins = [-1, 17, 24, 34, 44, 54, 64, 74, float("inf")]
    labels = ["<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]
    return pd.cut(ages, bins=bins, labels=labels).astype(object).where(ages.notna())
