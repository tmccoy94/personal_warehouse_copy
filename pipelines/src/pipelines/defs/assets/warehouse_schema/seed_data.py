import os
from datetime import date, timedelta

import pandas as pd
from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_schema.schema import (
    warehouse_funcs,
    warehouse_tables,
)
from pipelines.defs.resources import WarehousePgResource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the n-th occurrence (1-indexed) of weekday (0=Mon) in month/year."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of weekday (0=Mon) in month/year."""
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    delta = (last.weekday() - weekday) % 7
    return last - timedelta(days=delta)


def _us_bank_holidays(start_year: int, end_year: int) -> set:
    holidays = set()
    for yr in range(start_year, end_year + 1):
        holidays.add(date(yr, 1, 1))               # New Year's Day
        holidays.add(_nth_weekday(yr, 1, 0, 3))    # MLK Day (3rd Monday Jan)
        holidays.add(_nth_weekday(yr, 2, 0, 3))    # Presidents' Day (3rd Monday Feb)
        holidays.add(_last_weekday(yr, 5, 0))      # Memorial Day (last Monday May)
        holidays.add(date(yr, 7, 4))               # Independence Day
        holidays.add(_nth_weekday(yr, 9, 0, 1))    # Labor Day (1st Monday Sep)
        holidays.add(_nth_weekday(yr, 10, 0, 2))   # Columbus Day (2nd Monday Oct)
        holidays.add(date(yr, 11, 11))             # Veterans Day
        holidays.add(_nth_weekday(yr, 11, 3, 4))   # Thanksgiving (4th Thursday Nov)
        holidays.add(date(yr, 12, 25))             # Christmas
    return holidays


# ---------------------------------------------------------------------------
# dim_date seed
# ---------------------------------------------------------------------------

@asset(group_name="warehouse_schema", deps=[warehouse_tables])
def warehouse_dim_date(warehouse_pg: WarehousePgResource):
    engine = warehouse_pg.get_engine()
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()
    if existing:
        return {"status": "already seeded", "rows": existing}

    holidays = _us_bank_holidays(2020, 2030)
    dates = pd.date_range("2020-01-01", "2030-12-31", freq="D")

    rows = [
        {
            "date_key":    int(d.strftime("%Y%m%d")),
            "full_date":   d.date(),
            "year":        d.year,
            "month":       d.month,
            "month_id":    int(d.strftime("%Y%m")),
            "day":         d.day,
            "quarter":     (d.month - 1) // 3 + 1,
            "day_of_week": d.isoweekday(),  # 1=Monday
            "is_weekend":  d.dayofweek >= 5,
            "is_holiday":  d.date() in holidays,
        }
        for d in dates
    ]

    df = pd.DataFrame(rows)
    schema = os.environ.get("WAREHOUSE_SCHEMA", "dev")
    df.to_sql("dim_date", engine, schema=schema, if_exists="append", index=False, method="multi")
    return {"status": "seeded", "rows": len(df)}


# ---------------------------------------------------------------------------
# dim_branch sentinel seed
# ---------------------------------------------------------------------------

_SENTINELS = [
    {"branch_name": "CU Total",   "abbreviation": "TOTAL"},
    {"branch_name": "Unassigned", "abbreviation": "UNASSIGN"},
]


@asset(group_name="warehouse_schema", deps=[warehouse_tables])
def warehouse_branch_sentinels(warehouse_pg: WarehousePgResource):
    engine = warehouse_pg.get_engine()
    inserted = 0
    with engine.connect() as conn:
        for sentinel in _SENTINELS:
            result = conn.execute(
                text("""
                    INSERT INTO dim_branch
                        (source_system, source_branch_serial, branch_name, abbreviation)
                    VALUES (NULL, NULL, :branch_name, :abbreviation)
                    ON CONFLICT (branch_name) DO NOTHING
                """),
                sentinel,
            )
            inserted += result.rowcount
        conn.commit()
    return {"status": "ok", "rows_inserted": inserted}


# ---------------------------------------------------------------------------
# Schema readiness gate
# ---------------------------------------------------------------------------

@asset(
    group_name="warehouse_schema",
    deps=[
        warehouse_funcs,
        warehouse_dim_date,
        warehouse_branch_sentinels,
    ],
)
def warehouse_schema_ready():
    import os
    schema = os.environ.get("WAREHOUSE_SCHEMA", "dev")
    return {"status": "ready", "warehouse_schema": schema}
