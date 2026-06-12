"""
Historical backfill for fct_members_summary from TotalMembersStatsMonthly.csv.

Reads data/csv_backfill/fct_members_summary/TotalMembersStatsMonthly.csv and
inserts one row per month into the new fct_members_summary schema.

Skips any snapshot_date already in the table (ON CONFLICT DO NOTHING) so
the May 2026 row loaded by the monthly job is preserved.

Usage:
    docker exec dagster-webserver python /opt/dagster/app/src/backfill_fct_members_summary_history.py
"""
import csv
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, "/opt/dagster/app/src")
from sqlalchemy import create_engine, text
from pipelines.utils import last_day_of_month

CSV_PATH = Path("/tmp/TotalMembersStatsMonthly.csv")

PG_URL = (
    f"postgresql+psycopg://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
    f"@{os.environ['WAREHOUSE_HOST']}:{os.environ.get('WAREHOUSE_PORT', '5432')}"
    f"/{os.environ['WAREHOUSE_DB']}"
)
engine = create_engine(PG_URL, connect_args={"options": "-csearch_path=dev,public"})


def parse_int(val: str) -> int | None:
    v = val.strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None


rows_to_insert = []

with CSV_PATH.open(newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        month_str = row["Month"].strip()
        if not month_str:
            continue
        parts = month_str.split("/")
        month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
        snapshot_date = last_day_of_month(date(year, month, 1))

        rows_to_insert.append({
            "snapshot_date":            snapshot_date,
            "total_members":            parse_int(row["Total Members"]),
            "ks_members":               parse_int(row["Kansas Members"]),
            "mo_members":               parse_int(row["Missouri Members"]),
            "active_members":           parse_int(row["Active Members"]),
            "members_18_50":            parse_int(row["Members 18-50"]),
            "hb_mo_members":            parse_int(row["Home Branch MO"]),
            "hb_ks_members":            parse_int(row["Home Branch KS"]),
            "members_lost":             parse_int(row["Closed Accounts"]),
            "members_added":            parse_int(row["New Accounts"]),
            "non_kcpd_members":         None,
            "net_new_members":          parse_int(row["Net Additional Total Members"]),
            "net_new_ks_members":       parse_int(row["Net Additional Kansas Members"]),
            "net_new_mo_members":       parse_int(row["Net Additional Missouri Members"]),
            "net_new_active_members":   parse_int(row["Net Additional Active Members"]),
            "net_new_members_18_50":    parse_int(row["Net Additional Members 18-50"]),
            "net_new_hb_mo":            parse_int(row["Net HB_MO"]),
            "net_new_hb_ks":            parse_int(row["Net HB_KS"]),
            "net_new_non_kcpd_members": None,
        })

print(f"Read {len(rows_to_insert)} rows from CSV", flush=True)

inserted = 0
skipped  = 0

with engine.begin() as conn:
    for row in rows_to_insert:
        result = conn.execute(text("""
            INSERT INTO fct_members_summary (
                snapshot_date,
                total_members, ks_members, mo_members,
                active_members, members_18_50,
                hb_ks_members, hb_mo_members,
                members_lost, members_added, non_kcpd_members,
                net_new_members, net_new_ks_members, net_new_mo_members,
                net_new_active_members, net_new_members_18_50,
                net_new_hb_mo, net_new_hb_ks, net_new_non_kcpd_members
            ) VALUES (
                :snapshot_date,
                :total_members, :ks_members, :mo_members,
                :active_members, :members_18_50,
                :hb_ks_members, :hb_mo_members,
                :members_lost, :members_added, :non_kcpd_members,
                :net_new_members, :net_new_ks_members, :net_new_mo_members,
                :net_new_active_members, :net_new_members_18_50,
                :net_new_hb_mo, :net_new_hb_ks, :net_new_non_kcpd_members
            )
            ON CONFLICT (snapshot_date) DO NOTHING
        """), row)
        if result.rowcount:
            inserted += 1
        else:
            skipped += 1

print(f"Inserted {inserted} rows, skipped {skipped} (already present)", flush=True)

# Quick verification
with engine.connect() as conn:
    r = conn.execute(text("SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(*) FROM fct_members_summary"))
    row = r.fetchone()
    print(f"Table now has {row[2]} rows spanning {row[0]} → {row[1]}", flush=True)
