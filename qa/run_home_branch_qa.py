"""
Home branch QA: runs home_branch_zips.sql against a DB2 snapshot, applies ZIP territory
assignment using the same assign_branch_with_fallback function the asset uses,
then compares branch counts to what's loaded in fct_home_branch_members_summary.

Usage (inside dagster-webserver container):
    # Compare May 2026 against MONTH_END_2 (default)
    KEYSTONE_DB_OVERRIDE=D0001T32 python qa/run_home_branch_qa.py 2026-05-31

    # Compare a different month against LIVE
    KEYSTONE_DB_OVERRIDE=D0001LIV python qa/run_home_branch_qa.py 2026-04-30

    # No arg defaults to 2026-05-31
    KEYSTONE_DB_OVERRIDE=D0001T32 python qa/run_home_branch_qa.py
"""
import os
import sys
from collections import Counter
from datetime import date

sys.path.insert(0, "/opt/dagster/app/qa/src")
from qa.connections import LIVE, MONTH_END_2
from pathlib import Path

import ibm_db
import psycopg

sys.path.insert(0, "/opt/dagster/app/src")
from pipelines.defs.assets.warehouse_loaders.fct_members import assign_branch_with_fallback

WAREHOUSE_SCHEMA = os.environ.get("WAREHOUSE_SCHEMA", "prod")

# ── CLI arg: snapshot date ────────────────────────────────────────────────────
if len(sys.argv) > 1:
    snapshot_date_str = sys.argv[1]
else:
    snapshot_date_str = "2026-05-31"

snapshot_date = date.fromisoformat(snapshot_date_str)
SNAPSHOT_DATE = snapshot_date.isoformat()
MONTH_START   = snapshot_date.replace(day=1).isoformat()

# ── Connections ───────────────────────────────────────────────────────────────

db_name  = os.environ.get("KEYSTONE_DB_OVERRIDE") or MONTH_END_2
host     = os.environ["SOURCE_DB_HOST"]
port     = os.environ["SOURCE_DB_PORT"]
user     = os.environ["SOURCE_DB_USER"]
password = os.environ["SOURCE_DB_PASSWORD"]

def open_db2():
    return ibm_db.connect(
        f"DATABASE={db_name};HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password};",
        "", "",
    )

pg_conn = psycopg.connect(
    host=os.environ["DAGSTER_POSTGRES_HOST"],
    port=int(os.environ.get("DAGSTER_POSTGRES_PORT", "5432")),
    dbname=os.environ["DAGSTER_POSTGRES_DB"],
    user=os.environ["DAGSTER_POSTGRES_USER"],
    password=os.environ["DAGSTER_POSTGRES_PASSWORD"],
)

print(f"\nDB2 target:       {db_name}", flush=True)
print(f"Warehouse schema: {WAREHOUSE_SCHEMA}", flush=True)
print(f"Snapshot date:    {SNAPSHOT_DATE}", flush=True)
print(f"Month start:      {MONTH_START}", flush=True)

# ── Pull total members + ZIPs from DB2 ───────────────────────────────────────

sql_path = Path("/opt/dagster/app/src/pipelines/queries/home_branch_zips.sql")
sql = sql_path.read_text().replace(":as_of_date", f"'{SNAPSHOT_DATE}'")

print(f"\nRunning home_branch_zips.sql...", flush=True)
db2_conn = open_db2()
stmt = ibm_db.exec_immediate(db2_conn, sql)

db2_rows = []
row = ibm_db.fetch_assoc(stmt)
while row:
    db2_rows.append({k.lower(): v for k, v in row.items()})
    row = ibm_db.fetch_assoc(stmt)

ibm_db.close(db2_conn)
print(f"DB2 returned {len(db2_rows):,} member rows", flush=True)

# ── Pull opened (new members this month) from DB2 ────────────────────────────

opened_sql_path = Path("/opt/dagster/app/src/pipelines/queries/home_branch_opened.sql")
opened_sql = (
    opened_sql_path.read_text()
    .replace(":begin_date", f"'{MONTH_START}'")
    .replace(":end_date",   f"'{SNAPSHOT_DATE}'")
)

print(f"Running home_branch_opened.sql ({MONTH_START} → {SNAPSHOT_DATE})...", flush=True)
db2_conn2 = open_db2()
stmt2 = ibm_db.exec_immediate(db2_conn2, opened_sql)
opened_rows = []
row = ibm_db.fetch_assoc(stmt2)
while row:
    opened_rows.append({k.lower(): v for k, v in row.items()})
    row = ibm_db.fetch_assoc(stmt2)
ibm_db.close(db2_conn2)
print(f"DB2 returned {len(opened_rows):,} opened rows", flush=True)

# ── Apply ZIP territory assignment ───────────────────────────────────────────

db2_totals: Counter = Counter()
for r in db2_rows:
    branch = assign_branch_with_fallback(r.get("zip"), r.get("home_branch"))
    db2_totals[branch] += 1

db2_opened: Counter = Counter()
for r in opened_rows:
    branch = assign_branch_with_fallback(r.get("zip"), r.get("home_branch"))
    db2_opened[branch] += 1

# ── Pull warehouse counts ─────────────────────────────────────────────────────

cur = pg_conn.cursor()
cur.execute(f"""
    SELECT b.branch_name, h.total_members, h.members_added
    FROM {WAREHOUSE_SCHEMA}.fct_home_branch_members_summary AS h
    JOIN {WAREHOUSE_SCHEMA}.dim_branch AS b ON b.branch_key = h.branch_key
    WHERE h.snapshot_date = %s
    ORDER BY b.branch_name
""", (SNAPSHOT_DATE,))
wh_rows = cur.fetchall()
pg_conn.close()

wh_totals   = {name: total for name, total, added in wh_rows}
wh_opened   = {name: added for name, total, added in wh_rows}
wh_cu_total = wh_totals.get("CU Total", 0)

# ── Print comparison ──────────────────────────────────────────────────────────

def print_comparison(title, db2_counts, wh_counts, wh_grand_total):
    all_match = True
    branch_names = sorted(
        set(db2_counts.keys()) | {k for k in wh_counts if k not in ("CU Total",)}
    )
    print()
    print("─" * 68)
    print(f"  {title}")
    print("─" * 68)
    print(f"{'Branch':<35} {'DB2':>7} {'Warehouse':>10} {'Diff':>7}")
    print(f"{'-'*35} {'-'*7} {'-'*10} {'-'*7}")
    for name in branch_names:
        db2_val = db2_counts.get(name, 0)
        wh_val  = wh_counts.get(name, 0)
        diff    = wh_val - db2_val
        status  = "✓" if diff == 0 else f"✗ ({diff:+d})"
        if diff != 0:
            all_match = False
        print(f"{name:<35} {db2_val:>7,} {wh_val:>10,} {status:>7}")
    db2_total = sum(db2_counts.values())
    print(f"{'TOTAL':<35} {db2_total:>7,} {wh_grand_total:>10,}", end="")
    total_diff = wh_grand_total - db2_total
    if total_diff == 0:
        print("       ✓ MATCH")
    else:
        all_match = False
        print(f"  ✗ ({total_diff:+d})")
    print("  → EXACT MATCH ✓" if all_match else "  → MISMATCHES FOUND")
    return all_match

print_comparison(
    f"TOTAL MEMBERS by home branch  (snapshot {SNAPSHOT_DATE})",
    db2_totals, wh_totals, wh_cu_total,
)
print_comparison(
    f"OPENED (new members) by home branch  ({MONTH_START} – {SNAPSHOT_DATE})",
    db2_opened, wh_opened, sum(db2_opened.values()),
)
