import os, sys, ibm_db
from pathlib import Path
from collections import Counter

sys.path.insert(0, "/opt/dagster/app/qa/src")
from qa.connections import LIVE

db_name = os.environ.get("KEYSTONE_DB_OVERRIDE") or LIVE
conn_str = (
    f"DATABASE={db_name};HOSTNAME={os.environ['SOURCE_DB_HOST']};"
    f"PORT={os.environ['SOURCE_DB_PORT']};PROTOCOL=TCPIP;"
    f"UID={os.environ['SOURCE_DB_USER']};PWD={os.environ['SOURCE_DB_PASSWORD']};"
)
conn = ibm_db.connect(conn_str, "", "")
print(f"Connected to {db_name}", flush=True)

sql = Path("/opt/dagster/app/src/pipelines/queries/home_branch_opened.sql").read_text()

# Simulate notebook-style param substitution (same as warehouse `:param` but as literals)
sql = sql.replace(":begin_date", "'2026-05-01'").replace(":end_date", "'2026-05-31'")
# Remove the DATE keyword before the bare string if present
sql = sql.replace("DATE '2026-05-01'", "'2026-05-01'")

stmt = ibm_db.exec_immediate(conn, sql)
rows = []
row = ibm_db.fetch_assoc(stmt)
while row:
    rows.append({k.lower(): v for k, v in row.items()})
    row = ibm_db.fetch_assoc(stmt)
ibm_db.close(conn)

print(f"Total rows: {len(rows)}")
counts = Counter()
for r in rows:
    counts[r.get("home_branch") or "Unassigned"] += 1
for branch, n in sorted(counts.items()):
    print(f"  {branch}: {n}")
