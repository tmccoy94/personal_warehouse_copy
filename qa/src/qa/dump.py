"""Print raw results from both DB2 and warehouse for a named query pair.

Usage:
    docker exec dagster-webserver python /opt/dagster/app/qa/src/qa/dump.py member_count_debug
"""
import sys
from pathlib import Path
import ibm_db
import pandas as pd
from tabulate import tabulate
from qa.compare import DEFAULT_DB, get_db2, get_pg, run_db2, run_pg

QUERIES_DIR = Path("/opt/dagster/app/qa/queries")

def main():
    if len(sys.argv) < 2:
        print("Usage: dump.py <query_name>")
        sys.exit(1)
    name = sys.argv[1]

    db2_path = QUERIES_DIR / "db2"  / f"{name}.sql"
    wh_path  = QUERIES_DIR / "warehouse" / f"{name}.sql"

    db2_conn = get_db2()
    pg_conn  = get_pg()
    try:
        db2_df = run_db2(db2_conn, db2_path.read_text())
        wh_df  = run_pg(pg_conn, wh_path.read_text())
    finally:
        ibm_db.close(db2_conn)
        pg_conn.close()

    print(f"\n=== DB2 ({DEFAULT_DB}) — {len(db2_df)} rows ===")
    print(tabulate(db2_df, headers=db2_df.columns, tablefmt="simple", showindex=False))

    print(f"\n=== Warehouse — {len(wh_df)} rows ===")
    print(tabulate(wh_df, headers=wh_df.columns, tablefmt="simple", showindex=False))

if __name__ == "__main__":
    main()
