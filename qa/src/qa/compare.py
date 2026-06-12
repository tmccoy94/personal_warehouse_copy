#!/opt/dagster/app/.venv/bin/python
"""QA comparison runner — runs inside the dagster-webserver container.

Usage (from project root on host):
    docker exec dagster-webserver python /opt/dagster/app/qa/src/qa/compare.py
    docker exec dagster-webserver python /opt/dagster/app/qa/src/qa/compare.py member_count_by_branch

ibm_db and psycopg are already installed in the container venv.
Reads credentials from container env vars (injected by docker-compose).
"""
import os
import sys
from pathlib import Path

import ibm_db
import pandas as pd
import psycopg
from tabulate import tabulate

QUERIES_DIR = Path("/opt/dagster/app/qa/queries")
DB2_DIR     = QUERIES_DIR / "db2"
WH_DIR      = QUERIES_DIR / "warehouse"

# DB2 constants
LIVE        = "D0001LIV"
MONTH_END_1 = "D0001T31"
MONTH_END_2 = "D0001T32"
QUARTER_END = "D0001T33"

# Change this to target a different snapshot
DEFAULT_DB = os.environ.get("KEYSTONE_DB_OVERRIDE") or LIVE


# ── Connections ───────────────────────────────────────────────────────────────

def get_db2(db_name: str = DEFAULT_DB):
    conn_str = (
        f"DATABASE={db_name};"
        f"HOSTNAME={os.environ['SOURCE_DB_HOST']};"
        f"PORT={os.environ['SOURCE_DB_PORT']};"
        "PROTOCOL=TCPIP;"
        f"UID={os.environ['SOURCE_DB_USER']};"
        f"PWD={os.environ['SOURCE_DB_PASSWORD']};"
    )
    return ibm_db.connect(conn_str, "", "")


def get_pg():
    return psycopg.connect(
        host="warehouse-postgres",
        port=5432,
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
        user=os.environ.get("WAREHOUSE_USER", "warehouse_admin"),
        password=os.environ["WAREHOUSE_PASSWORD"],
    )


# ── Query runners ─────────────────────────────────────────────────────────────

def run_db2(conn, sql: str) -> pd.DataFrame:
    stmt = ibm_db.exec_immediate(conn, sql)
    cols, i = [], 0
    col = ibm_db.field_name(stmt, i)
    while col:
        cols.append(col.lower())
        i += 1
        col = ibm_db.field_name(stmt, i)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append({k.lower(): v for k, v in row.items()})
        row = ibm_db.fetch_assoc(stmt)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def run_pg(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ── Comparison ────────────────────────────────────────────────────────────────

def _is_numeric(series: pd.Series) -> bool:
    try:
        series.astype(float)
        return True
    except (ValueError, TypeError):
        return False


def compare(name: str, db2_df: pd.DataFrame, wh_df: pd.DataFrame):
    """Auto-detect comparison mode and print diff.

    Modes:
      scalar  — single row, single numeric column → show two numbers + diff
      grouped — first col = label, second col = numeric count → count diff table
      set     — two columns (id + label) → show which ids are only in DB2 or only in WH
    """
    ncols = len(db2_df.columns)

    # ── Scalar ────────────────────────────────────────────────────────────────
    if len(db2_df) == 1 and ncols == 1:
        db2_val = int(db2_df.iloc[0, 0])
        wh_val  = int(wh_df.iloc[0, 0])
        diff    = wh_val - db2_val
        status  = "✓ MATCH" if diff == 0 else f"✗ DIFF ({diff:+d})"
        print(f"  DB2: {db2_val:,}    Warehouse: {wh_val:,}    {status}")
        return

    # ── Grouped count ─────────────────────────────────────────────────────────
    if ncols >= 2 and _is_numeric(db2_df.iloc[:, 1]):
        label_col = db2_df.columns[0]
        count_col = db2_df.columns[1]

        d = db2_df.set_index(label_col)[count_col].rename("db2")
        w = wh_df.set_index(wh_df.columns[0])[wh_df.columns[1]].rename("warehouse")

        merged = d.to_frame().join(w.to_frame(), how="outer").fillna(0)
        merged[["db2", "warehouse"]] = merged[["db2", "warehouse"]].astype(int)
        merged["diff"]   = merged["warehouse"] - merged["db2"]
        merged["status"] = merged["diff"].apply(lambda d: "✓" if d == 0 else f"✗ ({d:+d})")

        total = pd.DataFrame(
            [{"db2": merged["db2"].sum(), "warehouse": merged["warehouse"].sum(),
              "diff": merged["diff"].sum()}],
            index=["TOTAL"]
        )
        total["status"] = total["diff"].apply(lambda d: "✓ MATCH" if d == 0 else f"✗ ({d:+d})")
        print(tabulate(pd.concat([merged, total]),
                       headers=["", "DB2", "Warehouse", "Diff", ""], tablefmt="simple"))
        mismatches = (merged["diff"] != 0).sum()
        if mismatches:
            print(f"\n  → {mismatches} row(s) differ, total gap: {merged['diff'].sum():+d}")
            for label, row in merged[merged["diff"] != 0].iterrows():
                print(f"     {label}: DB2={int(row.db2):,}  WH={int(row.warehouse):,}  ({int(row['diff']):+d})")
        else:
            print("\n  → EXACT MATCH ✓")
        return

    # ── Set diff ──────────────────────────────────────────────────────────────
    # Two-column result: id column + label column → find which ids are missing
    id_col = db2_df.columns[0]
    db2_ids = set(db2_df[id_col].astype(str))
    wh_ids  = set(wh_df[wh_df.columns[0]].astype(str))

    only_db2 = db2_ids - wh_ids
    only_wh  = wh_ids  - db2_ids

    print(f"  DB2 rows: {len(db2_ids):,}    Warehouse rows: {len(wh_ids):,}")
    if not only_db2 and not only_wh:
        print("  → EXACT MATCH ✓")
        return

    if only_db2:
        print(f"\n  IN DB2 ONLY ({len(only_db2)}) — these are the missing members:")
        subset = db2_df[db2_df[id_col].astype(str).isin(only_db2)]
        print(tabulate(subset.head(50), headers=subset.columns, tablefmt="simple", showindex=False))

    if only_wh:
        print(f"\n  IN WAREHOUSE ONLY ({len(only_wh)}):")
        subset = wh_df[wh_df[wh_df.columns[0]].astype(str).isin(only_wh)]
        print(tabulate(subset.head(50), headers=subset.columns, tablefmt="simple", showindex=False))


# ── Runner ────────────────────────────────────────────────────────────────────

def run_pair(name: str, db2_conn, pg_conn):
    db2_path = DB2_DIR / f"{name}.sql"
    wh_path  = WH_DIR  / f"{name}.sql"

    print(f"\n{'─' * 60}")
    print(f"  {name}  (DB2: {DEFAULT_DB})")
    print(f"{'─' * 60}")

    for path, label in [(db2_path, "DB2"), (wh_path, "Warehouse")]:
        if not path.exists():
            print(f"  [SKIP] missing {label} query: {path}")
            return

    try:
        db2_df = run_db2(db2_conn, db2_path.read_text())
        wh_df  = run_pg(pg_conn, wh_path.read_text())
        compare(name, db2_df, wh_df)
    except Exception as exc:
        print(f"  ERROR: {exc}")
        import traceback; traceback.print_exc()


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None

    print(f"\nConnecting (DB2 target: {DEFAULT_DB})...")
    db2_conn = get_db2()
    pg_conn  = get_pg()

    try:
        if target:
            run_pair(target, db2_conn, pg_conn)
        else:
            pairs = sorted(
                {p.stem for p in DB2_DIR.glob("*.sql")} &
                {p.stem for p in WH_DIR.glob("*.sql")}
            )
            if not pairs:
                print("No query pairs found.")
                return
            for name in pairs:
                run_pair(name, db2_conn, pg_conn)
    finally:
        ibm_db.close(db2_conn)
        pg_conn.close()

    print("\nDone.\n")


if __name__ == "__main__":
    main()
