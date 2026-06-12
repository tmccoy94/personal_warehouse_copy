"""copy_schema.py — Copy all data from one warehouse schema to another.

Usage:
    python copy_schema.py dev test
    python copy_schema.py dev prod

Both schemas must already exist and have their tables created (run
warehouse_tables in the target schema first via Dagster before copying).

Reads credentials from .env. Connects on localhost using POSTGRES_PORT.
"""
import os
import sys
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

HOST     = "localhost"
PORT     = int(os.environ.get("POSTGRES_PORT", 5433))
DB       = os.environ.get("POSTGRES_DB", "warehouse")
USER     = os.environ.get("POSTGRES_USER", "warehouse_admin")
PASSWORD = os.environ.get("DAGSTER_POSTGRES_PASSWORD", "")

VALID_SCHEMAS = {"dev", "test", "prod"}


def get_tables(conn, schema: str) -> list[str]:
    # Base tables only — exclude partition children (they're populated via their parent).
    rows = conn.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relkind IN ('r', 'p')        -- ordinary + partitioned parent tables
          AND NOT c.relispartition            -- skip partition children
        ORDER BY c.relname
        """,
        (schema,),
    ).fetchall()
    return [r[0] for r in rows]


def partition_key_column(conn, schema: str, table: str) -> str | None:
    """Return the range-partition key column for a table, or None if not partitioned."""
    row = conn.execute(
        """
        SELECT a.attname
        FROM pg_partitioned_table p
        JOIN pg_class c       ON c.oid = p.partrelid
        JOIN pg_namespace n   ON n.oid = c.relnamespace
        JOIN pg_attribute a   ON a.attrelid = c.oid AND a.attnum = ANY(p.partattrs)
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    ).fetchone()
    return row[0] if row else None


def ensure_target_partitions(conn, source: str, target: str, table: str, key_col: str) -> int:
    """Create monthly partitions in the target matching the source data's months.

    Mirrors ensure_snapshot_partition: one partition per calendar month named
    <table>_<YYYY>_<MM>, covering [month_start, next_month_start).
    """
    months = conn.execute(
        f"SELECT DISTINCT date_trunc('month', {key_col})::date AS m "
        f"FROM {source}.{table} WHERE {key_col} IS NOT NULL ORDER BY m"
    ).fetchall()
    created = 0
    for (start,) in months:
        end = date(start.year + 1, 1, 1) if start.month == 12 else date(start.year, start.month + 1, 1)
        pname = f"{table}_{start.year}_{start.month:02d}"
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {target}.{pname} "
            f"PARTITION OF {target}.{table} "
            f"FOR VALUES FROM ('{start}') TO ('{end}')"
        )
        created += 1
    return created


def get_columns(conn, schema: str, table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    ).fetchall()
    return [r[0] for r in rows]


INTENT_FILE = Path(__file__).parent / "pipelines" / "src" / "pipelines" / ".schema_intent"


def check_intent(target: str) -> None:
    """Verify the .schema_intent file matches the copy target.

    The intent file is written by switch_schema.py and reflects what schema
    the running containers are pointed at. Copying to a schema that doesn't
    match the current intent likely means you forgot to switch first.
    """
    if not INTENT_FILE.exists():
        print(
            f"WARNING: .schema_intent file not found at {INTENT_FILE}.\n"
            "         Cannot verify the active schema matches the copy target.\n"
            "         Run switch_schema.py to set the intent before copying to prod."
        )
        confirm = input(f"Continue copying to '{target}' anyway? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            sys.exit(0)
        return

    intent = INTENT_FILE.read_text().strip()
    if intent != target:
        print(
            f"ERROR: .schema_intent says '{intent}' but you are copying to '{target}'.\n"
            f"       Run switch_schema.py and select '{target}' before copying."
        )
        sys.exit(1)

    print(f"Schema intent verified: '{intent}' ✓")


def copy_schema(source: str, target: str) -> None:
    if source not in VALID_SCHEMAS or target not in VALID_SCHEMAS:
        print(f"ERROR: schemas must be one of {VALID_SCHEMAS}")
        sys.exit(1)
    if source == target:
        print("ERROR: source and target must be different")
        sys.exit(1)

    check_intent(target)

    connstr = f"host={HOST} port={PORT} dbname={DB} user={USER} password={PASSWORD}"
    print(f"Connecting to {DB} on {HOST}:{PORT}...")

    with psycopg.connect(connstr, autocommit=False) as conn:
        source_tables = get_tables(conn, source)
        target_tables = get_tables(conn, target)

        if not source_tables:
            print(f"ERROR: no tables found in schema '{source}' — has it been created?")
            sys.exit(1)
        if not target_tables:
            print(f"ERROR: no tables found in schema '{target}' — run warehouse_tables there first")
            sys.exit(1)

        # Only copy tables that exist in both schemas
        copyable = [t for t in source_tables if t in target_tables]
        skipped  = [t for t in source_tables if t not in target_tables]

        if skipped:
            print(f"WARNING: {len(skipped)} table(s) in '{source}' not found in '{target}' — skipping: {skipped}")

        print(f"\nCopying {len(copyable)} tables from '{source}' → '{target}'...\n")

        with conn.transaction():
            # Disable FK/trigger checks during the bulk load so table copy order
            # doesn't matter (e.g. fct_loans before person_identity). Requires
            # superuser — warehouse_admin has it. Reset at the end.
            conn.execute("SET session_replication_role = replica")

            for table in copyable:
                src = f"{source}.{table}"
                tgt = f"{target}.{table}"

                # Partitioned tables: pre-create the monthly partitions in the
                # target to match the source data, or INSERT routing has nowhere to land.
                key_col = partition_key_column(conn, target, table)
                part_note = ""
                if key_col:
                    n = ensure_target_partitions(conn, source, target, table, key_col)
                    part_note = f"  (+{n} partitions)"

                # Copy by explicit column NAME (not SELECT *) so a column-order
                # mismatch from an out-of-band ALTER doesn't misalign the copy.
                src_cols = get_columns(conn, source, table)
                tgt_cols = set(get_columns(conn, target, table))
                common   = [c for c in src_cols if c in tgt_cols]
                col_list = ", ".join(f'"{c}"' for c in common)

                count = conn.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
                conn.execute(f"TRUNCATE {tgt} CASCADE")
                conn.execute(f"INSERT INTO {tgt} ({col_list}) SELECT {col_list} FROM {src}")
                print(f"  {table:<45} {count:>7,} rows{part_note}")

            conn.execute("SET session_replication_role = DEFAULT")

            # Reset SERIAL/identity sequences so the next INSERT doesn't collide
            # with the copied id values. Copying data carries the id values but
            # not the sequence position — without this, nextval restarts at 1.
            print("\nResetting sequences...")
            conn.execute(
                """
                DO $$
                DECLARE r RECORD;
                BEGIN
                  FOR r IN
                    SELECT n.nspname AS sch, c.relname AS tbl, a.attname AS col,
                           pg_get_serial_sequence(n.nspname||'.'||c.relname, a.attname) AS seq
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_attribute a ON a.attrelid = c.oid
                    WHERE n.nspname = %s AND a.attnum > 0 AND NOT a.attisdropped
                      AND pg_get_serial_sequence(n.nspname||'.'||c.relname, a.attname) IS NOT NULL
                  LOOP
                    EXECUTE format(
                      'SELECT setval(%%L, COALESCE((SELECT MAX(%%I) FROM %%I.%%I), 1), '
                      '(SELECT MAX(%%I) IS NOT NULL FROM %%I.%%I))',
                      r.seq, r.col, r.sch, r.tbl, r.col, r.sch, r.tbl);
                  END LOOP;
                END $$;
                """,
                (target,),
            )

        print(f"\nDone. '{source}' → '{target}' complete.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python copy_schema.py <source> <target>")
        print("       e.g.  python copy_schema.py dev test")
        sys.exit(1)

    copy_schema(sys.argv[1], sys.argv[2])
