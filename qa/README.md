# Warehouse QA Tool

Runs matching DB2 source queries and PostgreSQL staging queries side by side
and shows exactly where the numbers differ.

## Setup

```bash
cd qa
uv sync
```

## Running comparisons

```bash
# Run all query pairs that have matching files in both folders
uv run python src/qa/compare.py

# Run a specific pair
uv run python src/qa/compare.py member_count_by_branch
```

## How it works

Each query pair is two SQL files with the same name:
- `queries/db2/<name>.sql` — runs against DB2 (defaults to D0001T32, Month End 2)
- `queries/warehouse/<name>.sql` — runs against PostgreSQL staging

The tool connects to both, runs each query, and prints a diff table.

## Adding a new comparison

1. Drop the DB2 query (copied from DataGrip) into `queries/db2/<name>.sql`
2. Write the PostgreSQL equivalent into `queries/warehouse/<name>.sql`
3. Add `"<name>": (label_col, count_col)` to `QUERY_CONFIG` in `src/qa/compare.py`
   — or `None` if the query returns a single scalar count
4. Run `uv run python src/qa/compare.py <name>`

## Changing the DB2 target

Edit `MONTH_END_2` in `src/qa/connections.py` or pass a different constant to
`get_db2_connection()` in `compare.py`. Available constants:
- `LIVE`        = D0001LIV (live production)
- `MONTH_END_1` = D0001T31 (Jan/Apr/Jul/Oct)
- `MONTH_END_2` = D0001T32 (Feb/May/Aug/Nov)  ← default for QA
- `QUARTER_END` = D0001T33 (Mar/Jun/Sep/Dec)
