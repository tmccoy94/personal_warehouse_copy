# Operations

How to run, switch, and validate the warehouse: schema setup assets, QA comparison tool, schema environments, jobs and schedules, secrets, and moving the database between machines.

**Contents**
- [Schema Environment Assets](#schema-environment-assets)
- [QA Comparison Tool](#qa-comparison-tool)
- [Schema Environments — dev / test / prod](#schema-environments--dev--test--prod)
- [Copying a schema (dev → test / prod)](#copying-a-schema-dev--test--prod)
- [Jobs and schedules](#jobs-and-schedules)
- [Environment & Secrets](#environment--secrets)
- [Moving the database to a new server](#moving-the-database-to-a-new-server)

---

## Schema Environment Assets

The `warehouse_schema` asset group contains the full setup pipeline. These assets must be run in order when creating a new schema environment or recreating the current one.

| Asset | Purpose |
|---|---|
| `warehouse_schema_mode_select` | Validation gate — reads `WAREHOUSE_SCHEMA` env var, checks it matches the `.schema_intent` file written by `switch_schema.py`, and confirms the value is a recognised schema name (dev/test/prod). All downstream schema and loader assets depend on this. |
| `warehouse_tables` | Reads `WAREHOUSE_SCHEMA`, issues `CREATE SCHEMA IF NOT EXISTS {schema}`, then runs all DDL from `warehouse_schema.sql`. No schema prefix in the SQL — `search_path` handles routing. |
| `warehouse_funcs` | Runs `warehouse_functions.sql` — creates `calc_age()` and `age_group()` utility functions. |
| `warehouse_dim_date` | Seeds `dim_date` with one row per day 2020–2030. Skips if already populated. |
| `warehouse_branch_sentinels` | Inserts `CU Total` and `Unassigned` sentinel rows into `dim_branch`. |
| `warehouse_schema_ready` | Convergence gate — no data, just confirms all setup assets completed. All Tier-1 loader assets depend on this. |

**Schema switching tools** (run from project root on host machine, not inside Docker):
- `switch_schema.py` — GUI with DEV / TEST / PROD buttons. Runs `Set-WarehouseSchema.ps1`, streams output, polls for Dagster to come back up, then opens `warehouse_schema_mode_select` in the browser.
- `Set-WarehouseSchema.ps1 <schema>` — PowerShell script used by the GUI (can also be run directly). Updates `WAREHOUSE_SCHEMA` in `.env` and runs `docker compose down && docker compose up -d`.
- `pipelines/src/pipelines/.schema_intent` — plain-text file written by `switch_schema.py` after a successful switch. Contains only the schema name (e.g., `dev`). Read by `warehouse_schema_mode_select` and by `audit_load()` in `loading.py` as a hard runtime check before any loader writes data.

---

## QA Comparison Tool

The `qa/` folder contains a self-contained tool for comparing DB2 source query results against PostgreSQL staging/warehouse equivalents. Runs **inside the `dagster-webserver` container** where `ibm_db` is already installed — no host-side setup required.

### Running comparisons (from project root)

```bash
# Run a specific named query pair
docker exec dagster-webserver bash -c "cd /opt/dagster/app/qa && PYTHONPATH=/opt/dagster/app/qa/src python src/qa/compare.py member_count_by_branch"

# Run all pairs that have matching files
docker exec dagster-webserver bash -c "cd /opt/dagster/app/qa && PYTHONPATH=/opt/dagster/app/qa/src python src/qa/compare.py"

# Dump raw results side-by-side (useful for row-level differences)
docker exec dagster-webserver bash -c "cd /opt/dagster/app/qa && PYTHONPATH=/opt/dagster/app/qa/src python src/qa/dump.py <query_name>"

# Run home-branch member comparison against the May snapshot DB
docker exec dagster-webserver bash -c "KEYSTONE_DB_OVERRIDE=D0001T32 PYTHONPATH=/opt/dagster/app/qa/src python /opt/dagster/app/qa/run_home_branch_qa.py 2026-05-31"
```

### How it works

- `qa/queries/db2/<name>.sql` runs against DB2 (uses `KEYSTONE_DB_OVERRIDE`; defaults to LIVE)
- `qa/queries/warehouse/<name>.sql` runs against PostgreSQL `staging` + active warehouse schema
- `compare.py` auto-detects comparison mode: scalar count, grouped count table, or set diff
- `dump.py` prints both raw DataFrames side by side — use when you need column-level values
- `run_home_branch_qa.py` applies the actual `assign_branch_with_fallback` Python logic and compares branch-by-branch against `fct_home_branch_members_summary`

### KEYSTONE_DB_OVERRIDE

Set in `.env` and restart containers to target a frozen DB2 snapshot for QA. Values:
- `D0001LIV` — live production (default when override unset or empty)
- `D0001T31` — Month End 1 (Jan/Apr/Jul/Oct)
- `D0001T32` — Month End 2 (Feb/May/Aug/Nov) — preferred QA target
- `D0001T33` — Quarter End (Mar/Jun/Sep/Dec)

**Important:** `KEYSTONE_DB_OVERRIDE` only takes effect when `get_connection()` is called with no explicit `db_name`. The env var is checked in `ks_db_connections.get_ks_connection()` only when `db_name is None`. Monthly assets (`job_type="monthly"`) override this and always pick the snapshot DB matching `context.partition_key` ([Decision 29](decisions_log.md)) — `KEYSTONE_DB_OVERRIDE` is ignored with a warning.

### Adding a new query pair

1. Drop the DB2 query into `qa/queries/db2/<name>.sql`
2. Write the PostgreSQL equivalent into `qa/queries/warehouse/<name>.sql`
3. Run it — comparison mode is auto-detected from the result shape

### Debugging discrepancies

1. Run the count comparison to see which labels differ
2. Write a `_debug` version that returns individual `(id, label)` rows for the differing subset
3. Run `compare.py` on the debug query — set-diff mode shows exactly which ids are missing
4. Run `dump.py` on the debug query to see column-level values

---

## Schema Environments — dev / test / prod

All three environments live in the **same Postgres database** — `dev`, `test`, `prod` are schema namespaces, not separate containers.

**How it works.** `WAREHOUSE_SCHEMA` env var in `.env` controls which schema is active. `WarehousePgResource.get_engine()` reads this and sets `search_path` at connection time — all SQL runs against the right schema with no code changes. `warehouse_schema.sql` and all inline SQL have no schema prefix; PostgreSQL resolves table names through `search_path`.

**The `staging` schema** is always explicitly qualified as `staging.` and is unaffected by `WAREHOUSE_SCHEMA`. It's shared across all environments in the same Postgres instance.

**Switching schemas:**
1. Run `python switch_schema.py` from the project root — a GUI opens with DEV / TEST / PROD buttons
2. Click the target schema — `Set-WarehouseSchema.ps1` runs, updates `.env`, restarts the containers
3. When containers are back up, click "Open validation asset in Dagster" in the GUI
4. In Dagster UI: materialize `warehouse_schema_mode_select` — validates the intent file matches the running env var
5. Materialize `warehouse_tables` — creates the schema if absent, then runs all DDL

**Hard runtime gate.** `audit_load()` in `loading.py` checks the `.schema_intent` file (written by `switch_schema.py`) against the running `WAREHOUSE_SCHEMA` env var before every loader run. Mismatch → asset fails immediately before writing any data. This is independent of Dagster's dep graph.

**Iteration workflow.** Drop the active schema in pgAdmin (`DROP SCHEMA IF EXISTS dev CASCADE`) and re-run `warehouse_tables` to rebuild from scratch. `staging` is untouched.

---

## Copying a schema (dev → test / prod)

Use `copy_schema.py` at the project root to copy all warehouse data from one schema to another:

```bash
python copy_schema.py dev test
python copy_schema.py dev prod
```

**Prerequisites:** The target schema must already have its tables created. Run `warehouse_tables` in Dagster targeting the destination schema first (via `switch_schema.py`), then run the copy.

The script truncates each table in the target and inserts from the source in a single transaction. Prints row counts for every table. Reads credentials from `.env`.

---

## Jobs and schedules

Two primary Keystone jobs (plus HubSpot):

| Job | Assets | Schedule |
|---|---|---|
| `ks_daily_job` | 14 `staging.ks_*` mirrors + `dim_branch` + `dim_employee` + `ks_person_snapshot_stage_table` + 6 daily snapshot assets | 11pm nightly (`nightly_ks_schedule`) |
| `ks_monthly_job` | Monthly person snapshots + `fct_teller_activity_table` + `fct_teller_out_of_balance_table` | 1am on 1st of month (`monthly_ks_schedule`) |
| `hubspot_nightly_job` | All `hubspot_loaders` group assets | 1am nightly |

The daily schedule passes today's date as a partition key — partitioned assets use it, non-partitioned assets ignore it.

**Staging freshness guard:** Every downstream snapshot asset in `ks_daily_job` calls `assert_staging_fresh(engine, "staging.ks_<table>", partition_date)` before writing. If the upstream staging table wasn't loaded today the asset fails immediately rather than writing stale data. Add this guard to any new downstream asset that reads from a staging table (see [`engineering.md → Core principles`](engineering.md#core-principles)).

---

## Environment & Secrets

- `.env` — runtime config and credentials, not committed to git
- `secrets/*.txt` — Docker secrets mounted at `/run/secrets/` inside containers
- All asset code reads credentials from `os.environ` — never hardcode

New external source credentials get added in two places:
1. `.env` (the value)
2. `docker-compose.yaml` under both `dagster-webserver` and `dagster-daemon` environment blocks

**SSL verification** is intentionally disabled (`verify=False`) on JasperServer requests — internal CA validation isn't feasible on-prem (everything runs on a secure private network behind the corporate gateway).

---

## Moving the database to a new server

**Design:** Moving the project directory + the latest file from `./backups/` to a new machine with Docker is sufficient to restore full operation — Dagster state lives in Postgres, captured by `pg_dump`. No external service dependencies for state.

The `warehouse-backup` sidecar container runs `pg_dump` daily and stores compressed backups in `./backups/`. Moving to a new machine is a matter of copying the project folder plus the latest backup file.

### Step 1 — Take a fresh backup on the current machine

Before moving, force a fresh dump so you have the latest data:

```bash
# From the project root
docker exec warehouse-backup /backup.sh
```

The backup lands in `./backups/daily/` as a `.sql.gz` file. The most recent file is what you want.

Alternatively, trigger a manual pg_dump directly:

```bash
docker exec warehouse-postgres pg_dump \
  -U warehouse_admin \
  -d warehouse \
  -F c \
  -f /tmp/warehouse_manual.dump

docker cp warehouse-postgres:/tmp/warehouse_manual.dump ./warehouse_manual.dump
```

### Step 2 — Copy the project to the new machine

Copy the entire project directory AND the backup file to the new server. The minimum you need:

```
dagster_postgres_docker/          ← full project folder
  docker-compose.yaml
  .env                            ← credentials — transfer securely (not over email)
  secrets/                        ← postgres_password.txt etc.
  pipelines/
  dagster/
  data/
  backups/daily/warehouse-XXXX.sql.gz   ← the backup you just took
```

Do **not** copy `dagster/storage/` — that holds Dagster's run history which will be rebuilt from the Postgres backend.

### Step 3 — Start the stack on the new machine

```bash
cd dagster_postgres_docker
docker compose up -d
```

Wait for `warehouse-postgres` to be healthy (check with `docker compose logs warehouse-postgres`).

### Step 4 — Restore the backup

The Postgres data volume starts empty on a new machine. Restore from the backup:

```bash
# Copy the backup file into the running container
docker cp ./backups/daily/warehouse-XXXX.sql.gz warehouse-postgres:/tmp/

# Restore (adjust filename)
docker exec warehouse-postgres bash -c \
  "gunzip -c /tmp/warehouse-XXXX.sql.gz | psql -U warehouse_admin -d warehouse"
```

If you used the custom-format dump (`-F c`) instead:

```bash
docker exec warehouse-postgres pg_restore \
  -U warehouse_admin \
  -d warehouse \
  --no-owner \
  /tmp/warehouse_manual.dump
```

### Step 5 — Verify

1. Open pgAdmin at `http://localhost:5050` and confirm schemas and row counts look right
2. Open Dagster at `http://localhost:3000` and confirm assets show their last materialization dates
3. Run `warehouse_schema_mode_select` to confirm the active schema
4. Check `SELECT COUNT(*) FROM load_audit ORDER BY started_at DESC LIMIT 10` to see the last few runs came through

### Notes

- The `staging` schema and `raw` schema are included in the pg_dump and will be restored
- `dev`/`test`/`prod` schemas are all included — you may want to `DROP SCHEMA IF EXISTS dev CASCADE` on the new prod server and rerun `warehouse_tables` for a clean slate
- `.env` must be set correctly on the new machine before starting — especially `WAREHOUSE_SCHEMA=prod` for the live server
- Backup retention is controlled by `BACKUP_KEEP_DAYS`, `BACKUP_KEEP_WEEKS`, `BACKUP_KEEP_MONTHS` in `.env`
