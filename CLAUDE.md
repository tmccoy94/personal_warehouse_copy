# CLAUDE.md

Guidance for Claude Code working in this repository. The authoritative reference for the warehouse — schema, DDL, engineering principles, testing, conventions, JasperServer/HubSpot patterns, QA tooling, schema environments, known issues — lives in **`docs/`** (entry point: `docs/README.md`). This file is short on purpose: project orientation, the few things that come up daily, and pointers into the docs when you need depth.

---

## Current stage

**v1 asset implementation complete — migrating to the production VM.** All v1 assets are functional, warehouse data is validated against source, and the Zoho connection is tested. The active task list is `docs/vm_migration.md` (one-time checklist; delete after the move). A fresh post-v1 roadmap will be written after the VM is live — the Stage 3/4 planning folder (`schema_design/`) was retired once `docs/` superseded it.

**Key context files:**
- `docs/README.md` — entry point and map; tells you which doc covers which area
- `docs/overview.md` — project summary, full table inventory, known issues
- `docs/tables.md` — DDL reference for every warehouse object (ctrl-F by table name)
- `docs/conventions.md` — naming + design + view rules
- `docs/engineering.md` — Dagster asset principles, testing, SQL style, JasperServer/HubSpot patterns
- `docs/operations.md` — schema env switching, QA tool, jobs/schedules, secrets, moving the DB
- `docs/development.md` — branching, local setup, PR + deploy workflow, commit standards
- `docs/bi_connections.md` — connecting Zoho Analytics and Power BI
- `docs/decisions_log.md` — chronological design decisions
- `docs/vm_migration.md` — VM move checklist (current focus)

Live DB2 source queries are in `pipelines/src/pipelines/queries/` — there is no separate reference copy; treat those files as the source of truth.

---

## Project purpose

A portable, local-first data platform that extracts data from internal credit union source systems (IBM DB2 / Keystone, JasperServer reports, HubSpot CRM, accounting CSVs) and loads it into a structured PostgreSQL warehouse for BI reporting (Zoho dashboards). The whole stack runs in Docker so it migrates with a directory copy + `pg_dump` restore.

**Design philosophy:** Build for flexibility and modularity. Avoid hard lock-in — any source system, tool, or Docker service should be swappable. Prefer simple, explicit solutions over clever abstractions. Detailed engineering principles (idempotency, fail-loud, zero tolerance for count discrepancies, etc.) are in `docs/engineering.md`.

**Network environment:** On-prem, secure private network. Source systems are only reachable from on-prem machines. SSL validation is disabled on JasperServer requests by design — proper CA validation isn't feasible internally and isn't a concern given the network boundary.

---

## Running the stack

```bash
# Start all services
docker compose up -d

# Rebuild pipelines image after dependency changes
docker compose up -d --build

# Logs
docker compose logs -f dagster-webserver
docker compose logs -f dagster-daemon

# Stop
docker compose down
```

**UIs:** Dagster at http://localhost:3000, pgAdmin at http://localhost:5050

**5 Docker services** on the `data-platform` bridge network: `warehouse-postgres` (postgres:16-alpine), `dagster-webserver` (custom image), `dagster-daemon` (same image), `warehouse-backup` (postgres-backup-local sidecar → `./backups/`), `pgadmin`.

---

## Editing pipelines

`pipelines/src/` is bind-mounted into both Dagster containers — Python changes are picked up on the next asset run without rebuilding. Dependency changes (`pyproject.toml`) require a rebuild.

The pipeline image uses `uv`. Add dependencies by editing `pipelines/pyproject.toml`, then run `uv lock` from `pipelines/`, then rebuild the image.

**Loading SQL files** — `.sql` files live in `pipelines/src/pipelines/queries/`. Use `load()` from that package to read them; never use `Path(__file__)` hacks:

```python
from pipelines.queries import load
sql = load("my_query.sql")
```

**Running unit tests** (no Docker required):

```bash
# From the pipelines/ directory
uv run pytest
```

---

## Commit cadence

Full standards live in `docs/development.md → Commit standards`. Behavioral expectation for Claude in **every** session — be proactive about this, the user has asked to be reminded:

- **Track accumulated changes as you work.** When a logical unit of work is complete (asset materializes, tests pass, edit is coherent), or when the uncommitted working-tree diff has grown past roughly **200 changed lines or ~5 files**, run `git status` / `git diff --stat` and **ask the user whether they'd like to commit.** Phrase it as an offer with a one-line summary of what's pending.
- **Offer, don't auto-commit.** Never commit without the user's explicit go-ahead. (One large logical change — e.g. a docs reorg — can legitimately exceed the size guard; use judgment, favor logical boundaries over the exact number.)
- **When the user agrees,** group the changes into cohesive commits — split unrelated work into separate commits rather than one mixed blob. Imperative subject line, body explaining the *why* when non-obvious, and the `Co-Authored-By` trailer.
- **Respect the branching strategy** (`docs/development.md`) — feature/fix/schema/etc. branches for real work; the user may opt to commit trivial changes straight to `main`.

---

## Pipeline design principles

These govern every Dagster asset. Full reasoning + examples in `docs/engineering.md`; the short list:

- **Idempotency.** Re-running an asset produces the same result as running it once. Upsert or truncate/reload — never plain `INSERT` that accumulates.
- **Cadence agnosticism.** `snapshot_date DATE` is the time key. Schema must not encode time grain. Dagster controls what date is passed in.
- **Fail loudly, not silently.** Raise on unknown lookup values, NULLs where unexpected, shape mismatches. Don't skip rows or insert defaults. Silent failures take months to find.
- **Zero tolerance for count discrepancies.** Warehouse counts must match source exactly. KPIs feed board reporting and regulatory filings — there is no "close enough." Trace every discrepancy to root cause.
- **Source fidelity.** Load raw values verbatim. Derivations, ratios, formatted display values belong in views or the BI tool.
- **Asset graph as documentation.** Declare `deps=[...]` explicitly. Assets write only to their declared output — no hidden side effects.
- **Partition immutability.** Once a `snapshot_date` partition is loaded, it's permanent. Re-runs truncate and reload only the current partition.
- **Observability by default.** Every load writes to `load_audit`: batch_id, row count, start/end, status.
- **Stage once, read many — two tracks.** Daily downstream assets read from `staging.ks_*` (refreshed nightly from LIVE). Monthly downstream assets read from `staging.ks_*_monthly` (refreshed by the monthly job from the partition's frozen snapshot DB). Never cross the tracks — monthly assets reading daily staging silently drift past EOM.
- **Staging freshness guard.** Every downstream snapshot asset calls `assert_staging_fresh(engine, "staging.ks_<table>", partition_date)` before writing — raises if staging wasn't loaded on the partition date.

**Source-system bug exception.** Default assumption is the DB2 query is correct. If something looks like a bug in the source query itself (stale hardcoded dates, broken math, contradictory exclusions), flag it explicitly and ask — don't blindly replicate broken behavior in the warehouse.

---

## SQL writing rules

- **No hardcoded date literals in `.sql` files.** Use `:start_date`, `:end_date`, `:snapshot_date` placeholders bound from the asset's partition context. A literal `'2026-01-01'` in a query file is always wrong.
- **Descriptive table aliases.** Strip the `dim_`/`fct_` prefix and use the entity name (`branch`, `loans`, `members_summary`). No single-letter aliases (`s`, `d`, `b`). Exception: `dim_date` keeps its full name since `date` is reserved.
- **When two tables of the same entity type are referenced**, qualify the alias (`person_daily`, `person_monthly`).
- In `ON CONFLICT DO UPDATE` clauses, use the `INSERT INTO ... AS <alias>` alias in the `WHERE`, not the schema-qualified table name.

---

## Warehouse schemas

- `staging` — All explicitly qualified as `staging.` and unaffected by `WAREHOUSE_SCHEMA`. Three layers:
  - **Pipeline intermediary:** `staging.person_snapshot_stage` — loaded fresh each run before downstream snapshot assets consume it
  - **Daily CORE mirrors:** `staging.ks_*` (14 tables) — nightly refresh from LIVE. ELT source for all daily downstream assets. Plus `staging.ks_env` (single-row CORE.ENV posting-date mirror).
  - **Monthly CORE mirrors:** `staging.ks_*_monthly` (14 tables) — refreshed per monthly partition from the frozen snapshot DB (MONTH_END_1/2 or QUARTER_END). ELT source for `fct_members_summary` and all monthly loan facts. Plus `staging.ks_env_monthly` holding the partition EOM. `ks_transaction_monthly`/`ks_monetary_monthly` are loaded by copying from the daily 90-day rolling staging (not a fresh DB2 pull) to save ~20 minutes per run.
- `dev` / `test` / `prod` — Analytics warehouse tables in the active environment. Same DDL (`warehouse_schema.sql`), different schema namespace. `staging` is unaffected by changes to these schemas. **Before changing or dropping anything in these schemas, see "Schema change guidance" below.**
- `raw` — Deprecated; previously held what's now in `staging.ks_*`. Safe to `DROP SCHEMA raw CASCADE` once confirmed empty.

### Schema environments — dev / test / prod

All three live in the **same Postgres database** as schema namespaces, not separate containers. `WAREHOUSE_SCHEMA` env var drives which is active; `WarehousePgResource.get_engine()` injects it as `search_path` at connection time. One DDL file (`warehouse_schema.sql`) creates any environment — no schema prefix in the SQL, `search_path` handles routing.

**To switch:**
1. `python switch_schema.py` from project root — GUI with DEV / TEST / PROD buttons
2. Click target — `Set-WarehouseSchema.ps1` runs, updates `.env`, restarts containers
3. When containers are back up, click "Open validation asset in Dagster" in the GUI
4. In Dagster UI: materialize `warehouse_schema_mode_select` — validates intent file matches running env var
5. Materialize `warehouse_tables` — creates the schema if absent, runs all DDL

**Hard runtime gate:** `audit_load()` in `loading.py` checks the `.schema_intent` file (written by `switch_schema.py`) against the running `WAREHOUSE_SCHEMA` before every loader write. Mismatch → asset fails before writing any data. Independent of Dagster's dep graph.

**Iteration (large restructures only):** Drop the active schema in pgAdmin (`DROP SCHEMA IF EXISTS <active_schema> CASCADE`) and re-run `warehouse_tables` to rebuild. `staging` is preserved. For anything smaller — see "Schema change guidance" immediately below.

More detail in `docs/operations.md → Schema Environments`.

### Schema change guidance — read before writing any schema-qualified SQL

**Check the active schema first.** Read `WAREHOUSE_SCHEMA` in `.env` (and confirm against `pipelines/src/pipelines/.schema_intent`) before writing any `ALTER`/`DROP`/schema-qualified statement. **Never assume `dev`.**

**Match the tool to the size of the change:**
- **Renaming/retyping a column, adding one column** → a plain `ALTER TABLE <active_schema>.<table> RENAME COLUMN ... / ALTER COLUMN ... / ADD COLUMN ...`. This is instant, non-destructive, preserves every row, and Postgres auto-updates dependent `v_*` view definitions (views track columns by position, not name) — recommend this as the default for small edits.
- **`DROP SCHEMA ... CASCADE` + re-materialize `warehouse_tables`** → reserved for genuinely large restructures (multiple tables, major DDL reshape) where hand-writing migrations isn't worth it. This destroys all data in that schema and drops every dependent view.

**Always confirm before any destructive operation** — `DROP SCHEMA`, `DROP TABLE`, `TRUNCATE`, anything with `CASCADE`. State which schema it targets and what will be lost, and wait for explicit go-ahead before running it. Only skip this confirmation if the user has explicitly said they're experienced with this workflow and don't need to be asked each time. Default to protecting data, especially in `prod`.

---

## Warehouse table naming conventions

| Prefix | Meaning | Examples |
|---|---|---|
| *(none)* | Canonical warehouse entity — spans source systems | `person_identity`, `person_source_identifier`, `person_contact` |
| `ks_` | Keystone (DB2/CORE) source data | `ks_credit_score_snapshot`, `ks_member_branch_snapshot` |
| `hs_` | HubSpot source data | `hs_contacts`, `hs_deals` |
| `dim_` | Curated analytics dimension | `dim_branch`, `dim_employee`, `dim_date` |
| `fct_` | Analytics fact or aggregate | `fct_members_summary`, `fct_loans` |
| `v_` | BI presentation view — no stored data | `v_members_monthly`, `v_home_branch_members_monthly` |

**Grain suffix** on `ks_`/`hs_` tables: *(none)* = current state truncate/reload, `_snapshot` = partitioned by `snapshot_date`, `_events` = append-only. Suffix is required even when only one variant exists — grain must be explicit.

**Time keys** — always `snapshot_date DATE`, never `month_id INT`. Cadence-agnostic. `month_id` exists only in `dim_date` as a GROUP BY convenience. For monthly loads, store the last calendar day (`2026-01-31`).

**Surrogate keys** — Every dim uses `<table>_key` (`branch_key`, `employee_key`, `person_key`). Source IDs in `source_*_serial` columns + `source_system TEXT` + `UNIQUE (source_system, source_*_serial)`. On core conversion, only `source_*_serial` values change — surrogate keys and fact FKs are untouched. Don't use `core_*` column names.

**Aggregate fact tables** use `_summary` suffix (signals pre-aggregated, not a time grain). Time-grain-specific views (`v_members_monthly`) sit on top.

**Data types** — money: `NUMERIC(14,2)`; percentages: stored as decimals (`0.1251` not `12.51`, divide by 100 on load); unbounded text: `TEXT`; timestamps: `TIMESTAMPTZ`.

Full conventions in `docs/conventions.md` (naming + design rules) and `docs/engineering.md → SQL conventions`.

---

## Asset naming + locations

**Asset name = target table name + `_table`** — `dim_branch_table`, `ks_fct_loans_table`, `hs_contacts_table`. Reads as "the dim_branch table" rather than "load dim_branch".

**Locations** (`pipelines/src/pipelines/defs/assets/<package>/`):
- `warehouse_schema/` — DDL + seed data (`warehouse_schema_ready` gate)
- `warehouse_loaders/` — Keystone-sourced (raw mirrors, dims, snapshots, fct_loans, fct_members)
- `jasperserver_loaders/` — JasperServer API (teller)
- `hubspot_loaders/` — HubSpot CRM API (inbound `hs_*` warehouse loaders)
- `hubspot_exports/` — Outbound HubSpot files (live Keystone pull + dropped CRM export → import CSV); writes a file, not the warehouse
- `file_loaders/` — Drop-folder CSV + manual entries + one-time backfills

**Group names** (by source and cadence): `warehouse_schema`, `ks_daily_loaders` (nightly staging mirrors, dims, daily snapshots — sourced from LIVE), `ks_monthly_loaders` (monthly-partitioned facts + person_snapshot_monthly — sourced from the prior-month frozen snapshot DB), `jasperserver_loaders`, `hubspot_loaders` (inbound `hs_*` warehouse loaders), `hubspot_exports` (outbound HubSpot file builders — no warehouse write), `file_loaders`, `repair_ops`. Both `ks_*_loaders` groups share the `ks_` prefix so they sort together in the Dagster UI sidebar.

**Asset deps — tiered:**
- Tier-1 (no warehouse data upstream): `deps=[warehouse_schema_ready]`
- Tier-2+ (depends on another loader): `deps=[<upstream loader>]` — schema readiness inherited transitively

Auto-discovered via `load_from_defs_folder` — no manual registration. Resources, JasperServer/HubSpot call patterns, and full examples are in `docs/engineering.md → Adding new assets`.

---

## CORE DB2 domain knowledge

- **Membership** is defined by an open `CORE.SHARE` with `ID = '0001'` (the General Share). A person is a member if and only if they have at least one open General Share.
- **Two member counting methodologies:** opening branch (`SHARE.BRANCH_SERIAL` of oldest share) and home branch (ZIP-territory assignment first, then `CORE.CU_HOME_BRANCH` fallback). Both legitimate; feed different reports; go into different warehouse tables.
- **Loan hierarchy:** `CORE.APP` → `CORE.LOAN_REQUEST` → `CORE.LOAN`. One app can have multiple loan requests; one loan request maps to at most one loan.
- **Branch NULL problem:** `LOAN_REQUEST.BRANCH_SERIAL` is NULL ~80% of the time. `LOAN.BRANCH_SERIAL` (disbursing branch on the funded loan) is fully populated and is the correct source for branch-level loan rollups. `fct_loans_by_branch_summary` is derived from `staging.ks_loan` for this reason.
- **Serials are integers**, not strings — `BIGINT` for person/loan, `INT` for employee/user, `SMALLINT` for branch.
- **OP Branch IS Overland Park Branch** — same physical branch, two names. CORE stores `BRANCH.DESCRIPTION = "Overland Park Branch"`; the warehouse renamed `dim_branch.branch_name` to `"OP Branch"` to match staff terminology. Anywhere a branch name comes from CORE (DB2 query results, CSV backfills using CORE labels) and gets matched against `dim_branch.branch_name`, normalize through `CORE_TO_WAREHOUSE_BRANCH_NAME` in `warehouse_loaders/branch.py`. Direct equality matches against `"Overland Park Branch"` in warehouse SQL are bugs that will silently drop members.
- **`record_hash`** on `person_snapshot_daily` is a SHA-256 of biographic columns. Written but not yet used for skip-if-unchanged deduplication (deferred to v1.1).

---

## PII

`person_snapshot_daily` / `person_snapshot_monthly` carry name + birth_date. `person_contact` carries address, phone, email (across all source systems). A masking view (`v_person_public` or similar) is intended for general BI access but not yet built.

---

## QA tool

`qa/` contains a comparison tool for DB2 source queries vs PostgreSQL warehouse equivalents. Runs inside `dagster-webserver` (has `ibm_db`).

```bash
# Compare a named query pair (DB2 vs warehouse)
docker exec dagster-webserver bash -c "cd /opt/dagster/app/qa && PYTHONPATH=/opt/dagster/app/qa/src python src/qa/compare.py <query_name>"

# Home-branch member comparison against frozen snapshot for May 2026
docker exec dagster-webserver bash -c "KEYSTONE_DB_OVERRIDE=D0001T32 PYTHONPATH=/opt/dagster/app/qa/src python /opt/dagster/app/qa/run_home_branch_qa.py 2026-05-31"
```

`KEYSTONE_DB_OVERRIDE` values: `D0001LIV` (default), `D0001T31` (Jan/Apr/Jul/Oct ME), `D0001T32` (Feb/May/Aug/Nov ME), `D0001T33` (Mar/Jun/Sep/Dec QE). Override is **ignored for monthly assets** — partition_key determines the snapshot DB. Full QA workflow + debugging in `docs/operations.md → QA Comparison Tool`.

---

## Common references

- **Engineering principles, testing philosophy, code style, SQL conventions, adding assets, JasperServer/HubSpot patterns** → `docs/engineering.md`
- **Schema environments, QA tool, jobs/schedules, secrets, moving the DB** → `docs/operations.md`
- **Branching, local setup, PR/deploy workflow, commit standards** → `docs/development.md`
- **Connecting BI tools (Zoho, Power BI)** → `docs/bi_connections.md`
- **Table naming + design + view rules** → `docs/conventions.md`
- **Per-table DDL** → `docs/tables.md`
- **Known issues / pending implementation** → `docs/overview.md → Known Issues / In Progress`
- **Why things were done the way they were** → `docs/decisions_log.md`

**When a schema or convention changes**, update the docs:
- `docs/tables.md` — DDL block + source notes for the affected table
- `docs/overview.md` — Full Table Inventory row, Summary counts if they shifted
- `docs/decisions_log.md` — add a numbered entry if the *why* isn't obvious from the diff
- `docs/conventions.md` / `docs/engineering.md` — only if you changed a rule that applies repo-wide

---

## Environment & secrets

- `.env` — runtime config + credentials, not committed
- `secrets/*.txt` — Docker secrets mounted at `/run/secrets/` in containers
- All asset code reads credentials from `os.environ` — never hardcode

Adding a new external credential: update `.env` AND `docker-compose.yaml` under both `dagster-webserver` and `dagster-daemon` environment blocks.

---

## Help

- `/help` — Claude Code usage help
- Feedback / issues: https://github.com/anthropics/claude-code/issues
