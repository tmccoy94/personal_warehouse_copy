# Engineering

Principles and patterns that apply to every Dagster asset in the project. Read this before writing a new asset or touching an existing one.

**Contents**
- [Core principles](#core-principles)
- [Testing philosophy](#testing-philosophy)
- [Code style](#code-style)
- [SQL conventions](#sql-conventions)
- [Adding new assets](#adding-new-assets)
- [Historical backfill assets](#historical-backfill-assets)
- [JasperServer report calls](#jasperserver-report-calls)
- [HubSpot API calls](#hubspot-api-calls)

---

## Core principles

These principles govern every Dagster asset in the project. They exist because pipeline bugs are expensive to detect after the fact — KPIs the organization depends on get rebuilt months later from incident postmortems. Default to loud failure, traceability, and idempotency over clever shortcuts.

**Idempotency.** Running an asset twice must produce the same result as running it once. Use upsert or truncate/reload patterns; never plain `INSERT` that accumulates. A re-run is a recovery, not a corruption.

**Cadence agnosticism.** Assets and schema must not encode time grain. `snapshot_date DATE` is the time key — Dagster controls what date gets passed in. Moving from monthly to daily loading must not require a DDL change. `month_id INT (YYYYMM)` exists only in `dim_date` as a GROUP BY convenience column.

**Fail loudly, not silently.** If data doesn't match expectations (unknown lookup value, NULL where unexpected, shape mismatch), raise an exception. Do not skip rows quietly or insert partial/default data. Silent failures are harder to find than loud ones. Concrete example: the home-branch member asset raises if `assign_branch_with_fallback` returns a name that has no `dim_branch` row — preventing the silent-drop bug that lost 15 Unassigned + 44 Overland-Park-fallback members per month before the guard was added ([Decisions 27, 28](decisions_log.md)).

**Zero tolerance for count discrepancies.** Member counts, financial ratios, and loan portfolio numbers are KPIs the credit union reports to its board and to the NCUA. There is no acceptable margin of error. A warehouse count either matches the source system exactly or it is wrong — there is no "close enough" or "timing artifact" that justifies a difference. Every discrepancy is traced to root cause and fixed before the data is considered valid. Exception: if a source query itself looks broken (stale hardcoded dates, math errors), flag it and ask before "fixing" by matching the broken behavior.

**Asset graph as documentation.** Declare dependencies with `deps=[...]` explicitly. If the Dagster UI asset graph doesn't reflect the real data flow, something is underdocumented. Assets write only to their declared output — no hidden side effects.

**Source fidelity.** Load values as close to verbatim from the source as possible. Transformations, ratios, formatted display values, and derived columns belong in views or the BI tool, not in load assets. Raw values must always be traceable back to the source query.

**Partition isolation / immutability.** Once a `snapshot_date` partition is fully loaded, it is permanent. A re-run truncates and reloads only the current partition — historical partitions are never touched.

**Observability by default.** Every load records `batch_id`, row count, start/end times, and status in `load_audit`. You must always be able to answer "what ran, when, and what did it produce?" without digging through Dagster logs.

**Configuration over code forks.** One codebase runs in dev/test/prod by changing env vars. No `if env == 'dev'` branches in asset code. `WAREHOUSE_SCHEMA` + PostgreSQL `search_path` enforces this for the database layer.

**Stage once, read many — daily and monthly tracks.** DB2 table mirrors are loaded into staging on two cadences. The daily `staging.ks_*` tables are refreshed nightly from LIVE by `ks_daily_job` and feed every daily downstream asset. The monthly `staging.ks_*_monthly` tables are refreshed by `ks_monthly_job` from the partition's frozen snapshot DB (MONTH_END_1/2 or QUARTER_END) and feed every monthly downstream asset (`fct_members_summary`, `fct_loan_*` monthly facts). Both tracks ensure assets within a run see identical data, reduce DB2 load, and stay inspectable in pgAdmin. **The two tracks must not cross** — monthly facts that read from daily staging silently drift (members joining/leaving between EOM and run time), and daily facts that read from monthly staging would be stuck on the snapshot DB's frozen date. See [Decision 36](decisions_log.md).

**Staging freshness guard.** Every downstream snapshot asset calls `assert_staging_fresh(engine, "staging.ks_<table>", partition_date)` at its start before writing any data. Raises `RuntimeError` if the upstream staging table was not loaded on the current partition date — prevents silently writing a partition with stale data. Add the guard to any new asset that reads from staging.

---

## Testing philosophy

**Unit tests** — pure Python logic, no database or external dependency. Any function that transforms data, validates input, or computes a value is a unit test candidate. Run locally with `uv run pytest` from `pipelines/` — no Docker. Current targets: `pipelines/utils.py` (age calc, hashing, date math) and `pipelines/defs/assets/warehouse_loaders/fct_members.py` (`assign_branch_with_fallback`).

**Integration tests** — full asset execution against the real warehouse Postgres container, in `pipelines/tests/integration/`. They catch what unit tests cannot: SQL bugs, FK constraint violations, upsert correctness, stale view column references. Marked `@pytest.mark.integration`; skipped automatically when Postgres is unreachable, so plain `uv run pytest` stays green without Docker.

How the suite isolates itself (`tests/integration/conftest.py`):
- Connects from the host on `localhost:POSTGRES_PORT` (credentials from real env vars or the project `.env`) and operates **exclusively in the disposable `test` schema** — `search_path` is pinned at connection time.
- Drops and rebuilds `test` from the real DDL (`warehouse_schema.sql` + `warehouse_functions.sql`) once per session, then seeds synthetic fixtures only. **Don't keep anything you care about in the `test` schema.**
- Never switches the running containers' `WAREHOUSE_SCHEMA` or touches the real `.schema_intent` file — the `audit_load()` runtime gate is monkeypatched in-process per test.
- A hard assert refuses to target `dev`/`prod`/`staging`/`public`.

**Asset checks** — inline post-load assertions using Dagster's `@asset_check`. Use for lightweight validation: row count > 0, no NULLs in key columns, `snapshot_date` matches expected value. Add incrementally as assets are built out.

**Priority order:**
1. Unit-test any pure utility function when it's written.
2. Integration-test any asset that does a destructive operation (truncate/reload, upsert).
3. Schema creation assets (`warehouse_tables`, `warehouse_funcs`) are exercised indirectly — the integration suite runs their DDL and asserts every expected table and view exists and selects cleanly.

```bash
# From the pipelines/ directory
uv run pytest                      # unit tests + integration (integration skips if Docker is down)
uv run pytest -m "not integration" # unit tests only
uv run pytest -m integration       # integration only (requires docker compose up -d)
```

---

## Code style

**Verbosity goal.** Prefer descriptive names and explicit code over terse shortcuts. The bar: another programmer should read the code at a glance, and a less-experienced developer should make reasonable sense of it without reverse-engineering conventions. Applies to variable names, function names, SQL aliases, and type annotations — be clear, not clever. Avoid extreme verbosity (fully schema-qualifying every column 8 times in one statement) but default toward the more readable option when there's a choice.

**Python type annotations.** When a function is not type-flexible — meaning passing the wrong type will silently produce wrong results or raise at runtime — annotate parameters explicitly, including container element types (e.g., `pd.Series[date]`, `pd.Series[int]`, `list[str]`). Don't rely on the caller to infer constraints from the function body.

---

## SQL conventions

**No hardcoded date literals in any query file.** Date parameters come from the asset's partition context at runtime. Query files use named placeholders (`:start_date`, `:end_date`, `:snapshot_date`) that the asset fills in from `partition_key` or computed values like `last_day_of_month(partition_key)`. A hardcoded date like `'2026-01-01'` in a `.sql` file is always wrong — makes the query non-reusable across partitions and silently loads wrong data.

**Table aliases must be descriptive — never single-letter shortcuts like `s`, `d`, or `b`.** Use the entity name as the alias — strip `dim_`/`fct_` and the schema prefix. Exception: `dim_date` keeps its full name since `date` is a reserved keyword.

```sql
-- correct
FROM dev.fct_members_summary AS members_summary
JOIN dev.dim_date AS dim_date ON dim_date.full_date = members_summary.snapshot_date
JOIN dev.dim_branch AS branch ON branch.branch_key = members_summary.branch_key

-- wrong
FROM dev.fct_members_summary AS s
JOIN dev.dim_date AS d ON d.full_date = s.snapshot_date
```

When a query references multiple tables of the same entity type, qualify the alias instead of fully spelling out the schema:

```sql
JOIN dev.person_snapshot_daily AS person_daily ON ...
JOIN dev.person_snapshot_monthly AS person_monthly ON ...
```

Applies to `.sql` files in `pipelines/src/pipelines/queries/`, inline SQL in Python assets, and the `WHERE` clause of `ON CONFLICT DO UPDATE` (use the alias from `INSERT INTO ... AS <alias>` instead of the schema-qualified name).

---

## Adding new assets

Assets are auto-discovered via `load_from_defs_folder` — there's no manual registration step. Entry point: `pipelines/src/pipelines/definitions.py`. Any `@asset`-decorated function under `pipelines/src/pipelines/defs/assets/<package>/` is picked up automatically.

**Asset naming convention:** Asset names describe what they *produce*, not the task they perform (Dagster best practice). Use the target table name with a `_table` suffix — `dim_branch_table`, `ks_fct_loans_table`, `hs_contacts_table`. Reads as "the dim_branch table" rather than "load dim_branch". The `ks_`/`hs_`/`fct_`/`dim_` prefixes carry the same meaning as on the table.

**Asset packages** (`pipelines/src/pipelines/defs/assets/<package>/`):
- `warehouse_schema/` — DDL creation and seed data (`warehouse_schema_ready` gate)
- `warehouse_loaders/` — Keystone-sourced assets: `raw_keystone.py` (14 `staging.ks_*` mirrors), `branch.py`, `employee.py`, `person_snapshots.py`, `ks_supplementals.py`, `fct_loans.py`, `fct_members.py`
- `jasperserver_loaders/` — JasperServer API (teller reports)
- `hubspot_loaders/` — HubSpot CRM API (inbound `hs_*` warehouse loaders)
- `hubspot_exports/` — Outbound HubSpot files built from a live Keystone pull + a dropped CRM export (`hubspot_live_import.py`); writes a CSV, **not** the warehouse
- `file_loaders/` — Drop-folder CSV (`csv_loads.py`), manual entries, one-time historical backfills (`csv_backfills.py`)

**Asset group names — assign by source system, not by tier:**

| `group_name` | Use for |
|---|---|
| `warehouse_schema` | DDL creation and seed data assets |
| `ks_daily_loaders` | DB2/CORE-sourced assets that run **nightly from LIVE** — staging mirrors, dim refreshes, daily person snapshot, daily KS supplementals, `ks_fct_loans_by_branch_summary_table` |
| `ks_monthly_loaders` | DB2/CORE-sourced assets that run **monthly from the frozen snapshot DB** (MONTH_END_1 / MONTH_END_2 / QUARTER_END) — monthly person snapshot + stage, members added/lost, home branch summary, members summary, loan applications/requests/loans |
| `jasperserver_loaders` | JasperServer API assets (`fct_teller_activity`, `fct_teller_out_of_balance`) |
| `hubspot_loaders` | HubSpot API assets — inbound `hs_*` warehouse loaders (`hs_contacts`, `hs_companies`, `hs_deals`) |
| `hubspot_exports` | Outbound HubSpot file builders (`hubspot_live_import_csv`) — live Keystone pull + dropped CRM export → CSV; no warehouse write, no `load_audit` |
| `file_loaders` | Drop-folder CSV, manual entry, and historical-backfill assets |
| `repair_ops` | Manually-triggered recovery assets (`ks_daily_repair_table`, `ks_monthly_repair_table`) |

Groups are UI-only metadata — Dagster's asset graph still shows cross-group dependencies. Grouping by source × cadence lets you filter and isolate by external system or by job rhythm in the Dagster UI. Both `ks_*` groups share the prefix so they sort together in the sidebar.

**Loader asset dependency tiers:**

`warehouse_schema_ready` (in `warehouse_schema/seed_data.py`) is the single dep point confirming tables, functions, dim_date, and branch sentinels are all materialized. Declare it explicitly only when there's no other upstream loader in the chain.

- **Tier-1 loaders** (no warehouse data upstream): `deps=[warehouse_schema_ready]`
- **All other loaders** (depend on another loader): `deps=[<upstream loader(s)>]` — schema readiness is inherited transitively

```python
# Tier-1: no warehouse data upstream
from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready

@asset(group_name="ks_daily_loaders", deps=[warehouse_schema_ready])
def dim_branch_table(...): ...

# Tier-2+: schema readiness inherited through dim_branch_table
@asset(group_name="ks_daily_loaders", deps=[dim_branch_table])
def dim_employee_table(...): ...
```

**Resources** live in `pipelines/src/pipelines/defs/resources.py`. Declare as an asset parameter; Dagster injects by name:

```python
from pipelines.defs.resources import WarehousePgResource, KeystoneDatabaseResource

@asset(group_name="ks_daily_loaders")
def my_asset(warehouse_pg: WarehousePgResource, keystone_database: KeystoneDatabaseResource):
    engine = warehouse_pg.get_engine()
    conn = keystone_database.get_connection(context=context)
```

Available resources: `WarehousePgResource`, `KeystoneDatabaseResource`, `JasperserverReportingApiResource`, `HubspotApiResource`. Credentials come from environment variables — never hardcode.

**File-output assets (no warehouse write).** Some assets produce a file rather than a warehouse table — e.g. `hubspot_live_import_csv` (group `hubspot_exports`) runs a live Keystone pull, joins a dropped HubSpot CRM export, and writes a dated CSV under `CSV_OUTPUT_PATH` (`/data/csv_output/<name>/`). These take only the resources they need (no `warehouse_pg`), declare `deps=[]`, skip `load_audit`, and surface stats via a returned `dagster.MaterializeResult(metadata=...)` instead. Output CSVs that carry member PII are gitignored.

**Editable reference lookups.** Small human-maintained lookups live as checked-in CSVs under `pipelines/src/pipelines/reference/` and load via a helper (e.g. `load_product_category_map()`), resolved with `importlib.resources` like the `queries.load` SQL loader. Because `pipelines/src` is bind-mounted, editing the CSV takes effect on the next run with no rebuild.

---

## Historical backfill assets

One-time backfill assets live in `file_loaders/csv_backfills.py`. Drop the source CSV(s) into the matching `data/csv_backfill/<table>/` subfolder, then materialize the asset in the Dagster UI.

| Asset | Folder | Source |
|---|---|---|
| `fct_members_summary_backfill` | `fct_members_summary/` | TotalMembersStatsMonthly.csv |
| `fct_home_branch_members_summary_backfill` | `fct_home_branch_members_summary/` | New Home Branch Members MO-MO.csv |
| `fct_financial_ratios_summary_backfill` | `fct_financial_ratios_summary/` | 8 single-metric CSVs (Net-Worth, ROA, etc.) |
| `fct_nps_summary_backfill` | `fct_nps_summary/` | NPS Score.csv |
| `fct_loan_portfolio_summary_backfill` | `fct_loan_portfolio_summary/` | CFO_Dashboard.csv + 3 others |
| `fct_loans_by_branch_summary_backfill` | `fct_loans_by_branch_summary/` | Loans_Per_Branch_Per_Month.csv |
| `fct_branch_profitability_summary_backfill` | `fct_branch_profitability_summary/` | Branch_Profitability.csv |
| `fct_atm_by_machine_summary_backfill` | `fct_atm_by_machine_summary/` | Number_of_Transactions_by_ATM.csv |
| `fct_digital_enrollments_backfill` | `fct_digital_enrollments/` | Wide-format TSV (3 rows, months as columns) |
| `fct_teller_activity_backfill` | `fct_teller_activity/` | TellerActivity.csv |
| `fct_teller_out_of_balance_backfill` | `fct_teller_out_of_balance/` | Teller_Transactions_out_of_Balance.csv |

Backfill assets use `COALESCE` on-conflict so multiple CSVs covering different columns for the same table can be dropped together and merged safely. All are idempotent — safe to re-run if a file is updated.

For setting the historical partition `start_date` when first deploying a partitioned asset to prod, see [`development.md → Prod backfill after feature merge`](development.md#prod-backfill-after-feature-merge).

---

## JasperServer report calls

`fetch_report_csv(report_path, params)` pulls a report as a CSV string. `report_path` is the path visible in JasperServer admin (omit the base URL and file extension). `params` are the report's input controls — every report has its own set; check the report definition in JasperServer admin for available parameter names.

```python
from io import StringIO
import pandas as pd

csv_text = jasperserver_reporting_api.fetch_report_csv(
    "organizations/d0001liv/Reports/Teller/Teller_Activity_Report",
    params={"StartDate": "2026-01-01", "EndDate": "2026-01-31"},
)
df = pd.read_csv(StringIO(csv_text))
```

Common params: `StartDate`, `EndDate` (YYYY-MM-DD strings), `page`. Each report may have different or additional input controls.

### Adding a new report call

**Step 1 — Find the report path and param names.** You need two things before writing any code: the exact report path and the exact parameter names.

- **JasperSoft Studio (most reliable for params):** Connect to the server, browse to the report in the Repository Explorer, right-click → **Open** to see its input controls (the exact parameter names the report expects). The repository file path (e.g. `organizations/d0001liv/Reports/Operations/TellerDrawerOver-UnderMonthly`) is the `report_path`.
- **Keystone online portal:** Log into the web portal → Ad Hoc Reporting, find the report; the URL or report viewer shows the path.

**Step 2 — Test it in the notebook.** Before writing a Dagster asset, verify the call works using `test_jasperserver.ipynb` in the project root:

```python
resp = fetch_csv(
    'organizations/d0001liv/Reports/YourFolder/YourReport',
    params={'StartDate': '2026-05-01', 'EndDate': '2026-05-31'},
)
print(resp.content[:500].decode('utf-8', errors='replace'))
```

What to check:
- **Status 200 + real CSV rows** = good. Parse with `pd.read_csv(StringIO(resp.content.decode('utf-8')))` and check `df.columns`.
- **Status 200 but empty CSV / no columns** = wrong param names. JasperServer silently returns an empty result rather than erroring. Compare your param names character-for-character against what Studio shows — underscores vs. no underscores (`StartDate` not `Start_Date`) is a common mismatch.
- **Status 200, Content-Type `application/vnd.ms-excel`** = CSV data with a wrong content-type header. `fetch_report_csv` handles this automatically — decode as UTF-8 and it reads fine.
- **Status 4xx or HTML error body** = wrong report path or authentication issue.

**Step 3 — Check the column names.** Run `list(df.columns)` after parsing. JasperServer column names come from the report design and may include spaces (`Transaction Count`), not camel case. Use them exactly as returned — pandas column access is case- and space-sensitive.

If the CSV has no header row (some reports skip it), read with explicit names:
```python
df = pd.read_csv(StringIO(csv_text), header=None, names=["col1", "col2", "col3"])
```

If cash/currency columns come back as strings like `$-10.00`, strip before casting:
```python
float(str(val).replace("$", "").replace(",", ""))
```

**Step 4 — Write the Dagster asset.** Assets live in `jasperserver_loaders/`; follow the pattern in `teller.py`:

```python
@asset(group_name="jasperserver_loaders", deps=[...], partitions_def=MonthlyPartitionsDefinition(...))
def my_report_table(context, jasperserver_reporting_api, warehouse_pg):
    partition_date = date.fromisoformat(context.partition_key)
    csv_text = jasperserver_reporting_api.fetch_report_csv(
        "organizations/d0001liv/Reports/YourFolder/YourReport",
        params={"StartDate": str(partition_date), "EndDate": str(last_day_of_month(partition_date))},
    )
    df = pd.read_csv(StringIO(csv_text))
    context.log.info(f"Columns: {list(df.columns)}, rows: {len(df)}")
    # ... insert into warehouse table
```

The `fetch_report_csv` method in `JasperserverReportingApiResource` handles auth, SSL, and content-type quirks automatically.

---

## HubSpot API calls

`hubspot_api.paginate(path, params)` pulls all records across pages from a HubSpot list endpoint, yielding individual result dicts. Pass desired properties as a comma-joined string. JSONB `properties` column receives the full `props` dict.

```python
import json

for row in hubspot_api.paginate(
    "/crm/v3/objects/contacts",
    params={"properties": "email,firstname,lastname,phone,lifecyclestage,createdate,hs_lastmodifieddate"},
):
    props = row.get("properties", {})
    record = {
        "hs_contact_id": row["id"],
        "email": props.get("email"),
        # ...
        "properties": json.dumps(props),
    }
```

For incremental loads, use the search endpoint (`POST /crm/v3/objects/contacts/search`) with a `hs_lastmodifieddate GTE` filter. For v1 full pulls, `paginate()` against the list endpoint is sufficient.

**`person_key` resolution for HubSpot contacts:** Match `hs_contacts.email` against `person_contact.contact_value` WHERE `contact_type = 'email'` AND `source_system = 'keystone'`. If exactly one match, set `person_key` and write a row to `person_source_identifier` with `source_system = 'hubspot'` / `source_entity_type = 'contact'`. Leave `person_key` NULL if no match or ambiguous.

**`hs_deals.amount`** is returned as a string by the HubSpot API — cast to `NUMERIC(14,2)` in the load asset before inserting.
