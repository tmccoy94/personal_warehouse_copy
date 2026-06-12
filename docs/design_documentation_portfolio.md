# Data Platform Design Documentation
### Portfolio Version: Centralization Approach & Documentation Practice

---

## Background: What Problem This Solves

This platform was built to fix a silent failure. The organization had five active dashboards (Board Reports, Operations, Marketing Goals, Member Satisfaction, and Org Goals), all fed by a collection of manually maintained CSV files scattered across network drives. Over time, the CSVs stopped being updated. Panels began showing "No Data Available." Leadership continued using the dashboards without realizing the data had stopped refreshing.

Before writing any code, I conducted a direct leadership survey to map which panels were actually in use and which were confirmed broken. Seven panels on the Board Reports dashboard alone had silently failed. This survey output became the requirements document for what the platform needed to deliver. These were confirmed business requirements, not assumptions, validated by the people who rely on the data.

The platform replaced the ad-hoc multi-file system with a single canonical extract per entity that all dashboards point to. It is live and in use. One source of truth. One place to update. No more duplicated CSVs, no more ownership ambiguity.

---

## Platform Architecture (High Level)

The stack is fully self-contained and portable:

- **PostgreSQL**: data warehouse (structured analytics schema)
- **Dagster**: orchestration layer; controls when data loads, what partitions to run, dependencies between assets
- **Python pipelines**: extract from source systems (IBM DB2, JasperServer API, HubSpot CRM, CSVs), load into warehouse

Four source system types feed the warehouse:
- **IBM DB2 / Keystone**: core transactional system (member data, loans, teller operations)
- **JasperServer**: internal reporting platform (teller activity, lending reports)
- **HubSpot CRM API**: contacts, companies, deals, and association data
- **Drop-folder CSVs and manual entries**: interim intake for data that comes from external portals or accounting workbooks with no API

The analytics layer sits on top: structured fact and dimension tables that Power BI and Zoho connect to directly, rather than to raw source files.

---

## Design Principles

These apply to every table, asset, and pipeline in the platform. They are documented here so that anyone contributing to the system, now or later, is working from the same foundation.

**Centralize, don't proliferate.** One canonical extract per entity. If two reports need member counts, they both point to the same warehouse table, not two separate CSVs that can drift out of sync.

**Raw values in, transforms out.** Warehouse tables hold measurements as close to verbatim from the source as possible. Ratios, formatted display values, and derived metrics belong in views or the BI tool, not in base tables. This keeps the warehouse traceable and reusable across multiple dashboards with different display needs.

**Cadence-agnostic schema.** All time-series tables use a `snapshot_date DATE` column as their time key, not a `month_id INT` (YYYYMM). The Dagster pipeline controls what date is passed in on each run. Changing load frequency from monthly to daily (or the reverse) requires no schema changes; only the schedule changes. A hardcoded integer month key would require a schema migration and renaming of every downstream reference.

**Idempotency.** Running a pipeline twice produces the same result as running it once. Every load uses upsert or truncate-and-reload patterns; no plain inserts that accumulate rows. A re-run is a recovery, not a corruption. This is essential for dashboards: if a load fails midway, re-running it fully restores the correct state without manual cleanup.

**Fail loudly, not silently.** If incoming data doesn't match expectations (an unknown lookup value, a NULL where one shouldn't exist), the pipeline raises an exception. It does not skip rows or insert partial data. Silent failures are the reason the old CSV pipeline broke without anyone noticing for months.

**Observability.** Every load records what ran, when, and how many rows it produced in an audit log. You can always answer "is this dashboard's data current?" without digging through logs or asking someone.

**Design for portability.** One codebase. Configuration (environment, credentials) comes from environment variables; no environment-specific code branches. The entire stack can be moved to a new machine by copying the project directory and restoring a database backup.

---

## Naming Conventions

Consistent, self-documenting naming is a documentation strategy. When a table name tells you the source system, the grain, and the role in the warehouse at a glance, you spend less time explaining and more time analyzing.

### Table Prefixes: What System Provided This Data

| Prefix | Meaning | Examples |
|---|---|---|
| *(none)* | Canonical entity; spans source systems, not tied to any one | `person_identity`, `person_contact` |
| `ks_` | Keystone (IBM DB2) raw source data | `ks_credit_score_snapshot`, `ks_person_branch_snapshot` |
| `hs_` | HubSpot CRM source data | `hs_contacts`, `hs_deals` |
| `dim_` | Curated analytics dimension; may be sourced from Keystone but enriched for BI | `dim_branch`, `dim_employee`, `dim_date` |
| `fct_` | Analytics fact or aggregate | `fct_members_summary`, `fct_loans` |
| `v_` | BI presentation view; no stored data | `v_members_monthly` |

**Why the source prefix matters:** The convention accommodates multiple source systems simultaneously without collisions. `ks_credit_score_snapshot` is immediately distinguishable from `hs_contacts`. No ambiguity about where data came from. Adding a third source system in the future requires no renaming of existing tables, only new prefixed tables following the same pattern.

### Grain Suffixes: What Kind of Table Is This

| Suffix | Meaning |
|---|---|
| `_snapshot` | Historical point-in-time series; one row per entity per date, partitioned |
| `_summary` | Pre-aggregated fact; one row per time period (and optionally per dimension like branch) |
| `_events` | Individual event log; append-only |
| *(none on source tables)* | Current state; one row per entity, truncated and reloaded |

**The `_summary` suffix, not `_monthly`.** Aggregate tables are named for their business subject + `_summary`. The suffix signals pre-aggregated data; it does not encode a time grain. `fct_members_summary` works for monthly, weekly, or daily loads. `fct_members_monthly` would be wrong the moment the cadence changes, requiring a table rename and updates to every dashboard connected to it.

### SQL Aliases: Readable, Not Terse

Table aliases in all queries strip the `dim_`/`fct_` prefix and schema name, using the entity name: `branch`, `person`, `loans`. Single-letter aliases (`b`, `s`, `d`) are not used. The goal is that someone unfamiliar with the query can read it without holding a mental key of what each letter stands for.

---

## How the Layers Relate

```
Source systems                  Warehouse                     BI tools
──────────────                  ─────────                     ────────
IBM DB2 / Keystone  ─────────► ks_* tables                   
JasperServer API    ─────────► fct_* fact tables  ─────────► Power BI
HubSpot CRM API     ─────────► dim_* dimensions              Zoho Analytics
CSV / manual entry  ─────────► fct_*_summary                 
                                     │
                               v_* views
                               (per-tool presentation)
```

The warehouse is the single layer that all dashboards connect to. Adding a new dashboard does not add a new data pipeline; it adds a new view or connects to an existing table. Changing a source system (e.g., converting from Keystone to a new core banking platform) means updating the extract layer. The warehouse tables, surrogate keys, and all downstream dashboards remain untouched.

---

## Multi-Source Support Without Schema Collisions

The platform is designed to support multiple source systems simultaneously without requiring schema changes when a new system is added.

**Canonical tables** (`person_identity`, `person_contact`) hold entities that span systems. A person might be a credit union member (from Keystone) and a HubSpot contact. They resolve to the same `person_key` in the warehouse. A dedicated mapping table (`person_source_identifier`) links each source system's ID to the warehouse surrogate key.

**Surrogate keys everywhere.** Every dimension table uses an auto-generated integer surrogate key (`branch_key`, `employee_key`, `person_key`) rather than the source system's natural ID. If the credit union converts to a new core banking system with different ID schemes, only the source ID columns need updating; the warehouse keys and all fact table foreign keys are untouched.

**Source-aware uniqueness constraints.** Where a table stores source-system IDs, the uniqueness constraint is always `(source_system, source_id)`, not just `source_id` alone. Two different source systems could theoretically assign the same integer ID to different entities; this constraint prevents collision.

---

## Discovery Before Implementation: The Leadership Survey

Before any schema design began, I surveyed leadership across the organization to determine which dashboard panels were actively used and which were broken. The survey covered all five dashboards:

- Board Reports (senior leadership / board audience)
- Operations (branch managers, ops staff)
- Marketing Goals (marketing and growth)
- Member Satisfaction (member experience team)
- Organizational Goals (CFO / executive)

This produced a panel-by-panel inventory: what metric each panel shows, what source file it was pulling from, and whether it was currently working or broken. Seven panels on Board Reports alone were confirmed broken. Their source files had stopped being updated months earlier, with no alert or notification.

**Why this matters for dashboard centralization:** The inventory answered two questions that code cannot answer: "what do people actually use?" and "what is confirmed broken?" Without that survey, the platform would have been built around assumptions about what leadership cared about. With it, every warehouse table maps directly to a confirmed dashboard requirement.

---

## Data Intake Patterns

Rather than a bespoke pipeline per data source, the platform standardizes intake into a small set of patterns. Anyone onboarding a new data source maps it to one of these patterns rather than inventing a new approach.

### Pattern 1: Direct database query (IBM DB2 / Keystone)
For transactional source data. Dagster assets run parameterized SQL queries against the CORE DB2 schema and load results into the warehouse. Queries use named placeholders (`:start_date`, `:snapshot_date`); no hardcoded dates in query files. The asset fills in dates from the partition context at runtime.

### Pattern 2: API pull (JasperServer, HubSpot)
For report platforms and CRM. Shared resource objects handle authentication and pagination. Assets call a method, get records back, upsert into the warehouse.

### Pattern 3: Drop-folder CSV
For data that arrives as manually downloaded files from external portals. Each table gets its own subfolder under `data/csv_drop/<table>/`. Drop the file in, run the asset. The asset loads all files present, upserts into the target table, and archives processed files to a `processed/` subfolder. No bespoke file path logic per table; the pattern is identical across all tables using it.

### Pattern 4: Manual entry (consolidated)
For metrics that come from accounting Excel workbooks on the network with no API. Rather than a separate CSV per metric (the old approach, which created the proliferation problem), all manually-entered scalar metrics consolidate into a single long-format file:

```
warehouse_table, warehouse_column, value, source_location, source_notes
```

`source_location` is the UNC path to the accounting workbook so the month-end user navigates directly to the source. `source_notes` captures which tab or cell to pull from. The person filling this in does not need to know anything about the pipeline. They open the file, follow the instructions per row, save it, and run one Dagster asset.

---

## Decisions Log Format

Every non-obvious architectural decision is recorded in this document with its rationale. The format is intentional:

> **Decision:** What was decided.  
> **Why:** The constraint, tradeoff, or risk that drove the decision.

The "why" is what matters most. Code communicates *what* was built. The decisions log communicates *why*, which is the part that gets lost and causes expensive mistakes when someone changes the system without understanding it.

Selected examples:

**`snapshot_date DATE` instead of `month_id INT` as the time key:**
A YYYYMM integer key locks every table to monthly granularity. Changing to daily or live data would require a schema migration and renaming every downstream reference. A `DATE` key is cadence-agnostic; the pipeline controls what date it passes in and the schema never needs to change.

**`_summary` suffix on aggregate tables instead of `_monthly`:**
The suffix signals pre-aggregated data, not a time grain. Time grain belongs in the pipeline schedule and in views layered on top, not in the base table name. If the cadence changes, the base table name stays correct.

**Person snapshot split into biographic vs. system-specific tables:**
Contact info, credit scores, and product memberships change at different rates and feed different BI use cases. Mixing them onto one large table makes change detection meaningless (a credit score change would flag a "person record changed" alert), bloats every query, and forces analysts to carry columns they never use. Separate tables let each domain change independently.

**Surrogate keys everywhere, natural keys stored alongside:**
If the source system converts to a new platform with different IDs, only the source ID columns update. The warehouse surrogate keys, and every fact table that references them, are untouched.

---

## Documentation Practice

`design_documentation.md` is the living schema reference. It is kept in sync with every schema change. Table additions, renames, column changes, and new decisions all update this document, not just the DDL file. The rule is explicit: if it changed in the database, it changed here.

This is the documentation equivalent of the platform's idempotency principle: the document is always in a valid state that reflects what is actually deployed.

**What the document contains:**
- Object inventory (every table, its grain, its status, its source)
- Full DDL for every table
- Named conventions with rationale
- Decisions log with the "why" behind every non-obvious choice
- Source mapping (which intake pattern feeds each table)
- Data quality notes (known NULL rates, known source gaps, what not to use for branch-level analysis and why)

**What it intentionally does not contain:**
- Historical context about past versions that no longer exist
- "TODO" items that belong in a backlog
- Instructions that apply to one person only

---

## View Strategy

BI tools connect to the `_summary` and `*_snapshot` base tables directly. Presentation views are layered on top when there is a concrete need:

- Renaming columns for a specific tool's label conventions
- Filtering out PII columns for general analyst access
- Joining two base tables into a frequently used report pattern

**The key benefit of the layered approach:** If the load cadence changes (monthly to daily to near-real-time), the base tables require no DDL changes. Only the view definitions change. Dashboards connecting through views are insulated from the underlying pipeline cadence.

This is the same principle that makes the platform relevant to a Databricks/Power BI environment: the semantic layer (views, Power BI semantic models) absorbs changes in the underlying data cadence without requiring dashboard rewrites.

---

## What This Approach Looks Like in a Power BI / Databricks Context

The patterns in this platform translate directly to centralized BI governance:

| This project | Equivalent in a PBI/Databricks stack |
|---|---|
| Single canonical warehouse table per entity | Single certified dataset / medallion layer table |
| `_summary` base tables + `v_*` views | Silver tables + Gold semantic model |
| `snapshot_date DATE` cadence-agnostic key | Fact tables that work for daily or monthly grain without restructuring |
| Intake pattern standardization (4 patterns) | Standardized ingestion templates / notebook patterns |
| Decisions log with "why" | Dataset documentation in the Databricks catalog / Power BI data lineage |
| Idempotent loads (upsert, truncate-reload) | Merge patterns in Delta Lake / incremental refresh in Power BI |
| Leadership survey before design | Requirements mapping before building; confirmed business need, not assumed |
| Audit log (what ran, when, how many rows) | Data freshness monitoring / Purview lineage |
