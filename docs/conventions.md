# Conventions

Naming, design, and view rules that apply across the warehouse. Read this before adding a new table or asset.

**Contents**
- [Table Naming Conventions](#table-naming-conventions)
- [Design Rules](#design-rules)
- [View Strategy](#view-strategy)

---

## Table Naming Conventions

### Source-system prefix

| Prefix | Meaning | Examples |
|---|---|---|
| *(none)* | Canonical warehouse entity — not tied to any one source system | `person_identity`, `person_source_identifier`, `person_contact` |
| `ks_` | Keystone (IBM DB2 / CORE) source data | `ks_credit_score_snapshot`, `ks_member_branch_snapshot` |
| `hs_` | HubSpot source data | `hs_contacts`, `hs_engagements` *(future)* |
| `dim_` | Curated analytics dimension — may be KS-sourced but enriched/curated for BI | `dim_branch`, `dim_employee`, `dim_date`, `dim_goals` |
| `fct_` | Analytics fact or aggregate | `fct_members_summary`, `fct_loans` |
| `v_` | BI presentation view — no stored data | `v_members_monthly` |

**The `ks_` / `hs_` layer vs `dim_` layer:** Source-prefixed tables are raw mirrors of operational data from that system — one row per entity per source load. `dim_*` tables are curated analytics dimensions derived from (potentially multiple) sources, enriched with warehouse-managed attributes (surrogate keys, abbreviations, sentinels). A Keystone branch is raw data; `dim_branch` is the curated dimension built from it.

**Canonical tables (no prefix):** Entities that span source systems. `person_identity` is the stable surrogate key registry; `person_source_identifier` maps each source-system ID to a `person_key`; `person_contact` holds contact information from any source system in a unified long format.

### Grain suffix (for source-system tables)

| Suffix | Meaning | Example |
|---|---|---|
| *(none)* | Current state — one row per entity, truncate/reload | `ks_contact` *(future)* |
| `_snapshot` | Historical point-in-time series — partitioned by `snapshot_date` | `ks_credit_score_snapshot` |
| `_events` | Individual events — append-only | `ks_credit_score_events` *(future)* |

The `_snapshot` suffix signals a partitioned table. When only the snapshot version exists (no current-state twin), still use the suffix so the grain is always explicit. Add a current-state sibling (no suffix) only when there is a real use case for it.

### Other conventions

| Pattern | Example | Purpose |
|---|---|---|
| `*_identity` | `person_identity` | Stable warehouse surrogate key registry — no descriptive attributes |
| `*_source_identifier` | `person_source_identifier` | Maps source-system IDs to the warehouse surrogate key |
| `*_snapshot_daily` / `*_snapshot_monthly` | `person_snapshot_daily`, `person_snapshot_monthly` | Canonical person biographic state — separate tables for daily and monthly cadences |

**Why `*_identity` is separate from `dim_*`:** A dimension implies descriptive attributes that BI tools query directly. An identity table is just a key registry. Keeping them separate means the identity table never changes shape even as the person's attributes evolve across snapshot loads.

**Why `*_snapshot` is separate from `fct_*`:** Snapshots are row-level state at a point in time (one row per person, not one row per event or aggregate). Fact tables hold measurements. Mixing them under `fct_*` obscures the grain and the load strategy.

---

## Design Rules

- Fact tables hold measurements. Dimensions hold descriptors. Nothing derived in the DB — views or BI tool handles that.
- All monetary amounts in `NUMERIC(14,2)`. All percentages stored as decimals (0.1251 not 12.51%) — divide by 100 on load from CSVs/queries.
- `snapshot_date DATE` is the time key on all aggregate and snapshot fact tables — cadence-agnostic. Dagster controls what date is passed in on each run. For monthly loads, use the last calendar day of the month (e.g., `2026-01-31`). Join to `dim_date` via `full_date = snapshot_date`. `dim_date.month_id INT` (YYYYMM) is a GROUP BY convenience column for analytics but is **not** used as a fact table key.
- **Surrogate keys:** All dimensions use a `SERIAL`/`BIGSERIAL` surrogate PK named `<table>_key`. Source-system IDs are stored in `source_*_serial` columns (e.g., `source_branch_serial`, `source_person_serial`) alongside a `source_system TEXT DEFAULT 'keystone'` column. Uniqueness constraints are source-aware: `UNIQUE (source_system, source_*_serial)`. On a core system conversion, only the `source_*_serial` columns and `person_identity.source_person_serial` need updating — surrogate keys and fact table FKs are untouched.
- **`source_system` column:** Added to all tables that directly store source-system IDs. Default value is `'keystone'` (the current Keystone/IBM DB2 source). Pure aggregate/summary tables that hold no source IDs do not get `source_system`. Sentinel rows in `dim_branch` have `source_system = NULL` since they are not sourced from any external system.
- Stub tables are created with full schema but remain empty until the call center migration.
- **SQL aliases:** Strip the `dim_`/`fct_` prefix and schema name — use the entity name (`branch`, `person`, `loans`). Exception: `dim_date` stays `dim_date` since `date` is a reserved keyword. When a query references multiple tables of the same entity type, add a qualifier (`person_daily`, `person_monthly`). In `ON CONFLICT DO UPDATE` statements, alias the target table in the `INSERT INTO ... AS <alias>` clause and use that alias in the `WHERE` instead of the full schema-qualified name.

---

## View Strategy

Aggregate fact tables are named for their business subject with a `_summary` suffix — the suffix signals pre-aggregated, not a time grain. The time grain is encoded in `snapshot_date` and enforced by Dagster's load schedule, not by the table name.

**BI presentation views (`v_*`)** sit on top of the base fact tables for direct BI consumption (Zoho, etc.). Defined in `warehouse_schema.sql` so they regenerate with every `warehouse_tables` materialization. Rule: views strip warehouse metadata (`loaded_at`, internal FKs other than PKs/SKs), join in branch names instead of `branch_key`, and apply roll-ups that aren't appropriate at the fact-table grain (e.g. Unassigned → Main Branch fold). Base fact tables stay raw for traceability.

**Current views:**

```sql
-- v_members_monthly — pass-through of fct_members_summary with all KPI columns.

-- v_home_branch_members_monthly — fct_home_branch_members_summary with Unassigned
-- folded into Main Branch via CASE WHEN ... THEN 'Main Branch'. One row per real
-- branch per snapshot_date for BI.

-- v_members_lost — slim projection of fct_members_lost (event_date, close_reason,
-- membership_duration_yrs, first_gensh_opened) for Zoho close-reason aggregation.

-- v_loans_by_branch_lsi_in_main — fct_loans_by_branch_summary with LSI Branch
-- branch_key remapped to Main Branch and re-aggregated. Base table keeps LSI
-- separate; this view is the LSI-in-Main version.

-- v_cc_loans_growth_ytd_monthly — YTD growth percentages (×100 for Zoho's pct
-- display) + dollar balances for CC and loans from fct_loan_portfolio_summary
-- (CSV-loaded), for the "Credit Cards & Loans" Zoho panel.

-- v_financial_ratios_monthly — percentage ratios from fct_financial_ratios_summary
-- (net worth, ROA, op expense, loan/share, delinquency, charge-off, loan growth).
-- All pct columns multiplied by 100 in the view (78.21 not 0.7821) because Zoho's
-- pct display type appends "%" without multiplying.

-- v_atm_network_monthly — ATM member uses + foreign uses + YTD network net income
-- from fct_financial_ratios_summary. Split out from the ratios view because these
-- are counts and dollars, not percentages.

-- v_nps_summary — NPS score time series from fct_nps_summary. Sparse: a row
-- exists only for months where a survey ran. Feeds the "Most Recent NPS Score" tile.
```

If the data strategy shifts to daily or live loads, the underlying `_summary` tables require no DDL changes — the load cadence changes in Dagster, and additional views (`v_members_daily`, etc.) can be added on top.
