# Tables (DDL Reference)

Per-table DDL and source notes for every object in the warehouse. Read top-to-bottom is fine for first exposure; in day-to-day use this is a ctrl-F reference — search by table name.

**Contents**
- [Dimensions](#dimensions)
- [Staging Tables](#staging-tables)
- [HubSpot Source Tables](#hubspot-source-tables)
- [Fact Tables — Active](#fact-tables--active)
- [Fact Tables — Stub Only](#fact-tables--stub-only-schema-exists-no-data-until-call-center-migration)
- [Drop-Folder CSV Intake](#drop-folder-csv-intake--datacsv_droptable)
- [Manual Data Intake](#manual-data-intake--datamanual_entriesmanual_entriescsv)
- [Operational Tables](#operational-tables)
- [BI Presentation Views](#bi-presentation-views)
- [Utility Functions](#utility-functions)

---

## Dimensions

### `warehouse.dim_date`

```sql
CREATE TABLE dev.dim_date (
    date_key      INT PRIMARY KEY,          -- YYYYMMDD
    full_date     DATE NOT NULL,
    year          SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,        -- 1–12
    month_id      INT NOT NULL,             -- YYYYMM; GROUP BY convenience for analytics, not a fact table key
    day           SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,        -- 1=Monday
    is_weekend    BOOLEAN NOT NULL,
    is_holiday    BOOLEAN NOT NULL
);
```

**Description:** Calendar table. Grain: one row per day.

**Source:** Generated in-process. Seeded by a Dagster asset, 2020–2030.

---

### `warehouse.dim_branch`

```sql
CREATE TABLE dev.dim_branch (
    branch_key           SERIAL PRIMARY KEY,
    source_system        TEXT DEFAULT 'keystone',        -- NULL for sentinel rows
    source_branch_serial SMALLINT,
    branch_name          TEXT NOT NULL UNIQUE,
    abbreviation         VARCHAR(10) NOT NULL UNIQUE,
    keystone_code        TEXT,                           -- CORE.BRANCH.CODE
    street               TEXT,
    city                 TEXT,
    state                CHAR(2),
    zip                  CHAR(5),
    time_zone            TEXT,
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (source_system, source_branch_serial)
);
```

**Description:** Credit union branches. Grain: one row per branch.

**Source:** `warehouse_seed_dim_branch` Dagster asset — queries `CORE.BRANCH` via Keystone DB on each run and upserts changed rows only. Branch abbreviations are maintained in `_BRANCH_ABBREVIATIONS` in `seed_data.py`; the asset raises an error if a branch from CORE has no mapping entry. `source_system = NULL` on sentinel rows since they are not sourced from any external system.

**Sentinel rows (inserted on first run, never overwritten):**
- `branch_name = 'CU Total'`, `source_branch_serial = NULL` — FK target for CU-wide aggregate rows in `fct_home_branch_members_summary`. `fct_members_summary` is now CU-total only (no branch_key) and does not reference this sentinel.
- `branch_name = 'Unassigned'`, `source_branch_serial = NULL` — catch-all for source records where `BRANCH_SERIAL` is NULL; treated as Main in monthly totals views (`SUM(Main + Unassigned)`)

---

### `warehouse.person_identity`

```sql
CREATE TABLE dev.person_identity (
    person_key  BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Stable one-row-per-person surrogate key registry. Grain: one row per real person, never partitioned. Holds only the warehouse surrogate key (`person_key`) — no source-system IDs, no descriptive attributes. Source identifiers live in `person_source_identifier`; descriptive state lives in the snapshot tables.

**Source:** Populated automatically on first load of each person (via the `person_snapshot_daily` Dagster asset). A new `person_key` is minted and immediately linked in `person_source_identifier`.

**Core conversion note:** On a core system conversion, update `person_source_identifier.source_identifier` to the new system's IDs. `person_key` values in all snapshot and fact tables are untouched.

---

### `warehouse.person_source_identifier`

```sql
CREATE TABLE dev.person_source_identifier (
    person_key          BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    source_system       TEXT NOT NULL,         -- 'keystone', 'hubspot'
    source_entity_type  TEXT NOT NULL,         -- 'member', 'contact'
    source_identifier   TEXT NOT NULL,         -- Keystone serial (as text) or HubSpot contact ID
    is_primary          BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from          TIMESTAMPTZ,           -- not actively populated in v1
    valid_to            TIMESTAMPTZ,           -- not actively populated in v1
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, source_entity_type, source_identifier),
    UNIQUE (person_key, source_system, source_entity_type)
);
```

**Description:** Maps each source-system identifier to a `person_key`. Supports multiple source systems (Keystone, HubSpot, etc.) and multiple entity types (`member`, `contact`, etc.) under one person. The `PRIMARY KEY (source_system, source_entity_type, source_identifier)` enforces that each source-system identity maps to exactly one person.

**Why `source_identifier TEXT`:** Accommodates both integer Keystone serials and string HubSpot IDs without schema changes when a new source system is added. The `ks_*` tables retain `source_person_serial BIGINT` as a typed audit column; they just look up `person_key` by casting through this table.

**`valid_from` / `valid_to`:** Included for future use (e.g., tracking identifier history after a core conversion). Not populated by any v1 asset — all rows leave these NULL.

**`is_primary`:** Intended for cases where a person has multiple identifiers in the same source system (e.g., merged accounts). All v1 rows default to `TRUE`.

---

### `warehouse.person_snapshot_daily`

```sql
CREATE TABLE dev.person_snapshot_daily (
    person_key           BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    first_name           TEXT,
    middle_name          TEXT,
    last_name            TEXT,
    birth_date           DATE,
    gender               TEXT,
    batch_id             TEXT,
    source_extracted_at  TIMESTAMPTZ,
    record_hash          TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Daily point-in-time person biographic state. Grain: one row per person per snapshot date. Full person list appended on each load run — rows never deleted. Enables trend analysis on core biographic attributes and demographics over time. Load cadence controlled by Dagster — schema is cadence-agnostic. Keystone-system-specific data (contact info, products, relationships, credit score, branch) lives in the supplemental tables below.

**Source:** `staging.person_snapshot_stage` — populated from DB2 `person_snapshot_daily.sql` once per job run by `ks_person_snapshot_stage_table`. Contains PII (name, birth_date) — consider a masking view for general BI access. `record_hash` covers only the columns on this table (first_name, middle_name, last_name, birth_date, gender) — contact and KS-specific changes are detected via their own tables.

**`record_hash` status (v1):** Computed as a SHA-256 hash of the five biographic columns and stored on every row. The intended future use is skip-if-unchanged deduplication: compare the incoming row's hash to the previous snapshot's hash for the same person — if they match, skip the write to avoid storing identical rows on days when nothing changed. This optimization is **not yet implemented** (deferred to v1.1). In v1, every daily run writes a new row regardless of whether any data changed.

---

### `warehouse.person_snapshot_monthly`

```sql
CREATE TABLE dev.person_snapshot_monthly (
    person_key           BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    first_name           TEXT,
    middle_name          TEXT,
    last_name            TEXT,
    birth_date           DATE,
    gender               TEXT,
    batch_id             TEXT,
    record_hash          TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Curated monthly point-in-time person biographic state. Grain: one row per person per month (keyed to last calendar day of month). Populated by the Dagster asset on the first run of each month from `person_snapshot_daily`. Identical structure to the daily table — only the load cadence differs. Prior months are permanent snapshots and never modified. Supplemental tables (contact, products, relationships, credit score, branch) have their own monthly partitions loaded in parallel.

**Source:** Populated from `person_snapshot_daily` on the first Dagster run of each month. Contains PII — same masking view applies. `record_hash` is copied directly from the daily snapshot row — same hash, same deduplication intent as the daily table (see note above).

---

### `warehouse.person_contact`

```sql
CREATE TABLE dev.person_contact (
    person_contact_key  BIGSERIAL,
    person_key          BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date       DATE NOT NULL,
    source_system       TEXT NOT NULL,         -- 'keystone', 'hubspot'
    source_identifier   TEXT NOT NULL,         -- KS serial (as text) or HubSpot contact ID
    contact_type        TEXT NOT NULL,         -- full description: 'Home phone', 'Personal cell', 'Mailing address', etc.
    contact_value       TEXT,
    is_primary          BOOLEAN,
    is_active           BOOLEAN,               -- derived from BAD_CONTACT + EXPIRATION_DATE
    last_verified_date  DATE,                  -- LAST_VERIFICATION_DATE from CORE.PERSON_CONTACT
    valid_to            DATE,                  -- EXPIRATION_DATE from CORE.PERSON_CONTACT
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_contact_key)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Canonical contact information across all source systems. Grain: one row per person × source system × contact type per snapshot date. Long/EAV format — one row per distinct contact entry (phone, email, address, fax, pager, web, etc.), regardless of which source system provided it. Partitioned snapshot table — contact history is preserved by `snapshot_date`, consistent with the rest of the warehouse. `contact_type` stores the full Keystone description string directly ("Home phone", "Personal cell", "Business email", "Mailing address", etc.) — no separate `contact_label` column. All KS category codes are mapped in `person_contact_details.sql`.

**Source:** DB2 `person_contact_details.sql` (phones/emails/fax/pager/web) and `person_address.sql` (mailing address) queried independently by `ks_person_contact_table`. The asset calls `upsert_person_identity()` directly — it is fully independent of the person snapshot pipeline. HubSpot rows will be loaded by the `hs_contacts` asset (future). Contains PII (address, phone, email) — masking view needed for general BI access.

**Load strategy:** DELETE for `(snapshot_date, source_system = 'keystone')`, then bulk INSERT. Idempotent per partition date. No ON CONFLICT — the DELETE-before-INSERT pattern is consistent with all other snapshot tables in the warehouse.

---

### `warehouse.ks_products_snapshot`

```sql
CREATE TABLE dev.ks_products_snapshot (
    person_key           BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    product_name         TEXT NOT NULL,
    product_count        SMALLINT NOT NULL DEFAULT 1,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key, product_name),
    UNIQUE (snapshot_date, source_system, source_person_serial, product_name)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Keystone product membership snapshot. Grain: one row per person × product type × snapshot date. A person who holds two "Additional Share" accounts gets one row with `product_count=2` — not two rows. Partitioned by `snapshot_date` — cadence-agnostic, controlled by Dagster. Products are sourced from five UNION ALL branches in the `ProductCounts` CTE in `person_snapshot_daily.sql`: open shares (share type description), open loans (loan type description), Online Banking (Access Softek login channel), Bill Payment (ACH/monetary source B within last 30 days), Direct Deposit (ACH deposit ≥ $500 within last 30 days).

**Source:** `staging.person_snapshot_stage`. The `products` field in staging contains a comma-separated string; the load asset counts occurrences of each product name and inserts one row per distinct product type with `product_count` set accordingly. Product change detection: compare a person's product set across two dates using `EXCEPT` — products present on date A but absent on date B were dropped; products present on B but absent on A were added.

---

### `warehouse.ks_relationships_snapshot`

```sql
CREATE TABLE dev.ks_relationships_snapshot (
    person_key           BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    status               TEXT,
    account_eligibility  TEXT,
    account_type         TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Keystone membership account standing snapshot. Grain: one row per person per snapshot date. `status` is the Platinum/Silver tier flag from `CORE.ACCOUNT_RELATIONSHIP`; `account_eligibility` is from `CORE.ACCOUNT_ELIGIBILITY` (what qualifies the person as a member); `account_type` is from `CORE.ACCOUNT_TYPE` (the type of account holder). Partitioned by `snapshot_date`.

**Source:** `person_snapshot_daily.sql` against CORE DB2. `account_eligibility` and `account_type` come from the `PersonAccountDetails` CTE (added 2026-05-21). `status` comes from the `relationshipStatus` CTE (captures Platinum tier). Note: a future `ks_member_status` SCD table will take over Silver/Platinum tier history — deferred pending query from Taylor.

---

### `warehouse.ks_credit_score_snapshot`

```sql
CREATE TABLE dev.ks_credit_score_snapshot (
    person_key           BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    credit_score         SMALLINT,
    credit_score_date    DATE,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Keystone credit score snapshot. Grain: one row per person per snapshot date. `credit_score_date` is the date the score was pulled in CORE — may lag the `snapshot_date`. Partitioned by `snapshot_date` so score history is preserved independently of other person attributes.

**Source:** `person_service_details_query.sql` against CORE DB2. Credit score is included in the `record_hash` in this table — a score change here is a genuine data change.

---

### `warehouse.ks_member_branch_snapshot`

```sql
CREATE TABLE dev.ks_member_branch_snapshot (
    person_key           BIGINT NOT NULL REFERENCES dev.person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    branch_key           INT REFERENCES dev.dim_branch(branch_key),
    home_branch_key      INT REFERENCES dev.dim_branch(branch_key),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);
```

**Description:** Keystone branch assignment snapshot. Grain: one row per person per snapshot date. Tracks opening branch (`branch_key`) and home branch (`home_branch_key`) as a time series — branch assignments can change over a person's membership. Partitioned by `snapshot_date`.

**Source:** `staging.person_snapshot_stage`. `branch_key` comes from `BRANCH_SERIAL` in the `PersonOldestShare` CTE (in the DB2 query that loads staging). `home_branch_key` comes from the `HomeBranchAssignment` CTE which joins `CORE.CU_HOME_BRANCH` using a `NOT EXISTS` anti-join to get the most-recent home branch record per person. Both branch serials are resolved to `branch_key` via `dim_branch.source_branch_serial` in a single `resolve_branch_keys()` call. See [Decision 9](decisions_log.md) for why `home_branch_key` stores raw `CU_HOME_BRANCH` rather than the ZIP-territory-assigned value.

---

### `warehouse.dim_goals` *(stub — schema only, no data)*

```sql
CREATE TABLE dev.dim_goals (
    goal_key        SERIAL PRIMARY KEY,
    metric_name     TEXT NOT NULL,
    fiscal_year     SMALLINT NOT NULL,
    threshold_type  VARCHAR(20) NOT NULL,   -- 'goal', 'min', 'max', 'warning'
    value           NUMERIC(14,4) NOT NULL, -- percentages as decimals; counts as integers
    display_label   TEXT,
    UNIQUE (metric_name, fiscal_year, threshold_type)
);
```

**Description:** Annual targets and regulatory thresholds. Grain: one row per metric + fiscal year + threshold type.

**Source:** Manually seeded — schema created now, values to be confirmed and loaded in a later pass. Will replace hardcoded thresholds currently embedded in BI tools.

---

### `warehouse.dim_employee`

```sql
CREATE TABLE dev.dim_employee (
    employee_key            SERIAL PRIMARY KEY,
    source_system           TEXT NOT NULL DEFAULT 'keystone',
    source_employee_serial  INT NOT NULL,
    teller_user             VARCHAR(40) NOT NULL,
    full_name               TEXT NOT NULL,
    role_descriptions       TEXT,
    termination_date        DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_employee_serial),
    UNIQUE (source_system, teller_user)
);
```

**Description:** Credit union employees. Grain: one row per employee. Used as the FK target for teller fact tables and loan processing fields. Employees do not carry a branch assignment — branch context for teller activity is captured on `fct_teller_activity` directly.

**Source:** DB2 query `user_roles.sql`. `source_employee_serial` is the integer user serial from CORE; `teller_user` is the login name used in teller system reports. `role_descriptions` is a comma-separated list of role names assigned to the user. `termination_date` is NULL for active employees. Upserted on each load — diff-only update guards against unnecessary writes.

**Note on `full_name`:** `CORE.USER` does not expose first or last name columns — only `USERNAME`. For v1, `full_name` is populated as the username value (same as `teller_user`). If a name field becomes available in a future CORE release, update `user_roles.sql` and the asset mapping.

---

## Staging Tables

Staging tables are in the `staging` schema — a separate schema from `dev`. They hold intermediate operational state within a single job run and are not intended for BI queries or downstream analysis. Dropping and recreating the `dev` schema does not affect `staging`.

### `staging.person_snapshot_stage`

```sql
CREATE TABLE IF NOT EXISTS staging.person_snapshot_stage (
    person_serial        BIGINT PRIMARY KEY,
    first_name           TEXT,
    middle_name          TEXT,
    last_name            TEXT,
    birth_date           DATE,
    age                  SMALLINT,
    gender               TEXT,
    street               TEXT,
    ad2                  TEXT,
    city                 TEXT,
    state                TEXT,
    zip                  TEXT,
    phone                TEXT,
    email                TEXT,
    status               TEXT,
    cred_score           SMALLINT,
    score_date           DATE,
    branch_serial        SMALLINT,
    home_branch_serial   SMALLINT,
    product_count        SMALLINT,
    products             TEXT,
    account_eligibility  TEXT,
    account_type         TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Intermediate holding table for the current DB2 person snapshot. Populated once per job run by `ks_person_snapshot_stage_table` (TRUNCATE + INSERT from `person_snapshot_daily.sql`). All downstream supplemental assets read from this table via `fetch_person_snapshot_stage()` — DB2 is only queried once per run.

**Why a staging table instead of in-memory passing:** Dagster assets are isolated units — there is no native way to pass a Python list from one asset into several downstream assets without writing to a shared store. A staging table is the simplest shared store that works within Dagster's execution model while remaining inspectable (rows visible in pgAdmin) and idempotent (TRUNCATE before reload).

---

### `staging.ks_env`

```sql
CREATE TABLE staging.ks_env (
    posting_date   DATE NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Single-row mirror of `CORE.ENV.POSTING_DATE` — the DB2 system's current business date. Read by warehouse-side queries that need date-window logic that matches CORE exactly (direct-deposit "last 30 days", bill-pay "last 30 days", deceased-note expiration check, etc.). Without this mirror, the warehouse would have to substitute `CURRENT_DATE` and silently disagree with CORE on days where POSTING_DATE has not yet rolled (CORE's day end runs late evening). Refreshed once per `ks_daily_job` run by `ks_staging_env_table` (TRUNCATE + INSERT).

---

### `staging.ks_*_monthly` — point-in-time monthly mirrors

Parallel set of 14 mirror tables suffixed `_monthly`, loaded from the partition's frozen snapshot DB (MONTH_END_1 / MONTH_END_2 / QUARTER_END) instead of LIVE. Schemas are identical to the daily `staging.ks_*` tables — different data source, different cadence.

```
staging.ks_person_monthly         staging.ks_loan_monthly
staging.ks_ac_note_monthly        staging.ks_loan_request_monthly
staging.ks_share_monthly          staging.ks_app_monthly
staging.ks_account_monthly        staging.ks_applicant_monthly
staging.ks_login_monthly          staging.ks_applicant_score_monthly
staging.ks_transaction_monthly    staging.ks_decision_monthly
staging.ks_monetary_monthly       staging.ks_env_monthly
```

**Why a separate layer:** Monthly fact assets (`fct_members_summary`, `fct_loan_applications/requests/loans`) read from these instead of the daily `staging.ks_*` mirrors so their counts are point-in-time accurate against the snapshot month-end. Before this layer existed, those facts read from daily staging refreshed from LIVE, which silently drifted from the snapshot DB as members joined/left in the days between EOM and the actual job run. See [Decision 36](decisions_log.md).

**Loaders (`staging_tables_ks_monthly.py`):** `MonthlyPartitionsDefinition(start_date="2026-05-01")`. Each asset opens a DB2 connection via `keystone_database.get_connection(context=context, job_type="monthly")` (which routes to the partition month's snapshot DB via `MONTH_SNAPSHOT_MAP`), runs `assert_snapshot_db_matches_partition(conn, partition_date)` as the first step (fail-loud guard against running a future partition before the snapshot DB rolls), then TRUNCATEs + INSERTs from the same `staging_ks_*.sql` query used by the daily mirror.

**`ks_transaction_monthly` / `ks_monetary_monthly` — copy-from-daily pattern:** These two CORE tables are big enough (~3.5M / ~530K rows per month) that a fresh DB2 pull every month-end run is ~20 minutes total. Instead, the monthly loaders run `INSERT INTO staging.ks_transaction_monthly SELECT ... FROM staging.ks_transaction WHERE posting_date BETWEEN snapshot_date - 30 days AND snapshot_date`. The daily 90-day rolling staging covers any partition within the last ~2 months. If the daily window doesn't cover the partition's 30-day window the asset fails loudly with a clear error — fall back to a fresh DB2 pull at that point.

**`ks_env_monthly`** holds the snapshot DB's `POSTING_DATE` (i.e. the partition EOM) and is the gate for every other monthly staging asset: it must materialise first because the `assert_snapshot_db_matches_partition` guard depends on its presence; downstream `_compute_fct_members_summary` SQL also reads it for "30 days before snapshot_date" filters.

---

## HubSpot Source Tables

These tables are raw mirrors of HubSpot CRM data. No partitioning — current-state only, UPSERT on HubSpot's stable numeric ID (stored as TEXT). The `properties JSONB` column holds the full HubSpot properties dict for any fields not promoted to named columns. Association tables track many-to-many relationships between objects.

`person_key` on `hs_contacts` is nullable — not every HubSpot contact is a CU member. Resolution happens at load time by matching `email` against `person_contact` WHERE `contact_type = 'email'` AND `source_system = 'keystone'`. When a match is found, `person_source_identifier` is also updated with `source_system = 'hubspot'` / `source_entity_type = 'contact'`.

### Querying the `properties` column

Each HubSpot table has a `properties` column that stores the full set of fields returned by the HubSpot API as a JSON object — think of it like a dictionary where each key is a HubSpot property name and each value is what HubSpot returned for that contact/company/deal.

The commonly used fields (email, name, phone, etc.) are already promoted to their own regular columns and can be queried normally. The `properties` column exists as a safety net — any HubSpot field that wasn't promoted to a named column is still stored there and can be retrieved at any time without needing a reload.

**To pull a single field out of `properties`:**
```sql
-- Use ->>'field_name' to get the value as plain text
SELECT
    email,
    properties->>'lifecyclestage'   AS lifecycle_stage,
    properties->>'hs_lead_status'   AS lead_status
FROM dev.hs_contacts
```

The `->>` operator means "look up this key and give me the value as text." Think of it as `properties["lifecyclestage"]` in Python dict syntax — the double `>` (vs single `>`) is what tells Postgres to return plain text instead of a JSON fragment.

**To filter rows based on a value inside `properties`:**
```sql
SELECT email, firstname, lastname
FROM dev.hs_contacts
WHERE properties->>'hs_lead_status' = 'IN_PROGRESS'
```

**To see all the keys available in `properties` for a given table** (useful when you don't know what HubSpot fields were loaded):
```sql
-- Returns every key name stored in properties, with a count of how many rows have it
SELECT key, COUNT(*) AS row_count
FROM dev.hs_contacts,
     jsonb_each(properties) AS fields(key, value)
GROUP BY key
ORDER BY row_count DESC
```

This is handy for discovery — run it once to see what's available before writing a query that references a specific field name.

**To find a contact where you know a value but not which field it's in** (rare, but occasionally useful):
```sql
-- Check if properties contains a specific key-value pair anywhere
SELECT email, properties
FROM dev.hs_contacts
WHERE properties @> '{"hs_lead_status": "IN_PROGRESS"}'
```

The `@>` operator means "does this JSON object contain all of these key-value pairs?" — again, think of it like checking `"hs_lead_status" in props and props["hs_lead_status"] == "IN_PROGRESS"` in Python.

**What field names are available?** HubSpot property names are visible in the HubSpot portal under Settings → Properties, or by running the `jsonb_each` discovery query above after the first load. They are always lowercase with underscores in the API (e.g., `hs_lead_status`, `lifecyclestage`, `num_associated_deals`).

### `warehouse.hs_contacts`

```sql
CREATE TABLE dev.hs_contacts (
    hs_contact_id   TEXT PRIMARY KEY,
    person_key      BIGINT REFERENCES dev.person_identity(person_key),
    email           TEXT,
    firstname       TEXT,
    lastname        TEXT,
    phone           TEXT,
    company         TEXT,
    lifecyclestage  TEXT,
    hs_created_at   TIMESTAMPTZ,
    hs_updated_at   TIMESTAMPTZ,
    properties      JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Current-state HubSpot contacts. Grain: one row per HubSpot contact. UPSERT on `hs_contact_id`.

**Source:** HubSpot CRM API `/crm/v3/objects/contacts`. `amount` is returned as a string by the API — cast to appropriate type in load asset. Full pull in v1; incremental via `hs_lastmodifieddate` filter on the search endpoint in future.

---

### `warehouse.hs_companies`

```sql
CREATE TABLE dev.hs_companies (
    hs_company_id   TEXT PRIMARY KEY,
    name            TEXT,
    domain          TEXT,
    industry        TEXT,
    city            TEXT,
    state           TEXT,
    hs_created_at   TIMESTAMPTZ,
    hs_updated_at   TIMESTAMPTZ,
    properties      JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Current-state HubSpot companies. Grain: one row per HubSpot company. UPSERT on `hs_company_id`.

**Source:** HubSpot CRM API `/crm/v3/objects/companies`.

---

### `warehouse.hs_deals`

```sql
CREATE TABLE dev.hs_deals (
    hs_deal_id      TEXT PRIMARY KEY,
    dealname        TEXT,
    amount          NUMERIC(14,2),
    pipeline        TEXT,
    dealstage       TEXT,
    dealtype        TEXT,
    closedate       DATE,
    hs_created_at   TIMESTAMPTZ,
    hs_updated_at   TIMESTAMPTZ,
    properties      JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Current-state HubSpot deals. Grain: one row per HubSpot deal. UPSERT on `hs_deal_id`. `amount` is returned as a string from HubSpot — the load asset casts it to `NUMERIC(14,2)`.

**Source:** HubSpot CRM API `/crm/v3/objects/deals`.

---

### `warehouse.hs_contact_company_association`

```sql
CREATE TABLE dev.hs_contact_company_association (
    hs_contact_id   TEXT NOT NULL REFERENCES dev.hs_contacts(hs_contact_id),
    hs_company_id   TEXT NOT NULL REFERENCES dev.hs_companies(hs_company_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hs_contact_id, hs_company_id)
);
```

**Description:** Many-to-many: contacts ↔ companies.

---

### `warehouse.hs_deal_contact_association`

```sql
CREATE TABLE dev.hs_deal_contact_association (
    hs_deal_id      TEXT NOT NULL REFERENCES dev.hs_deals(hs_deal_id),
    hs_contact_id   TEXT NOT NULL REFERENCES dev.hs_contacts(hs_contact_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hs_deal_id, hs_contact_id)
);
```

**Description:** Many-to-many: deals ↔ contacts.

---

### `warehouse.hs_deal_company_association`

```sql
CREATE TABLE dev.hs_deal_company_association (
    hs_deal_id      TEXT NOT NULL REFERENCES dev.hs_deals(hs_deal_id),
    hs_company_id   TEXT NOT NULL REFERENCES dev.hs_companies(hs_company_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hs_deal_id, hs_company_id)
);
```

**Description:** Many-to-many: deals ↔ companies.

---

## Fact Tables — Active

### `warehouse.fct_members_summary`

CU-total only — one row per snapshot_date, no branch dimension. Branch-level member counts are in `fct_home_branch_members_summary` (home-branch ZIP-territory methodology).

```sql
CREATE TABLE dev.fct_members_summary (
    snapshot_date               DATE PRIMARY KEY,
    total_members               INT,
    ks_members                  INT,   -- Overland Park Branch (only KS branch)
    mo_members                  INT,   -- total_members − ks_members
    active_members              INT,   -- 3+ services count
    members_18_50               INT,
    hb_ks_members               INT,   -- OP from home-branch ZIP-territory query
    hb_mo_members               INT,   -- home-branch total − hb_ks − Unassigned
    members_lost                INT,
    members_added               INT,
    non_kcpd_members            INT,   -- account_eligibility != 'KCPD (Missouri)'
    net_new_members             INT,
    net_new_ks_members          INT,
    net_new_mo_members          INT,
    net_new_active_members      INT,
    net_new_members_18_50       INT,
    net_new_hb_mo               INT,
    net_new_hb_ks               INT,
    net_new_non_kcpd_members    INT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** CU-total member summary. All counts use validated QA logic against staging tables. `net_new_*` = current − prior month (NULL for the first loaded row). `ks_members` = Overland Park Branch count by opening branch. `hb_*` = counts from home-branch ZIP-territory methodology (slightly different population — see `fct_home_branch_members_summary`). `non_kcpd_members` from `ks_relationships_snapshot.account_eligibility`.

---

### `warehouse.fct_home_branch_members_summary`

```sql
CREATE TABLE dev.fct_home_branch_members_summary (
    snapshot_date   DATE        NOT NULL,
    branch_key      INT         NOT NULL REFERENCES dev.dim_branch(branch_key),
    total_members   INT,
    members_added   INT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, branch_key)
);
```

**Description:** Pre-aggregated member counts using **home branch** methodology (ZIP territory → CORE.CU_HOME_BRANCH fallback). Separate table from `fct_members_summary` because the numbers intentionally differ from opening-branch counts. Grain: one row per snapshot date × branch — including a row for the `Unassigned` sentinel `branch_key` so members with no territory match and no `CU_HOME_BRANCH` aren't silently dropped. CU-total roll-ups (`hb_ks_members`, `hb_mo_members`) live in `fct_members_summary` and are computed by `_compute_fct_members_summary` from this table.

**Source:** Dagster asset `ks_fct_home_branch_members_summary_table` runs two DB2 queries against the partition's frozen snapshot DB: `home_branch_zips.sql` (totals as-of `last_day_of_month(partition_key)`) and `home_branch_opened.sql` (new adds for the month). Each returned row is bucketed into one of `{"Main Branch", "OP Branch", "North Branch", "East Branch", "South Branch", "Unassigned"}` via `assign_branch_with_fallback(zip, home_branch)` in `fct_members.py`:
1. ZIP matches a hardcoded territory list → that branch
2. ZIP is absent or unmatched → CORE `home_branch` name, normalized through `CORE_TO_WAREHOUSE_BRANCH_NAME` (`"Overland Park Branch"` → `"OP Branch"`)
3. Neither resolves → `"Unassigned"`

The asset writes one row per branch_key the function returned. The asset raises if any bucketed name isn't found in `dim_branch` (fail-loud, prevents silent drops). Presentation: `v_home_branch_members_monthly` folds Unassigned into Main Branch for BI.

**CSV backfill (historical, superseded):** `fct_home_branch_members_summary_backfill` in `file_loaders/csv_backfills.py` reads `Home Branch Members Report.csv` and `New Home Branch Members MO-MO.csv` for Jan 2025 – Apr 2026. Still runs for those partitions; new months come from the DB2-derived asset.

---

### `warehouse.fct_members_added`

```sql
CREATE TABLE dev.fct_members_added (
    event_id                SERIAL PRIMARY KEY,
    source_system           TEXT NOT NULL DEFAULT 'keystone',
    month_id                INT NOT NULL,
    source_person_serial    BIGINT NOT NULL,
    person_key              BIGINT REFERENCES dev.person_identity(person_key),
    event_date              DATE NOT NULL,         -- first account open date
    agency                  TEXT,
    account_category        TEXT,
    returning_member        BOOLEAN,
    first_gensh_opened      DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_person_serial, month_id)
);
```

**Description:** Membership additions — one row per person per month they joined (or rejoined). Grain: person × month. Membership = open `CORE.SHARE` with `ID = '0001'` (General Share). `returning_member` flags people whose oldest account open date predates their first General Share open — i.e. they had accounts before but never a General Share, so this is their first time becoming a true member.

**Source:** DB2 `members_added.sql` against `CORE.PERSON`, `CORE.ACCOUNT`, `CORE.SHARE`, `CORE.AGENCY`. Loaded by `ks_fct_members_added_table` (`MonthlyPartitionsDefinition`, partition_key = first day of month). `person_key` resolved via `person_source_identifier`. Idempotent via `ON CONFLICT (source_system, source_person_serial, month_id) DO NOTHING`.

---

### `warehouse.fct_members_lost`

```sql
CREATE TABLE dev.fct_members_lost (
    event_id                SERIAL PRIMARY KEY,
    source_system           TEXT NOT NULL DEFAULT 'keystone',
    month_id                INT NOT NULL,
    source_person_serial    BIGINT NOT NULL,
    person_key              BIGINT REFERENCES dev.person_identity(person_key),
    event_date              DATE NOT NULL,         -- last General Share close date
    close_reason            TEXT,
    membership_duration_yrs SMALLINT,
    first_gensh_opened      DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_person_serial, month_id)
);
```

**Description:** Membership terminations — one row per person per month they closed their last General Share. Grain: person × month. A person is LOST when no open General Shares remain after the close event. `close_reason` is `CORE.ACCOUNT_CLOSE_REASON.DESCRIPTION` on the last closed General Share. `membership_duration_yrs` is computed from `first_gensh_opened` to `event_date`.

**Source:** DB2 `members_lost.sql` against `CORE.PERSON`, `CORE.ACCOUNT`, `CORE.SHARE`, `CORE.ACCOUNT_CLOSE_REASON`. Loaded by `ks_fct_members_lost_table` (`MonthlyPartitionsDefinition`). Same `person_key` resolution + ON CONFLICT pattern as `fct_members_added`.

**Presentation view:** `v_members_lost` exposes only the columns Zoho aggregates against — drops internal IDs and metadata.

---

### `warehouse.fct_financial_ratios_summary`

```sql
CREATE TABLE dev.fct_financial_ratios_summary (
    snapshot_date           DATE PRIMARY KEY,
    net_worth_pct           NUMERIC(8,4),
    roa_pct                 NUMERIC(8,4),       -- annualized
    op_expense_pct          NUMERIC(8,4),
    loan_to_share_pct       NUMERIC(8,4),
    loan_delinquency_pct    NUMERIC(8,4),
    loan_charge_off_ratio   NUMERIC(8,4),
    loan_growth_pct         NUMERIC(8,4),
    atm_member_uses         INT,                -- own-network ATM uses
    atm_foreign_uses        INT,                -- surcharge ATM uses
    atm_network_ytd_net_income  NUMERIC(14,2),  -- from ATM_Network_Profitability.csv
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Regulatory and financial KPI time series. Grain: one row per snapshot date. Load cadence controlled by Dagster — schema is cadence-agnostic. The `nps_score` column was removed in [Decision 22](decisions_log.md) and lives in `fct_nps_summary` instead.

**Sources:** `Net-Worth.csv`, `ROA.csv`, `Operating_Expense.csv`, `Loan_to_Share.csv`, `Loan_Delinquency.csv`, `Loan_Charge_Off_Ratio.csv`, `Loan_Growth.csv`, `Prior_ATM_Usage.csv`, `ATM_Network_Profitability.csv`. NPS is in `fct_nps_summary`.

---

### `warehouse.fct_nps_summary`

```sql
CREATE TABLE dev.fct_nps_summary (
    snapshot_date   DATE PRIMARY KEY,
    nps_score       NUMERIC(8,2),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Net Promoter Score time series. Grain: one row per snapshot date. Sparse — only populated for months where a survey actually ran. Separated from `fct_financial_ratios_summary` because NPS is a member-satisfaction signal, not a financial ratio ([Decision 22](decisions_log.md)).

**Source:** `NPS Score.csv` via manual entry workflow.

---

### `warehouse.fct_loan_portfolio_summary`

```sql
CREATE TABLE dev.fct_loan_portfolio_summary (
    snapshot_date               DATE PRIMARY KEY,
    loan_total_balance          NUMERIC(14,2),
    loan_income_monthly         NUMERIC(14,2),
    loan_growth_ytd_pct         NUMERIC(8,4),
    share_total_balance         NUMERIC(14,2),
    share_income_monthly        NUMERIC(14,2),
    share_growth_ytd_pct        NUMERIC(8,4),
    cc_total_balance            NUMERIC(14,2),
    cc_income_monthly           NUMERIC(14,2),
    cc_growth_ytd_pct           NUMERIC(8,4),
    loan_charge_offs            NUMERIC(14,2),
    loan_charge_off_recoveries  NUMERIC(14,2),
    cp_charge_offs              NUMERIC(14,2),
    cp_charge_off_recoveries    NUMERIC(14,2),
    atm_fraud_mtd               NUMERIC(14,2),
    check_loss_fraud_mtd        NUMERIC(14,2),
    cc_fraud_mtd                NUMERIC(14,2),
    debit_fraud_mtd             NUMERIC(14,2),
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Pre-aggregated loan and credit card portfolio metrics. Grain: one row per snapshot date. Load cadence controlled by Dagster — schema is cadence-agnostic.

**Sources:** `CFO_Dashboard.csv`, `LoanCreditCardTotals.csv`, `MarketingDBData.csv`, `_Fraud_Charge_Off_Long.csv`.

---

### `warehouse.fct_loans_by_branch_summary`

```sql
CREATE TABLE dev.fct_loans_by_branch_summary (
    snapshot_date         DATE NOT NULL,
    branch_key            INT NOT NULL REFERENCES dev.dim_branch(branch_key),
    loan_count            INT,
    payday_loan_count     INT,
    non_payday_loan_count INT,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, branch_key)
);
```

**Description:** Loans opened per branch per month, with payday-alternative-loan breakout. Grain: one row per snapshot date × branch. `loan_count = payday_loan_count + non_payday_loan_count`. Load cadence controlled by Dagster — schema is cadence-agnostic.

**Source:** `ks_fct_loans_by_branch_summary_table` truncate-reloads from `staging.ks_loan` per partition month. `CORE.LOAN.BRANCH_SERIAL` (disbursing branch, **fully populated** — distinct from the sparsely-populated `CORE.LOAN_REQUEST.BRANCH_SERIAL`) is the source. Payday flag uses `loan_type_description = 'Payday Alternative Loan'`.

**Presentation view:** `v_loans_by_branch_lsi_in_main` re-maps the LSI Branch (online applications) `branch_key` to Main Branch and re-aggregates — Zoho consumes the base table for the LSI-separate view and this view for the LSI-in-Main view.

**Historical backfill:** `fct_loans_by_branch_summary_backfill` reads `Loans_Per_Branch_Per_Month.csv` for 2020–2024 (before staging.ks_loan covers the period). Superseded going forward.

---

### `warehouse.fct_loan_applications`

```sql
CREATE TABLE dev.fct_loan_applications (
    application_key                   BIGSERIAL PRIMARY KEY,
    source_system                     TEXT NOT NULL DEFAULT 'keystone',
    source_app_serial                 BIGINT NOT NULL,
    application_number                TEXT NOT NULL,
    application_date                  DATE NOT NULL,
    app_type                          TEXT,
    withdrawn_status                  VARCHAR(20),
    source_primary_person_serial      BIGINT,
    primary_person_key                BIGINT REFERENCES dev.person_identity(person_key),
    primary_credit_score              SMALLINT,
    source_co_applicant_person_serial BIGINT,
    co_applicant_person_key           BIGINT REFERENCES dev.person_identity(person_key),
    co_applicant_name                 TEXT,
    co_applicant_credit_score         SMALLINT,
    app_processor_key                 INT REFERENCES dev.dim_employee(employee_key),
    branch_key                        INT REFERENCES dev.dim_branch(branch_key),
    loaded_at                         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_app_serial),
    UNIQUE (source_system, application_number)
);
CREATE INDEX IF NOT EXISTS idx_fct_loan_applications_primary_person_key
    ON warehouse.fct_loan_applications (primary_person_key);
CREATE INDEX IF NOT EXISTS idx_fct_loan_applications_co_applicant_person_key
    ON warehouse.fct_loan_applications (co_applicant_person_key);
CREATE INDEX IF NOT EXISTS idx_fct_loan_applications_source_primary_person
    ON warehouse.fct_loan_applications (source_system, source_primary_person_serial);
```

**Description:** Loan application header. Grain: one row per application. Captures who applied, when, the app type, applicant identifiers, and credit scores. The application is the housing structure — loan requests are its children.

**Person key resolution:** `primary_person_key` resolved via `person_identity` on `(source_system, source_primary_person_serial)`. `co_applicant_person_key` resolved the same way but left NULL if the co-applicant is not in the warehouse (co-applicants may not be members).

**Source:** `load_ks_fct_loan_applications.sql` against `CORE.APP`, `CORE.APPLICANT`, `CORE.APPLICANT_SCORE`. Loaded for current month; rolling history retained.

**Note:** `branch_key` is derived from the first loan request on the application and will be NULL ~80% of the time — `LOAN_REQUEST.BRANCH_SERIAL` is sparsely populated in CORE. Do not use for branch-level analysis.

---

### `warehouse.fct_loan_requests`

```sql
CREATE TABLE dev.fct_loan_requests (
    loan_request_key            BIGSERIAL PRIMARY KEY,
    source_loan_request_serial    BIGINT NOT NULL UNIQUE,     -- CORE.LOAN_REQUEST.SERIAL
    application_key             BIGINT NOT NULL REFERENCES dev.fct_loan_applications(application_key),
    application_number          TEXT NOT NULL,              -- denormalized for easy filtering
    loan_type                   TEXT,                       -- from CORE.LN_TYPE
    requested_amount            NUMERIC(14,2),              -- CREDIT_LIMIT
    loan_request_credit_score   SMALLINT,                   -- score on the LR itself
    branch_key                  INT REFERENCES dev.dim_branch(branch_key),
    decision_category           TEXT,                -- Pending, Approved, Denied, Countered, Unknown
    decision_description        TEXT,
    decision_date               DATE,
    decision_maker_key          INT REFERENCES dev.dim_employee(employee_key),
    processor_key               INT REFERENCES dev.dim_employee(employee_key),
    source_approved_loan_serial   BIGINT,                     -- CORE.LOAN.SERIAL if opened; NULL otherwise
    loan_open_date              DATE,                       -- LOAN.OPEN_DATE if approved
    is_payday_alt               BOOLEAN,                    -- LN_TYPE = 'Payday Alternative Loan'
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ON warehouse.fct_loan_requests (application_number);
CREATE INDEX ON warehouse.fct_loan_requests (decision_category);
```

**Description:** Individual loan requests within an application. Grain: one row per loan request. This is where loan type (credit card, auto, PAL, etc.), decision, and branch live — not at the application level.

**Source:** `load_ks_fct_loan_requests.sql` against `CORE.LOAN_REQUEST`, `CORE.DECISION`, `CORE.DECISION_TYPE`, `CORE.LN_TYPE`. Most-recent-decision pattern uses the same `NOT EXISTS` anti-join as the source query.

**Note:** `branch_key` will be NULL ~80% of the time — `LOAN_REQUEST.BRANCH_SERIAL` is sparsely populated in CORE. Do not use for branch-level analysis.

---

### `warehouse.fct_loans`

```sql
CREATE TABLE dev.fct_loans (
    loan_key                    BIGSERIAL NOT NULL,
    snapshot_date               DATE NOT NULL,
    source_system               TEXT NOT NULL DEFAULT 'keystone',
    source_loan_serial          BIGINT NOT NULL,
    source_loan_request_serial  BIGINT,
    source_person_serial        BIGINT NOT NULL,
    person_key                  BIGINT REFERENCES dev.person_identity(person_key),
    branch_key                  INT REFERENCES dev.dim_branch(branch_key),
    loan_type                   TEXT,
    loan_status                 VARCHAR(40),
    original_amount             NUMERIC(14,2),
    current_balance             NUMERIC(14,2),
    open_date                   DATE,
    close_date                  DATE,
    maturity_date               DATE,
    interest_rate               NUMERIC(8,4),
    is_delinquent               BOOLEAN,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, loan_key),
    UNIQUE (snapshot_date, source_system, source_loan_serial)
) PARTITION BY RANGE (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_fct_loans_person_key
    ON warehouse.fct_loans (person_key);
CREATE INDEX IF NOT EXISTS idx_fct_loans_snapshot_person
    ON warehouse.fct_loans (snapshot_date, person_key);
CREATE INDEX IF NOT EXISTS idx_fct_loans_source_person
    ON warehouse.fct_loans (source_system, source_person_serial);
```

**Description:** Active loan portfolio. Grain: one row per open loan per snapshot date. Current snapshot_date partition is truncated and refilled on each load; prior dates are permanent snapshots. Load cadence (daily, weekly, monthly) is controlled by Dagster — schema is cadence-agnostic.

**`person_key`** resolved via `person_identity` on `(source_system, source_person_serial)`. Use `person_key` for warehouse joins; `source_person_serial` for audit.

**Source:** `load_ks_fct_loans.sql` against `CORE.LOAN`, `CORE.LN_TYPE`. Current snapshot_date partition truncated and refilled on each load; load cadence controlled by Dagster. Feeds `fct_loan_portfolio_summary` roll-ups going forward.

**Note:** `branch_key` will be NULL ~80% of the time — `LOAN_REQUEST.BRANCH_SERIAL` (the source for branch on loans) is sparsely populated in CORE. Do not rely on this for branch-level loan analysis; see `fct_loans_by_branch_summary` note.

---

### `warehouse.fct_digital_channels_transactions`

```sql
CREATE TABLE dev.fct_digital_channels_transactions (
    snapshot_date               DATE PRIMARY KEY,
    bill_pay_transactions       INT,
    mobile_online_transactions  INT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Pre-aggregated digital channel transaction activity. Grain: one row per snapshot date. Both columns are sourced from `manual_entries.csv` ([Decision 42](decisions_log.md)). The former `home_banking_users` column moved to `fct_digital_enrollments` as a `home_banking` service row, and the table was renamed from `fct_digital_channels_summary` to match its transactions-only contents ([Decision 47](decisions_log.md)).

---

### `warehouse.fct_digital_enrollments`

```sql
CREATE TABLE dev.fct_digital_enrollments (
    snapshot_date   DATE NOT NULL,
    service         TEXT NOT NULL,           -- 'home_banking', 'bill_pay', 'rdc'
    enrolled_count  INT NOT NULL,
    active_count    INT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, service)
);
```

**Description:** Pre-aggregated enrolled and active user counts per digital service. Grain: one row per snapshot date × service. Three active services ([Decision 47](decisions_log.md)): `home_banking` (hand-entered monthly via `manual_entries.csv` — the platform dashboard only exposes a single total), plus `bill_pay` and `rdc` (loaded from CSV drops). The DELETE in `fct_digital_enrollments_table` is narrowed to `service IN ('bill_pay', 'rdc')` so the drop asset never touches `home_banking` rows.

**Sources:** `manual_entries.csv` (`fct_digital_enrollments,enrolled_count,home_banking,...` row), `rdcusers.csv` (rdc counts) and `billpayusers.csv` (bill_pay counts) dropped into `data/csv_drop/fct_digital_enrollments/<service>/`. Member-level detail (individual enrollment dates, RDC risk limits) deferred to a future table.

---

### `warehouse.fct_atm_by_machine_summary`

```sql
CREATE TABLE dev.fct_atm_by_machine_summary (
    snapshot_date   DATE NOT NULL,
    atm_name        TEXT NOT NULL,
    transactions    INT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, atm_name)
);
```

**Description:** Pre-aggregated transaction counts per ATM machine. Grain: one row per snapshot date × ATM. Network-level ATM totals (member uses, foreign uses, net income) live in `fct_financial_ratios_summary`. Load cadence controlled by Dagster — schema is cadence-agnostic.

**Source:** `manual_entries.csv` ([Decisions 42, 43](decisions_log.md)). Row format: `fct_atm_by_machine_summary,transactions,<atm_name>,<count>,...`.

---

### `warehouse.fct_branch_profitability_summary`

```sql
CREATE TABLE dev.fct_branch_profitability_summary (
    snapshot_date   DATE NOT NULL,
    branch_key      INT NOT NULL REFERENCES dev.dim_branch(branch_key),
    net_income      NUMERIC(14,2) NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, branch_key)
);
```

**Description:** Pre-aggregated branch net income. Grain: one row per snapshot date × branch. Load cadence controlled by Dagster — schema is cadence-agnostic.

**Source:** `manual_entries.csv` ([Decisions 42, 43](decisions_log.md)). Row format: `fct_branch_profitability_summary,net_income,<branch_name>,<net_income>,...`. The asset resolves `branch_name` to `branch_key` via `dim_branch`.

---

### `warehouse.fct_teller_activity`

```sql
CREATE TABLE dev.fct_teller_activity (
    activity_date       DATE NOT NULL,
    employee_key        INT NOT NULL REFERENCES dev.dim_employee(employee_key),
    branch_key          INT REFERENCES dev.dim_branch(branch_key),
    transaction_count   INT NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_date, employee_key)
);
```

**Description:** Teller transaction counts by teller by date. Grain: one row per teller × date.

**Source:** JasperServer API — report `organizations/d0001liv/Reports/Operations/TellerDrawerOver-UnderMonthly`, params `Start_Date` / `End_Date`. Column names (teller identifier, date, transaction count, branch) are logged on first run via `context.log.info` and should be confirmed before setting the column-name constants in `teller.py`.

---

### `warehouse.fct_teller_out_of_balance`

```sql
CREATE TABLE dev.fct_teller_out_of_balance (
    event_id            SERIAL PRIMARY KEY,
    employee_key        INT NOT NULL REFERENCES dev.dim_employee(employee_key),
    transaction_date    DATE NOT NULL,
    prev_cash           NUMERIC(12,2),
    new_cash            NUMERIC(12,2),
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Teller drawer out-of-balance exception events. Grain: one row per event.

**Source:** JasperServer API — report `organizations/d0001liv/Reports/Operations/Transactions_By_Role_Type`, params `Start_Date` / `End_Date` / `RoleDescriptionList=Teller`. Column names (teller identifier, date, prev_cash, new_cash) are logged on first run via `context.log.info` and should be confirmed before setting the column-name constants in `teller.py`.

---

## Fact Tables — Stub Only (schema exists, no data until call center migration)

### `warehouse.fct_call_center_summary`

```sql
CREATE TABLE dev.fct_call_center_summary (
    snapshot_date               DATE PRIMARY KEY,
    calls_presented             INT,
    calls_answered              INT,
    calls_abandoned             INT,
    pct_answered                NUMERIC(6,4),
    pct_abandoned               NUMERIC(6,4),
    inbound_calls               INT,
    avg_call_duration_sec       NUMERIC(8,1),
    avg_speed_of_answer_sec     NUMERIC(8,1),
    answered_within_15_sec      INT,
    longest_wait_sec            INT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Pre-aggregated call center performance. Grain: one row per snapshot date. Schema based on `BI_Call_Performance.csv`. Load cadence controlled by Dagster — schema is cadence-agnostic.

---

### `warehouse.fct_call_center_agents`

```sql
CREATE TABLE dev.fct_call_center_agents (
    snapshot_date               DATE NOT NULL,
    employee_key                INT NOT NULL REFERENCES dev.dim_employee(employee_key),
    call_count                  INT,
    avg_time_to_answer_sec      NUMERIC(8,1),
    avg_talking_duration_sec    NUMERIC(8,1),
    abandoned_calls             INT,
    missed_calls                INT,
    avg_abandoned_duration_sec  NUMERIC(8,1),
    abandoned_pct               NUMERIC(6,4),
    total_login_duration_sec    NUMERIC(12,1),
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, employee_key)
);
```

**Description:** Agent-level call metrics. Grain: one row per snapshot date × employee. Agents are employees — references `dim_employee`. Schema based on `Call_Center_Monthly.csv`. Load cadence controlled by Dagster — schema is cadence-agnostic.

---

### `warehouse.fct_survey_responses`

```sql
CREATE TABLE dev.fct_survey_responses (
    engagement_id       TEXT PRIMARY KEY,
    response_date       DATE NOT NULL,
    queue_flow          TEXT,           -- Call Center, Teller, Loan Process, New Account, Chat
    channel             TEXT,           -- Phone, Chat, In Person, Online, Email
    survey_format       TEXT,
    rating              TEXT,           -- text label as given in the survey: 'Excellent', 'Good', 'Poor', etc.
    additional_feedback TEXT,
    member_name         TEXT,           -- PII
    email               TEXT,           -- PII
    phone               TEXT,           -- PII
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Description:** Member satisfaction survey responses. Grain: one row per survey response. Schema based on `transformed_survey_results.csv`. Contains PII (member_name, email, phone). `rating` is the verbatim text label from the survey (e.g. "Excellent", "Good", "Poor"), not a numeric score — BI tools map labels to scores as needed.

---

## Drop-Folder CSV Intake — `data/csv_drop/<table>/`

Some warehouse tables are sourced from CSV files downloaded manually from online portals. Rather than hard-coding file paths, each table gets its own subfolder. Drop the file in, run the asset.

**Folder structure:**
```
data/csv_drop/
  fct_digital_enrollments/
    bill_pay/         bill pay platform export (4 metadata rows before header)
    rdc/              RDC export (clean header, no skip)
  fct_survey_responses/             member_survey_results.csv (any filename)
```

After [Decisions 43 + 44](decisions_log.md), only `fct_digital_enrollments_table` (bill_pay + rdc) and `fct_survey_responses_table` remain on the drop-folder path. ATM by machine, branch profitability, and home banking enrollments moved to `manual_entries.csv` — the data is hand-entered anyway, and consolidating into one file beats juggling individual drop folders.

**Asset behavior:** Load the first .csv file found in the subfolder. For enrollment files, count all rows and active rows per service. Fail loudly if the subfolder is empty.

---

## Manual Data Intake — `data/manual_entries/manual_entries.csv`

Several warehouse tables are sourced from accounting Excel workbooks on the network (`\\fileserver\accounting\...`). No automated pipeline is feasible for these yet — data must be manually pulled by whoever does month-end reporting. Rather than maintaining ~20 individual CSVs (the old Zoho pipeline approach), all manually-entered scalar metrics are consolidated into a single long-format intake file.

**File location:** `data/manual_entries/manual_entries.csv`

**Format:**
```
warehouse_table,warehouse_column,row_key,value,source_location,source_notes
__meta__,snapshot_date,,2026-05-31,,Set to the EOM date this run targets
fct_financial_ratios_summary,net_worth_pct,,0.1263,...
fct_nps_summary,nps_score,,72.4,...
fct_loan_portfolio_summary,loan_total_balance,,129819544.00,...
fct_digital_channels_transactions,bill_pay_transactions,,12345,...
fct_branch_profitability_summary,net_income,Main Branch,123456.78,...
fct_atm_by_machine_summary,transactions,HQ,1234,...
```

- **The `__meta__,snapshot_date,...` row at the top defines the target date** — the asset reads that value and writes it as `snapshot_date` on every row below. Editing one cell retargets the whole load. Asset fails loudly if the row is missing or the date isn't ISO format (`YYYY-MM-DD`).
- **`row_key` identifies which row of a multi-row table this value applies to.** Single-row tables (PK = snapshot_date alone) leave it blank. Multi-row tables use it: branch profitability uses `branch_name` (looked up against `dim_branch`), ATM-by-machine uses `atm_name` directly. The asset has table-specific handlers that know the PK structure.
- `source_location` is the UNC path to the accounting workbook so the month-end person can navigate directly to the source without hunting.
- `source_notes` captures which tab, column, or specific cell to grab the value from.
- `value` is left blank each month — fill it in, then run the asset.
- **Blank cells preserve existing values.** Rows with blank `value` are skipped at the filter stage (no INSERT), and even when an INSERT runs, every `ON CONFLICT DO UPDATE` column is wrapped in `COALESCE(EXCLUDED.col, table.col)` — so editing a single cell only changes that cell on that row, nothing else. The CSV is safe to edit incrementally; clearing a cell never nukes existing data.

**Month-end workflow:**
1. Open `manual_entries.csv`
2. Update the `__meta__,snapshot_date` row's `value` cell to the target EOM
3. For each data row, navigate to `source_location`, find the value per `source_notes`
4. Paste the value into the `value` column
5. Save the file and materialize `manual_entries_table` (no partition needed — date comes from the CSV)

**Tables currently covered:**
- `fct_financial_ratios_summary` — net worth, ROA, op expense, loan ratios, ATM uses & income (single-row)
- `fct_nps_summary` — NPS score, single-row, sparse (only months a survey ran)
- `fct_loan_portfolio_summary` — loan/share/CC balances, MoM differences, YTD growth, charge-offs, fraud (single-row)
- `fct_digital_channels_transactions` — `bill_pay_transactions`, `mobile_online_transactions` (single-row; home banking total routes to `fct_digital_enrollments` per Decision 47)
- `fct_branch_profitability_summary` — `net_income` per branch (multi-row; `row_key` = branch name, resolved to `branch_key` via `dim_branch`)
- `fct_atm_by_machine_summary` — `transactions` per ATM (multi-row; `row_key` = `atm_name` directly)

**Design note:** Getting off manual entry is not a v1 MVP priority. The CSV intake consolidates the work that was previously scattered across individual CSVs and Excel copy-paste steps. Future automation would replace the fill-in step, not the asset or file format.

---

## Operational Tables

### `warehouse.load_audit`
Load run audit log. One row per Dagster asset execution that writes to the warehouse. Used for lineage, debugging, and data freshness checks.

`source_system` is nullable because some loads are generated or manual (e.g., `dim_date` seed, `dim_goals` seed, derived aggregates). For all Keystone-sourced loads populate `source_system = 'keystone'`.

```sql
CREATE TABLE dev.load_audit (
    batch_id             TEXT NOT NULL,
    source_system        TEXT,
    asset_name           TEXT NOT NULL,
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    source_extracted_at  TIMESTAMPTZ,
    row_count            INT,
    status               TEXT NOT NULL,   -- 'success', 'failed', 'partial'
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, asset_name)
);
```

**`batch_id`** is the Dagster run ID. PK is composite (`batch_id, asset_name`) because a single Dagster run materializes multiple assets, each getting its own audit row. `audit_load()` in `loading.py` opens the row with `status = 'running'` and updates to `'success'`/`'failed'` on context exit; it also enforces the `WAREHOUSE_SCHEMA` env var vs `.schema_intent` file consistency check before any data is written.

---

## BI Presentation Views

Views in `warehouse_schema.sql` regenerate every time `warehouse_tables` is materialized. They strip metadata, join in branch names, and apply BI-specific roll-ups that don't belong at the fact-table grain. BI tools (Zoho, Power BI) read from views; the base facts are reserved for analyst and ad-hoc work.

### `v_members_monthly`

Pass-through projection of `fct_members_summary` with the 18 KPI columns BI tools consume directly. No reshaping or aggregation — drops the `loaded_at` metadata column only.

```sql
CREATE OR REPLACE VIEW v_members_monthly AS
SELECT snapshot_date, total_members, ks_members, mo_members, active_members,
       members_18_50, members_added, members_lost, non_kcpd_members,
       hb_ks_members, hb_mo_members, net_new_members, net_new_ks_members,
       net_new_mo_members, net_new_active_members, net_new_members_18_50,
       net_new_hb_mo, net_new_hb_ks, net_new_non_kcpd_members
FROM fct_members_summary;
```

### `v_home_branch_members_monthly`

`fct_home_branch_members_summary` joined to `dim_branch` for the human-readable branch name and abbreviation. **Unassigned members are folded into Main Branch** via `CASE WHEN branch_name = 'Unassigned' THEN 'Main Branch'` and re-aggregated — the base fact table keeps Unassigned as its own row for traceability ([Decision 28](decisions_log.md)).

```sql
CREATE OR REPLACE VIEW v_home_branch_members_monthly AS
WITH rolled AS (
    SELECT hbms.snapshot_date,
           CASE WHEN branch.branch_name = 'Unassigned' THEN 'Main Branch'
                ELSE branch.branch_name END AS branch_name,
           hbms.total_members, hbms.members_added
    FROM fct_home_branch_members_summary AS hbms
    JOIN dim_branch AS branch ON branch.branch_key = hbms.branch_key
)
SELECT rolled.snapshot_date, rolled.branch_name, branch.abbreviation,
       SUM(rolled.total_members)::INTEGER AS total_members,
       SUM(rolled.members_added)::INTEGER AS members_added
FROM rolled
JOIN dim_branch AS branch ON branch.branch_name = rolled.branch_name
GROUP BY rolled.snapshot_date, rolled.branch_name, branch.abbreviation;
```

### `v_members_lost`

Slim projection of `fct_members_lost` for Zoho close-reason aggregation. Drops `event_id`, `source_system`, `source_person_serial`, `person_key`, `month_id`, `loaded_at`.

```sql
CREATE OR REPLACE VIEW v_members_lost AS
SELECT event_date, close_reason, membership_duration_yrs, first_gensh_opened
FROM fct_members_lost;
```

### `v_cc_loans_growth_ytd_monthly`

YTD growth percentages + total dollar balances for credit cards and loans, projected from `fct_loan_portfolio_summary`. Feeds the "Credit Cards & Loans" Zoho panel (growth bars) and any tile that needs the underlying balances. **All four columns are CSV-sourced** via the manual-entries / backfill flow into `fct_loan_portfolio_summary` — accounting does math on the balances that isn't visible in staging, so we never derive these from `staging.ks_loan` or `fct_loans`.

**Storage vs display:** the base columns store growth as decimals (0.0133 for 1.33%). The view multiplies the growth columns by 100 so Zoho's pct display type renders them correctly. Balances are full dollar amounts and pass through unchanged.

```sql
CREATE OR REPLACE VIEW v_cc_loans_growth_ytd_monthly AS
SELECT snapshot_date,
       (cc_growth_ytd_pct   * 100)::NUMERIC(8,4) AS credit_cards_growth_ytd_pct,
       (loan_growth_ytd_pct * 100)::NUMERIC(8,4) AS loans_growth_ytd_pct,
       cc_total_balance                          AS credit_cards_total_balance,
       loan_total_balance                        AS loans_total_balance
FROM fct_loan_portfolio_summary;
```

### `v_loans_by_branch_lsi_in_main`

`fct_loans_by_branch_summary` with the LSI Branch (online applications) `branch_key` re-mapped to Main Branch and re-aggregated. Base table keeps LSI separate; this view is the LSI-in-Main version. Zoho consumes the base table for the LSI-separate panel and this view for the LSI-in-Main panel.

```sql
CREATE OR REPLACE VIEW v_loans_by_branch_lsi_in_main AS
WITH remapped AS (
    SELECT loans.snapshot_date,
           CASE WHEN src_branch.branch_name = 'LSI Branch - Online Applications'
                THEN (SELECT branch_key FROM dim_branch WHERE branch_name = 'Main Branch')
                ELSE loans.branch_key END AS branch_key,
           loans.loan_count, loans.payday_loan_count, loans.non_payday_loan_count
    FROM fct_loans_by_branch_summary AS loans
    JOIN dim_branch AS src_branch ON src_branch.branch_key = loans.branch_key
)
SELECT remapped.snapshot_date, branch.branch_name,
       SUM(remapped.loan_count) AS loan_count,
       SUM(remapped.payday_loan_count) AS payday_loan_count,
       SUM(remapped.non_payday_loan_count) AS non_payday_loan_count
FROM remapped
JOIN dim_branch AS branch ON branch.branch_key = remapped.branch_key
GROUP BY remapped.snapshot_date, branch.branch_name;
```

### `v_financial_ratios_monthly`

Percentage ratios from `fct_financial_ratios_summary`. Powers the Org Goals dashboard's pct panels and KPI tiles (Loan/Share, Delinquency, ROA, Operation Expense, Net Worth, Loan Growth, Loan Charge-Off). All dashboard threshold/goal lines (75%/85% L/S, 1% delinquency, 11% net worth, ROA/OpExp goals) are entered directly in Zoho.

**Storage vs display:** the base columns store percentages as decimals (0.7821 for 78.21%). Zoho's percentage data type appends "%" without multiplying by 100, so the view multiplies each pct column by 100 before returning. Numeric (non-pct) ATM-network columns live in `v_atm_network_monthly` because they don't need this transformation.

```sql
CREATE OR REPLACE VIEW v_financial_ratios_monthly AS
SELECT snapshot_date,
       (net_worth_pct          * 100)::NUMERIC(8,4) AS net_worth_pct,
       (roa_pct                * 100)::NUMERIC(8,4) AS roa_pct,
       (op_expense_pct         * 100)::NUMERIC(8,4) AS op_expense_pct,
       (loan_to_share_pct      * 100)::NUMERIC(8,4) AS loan_to_share_pct,
       (loan_delinquency_pct   * 100)::NUMERIC(8,4) AS loan_delinquency_pct,
       (loan_charge_off_ratio  * 100)::NUMERIC(8,4) AS loan_charge_off_ratio,
       (loan_growth_pct        * 100)::NUMERIC(8,4) AS loan_growth_pct
FROM fct_financial_ratios_summary;
```

### `v_atm_network_monthly`

ATM network usage (member + foreign transaction counts) and YTD network net income from `fct_financial_ratios_summary`. Split out from `v_financial_ratios_monthly` because these are counts and dollars, not percentages, so they don't need the ×100 transformation.

```sql
CREATE OR REPLACE VIEW v_atm_network_monthly AS
SELECT snapshot_date,
       atm_member_uses,
       atm_foreign_uses,
       atm_network_ytd_net_income
FROM fct_financial_ratios_summary;
```

### `v_nps_summary`

NPS score time series from `fct_nps_summary`. Sparse — a row exists only for months when a survey actually ran. Feeds the "Most Recent NPS Score" tile (`MAX(snapshot_date)`).

```sql
CREATE OR REPLACE VIEW v_nps_summary AS
SELECT snapshot_date,
       nps_score
FROM fct_nps_summary;
```

---

## Utility Functions

### Age calculation

`birth_date DATE` is stored on `person_snapshot_daily` and `person_snapshot_monthly`. Age and age group are never stored — they are computed at query or report time.

**Age buckets:** `<18` / `18-24` / `25-34` / `35-44` / `45-54` / `55-64` / `65-74` / `75+`

**Two implementations kept in sync:**

**PostgreSQL** (`queries/warehouse_functions.sql`, loaded by `warehouse_create_functions` asset):
```sql
-- Age in whole years as of a reference date (default: today)
dev.calc_age(birth_date DATE, as_of DATE DEFAULT CURRENT_DATE) → SMALLINT

-- Standard age group bucket string
dev.age_group(age SMALLINT) → TEXT
```

Usage in views:
```sql
dev.calc_age(person_daily.birth_date)            AS age,
dev.age_group(
    dev.calc_age(person_daily.birth_date))       AS age_group
```

**Python** (`pipelines/src/pipelines/utils.py`):
```python
from pipelines.utils import calc_age, age_group

df["age"]       = calc_age(df["birth_date"], as_of=snapshot_date)
df["age_group"] = age_group(df["age"])
```

Both implementations use the same birthday-aware logic: subtract 1 year if the birthday has not yet occurred in the reference year (matches `person_service_details_query.sql`). If the bucket definitions ever change, update both files and this document.

The Python type annotation rule that used to live here is now in [engineering.md → Code style](engineering.md#code-style) since it applies repo-wide, not just here.
