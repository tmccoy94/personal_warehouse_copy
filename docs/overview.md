# Warehouse Overview

High-level picture of what's in the warehouse: object counts, full table inventory, dormant helpers, and the running "in progress" list.

**Contents**
- [Summary](#summary)
- [Full Table Inventory](#full-table-inventory)
- [Dormant Scripts](#dormant-scripts)
- [Known Issues / In Progress](#known-issues--in-progress)

---

## Summary

**49 warehouse objects** across 3 active dimensions (+ 1 stub), 3 canonical person tables, 2 person snapshot tables, 4 Keystone supplemental snapshot tables, 6 HubSpot source tables, 17 active fact tables (+ 2 stub), 1 NPS table, 8 BI presentation views, and 1 operational table. The `staging` schema additionally holds 14 Keystone CORE daily mirror tables (`staging.ks_*`), 14 matching monthly mirror tables (`staging.ks_*_monthly`), 1 daily ENV posting-date mirror, 1 monthly ENV mirror, and 1 pipeline intermediary table, counted separately.

| Category | Count |
|---|---|
| Active dimensions | 3 |
| Stub dimensions (schema only) | 1 |
| Canonical person tables | 3 |
| Person snapshot tables | 2 |
| Keystone supplemental snapshot tables | 4 |
| HubSpot source tables | 6 |
| Active fact tables | 17 |
| Stub fact tables (schema only) | 2 |
| Member satisfaction (NPS) | 1 |
| BI presentation views (`v_*`) | 8 |
| Operational tables | 1 |
| **Warehouse total** | **48** |
| `staging.ks_*` CORE daily mirrors (counted separately) | 14 |
| `staging.ks_*_monthly` CORE point-in-time mirrors | 14 |
| `staging.ks_env` + `staging.ks_env_monthly` posting-date mirrors | 2 |
| `staging` pipeline intermediaries | 1 |

**Scope:**
- v1 covers members, financial ratios, loan portfolio aggregates, digital channels, ATM, branch profitability, teller operations, and home branch member tracking.
- Lending detail dashboard, EasyVest, and call center data are explicitly out of v1. Call center gets stub tables so the schema is ready when the migration happens.
- Members added/lost comes from DB2 queries against the CORE schema — not the old CSVs.
- Home branch member counts are a separate set of tables from regular member counts because they use a different counting methodology (see note below).

**Source DB structure (learned from queries):**
The core system is Keystone/IBM DB2 under a `CORE` schema. Membership is defined by an open `CORE.SHARE` with `ID = '0001'` (the General Share). The two member counting methodologies that explain the number differences across reports:
- **Opening branch:** `SHARE.BRANCH_SERIAL` on the oldest share — the branch where the member first opened
- **Home branch:** ZIP-code-based territory assignment (checked first), falling back to `CORE.CU_HOME_BRANCH` if the ZIP is null or unmatched. Because home branch is assigned by territory logic in Python — not read directly from CORE — the counts can differ from `CORE.CU_HOME_BRANCH` alone. The production implementation is `assign_branch_with_fallback` in `pipelines/src/pipelines/defs/assets/warehouse_loaders/fct_members.py` (loads `fct_home_branch_members_summary`); the original exploratory notebook is preserved at `qa/homebranch script check/home_branch_member_count.ipynb`.

Both are legitimate and used in different reports. They go into different tables in the warehouse.

---

## Full Table Inventory

| Table | Type | Grain | Status | Primary Source |
|---|---|---|---|---|
| `dim_date` | Dimension | Day | Active | Generated |
| `dim_branch` | Dimension | Branch | Active | query keystone db branch table |
| `person_identity` | Canonical Person | Person | Active | Populated on first person snapshot load |
| `person_source_identifier` | Canonical Person | Person × Source | Active | Populated alongside `person_identity` on first load per source |
| `person_contact` | Canonical Person | Person × Contact Type × Snapshot Date | Active | DB2 via `person_contact_details.sql` + `person_address.sql`; partitioned snapshot; multi-source EAV |
| `dim_employee` | Dimension | Employee | Active | `user_roles.sql` |
| `dim_goals` | Dimension | Metric + Year | **Stub** | Manual seed (future) |
| `staging.person_snapshot_stage` | Staging | Current person state | Operational | DB2 `person_snapshot_daily.sql` — truncate/reload once per job run |
| `staging.ks_person_monthly_snapshot_stage` | Staging | Current person state (month-end) | Operational | DB2 snapshot environment — used by monthly pipeline on 1st of each month |
| `staging.ks_person` | CORE Mirror | Current CORE.PERSON | Operational | `ks_person_mirror_table` — simple CORE.PERSON mirror, truncate/reload nightly |
| `staging.ks_account` | CORE Mirror | Current CORE.ACCOUNT | Operational | `ks_staging_account_table` — includes eligibility/type/relationship descriptions |
| `staging.ks_share` | CORE Mirror | Current CORE.SHARE | Operational | `ks_staging_share_table` — joins ACCOUNT for person_serial; includes open/close/charge-off dates |
| `staging.ks_home_branch` | CORE Mirror | Current CU_HOME_BRANCH | Operational | `ks_staging_home_branch_table` — NOT EXISTS anti-join for latest per person |
| `staging.ks_person_score` | CORE Mirror | Current PERSON_SCORE | Operational | `ks_staging_person_score_table` — latest score per person via NOT EXISTS |
| `staging.ks_login` | CORE Mirror | Current CORE.LOGIN | Operational | `ks_staging_login_table` |
| `staging.ks_transaction` | CORE Mirror | Rolling 90 days | Operational | `ks_staging_transaction_table` — **incremental**: append new + trim >90 days |
| `staging.ks_monetary` | CORE Mirror | Rolling 90 days | Operational | `ks_staging_monetary_table` — **incremental**: append new + trim >90 days |
| `staging.ks_loan` | CORE Mirror | Current CORE.LOAN | Operational | `ks_staging_loan_table` |
| `staging.ks_app` | CORE Mirror | Current CORE.APP | Operational | `ks_staging_app_table` |
| `staging.ks_loan_request` | CORE Mirror | Current CORE.LOAN_REQUEST | Operational | `ks_staging_loan_request_table` |
| `staging.ks_applicant` | CORE Mirror | Current CORE.APPLICANT | Operational | `ks_staging_applicant_table` |
| `staging.ks_applicant_score` | CORE Mirror | Current CORE.APPLICANT_SCORE | Operational | `ks_staging_applicant_score_table` |
| `staging.ks_decision` | CORE Mirror | Current CORE.DECISION | Operational | `ks_staging_decision_table` |
| `staging.ks_env` | CORE Mirror | Single-row CORE.ENV posting date | Operational | `ks_staging_env_table` — mirrors the DB2 business date so warehouse date-window queries align with CORE |
| `staging.ks_*_monthly` (14 tables) | CORE Monthly Mirror | Point-in-time at partition EOM | Operational | `ks_staging_*_monthly_table` (in `staging_tables_ks_monthly.py`) — MonthlyPartitionsDefinition loaders that pull from the frozen MONTH_END_x snapshot DB. `ks_transaction_monthly` + `ks_monetary_monthly` are loaded by copying from the daily 90-day rolling staging filtered to the partition's 30-day window. Backs monthly fact accuracy. |
| `staging.ks_env_monthly` | CORE Monthly Mirror | Single-row snapshot POSTING_DATE | Operational | `ks_staging_env_monthly_table` — equals the partition's EOM after the `assert_snapshot_db_matches_partition` guard passes |
| `person_snapshot_daily` | Person Snapshot | Date × Person | Active | `staging.person_snapshot_stage` (via `ks_person_snapshot_stage_table`) |
| `person_snapshot_monthly` | Person Snapshot | Month × Person | Active | Populated from `person_snapshot_daily` on first run of month |
| `ks_products_snapshot` | KS Supplemental Snapshot | Date × Person × Product | Active | `staging.ks_share` |
| `ks_relationships_snapshot` | KS Supplemental Snapshot | Date × Person | Active | `staging.ks_account` |
| `ks_credit_score_snapshot` | KS Supplemental Snapshot | Date × Person | Active | `staging.ks_person_score` |
| `ks_member_branch_snapshot` | KS Supplemental Snapshot | Date × Person | Active | `staging.ks_share` + `staging.ks_home_branch` |
| `hs_contacts` | HubSpot Source | Contact (current state) | Active | HubSpot CRM API `/crm/v3/objects/contacts` |
| `hs_companies` | HubSpot Source | Company (current state) | Active | HubSpot CRM API `/crm/v3/objects/companies` |
| `hs_deals` | HubSpot Source | Deal (current state) | Active | HubSpot CRM API `/crm/v3/objects/deals` |
| `hs_contact_company_association` | HubSpot Source | Contact × Company | Active | HubSpot Associations API |
| `hs_deal_contact_association` | HubSpot Source | Deal × Contact | Active | HubSpot Associations API |
| `hs_deal_company_association` | HubSpot Source | Deal × Company | Active | HubSpot Associations API |
| `fct_members_summary` | Fact | Snapshot Date | Active | `staging.ks_*_monthly` point-in-time mirrors via validated QA logic (active, 18-50, non-KCPD, hb figures) |
| `fct_home_branch_members_summary` | Fact | Snapshot Date × Branch | Active | DB2 `home_branch_zips.sql` + `home_branch_opened.sql` with ZIP-territory assignment (CSV backfill superseded) |
| `fct_members_added` | Fact | Person × Month | Active | DB2 `members_added.sql` (per-month load via Dagster monthly partition) |
| `fct_members_lost` | Fact | Person × Month | Active | DB2 `members_lost.sql` (per-month load via Dagster monthly partition) |
| `fct_financial_ratios_summary` | Fact | Snapshot Date | Active | Net-Worth, ROA, OpExp, Loan_to_Share, Delinquency, Charge-Off, Loan_Growth, ATM network CSVs |
| `fct_nps_summary` | Member Satisfaction | Snapshot Date | Active | NPS Score.csv (split from financial ratios — NPS is member satisfaction, not a financial ratio) |
| `fct_loan_portfolio_summary` | Fact | Snapshot Date | Active | CFO_Dashboard + LoanCreditCardTotals + MarketingDBData + _Fraud_Charge_Off_Long |
| `fct_loans_by_branch_summary` | Fact | Snapshot Date × Branch | Active | Derived from `staging.ks_loan` (`LOAN.BRANCH_SERIAL` is fully populated, unlike `LOAN_REQUEST.BRANCH_SERIAL`); CSV backfill superseded |
| `fct_loan_applications` | Fact | Application | Active | `staging.ks_app_monthly` + `staging.ks_applicant_monthly` + `staging.ks_applicant_score_monthly` + `staging.ks_loan_request_monthly` (PostgreSQL transforms) |
| `fct_loan_requests` | Fact | Loan Request | Active | `staging.ks_loan_request_monthly` + `staging.ks_decision_monthly` (PostgreSQL transforms) |
| `fct_loans` | Fact | Loan × Snapshot Date | Active | `staging.ks_loan_monthly` + `staging.ks_loan_request_monthly` (PostgreSQL transforms, truncate/refill) |
| `fct_digital_channels_transactions` | Fact | Snapshot Date | Active | `manual_entries.csv` (bill pay + mobile/online transaction counts, hand-entered from platform dashboards) |
| `fct_digital_enrollments` | Fact | Snapshot Date × Service | Active | rdcusers + billpayusers CSV drops + `manual_entries.csv` (home_banking row) — enrolled/active counts |
| `fct_atm_by_machine_summary` | Fact | Snapshot Date × ATM | Active | Number_of_Transactions_by_ATM |
| `fct_branch_profitability_summary` | Fact | Snapshot Date × Branch | Active | Branch_Profitability |
| `fct_teller_activity` | Fact | Employee × Date | Active | TellerActivity |
| `fct_teller_out_of_balance` | Fact | Event | Active | Teller_Transactions_out_of_Balance |
| `fct_survey_responses` | Fact | Response | Active | `transformed_survey_results.csv` via drop-folder |
| `fct_call_center_summary` | Fact | Snapshot Date | **Stub** | BI_Call_Performance (future) |
| `fct_call_center_agents` | Fact | Snapshot Date × Employee | **Stub** | Call_Center_Monthly (future); agents reference dim_employee |
| `v_members_monthly` | BI View | Snapshot Date | Active | Pass-through of `fct_members_summary` for BI consumption |
| `v_home_branch_members_monthly` | BI View | Snapshot Date × Branch | Active | `fct_home_branch_members_summary` with Unassigned folded into Main Branch |
| `v_members_lost` | BI View | Person × Month | Active | Slim projection of `fct_members_lost` for Zoho |
| `v_loans_by_branch_lsi_in_main` | BI View | Snapshot Date × Branch | Active | `fct_loans_by_branch_summary` with LSI Branch rolled into Main Branch |
| `v_cc_loans_growth_ytd_monthly` | BI View | Snapshot Date | Active | Credit card + loan YTD growth % + total balances ($), projected from `fct_loan_portfolio_summary` for the "Credit Cards & Loans" Zoho panel |
| `v_financial_ratios_monthly` | BI View | Snapshot Date | Active | Percentage ratios from `fct_financial_ratios_summary` (Loan/Share, Delinquency, ROA, OpExp, Net Worth, Loan Growth, Loan Charge-Off) — multiplied by 100 in the view for Zoho's pct display type |
| `v_atm_network_monthly` | BI View | Snapshot Date | Active | ATM network counts + YTD net income from `fct_financial_ratios_summary` (split out from the ratios view because these are counts and dollars, not percentages) |
| `v_nps_summary` | BI View | Snapshot Date | Active | NPS score time series from `fct_nps_summary` (Org Goals "Most Recent NPS Score" tile) |

---

## Dormant Scripts

### `queries/core_conversion_xref.sql`
Not imported or run by any Dagster asset. Creates `warehouse.person_source_xref`, a crosswalk table mapping old and new `source_person_serial` values to the stable warehouse `person_key` in `person_identity`. Only run this script if the credit union converts to a new core system. After populating the crosswalk, update `person_identity.source_person_serial` to the new source IDs.

---

## Known Issues / In Progress

**Intentional / by design:**
- SSL verification disabled on JasperServer (internal CA validation infeasible — see [operations.md → Environment & Secrets](operations.md#environment--secrets))

**Operational notes:**
- `staging` schema tables persist across active-schema rebuilds (not dropped by `DROP SCHEMA dev CASCADE`)
- `ks_products_snapshot` is long format: one row per person × product type per snapshot date; `product_count SMALLINT` counts accounts of that type
- `ks_person_contact_table` queries DB2 independently via `person_contact_details.sql` + `person_address.sql`; calls `upsert_person_identity()` directly; DELETE-before-INSERT per partition
- `staging.ks_transaction` and `staging.ks_monetary` use **incremental loading** (append since `MAX(posting_date)`, trim >90 days) — full reload was taking 15+ minutes
- `pipelines/src/pipelines/.schema_intent` — plain-text schema-name file written by `switch_schema.py`. Not committed to git

**Pending implementation:**
- `record_hash` deduplication: computed on `person_snapshot_daily` but skip-if-unchanged optimization is deferred to v1.1 — every daily run still writes a new row
- Masking view for PII on person snapshot tables not yet created
- `fct_loans` has `TO_RESOLVE` comments for a few CORE.LOAN column names — verify in DataGrip before relying
- HubSpot property names need verification against the live sandbox — marked `TO_RESOLVE` in `hubspot_loaders/hs_objects.py`
- `ks_person_mirror_table` (`staging.ks_person`) may be redundant with `staging.person_snapshot_stage` — evaluate for removal before going live
- Monthly fact assets (`ks_fct_loans_*`, `ks_fct_members_*`) not yet added to `ks_monthly_job` — add once daily pipeline is stable
- `restart: unless-stopped` commented out on all Docker services — uncomment before deploying to the VM
- `copy_schema.py` at project root copies all data from one schema to another (e.g., `python copy_schema.py dev prod`) — target schema must have tables created via `warehouse_tables` first
- `fct_members_branch_summary` physical table may still exist in prod from before [Decision 31](decisions_log.md#31); safe to `DROP TABLE IF EXISTS prod.fct_members_branch_summary`
