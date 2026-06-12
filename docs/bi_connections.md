# BI Tool Connections

How downstream BI tools connect to the warehouse. Covers Zoho Analytics (via DataBridge) and Power BI (native connector). Both read from the `warehouse` database; point them at the `prod` schema once it's populated (`dev` for local testing).

**Contents**
- [Connecting Zoho Analytics](#connecting-zoho-analytics)
- [Connecting Power BI](#connecting-power-bi)
- [Suggested tables to import](#suggested-tables-to-import)

> **Port note (applies to both tools):** On the VM the stack runs on `5432:5432` (standard). On a local dev machine with an existing Postgres install, the host port is mapped to `5433` — use `5433` in the connection URL if connecting to a dev machine.

---

## Connecting Zoho Analytics

Zoho Analytics connects to the warehouse via JDBC through the **DataBridge** agent. The DataBridge is installed on a machine on the same internal network as the server running Docker (the BI-server DataBridge is already configured). It creates an outbound tunnel to Zoho's cloud — no inbound firewall ports required.

### Prerequisites

1. **PostgreSQL JDBC driver** must be in `<DataBridge install dir>/lib/drivers/`. Download `postgresql-<version>.jar` from the PostgreSQL JDBC site and drop it in. Restart the DataBridge after adding it.
2. The DataBridge agent must show **Active** in Zoho Analytics → Data Sources.

### Connection details

In Zoho Analytics → New Data Source → Connect using JDBC URL:

| Field | Value |
|---|---|
| Driver Class | `org.postgresql.Driver` |
| JDBC URL | `jdbc:postgresql://10.0.0.10:5432/warehouse` |
| Username | `warehouse_admin` |
| Password | from `.env` (`DAGSTER_POSTGRES_PASSWORD`) |
| Databridge | select the active BI-server agent |

**Schema to point at:** Use `prod` once the production schema is populated. The JDBC URL connects to the `warehouse` database; Zoho will let you browse schemas and select tables from there.

### Authentication note — "authentication type 10 is not supported"

If the connection fails with **"The authentication type 10 is not supported. Check that you have configured the pg_hba.conf file..."**, the DataBridge's bundled PostgreSQL JDBC driver is too old to speak SCRAM-SHA-256 (auth type 10, the postgres:16 default). The error text is misleading — it is not actually a `pg_hba.conf` IP/subnet problem.

The fix used on this warehouse is to downgrade the server to md5 auth for compatibility (acceptable on the private on-prem network — same network-boundary reasoning as the JasperServer SSL exception). **This configuration lives in the Postgres data volume and role catalog, NOT in the pg_dump** — it must be re-applied any time the database is restored onto a fresh volume (e.g., after moving to a new server):

```bash
# 1. Switch password hashing to md5, then re-hash the role's password.
#    ALTER USER must run AFTER the setting change. Use the exact password
#    from .env (DAGSTER_POSTGRES_PASSWORD).
docker exec -i warehouse-postgres psql -U warehouse_admin -d warehouse <<'EOF'
ALTER SYSTEM SET password_encryption = 'md5';
SELECT pg_reload_conf();
EOF
docker exec -i warehouse-postgres psql -U warehouse_admin -d warehouse \
  -c "ALTER USER warehouse_admin WITH PASSWORD '<password from .env>';"

# 2. Change the pg_hba rule from scram-sha-256 to md5 and reload.
docker exec warehouse-postgres sh -c \
  "sed -i 's/host all all all scram-sha-256/host all all all md5/' /var/lib/postgresql/data/pg_hba.conf"
docker exec warehouse-postgres psql -U warehouse_admin -d warehouse -c "SELECT pg_reload_conf();"

# 3. Verify: should print 'md5' and a password hash starting with 'md5'.
docker exec warehouse-postgres psql -U warehouse_admin -d warehouse \
  -c "SHOW password_encryption;" \
  -c "SELECT substring(rolpassword,1,6) FROM pg_authid WHERE rolname='warehouse_admin';"
```

No container restart is needed — `pg_reload_conf()` picks up both changes. The Dagster containers and backup sidecar are unaffected (psycopg and pg_dump speak both auth types). Retry the Zoho connection afterwards.

Both halves matter: with an md5 `pg_hba` rule but a SCRAM-stored password, the server still demands SCRAM from the client and the old driver still fails — the password must be re-hashed (step 1) *and* the hba rule changed (step 2).

**Long-term alternative:** drop a current PostgreSQL JDBC driver (`postgresql-42.7.x.jar`) into `<DataBridge install dir>/lib/drivers/` and restart the DataBridge — then the server could return to SCRAM defaults. Not done so far because restarting the shared DataBridge is disruptive.

---

## Connecting Power BI

Power BI Desktop connects to PostgreSQL natively via the built-in PostgreSQL connector. No DataBridge or extra agent needed — it connects directly from the machine running Power BI Desktop to the server IP.

### Prerequisites

1. **Npgsql driver** — Power BI's PostgreSQL connector requires the Npgsql data provider. Download `Npgsql-<version>.msi` from the Npgsql GitHub releases page and install it on the machine running Power BI Desktop. Restart Power BI after installing.
2. The server's firewall must allow inbound on port `5432` from the Power BI machine's IP.

### Connecting

In Power BI Desktop → Get Data → PostgreSQL database:

| Field | Value |
|---|---|
| Server | `10.0.0.10:5432` |
| Database | `warehouse` |
| Data Connectivity mode | **Import** (recommended for dashboards) or DirectQuery |
| Username | `warehouse_admin` |
| Password | from `.env` (`DAGSTER_POSTGRES_PASSWORD`) |

After connecting, Power BI shows a navigator with all schemas. Expand `prod` (or `dev` for local testing) and select the tables you want.

**Import vs DirectQuery:** Import pulls a snapshot into Power BI's in-memory model — fast dashboards, scheduled refresh. DirectQuery queries live against the DB on every interaction — always current but slower. Given the warehouse already aggregates data nightly into `fct_*` tables, Import with a scheduled refresh (e.g., daily after the `ks_daily_job` completes) is the right call.

### Scheduled refresh (Power BI Service)

To publish reports to Power BI Service and keep them fresh:

1. Publish the `.pbix` from Power BI Desktop to your workspace
2. Install the **On-premises data gateway** on the same machine/server running Docker — this is Power BI's equivalent of Zoho's DataBridge, needed for the Service to reach an on-prem database
3. In Power BI Service → Dataset settings → Gateway connection → map to the gateway
4. Set a scheduled refresh — daily at a time after `nightly_ks_schedule` (11pm Central) finishes is ideal

---

## Suggested tables to import

Start with the pre-aggregated fact tables — these are what the dashboards consume directly:

- `fct_members_summary` — monthly member counts by branch
- `fct_home_branch_members_summary` — home branch member counts
- `fct_financial_ratios_summary` — net worth, ROA, loan-to-share, delinquency, etc.
- `fct_nps_summary` — NPS scores
- `fct_loan_portfolio_summary` — loan/share/CC balances and fraud figures
- `fct_loans_by_branch_summary` — loan originations by branch
- `fct_atm_by_machine_summary` — ATM transaction counts
- `fct_branch_profitability_summary` — branch net income
- `fct_teller_activity` — teller transaction counts
- `fct_digital_enrollments` — home banking / bill pay / RDC enrollment counts
- `fct_survey_responses` — member survey ratings
- `dim_branch` — branch dimension (for labels/filters)
- `dim_date` — date dimension (for time-based filtering)
