# Credit Union Data Warehouse & Pipeline Platform

A production data platform I designed and built solo for a credit union: it extracts from four source-system types (IBM DB2 core banking, a JasperServer reporting API, the HubSpot CRM API, and hand-maintained accounting spreadsheets), loads a structured PostgreSQL warehouse, and feeds the BI dashboards leadership actually uses for board reporting.

It replaced a silently-failing mesh of manually maintained CSVs — dashboards had been showing stale or empty panels for months without anyone noticing. The platform is live: nightly and monthly Dagster jobs, an auditable load trail, and a month-end workflow that a non-technical accounting user drives by editing a single CSV.

> **Note:** This is a sanitized portfolio copy of the production repository. All credentials, member data, internal hostnames/IPs, and real financial values are excluded or genericized; the gitignored `data/`, `secrets/`, and `backups/` directories ship empty by design.

## Stack

- **PostgreSQL 16** — warehouse, with `dev`/`test`/`prod` as schema namespaces in one database, switched via `search_path`
- **Dagster** — orchestration: partitioned assets, schedules, drop-folder sensors, asset-graph-as-documentation
- **Python / pandas / SQLAlchemy** — extract + load code; `ibm_db` for DB2, REST for JasperServer and HubSpot
- **Docker Compose** — five services (Postgres, Dagster webserver + daemon, pg_dump backup sidecar, pgAdmin); the whole stack migrates with a directory copy + backup restore (it has actually done so — laptop to VM)

## Engineering practices

- **Idempotency everywhere** — every load is upsert or truncate-and-reload; a re-run is a recovery, not a corruption
- **Cadence-agnostic schema** — `snapshot_date DATE` is the universal time key; switching a pipeline from monthly to daily is a schedule change, not a migration
- **Fail loudly** — unknown lookup values, unexpected NULLs, and shape mismatches raise; nothing is silently skipped (silent failure is the disease this platform cured)
- **Zero tolerance for count discrepancies** — warehouse counts must match the source system exactly; a QA tool (`qa/`) runs paired DB2-vs-warehouse queries to prove it
- **Observability by default** — every load writes batch ID, row counts, timing, and status to a `load_audit` table
- **Hard runtime guards** — a schema-intent gate blocks writes when the running environment doesn't match the operator's declared intent; staging-freshness and snapshot-DB-date assertions catch stale-upstream and wrong-partition loads before they write
- **Integration tests against a disposable schema** — the suite drops/rebuilds a `test` schema from the real DDL on each run, exercises real assets end-to-end (including the month-end CSV workflow), and refuses to target any non-disposable schema

## Repo tour

| Path | What it is |
|---|---|
| `pipelines/src/pipelines/` | Dagster assets, resources, schedules, sensors, shared loading helpers |
| `pipelines/src/pipelines/queries/` | All DB2 + warehouse SQL (DDL in `warehouse_schema.sql`) |
| `pipelines/tests/` | Unit tests + the integration suite (`tests/integration/`) |
| `docs/` | Full documentation set — start at `docs/README.md` |
| `docs/design_documentation_portfolio.md` | Design-decision narrative: the why behind the architecture |
| `docs/decisions_log.md` | 47 numbered decisions with rationale, in chronological order |
| `qa/` | Source-vs-warehouse comparison tooling and operational scripts |
| `CLAUDE.md` | Working agreement for AI-assisted development on this repo |

## Running it

```bash
cp .env.example .env        # fill in values; secrets/ needs postgres_password.txt
docker compose up -d        # Dagster at :3000, pgAdmin at :5050
```

Materialize `warehouse_schema_mode_select` → `warehouse_tables` in Dagster to build the schema. Source-system assets need network reach to a DB2 instance / JasperServer / HubSpot account and won't run outside that environment — but the schema, seed data, file-loader assets, and the full test suite (`cd pipelines && uv run pytest`) work anywhere Docker does.
