# Development

The branching workflow, local development setup, and PR/deploy process for the warehouse pipeline. Written for a solo developer today but structured to scale cleanly if the team grows.

**Contents**
- [Environment mapping](#environment-mapping)
- [Local dev setup](#local-dev-setup)
- [Branching strategy](#branching-strategy)
- [Development workflow](#development-workflow)
- [Commit standards](#commit-standards)
- [What goes in a PR vs what doesn't](#what-goes-in-a-pr-vs-what-doesnt)
- [Prod backfill after feature merge](#prod-backfill-after-feature-merge)
- [If something goes wrong in prod](#if-something-goes-wrong-in-prod)

---

## Environment mapping

| Environment | Where it runs | Schema | How to set |
|---|---|---|---|
| Dev | Developer's local machine | `dev` | `WAREHOUSE_SCHEMA=dev` in `.env` |
| Test | VM `10.0.0.10` | `test` | `WAREHOUSE_SCHEMA=test` in `.env` |
| Prod | VM `10.0.0.10` | `prod` | `WAREHOUSE_SCHEMA=prod` in `.env` |

The same codebase runs in all environments — `WAREHOUSE_SCHEMA` in `.env` controls which schema is active. Use `switch_schema.py` (GUI) or `Set-WarehouseSchema.ps1` (terminal) to switch and restart containers. See [`operations.md → Schema Environments`](operations.md#schema-environments--dev--test--prod) for how `search_path` routing works.

---

## Local dev setup

```powershell
# Clone and enter the project
git clone <repo> dagster_postgres_docker
cd dagster_postgres_docker

# Add credentials to .env (copy from a teammate or 1Password — never commit this file)
# Ensure WAREHOUSE_SCHEMA=dev is set in .env

# Start the stack
docker compose up -d

# Dagster UI: http://localhost:3000
# pgAdmin:    http://localhost:5050
```

On first run, materialize `warehouse_schema_mode_select` → `warehouse_tables` → `warehouse_schema_ready` to initialize the dev schema. All data loaded locally stays in the `dev` schema and does not touch prod.

---

## Branching strategy

### Branch naming

```
feature/<short-description>     # new asset, table, or pipeline capability
fix/<short-description>         # bug fix in an existing asset
schema/<short-description>      # DDL change to warehouse_schema.sql
refactor/<short-description>    # code restructure with no behavior change
docs/<short-description>        # documentation only
```

Examples:
```
feature/hs-contacts-loader
fix/fct-loans-partition-missing
schema/add-product-count-column
docs/branching-workflow
```

### One branch = one logical change

A branch should represent one cohesive unit of work — a new loader, a schema addition, a bug fix. Keep branches small and short-lived. Long-lived branches diverge from main and become painful to merge.

---

## Development workflow

```
main (prod-ready code)
  └── feature/my-new-asset  ← you work here
```

### Step 1 — Branch from main

```powershell
git checkout main
git pull
git checkout -b feature/my-new-asset
```

### Step 2 — Develop locally against dev schema

- Write assets, DDL changes, queries, and docs
- Test by materializing in the Dagster UI against your local `dev` schema
- Drop and recreate `dev` freely: `DROP SCHEMA IF EXISTS dev CASCADE` in pgAdmin, then rerun `warehouse_tables`
- Never point your local stack at `prod` during active development

### Step 3 — Before opening a PR

Run through this checklist:

- [ ] Asset materializes cleanly in dev with no errors
- [ ] `warehouse_schema.sql` DDL updated if any new tables or column changes
- [ ] `docs/tables.md` updated (new table section, DDL block) and `docs/decisions_log.md` updated (Decisions Log entry if non-obvious); update `docs/overview.md` inventory + counts if shifted
- [ ] `CLAUDE.md` Known Issues / Pending Implementation updated if applicable
- [ ] No hardcoded dates in any `.sql` file
- [ ] No credentials, `.env` changes, or secrets committed
- [ ] Partition `start_date` set to a recent date for dev (will be updated to historical start before prod backfill)

### Step 4 — Open a PR into main

PR title format: `[type] short description`
Examples:
```
[feature] HubSpot contacts loader
[fix] fct_loans partition missing ensure_snapshot_partition
[schema] Add product_count to ks_products_snapshot
```

PR description should answer:
- **What changed**: which assets, tables, or queries
- **Why**: the business reason or bug being fixed
- **How to test**: which assets to materialize and what to verify in pgAdmin

### Step 5 — Merge and deploy to prod

```powershell
# After PR is merged to main, on the VM:
git pull origin main

# Switch to prod schema and restart
.\Set-WarehouseSchema.ps1 prod

# In Dagster UI: materialize warehouse_schema_mode_select
# If DDL changed: materialize warehouse_tables
# Then materialize the new/changed assets
```

If the PR includes a schema change (`warehouse_schema.sql`), drop and recreate `prod` before running:
```sql
-- In pgAdmin on the VM (only for DDL changes):
DROP SCHEMA IF EXISTS prod CASCADE;
```
Then rerun `warehouse_tables` to rebuild.

---

## Commit standards

**Commit small and often.** Each commit should be one cohesive, self-consistent change — a single asset, one bug fix, one doc update. Don't batch unrelated work into one commit, and don't let a large body of uncommitted work pile up. Small atomic commits are easier to review, revert, and trace back to a `load_audit` incident.

**When to commit:**
- Whenever a logical unit of work is complete and the tree is in a working state — the asset materializes cleanly, tests pass, the doc edit is coherent. That's the primary trigger, not a line count.
- Before switching to an unrelated task, so the two don't end up entangled in one commit.
- At the end of a working session — never leave completed work uncommitted overnight.

**Size guard (soft heuristic, not a gate).** As a backstop for long sessions, if the uncommitted working-tree diff has grown past roughly **200 changed lines or ~5 files** since the last commit, treat that as a prompt to either commit the completed portion or split it into several cohesive commits. Large *single* logical changes (a docs reorg, a generated migration) can legitimately exceed this — favor logical boundaries over hitting an exact number.

**Message format.** Imperative subject line (`Add fct_loans branch rollup`, not `Added…`). Add a body explaining the *why* whenever it isn't obvious from the diff. Reference a `[Decision N]` from `decisions_log.md` when the commit implements a logged decision.

**Never commit** `.env`, files under `secrets/`, or generated artifacts — see [What goes in a PR vs what doesn't](#what-goes-in-a-pr-vs-what-doesnt).

> Claude Code tracks this cadence automatically — it checks accumulated changes during a session and will ask whether you want to commit once a logical unit is done or the size guard is exceeded. It never commits without your go-ahead. See `CLAUDE.md → Commit cadence`.

---

## What goes in a PR vs what doesn't

| Goes in the PR | Does NOT go in a PR |
|---|---|
| Asset `.py` files | `.env` (gitignored — never commit) |
| SQL query files | `dagster/logs/` or `dagster/storage/` |
| `warehouse_schema.sql` DDL | `backups/` directory |
| `docs/*.md` updates | Python `.pyc` / `__pycache__` |
| `CLAUDE.md` updates | Any file with credentials or API keys |

---

## Prod backfill after feature merge

When a new asset is merged and deployed to prod for the first time, set the correct historical `start_date` on any `MonthlyPartitionsDefinition` or `DailyPartitionsDefinition` before materializing. Dev uses a recent date to keep testing fast; prod needs the real historical start.

```python
# Dev (fast testing)
partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01")

# Prod (full history)
partitions_def=MonthlyPartitionsDefinition(start_date="2023-07-01")
```

Update the start_date in a `schema/<name>` branch, merge, then deploy and backfill.

For one-time historical CSV backfills (drop-folder assets), see [`engineering.md → Historical backfill assets`](engineering.md#historical-backfill-assets).

---

## If something goes wrong in prod

1. Do not hotfix directly on `main` unless it is truly one line and low risk
2. Branch from main (`fix/<description>`), reproduce locally in dev, fix, PR
3. For data corruption: the `load_audit` table records every run with batch_id, asset, status, and row count — use it to identify exactly what ran and when
4. For schema issues: `pg_dump` backups run daily to `./backups/` — the latest is always in `backups/daily/warehouse-latest.sql.gz`
