# VM Migration Checklist

Next-steps guide for the one-time move from the dev laptop to the on-prem VM,
via the GitHub repo. The generic restore mechanics live in
[`operations.md → Moving the database to a new server`](operations.md#moving-the-database-to-a-new-server) —
this checklist wraps them with everything specific to *this* move: what git
doesn't carry, what changes on the VM, and what to verify before calling it live.

Delete this file (or move its leftovers into `operations.md`) once the VM is live.

---

## 1 — Before leaving the laptop

- [ ] Final cleanup pass on anything you don't want published (e.g. `schema_design/` pruning), commit, and push to the GitHub repo.
- [ ] Take a fresh database backup so the VM restore has current data:
  ```bash
  docker exec warehouse-backup /backup.sh
  ```
  The newest file in `./backups/daily/` is the one to carry over.
- [ ] Gather the **gitignored items** — a clone won't bring these, transfer them securely (USB / network share, never email):

  | Item | Why it's needed |
  |---|---|
  | `.env` | All runtime config + credentials. Verify `WAREHOUSE_SCHEMA=prod` before first start. |
  | `secrets/` | Docker secrets (`postgres_password.txt`, `gmail_service_account.json`) |
  | `backups/daily/warehouse-<latest>.sql.gz` | The database itself |
  | `data/csv_drop/` contents *(optional)* | Already loaded into the warehouse; carry over only if you want the file history on the VM |
  | `data/csv_backfill/` *(optional)* | Same — one-time backfills already in the DB |

  Not needed: `dagster/storage/` (run history rebuilds from Postgres),
  `hubspot_contact_update/` (superseded by the `hubspot_live_import_csv` asset),
  `data/csv_output/` (generated files).

---

## 2 — On the VM: install and restore

- [ ] Install Docker Engine + Compose plugin; confirm `docker compose version` works.
- [ ] Clone the GitHub repo.
- [ ] Drop `.env` and `secrets/` into the project root; create the data folders the
      compose bind mounts expect if you didn't carry them over
      (`data/csv_drop/`, `data/csv_backfill/`, `data/csv_output/`, `backups/`).
- [ ] Start the stack and restore the backup — follow
      [`operations.md` Steps 3–4](operations.md#step-3--start-the-stack-on-the-new-machine)
      (`docker compose up -d --build`, wait for healthy Postgres, `gunzip | psql` restore).
- [ ] Run `python switch_schema.py` → PROD (or confirm `.schema_intent` says `prod`
      and matches `.env`), then materialize `warehouse_schema_mode_select` in Dagster
      to prove the gate passes.

---

## 3 — VM-only configuration (deliberately not done on the laptop)

- [ ] **Add `restart: unless-stopped`** to all five services in `docker-compose.yaml`
      so the stack survives VM reboots, then `docker compose up -d` to apply.
      This was intentionally left out of the repo because the dev laptop shouldn't
      auto-start the stack — add it on the VM and commit from there.
- [ ] **Enable schedules + sensors** in the Dagster UI → Automation tab
      (`ks_daily_job` 11pm, `ks_monthly_job` 1am on the 1st, CSV drop sensors).
      They ship disabled; nothing runs on a schedule until this is flipped.
- [ ] **Repoint Zoho Data Bridge** at the VM's IP (port stays `POSTGRES_PORT`).
      The Postgres port is already bound to all interfaces for exactly this —
      see the comment in `docker-compose.yaml`.
- [ ] **Re-apply the md5 auth downgrade** for the DataBridge's old JDBC driver.
      The fresh data volume comes up on SCRAM defaults (the md5 config does NOT
      travel in the pg_dump), so Zoho will fail with "authentication type 10 is
      not supported" until this is re-applied — exact commands in
      [`bi_connections.md → Authentication note`](bi_connections.md#authentication-note--authentication-type-10-is-not-supported).

---

## 4 — Verify before calling it live

- [ ] **Source connectivity from the VM** (on-prem network reach is the whole point):
  - DB2/Keystone: materialize one small daily staging asset (e.g. `staging.ks_env`'s loader) and check `load_audit`
  - JasperServer: materialize the teller asset for a past month, or hit the API once
  - HubSpot: materialize one `hs_*` loader (internet egress check)
- [ ] **Restored data sanity**: pgAdmin row counts on `prod.fct_members_summary` /
      `prod.fct_loan_portfolio_summary` match the laptop; recent `load_audit` rows present.
- [ ] **Integration tests** (optional but cheap): with the stack up, from `pipelines/`:
  ```bash
  uv run pytest                    # unit + integration; integration targets only the disposable test schema
  ```
  Requires `uv` on the VM host. Skip if you don't want Python tooling on the VM —
  the suite is laptop-runnable against the VM later by pointing `WAREHOUSE_TEST_HOST` at it.
- [ ] **First scheduled run**: after the first 11pm `ks_daily_job`, check Dagster for
      green runs and `load_audit` for the night's rows. The first monthly run
      (1st of the month) is worth watching live.
- [ ] **Email alerts**: confirm the failure-notification path works from the VM
      (Gmail API service account key reachable, test with `qa/test_gmail_api.py` if needed).

---

## 5 — Decommission the laptop copy

Only after the VM has run clean for a few days:

- [ ] Disable any local schedules (if you enabled them historically) and stop the local stack: `docker compose down`.
- [ ] Keep the laptop's `backups/` until the VM's backup sidecar has produced its own first few dumps.
- [ ] The laptop clone becomes a normal dev checkout: work happens on branches,
      PRs deploy to the VM per `development.md`.
