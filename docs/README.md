# Warehouse Documentation

Start here. This folder is the authoritative documentation for the credit union warehouse pipeline — what's in it, how it's organized, how to work in it, and why decisions were made the way they were.

This document used to be one ~140 KB file; it's now split by purpose. Pick the file that matches what you're trying to do.

---

## What to read for what

| If you want to… | Read |
|---|---|
| **Get a feel for the project** — what tables exist, what data flows in, what's still pending | [`overview.md`](overview.md) |
| **Look up a specific table's DDL or source** | [`tables.md`](tables.md) (ctrl-F by table name) |
| **Understand naming and shape rules** before adding a new table | [`conventions.md`](conventions.md) |
| **Write or modify a Dagster asset** — principles, testing, SQL style, backfills, JasperServer/HubSpot patterns | [`engineering.md`](engineering.md) |
| **Run the system** — schema environments, switch tooling, QA comparison tool, jobs/schedules, secrets, moving the DB | [`operations.md`](operations.md) |
| **Contribute code** — branching, local setup, PR process, deploy to prod | [`development.md`](development.md) |
| **Connect a BI tool** — Zoho Analytics, Power BI | [`bi_connections.md`](bi_connections.md) |
| **Move the stack to the VM** — one-time migration checklist (delete after) | [`vm_migration.md`](vm_migration.md) |
| **Understand why something was done a certain way** — historical context, trade-offs, bug fixes | [`decisions_log.md`](decisions_log.md) |

---

## How these docs relate

```
README.md          ── you are here
│
├── overview.md          summary, full table inventory, dormant scripts, known issues
├── conventions.md       naming rules, design rules, BI view strategy
├── tables.md            DDL reference for every object in the warehouse
├── engineering.md       Dagster + SQL + integration patterns
├── operations.md        running, switching, validating, jobs, moving the DB
├── development.md       branching, local setup, PR + deploy workflow
├── bi_connections.md    connecting Zoho Analytics and Power BI
└── decisions_log.md     numbered chronological log; cross-referenced from the others as [Decision N]
```

Each non-README file starts with a TOC. Cross-doc links use relative paths (e.g. `[Decision 27](decisions_log.md)`); section links inside a file use the standard `#anchor` form.

---

## Maintaining these docs

When schema or convention changes:

1. Update `tables.md` (DDL block, source notes).
2. Update `overview.md` (Full Table Inventory row, Summary count if it changed).
3. Add a new entry to `decisions_log.md` if the *why* isn't obvious from the diff.
4. Update `conventions.md` only if you changed a rule everyone follows.
5. Update `engineering.md` / `operations.md` only if the change touches how assets are written or run.

Keep `README.md` lean — it's the map, not the territory.
