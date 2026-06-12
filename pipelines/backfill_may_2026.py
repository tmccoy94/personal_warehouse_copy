"""
May 2026 backfill — run once inside the dagster-webserver container.

Populates May 31, 2026 data for:
  1. person_snapshot_monthly  (copy from person_snapshot_daily[2026-06-02])
  2. ks_member_branch_snapshot (copy from ks_member_branch_snapshot[2026-06-02])
  3. fct_members_added_lost    (DB2 query against D0001T32 for May 1-31)
  4. fct_members_summary       (computed from the above after they're loaded)

Usage (from project root):
    docker exec dagster-webserver python /opt/dagster/app/src/backfill_may_2026.py
"""
import os
import sys
from datetime import date

import ibm_db
from sqlalchemy import create_engine, text

sys.path.insert(0, "/opt/dagster/app/src")
from pipelines.loading import db2_fetch_all, ensure_snapshot_partition, resolve_person_keys
from pipelines.queries import load
from pipelines.utils import last_day_of_month, month_id_from_date

SNAPSHOT_DATE  = date(2026, 5, 31)
MONTH_START    = date(2026, 5, 1)
MONTH_ID       = month_id_from_date(MONTH_START)
SOURCE_DATE    = date(2026, 6, 2)   # oldest available daily snapshot — same D0001T32 data
BATCH_ID       = "backfill-may-2026"

PG_URL = (
    f"postgresql+psycopg://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
    f"@{os.environ['WAREHOUSE_HOST']}:{os.environ.get('WAREHOUSE_PORT', '5432')}"
    f"/{os.environ['WAREHOUSE_DB']}"
)

engine = create_engine(PG_URL, connect_args={"options": "-csearch_path=dev,public"})


# ── Step 1: person_snapshot_monthly ──────────────────────────────────────────

print(f"\n[1/4] Backfilling person_snapshot_monthly for {SNAPSHOT_DATE}...", flush=True)

ensure_snapshot_partition(engine, "person_snapshot_monthly", SNAPSHOT_DATE)

with engine.begin() as conn:
    conn.execute(
        text("DELETE FROM person_snapshot_monthly WHERE snapshot_date = :d"),
        {"d": SNAPSHOT_DATE},
    )
    result = conn.execute(text("""
        INSERT INTO person_snapshot_monthly
            (person_key, snapshot_date, source_system, source_person_serial,
             first_name, middle_name, last_name, birth_date, gender,
             record_hash, batch_id)
        SELECT
            person_key,
            :snapshot_date,
            source_system,
            source_person_serial,
            first_name,
            middle_name,
            last_name,
            birth_date,
            gender,
            record_hash,
            :batch_id
        FROM person_snapshot_daily
        WHERE snapshot_date = :source_date
    """), {"snapshot_date": SNAPSHOT_DATE, "source_date": SOURCE_DATE, "batch_id": BATCH_ID})
    print(f"  Inserted {result.rowcount:,} rows into person_snapshot_monthly", flush=True)

# Insert the 5 members who are in the validated member list but were missing
# from person_snapshot_daily[2026-06-02]. Their data comes from staging directly.
with engine.begin() as conn:
    result2 = conn.execute(text("""
        INSERT INTO person_snapshot_monthly
            (person_key, snapshot_date, source_system, source_person_serial,
             first_name, middle_name, last_name, birth_date, gender,
             record_hash, batch_id)
        SELECT
            psi.person_key,
            :snapshot_date,
            'keystone',
            pss.person_serial,
            pss.first_name,
            pss.middle_name,
            pss.last_name,
            pss.birth_date,
            pss.gender,
            NULL,
            :batch_id
        FROM staging.person_snapshot_stage AS pss
        JOIN dev.person_source_identifier AS psi ON
            psi.source_system      = 'keystone' AND
            psi.source_entity_type = 'member'  AND
            psi.source_identifier  = pss.person_serial::TEXT
        WHERE pss.person_serial IN (14928, 30969, 35558, 49142, 138648)
          AND NOT EXISTS (
              SELECT 1 FROM dev.person_snapshot_monthly AS existing
              WHERE existing.person_key    = psi.person_key
                AND existing.snapshot_date = :snapshot_date
          )
    """), {"snapshot_date": SNAPSHOT_DATE, "batch_id": BATCH_ID})
    print(f"  Inserted {result2.rowcount} missing members into person_snapshot_monthly", flush=True)


# ── Step 2: ks_member_branch_snapshot ────────────────────────────────────────

print(f"\n[2/4] Backfilling ks_member_branch_snapshot for {SNAPSHOT_DATE}...", flush=True)

ensure_snapshot_partition(engine, "ks_member_branch_snapshot", SNAPSHOT_DATE)

with engine.begin() as conn:
    conn.execute(
        text("DELETE FROM ks_member_branch_snapshot WHERE snapshot_date = :d"),
        {"d": SNAPSHOT_DATE},
    )
    result = conn.execute(text("""
        WITH OldestGeneralShare AS (
            SELECT DISTINCT ON (share.person_serial)
                share.person_serial,
                share.branch_serial
            FROM staging.ks_share AS share
            WHERE share.share_id        = '0001'
              AND share.close_date      IS NULL
              AND share.charge_off_date IS NULL
            ORDER BY share.person_serial, share.share_serial ASC
        )
        INSERT INTO ks_member_branch_snapshot
            (person_key, snapshot_date, source_system, source_person_serial,
             branch_key, home_branch_key)
        SELECT
            psi.person_key,
            :snapshot_date,
            'keystone',
            gs.person_serial,
            branch_key_map.branch_key,
            home_key_map.branch_key
        FROM OldestGeneralShare AS gs
        JOIN staging.ks_person AS person ON person.person_serial = gs.person_serial
        JOIN person_source_identifier AS psi ON
            psi.source_system      = 'keystone' AND
            psi.source_entity_type = 'member'  AND
            psi.source_identifier  = gs.person_serial::TEXT
        LEFT JOIN staging.ks_home_branch AS hb ON
            hb.person_serial = gs.person_serial
        LEFT JOIN dim_branch AS branch_key_map ON
            branch_key_map.source_system        = 'keystone' AND
            branch_key_map.source_branch_serial = gs.branch_serial
        LEFT JOIN dim_branch AS home_key_map ON
            home_key_map.source_system          = 'keystone' AND
            home_key_map.source_branch_serial   = hb.branch_serial
        WHERE gs.person_serial <> 0
          AND (
              EXISTS (
                  SELECT 1 FROM staging.ks_account AS trust_acct
                  WHERE trust_acct.primary_person_serial = gs.person_serial
                    AND trust_acct.close_date IS NULL
                    AND TRIM(trust_acct.type_description) = 'Trust'
              )
              OR (
                  COALESCE(person.person_type, '') <> 'Vendor'
                  AND NOT EXISTS (
                      SELECT 1 FROM staging.ks_ac_note AS note
                      WHERE note.person_serial = gs.person_serial
                        AND note.note_type     = 'Deceased Member'
                        AND (note.expiration_date IS NULL
                             OR note.expiration_date > (SELECT posting_date FROM staging.ks_env))
                  )
              )
          )
    """), {"snapshot_date": SNAPSHOT_DATE})
    print(f"  Inserted {result.rowcount:,} rows into ks_member_branch_snapshot", flush=True)


# ── Step 3: fct_members_added_lost ───────────────────────────────────────────

print(f"\n[3/4] Loading fct_members_added_lost for May 2026 from DB2...", flush=True)

db_name  = os.environ.get("KEYSTONE_DB_OVERRIDE", "D0001LIV")
host     = os.environ["SOURCE_DB_HOST"]
port     = os.environ["SOURCE_DB_PORT"]
user     = os.environ["SOURCE_DB_USER"]
password = os.environ["SOURCE_DB_PASSWORD"]
conn_str = f"DATABASE={db_name};HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password};"

db2_conn = ibm_db.connect(conn_str, "", "")
print(f"  Connected to {db_name}", flush=True)

try:
    added_rows = db2_fetch_all(
        db2_conn,
        load("members_added.sql"),
        {"start_date": MONTH_START, "end_date": SNAPSHOT_DATE},
    )
    lost_rows = db2_fetch_all(
        db2_conn,
        load("members_lost.sql"),
        {"start_date": MONTH_START, "end_date": SNAPSHOT_DATE},
    )
finally:
    ibm_db.close(db2_conn)

print(f"  DB2 returned {len(added_rows)} added, {len(lost_rows)} lost", flush=True)

all_serials = (
    [int(r["person_serial"]) for r in added_rows]
    + [int(r["person_serial"]) for r in lost_rows]
)
person_key_map = resolve_person_keys(engine, all_serials) if all_serials else {}

insert_rows = []
for row in added_rows:
    serial = int(row["person_serial"])
    insert_rows.append({
        "source_person_serial":    serial,
        "person_key":              person_key_map.get(serial),
        "month_id":                MONTH_ID,
        "event_type":              "added",
        "event_date":              row.get("first_acct_open_date") or MONTH_START,
        "agency":                  row.get("agency"),
        "account_category":        row.get("category"),
        "returning_member":        row.get("returning_member") == "YES",
        "close_reason":            None,
        "membership_duration_yrs": None,
        "first_gensh_opened":      row.get("first_acct_open_date"),
    })
for row in lost_rows:
    serial = int(row["person_serial"])
    insert_rows.append({
        "source_person_serial":    serial,
        "person_key":              person_key_map.get(serial),
        "month_id":                MONTH_ID,
        "event_type":              "lost",
        "event_date":              MONTH_START,
        "agency":                  None,
        "account_category":        None,
        "returning_member":        None,
        "close_reason":            row.get("ac_close_reason"),
        "membership_duration_yrs": row.get("membership_duration"),
        "first_gensh_opened":      row.get("first_gensh_opened"),
    })

upserted = 0
with engine.begin() as conn:
    conn.execute(
        text("DELETE FROM fct_members_added_lost WHERE month_id = :m"),
        {"m": MONTH_ID},
    )
    for row in insert_rows:
        result = conn.execute(text("""
            INSERT INTO fct_members_added_lost
                (source_system, month_id, source_person_serial, person_key,
                 event_type, event_date, agency, account_category, returning_member,
                 close_reason, membership_duration_yrs, first_gensh_opened)
            VALUES
                ('keystone', :month_id, :source_person_serial, :person_key,
                 :event_type, :event_date, :agency, :account_category, :returning_member,
                 :close_reason, :membership_duration_yrs, :first_gensh_opened)
            ON CONFLICT (source_system, source_person_serial, event_type, event_date)
            DO NOTHING
        """), row)
        upserted += result.rowcount
print(f"  Upserted {upserted} rows into fct_members_added_lost", flush=True)


# ── Step 4: fct_members_summary ──────────────────────────────────────────────

print(f"\n[4/4] Computing fct_members_summary for {SNAPSHOT_DATE}...", flush=True)

with engine.begin() as conn:
    conn.execute(
        text("DELETE FROM fct_members_summary WHERE snapshot_date = :d"),
        {"d": SNAPSHOT_DATE},
    )

    # Branch-level rows from ks_member_branch_snapshot
    result = conn.execute(text("""
        INSERT INTO fct_members_summary
            (snapshot_date, branch_key, total_members, active_members,
             net_new_members, members_18_50, ks_members, mo_members, non_kcpd_members)
        SELECT
            :snapshot_date,
            branch.branch_key,
            COUNT(DISTINCT person_branch.person_key)                                    AS total_members,
            COUNT(DISTINCT person_branch.person_key)                                    AS active_members,
            COALESCE(SUM(CASE WHEN events.event_type = 'added' THEN 1 ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN events.event_type = 'lost'  THEN 1 ELSE 0 END), 0) AS net_new_members,
            NULL AS members_18_50,
            NULL AS ks_members,
            NULL AS mo_members,
            NULL AS non_kcpd_members
        FROM ks_member_branch_snapshot AS person_branch
        JOIN dim_branch AS branch ON branch.branch_key = person_branch.branch_key
        LEFT JOIN fct_members_added_lost AS events
            ON events.person_key = person_branch.person_key
           AND events.month_id   = :month_id
        WHERE person_branch.snapshot_date = :snapshot_date
          AND branch.source_system IS NOT NULL
        GROUP BY branch.branch_key
    """), {"snapshot_date": SNAPSHOT_DATE, "month_id": MONTH_ID})
    branch_rows = result.rowcount
    print(f"  Inserted {branch_rows} branch-level rows", flush=True)

    # CU Total — drive off ks_member_branch_snapshot (authoritative member list).
    # LEFT JOIN monthly only for birth_date.
    result = conn.execute(text("""
        INSERT INTO fct_members_summary
            (snapshot_date, branch_key, total_members, active_members,
             net_new_members, members_18_50, ks_members, mo_members, non_kcpd_members)
        SELECT
            :snapshot_date,
            (SELECT branch_key FROM dim_branch WHERE branch_name = 'CU Total'),
            COUNT(DISTINCT member_branch.person_key)                                     AS total_members,
            COUNT(DISTINCT member_branch.person_key)                                     AS active_members,
            (SELECT COUNT(*) FROM fct_members_added_lost
             WHERE month_id = :month_id AND event_type = 'added')
            - (SELECT COUNT(*) FROM fct_members_added_lost
               WHERE month_id = :month_id AND event_type = 'lost')                       AS net_new_members,
            COUNT(DISTINCT CASE
                WHEN monthly.birth_date <= :snapshot_date - INTERVAL '18 years'
                 AND monthly.birth_date >  :snapshot_date - INTERVAL '51 years'
                THEN member_branch.person_key END)                                       AS members_18_50,
            NULL AS ks_members,
            NULL AS mo_members,
            NULL AS non_kcpd_members
        FROM ks_member_branch_snapshot AS member_branch
        LEFT JOIN person_snapshot_monthly AS monthly
            ON monthly.person_key    = member_branch.person_key
           AND monthly.snapshot_date = :snapshot_date
        WHERE member_branch.snapshot_date = :snapshot_date
    """), {"snapshot_date": SNAPSHOT_DATE, "month_id": MONTH_ID})
    print(f"  Inserted CU Total row", flush=True)

print("\n✓ May 2026 backfill complete.", flush=True)

# Quick verification
with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT b.branch_name, s.total_members, s.net_new_members, s.members_18_50
        FROM fct_members_summary s
        JOIN dim_branch b ON b.branch_key = s.branch_key
        WHERE s.snapshot_date = :d
        ORDER BY b.branch_name
    """), {"d": SNAPSHOT_DATE})
    print(f"\nfct_members_summary for {SNAPSHOT_DATE}:")
    print(f"  {'Branch':<30} {'Total':>7} {'Net New':>8} {'18-50':>7}")
    for row in r:
        print(f"  {row[0]:<30} {str(row[1] or 'NULL'):>7} {str(row[2] or 'NULL'):>8} {str(row[3] or 'NULL'):>7}")
