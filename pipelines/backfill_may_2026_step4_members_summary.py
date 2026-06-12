"""
May 2026 backfill — Step 4 only: recompute fct_members_summary for 2026-05-31.

Prereqs (already done by earlier steps):
  - person_snapshot_monthly has May 31 data (13,521 rows)
  - ks_member_branch_snapshot has May 31 data (13,515 rows)
  - fct_members_added_lost has May 2026 data (87 rows)

Usage:
    docker exec dagster-webserver python /opt/dagster/app/src/backfill_may_2026_step4_members_summary.py
"""
import os, sys
from datetime import date

sys.path.insert(0, "/opt/dagster/app/src")
from sqlalchemy import create_engine, text
from pipelines.utils import month_id_from_date

SNAPSHOT_DATE = date(2026, 5, 31)
MONTH_ID      = month_id_from_date(date(2026, 5, 1))

PG_URL = (
    f"postgresql+psycopg://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
    f"@{os.environ['WAREHOUSE_HOST']}:{os.environ.get('WAREHOUSE_PORT', '5432')}"
    f"/{os.environ['WAREHOUSE_DB']}"
)
engine = create_engine(PG_URL, connect_args={"options": "-csearch_path=dev,public"})

print(f"Recomputing fct_members_summary for {SNAPSHOT_DATE}...", flush=True)

with engine.begin() as conn:
    conn.execute(
        text("DELETE FROM fct_members_summary WHERE snapshot_date = :d"),
        {"d": SNAPSHOT_DATE},
    )

    # Branch-level rows
    result = conn.execute(text("""
        INSERT INTO fct_members_summary
            (snapshot_date, branch_key, total_members, active_members,
             net_new_members, members_18_50, ks_members, mo_members, non_kcpd_members)
        SELECT
            :snapshot_date,
            branch.branch_key,
            COUNT(DISTINCT person_branch.person_key)                                     AS total_members,
            COUNT(DISTINCT person_branch.person_key)                                     AS active_members,
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
    print(f"  Inserted {result.rowcount} branch-level rows", flush=True)

    # CU Total — drive off ks_member_branch_snapshot, members_18_50 via exact QA logic
    conn.execute(text("""
        INSERT INTO fct_members_summary
            (snapshot_date, branch_key, total_members, active_members,
             net_new_members, members_18_50, ks_members, mo_members, non_kcpd_members)
        SELECT
            :snapshot_date,
            (SELECT branch_key FROM dim_branch WHERE branch_name = 'CU Total'),
            COUNT(DISTINCT member_branch.person_key)                                     AS total_members,
            -- active_members = members with 3+ services, exact QA logic (validated 9,876)
            (WITH ProductCounts AS (
                SELECT person_serial, COUNT(product) AS service_count
                FROM (
                    SELECT share.person_serial, share.share_type AS product
                    FROM staging.ks_share AS share
                    WHERE share.close_date IS NULL AND share.charge_off_date IS NULL
                      AND share.share_type IS NOT NULL
                    UNION ALL
                    SELECT loan.person_serial, COALESCE(loan.loan_type_description, 'Unknown Loan') AS product
                    FROM staging.ks_loan AS loan
                    WHERE loan.close_date IS NULL AND loan.charge_off_date IS NULL
                    UNION ALL
                    SELECT DISTINCT login.person_serial, 'Online Banking' AS product
                    FROM staging.ks_login AS login
                    WHERE login.channel = 'Access_Softek'
                    UNION ALL
                    SELECT DISTINCT s2.person_serial, 'Direct Deposit' AS product
                    FROM staging.ks_share AS s2
                    WHERE s2.close_date IS NULL AND s2.charge_off_date IS NULL
                      AND s2.ach_deposit_last_amount >= 500.00
                      AND s2.ach_deposit_last_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'
                    UNION ALL
                    SELECT DISTINCT txn.person_serial, 'Bill Payment' AS product
                    FROM staging.ks_transaction AS txn
                    JOIN staging.ks_monetary AS monetary ON monetary.transaction_serial = txn.transaction_serial
                    WHERE monetary.source = 'B'
                      AND txn.posting_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'
                ) AS products
                WHERE product IS NOT NULL
                GROUP BY person_serial
            ),
            GeneralShareHolders AS (
                SELECT DISTINCT ON (gs.person_serial) gs.person_serial
                FROM staging.ks_share AS gs
                WHERE gs.share_id = '0001' AND gs.close_date IS NULL AND gs.charge_off_date IS NULL
                ORDER BY gs.person_serial, gs.share_serial ASC
            )
            SELECT COUNT(*)
            FROM GeneralShareHolders AS mb2
            JOIN staging.ks_person AS p2 ON p2.person_serial = mb2.person_serial
            LEFT JOIN ProductCounts AS pc ON pc.person_serial = mb2.person_serial
            WHERE mb2.person_serial <> 0
              AND COALESCE(pc.service_count, 0) >= 3
              AND COALESCE(p2.person_type, '') <> 'Vendor'
              AND NOT EXISTS (
                  SELECT 1 FROM staging.ks_ac_note AS note
                  WHERE note.person_serial = mb2.person_serial
                    AND note.note_type     = 'Deceased Member'
                    AND (note.expiration_date IS NULL
                         OR note.expiration_date > (SELECT posting_date FROM staging.ks_env))
              )
            )                                                                            AS active_members,
            (SELECT COUNT(*) FROM fct_members_added_lost
             WHERE month_id = :month_id AND event_type = 'added')
            - (SELECT COUNT(*) FROM fct_members_added_lost
               WHERE month_id = :month_id AND event_type = 'lost')                       AS net_new_members,
            (SELECT COUNT(DISTINCT share.person_serial)
             FROM staging.ks_share AS share
             JOIN staging.ks_person AS person ON person.person_serial = share.person_serial
             WHERE share.share_id        = '0001'
               AND share.close_date      IS NULL
               AND share.charge_off_date IS NULL
               AND share.person_serial  <> 0
               AND (
                   EXISTS (
                       SELECT 1 FROM staging.ks_account AS trust_acct
                       WHERE trust_acct.primary_person_serial = share.person_serial
                         AND trust_acct.close_date IS NULL
                         AND TRIM(trust_acct.type_description) = 'Trust'
                   )
                   OR (
                       COALESCE(person.person_type, '') <> 'Vendor'
                       AND NOT EXISTS (
                           SELECT 1 FROM staging.ks_ac_note AS note
                           WHERE note.person_serial = share.person_serial
                             AND note.note_type     = 'Deceased Member'
                             AND (note.expiration_date IS NULL
                                  OR note.expiration_date > (SELECT posting_date FROM staging.ks_env))
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM staging.ks_account AS excl_acct
                           WHERE excl_acct.primary_person_serial = share.person_serial
                             AND excl_acct.close_date IS NULL
                             AND TRIM(excl_acct.type_description) IN ('Reserves', '1-Choose One', 'Trust')
                       )
                       AND person.birth_date <= (SELECT posting_date FROM staging.ks_env) - INTERVAL '18 years'
                       AND person.birth_date >  (SELECT posting_date FROM staging.ks_env) - INTERVAL '51 years'
                   )
               )
            )                                                                            AS members_18_50,
            NULL AS ks_members,
            NULL AS mo_members,
            NULL AS non_kcpd_members
        FROM ks_member_branch_snapshot AS member_branch
        LEFT JOIN person_snapshot_monthly AS monthly
            ON monthly.person_key    = member_branch.person_key
           AND monthly.snapshot_date = :snapshot_date
        WHERE member_branch.snapshot_date = :snapshot_date
    """), {"snapshot_date": SNAPSHOT_DATE, "month_id": MONTH_ID})
    print("  Inserted CU Total row", flush=True)

print("\nVerification:", flush=True)
with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT b.branch_name, s.total_members, s.net_new_members, s.members_18_50
        FROM fct_members_summary s
        JOIN dim_branch b ON b.branch_key = s.branch_key
        WHERE s.snapshot_date = :d
        ORDER BY b.branch_name
    """), {"d": SNAPSHOT_DATE})
    print(f"  {'Branch':<30} {'Total':>7} {'Net New':>8} {'18-50':>7}")
    for row in r:
        print(f"  {row[0]:<30} {str(row[1] or 'NULL'):>7} {str(row[2] or 'NULL'):>8} {str(row[3] or 'NULL'):>7}")
