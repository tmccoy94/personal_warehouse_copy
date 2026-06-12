-- Warehouse: Members added in Feb 2026 (new members, not returning)
-- Matches members_added.sql pipeline query logic against staging tables.
WITH active_before AS (
    SELECT DISTINCT share.person_serial
    FROM staging.ks_share AS share
    WHERE share.share_id        = '0001'
      AND share.open_date        < '2026-02-01'
      AND (share.close_date      IS NULL OR share.close_date      > '2026-02-01')
      AND (share.charge_off_date IS NULL OR share.charge_off_date > '2026-02-01')
),
new_accounts AS (
    SELECT acct.primary_person_serial AS person_serial
    FROM staging.ks_account AS acct
    JOIN staging.ks_person  AS person ON person.person_serial = acct.primary_person_serial
    WHERE acct.close_date IS NULL
      AND COALESCE(person.person_type, '') <> 'Vendor'
    GROUP BY acct.primary_person_serial
    HAVING MIN(acct.open_date) BETWEEN '2026-02-01' AND '2026-02-28'
)
SELECT COUNT(*) AS members_added
FROM new_accounts AS nm
WHERE NOT EXISTS (
    SELECT 1 FROM active_before AS ab
    WHERE ab.person_serial = nm.person_serial
)
