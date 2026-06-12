-- Warehouse: Members lost in Feb 2026 (closed general share, no remaining open general share)
-- Matches members_lost.sql pipeline query logic against staging tables.
WITH closed_gen_shares AS (
    SELECT DISTINCT share.person_serial
    FROM staging.ks_share  AS share
    JOIN staging.ks_person AS person ON person.person_serial = share.person_serial
    WHERE share.share_id   = '0001'
      AND share.close_date IS NOT NULL
      AND share.close_date BETWEEN '2026-02-01' AND '2026-02-28'
      AND COALESCE(person.person_type, '') <> 'Vendor'
)
SELECT COUNT(*) AS members_lost
FROM closed_gen_shares AS cgs
WHERE NOT EXISTS (
    SELECT 1 FROM staging.ks_share AS open_share
    WHERE open_share.person_serial = cgs.person_serial
      AND open_share.share_id      = '0001'
      AND open_share.close_date    IS NULL
)
