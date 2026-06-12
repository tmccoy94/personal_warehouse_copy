-- Check why the 10 missing persons aren't showing up in our warehouse query
-- Checks: (1) are they in staging.ks_share? (2) do they have an open 0001 share?
-- (3) are they in staging.ks_person? (4) are they being excluded by our filters?
SELECT
    p.person_serial,
    p.person_type,
    -- Do they have any 0001 share in staging at all?
    COUNT(s.share_serial) FILTER (WHERE s.share_id = '0001') AS total_0001_shares,
    -- How many are open?
    COUNT(s.share_serial) FILTER (WHERE s.share_id = '0001' AND s.close_date IS NULL AND s.charge_off_date IS NULL) AS open_0001_shares,
    -- Are they excluded as vendor?
    CASE WHEN COALESCE(p.person_type, '') = 'Vendor' THEN 'YES' ELSE 'NO' END AS is_vendor,
    -- Do they have a deceased note?
    EXISTS (
        SELECT 1 FROM staging.ks_ac_note n
        WHERE n.person_serial = p.person_serial
          AND n.note_type     = 'Deceased Member'
          AND (n.expiration_date IS NULL OR n.expiration_date > CURRENT_DATE)
    ) AS is_deceased,
    -- Trust account?
    EXISTS (
        SELECT 1 FROM staging.ks_account a
        WHERE a.primary_person_serial = p.person_serial
          AND a.close_date IS NULL
          AND TRIM(a.type_description) = 'Trust'
    ) AS has_trust_acct
FROM staging.ks_person AS p
LEFT JOIN staging.ks_share AS s ON s.person_serial = p.person_serial
WHERE p.person_serial IN (10782, 31413, 34622, 49142, 5945, 14928, 27496, 29851, 30969, 138648)
GROUP BY p.person_serial, p.person_type
ORDER BY p.person_serial
