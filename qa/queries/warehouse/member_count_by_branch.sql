-- Warehouse equivalent: Member Count by Opening Branch
-- Uses staging tables. Must match the DB2 query exactly.
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
SELECT
    COALESCE(branch.branch_name, 'Unknown') AS branch,
    COUNT(*)                                 AS member_count
FROM OldestGeneralShare AS mb
JOIN staging.ks_person AS person ON person.person_serial = mb.person_serial
LEFT JOIN dev.dim_branch AS branch ON
    branch.source_system         = 'keystone'
    AND branch.source_branch_serial = mb.branch_serial
WHERE mb.person_serial <> 0
  AND (
      EXISTS (
          SELECT 1 FROM staging.ks_account AS trust_acct
          WHERE trust_acct.primary_person_serial = mb.person_serial
            AND trust_acct.close_date IS NULL
            AND TRIM(trust_acct.type_description) = 'Trust'
      )
      OR (
          COALESCE(person.person_type, '') <> 'Vendor'
          AND NOT EXISTS (
              SELECT 1 FROM staging.ks_ac_note AS note
              WHERE note.person_serial = mb.person_serial
                AND note.note_type     = 'Deceased Member'
                AND (note.expiration_date IS NULL OR note.expiration_date > CURRENT_DATE)
          )
      )
  )
GROUP BY branch.branch_name
ORDER BY branch.branch_name
