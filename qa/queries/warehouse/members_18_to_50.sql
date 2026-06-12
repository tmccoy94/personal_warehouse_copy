-- Warehouse: Members aged 18-50 using staging.ks_env.posting_date
-- Uses same exclusion logic as member count queries
SELECT COUNT(DISTINCT share.person_serial) AS members_18_to_50
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
                AND (note.expiration_date IS NULL OR note.expiration_date > (SELECT posting_date FROM staging.ks_env))
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
