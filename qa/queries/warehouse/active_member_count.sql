-- Warehouse: Active Member Count (3+ services)
-- PostgreSQL equivalent of Active Member Count.sql
WITH PersonProducts AS (
    SELECT share.person_serial, share.share_type AS product
    FROM staging.ks_share AS share
    WHERE share.close_date IS NULL AND share.charge_off_date IS NULL AND share.share_type IS NOT NULL
    UNION ALL
    SELECT loan.person_serial, COALESCE(loan.loan_type_description, 'Unknown Loan') AS product
    FROM staging.ks_loan AS loan
    WHERE loan.close_date IS NULL AND loan.charge_off_date IS NULL
    UNION ALL
    SELECT DISTINCT login.person_serial, 'Online Banking' AS product
    FROM staging.ks_login AS login WHERE login.channel = 'Access_Softek'
    UNION ALL
    SELECT DISTINCT share.person_serial, 'Direct Deposit' AS product
    FROM staging.ks_share AS share
    WHERE share.close_date IS NULL AND share.charge_off_date IS NULL
      AND share.ach_deposit_last_amount >= 500.00
      AND share.ach_deposit_last_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'
    UNION ALL
    SELECT DISTINCT txn.person_serial, 'Bill Payment' AS product
    FROM staging.ks_transaction AS txn
    JOIN staging.ks_monetary AS monetary ON monetary.transaction_serial = txn.transaction_serial
    WHERE monetary.source = 'B' AND txn.posting_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'
),
ProductCounts AS (
    SELECT person_serial, COUNT(product) AS service_count   -- COUNT not DISTINCT: matches DB2 which counts each account instance
    FROM PersonProducts WHERE product IS NOT NULL
    GROUP BY person_serial
),
GeneralShareHolders AS (
    SELECT DISTINCT ON (share.person_serial) share.person_serial
    FROM staging.ks_share AS share
    WHERE share.share_id = '0001' AND share.close_date IS NULL AND share.charge_off_date IS NULL
    ORDER BY share.person_serial, share.share_serial ASC
)
SELECT COUNT(*) AS active_member_count
FROM GeneralShareHolders AS mb
JOIN staging.ks_person AS person ON person.person_serial = mb.person_serial
LEFT JOIN ProductCounts AS pc ON pc.person_serial = mb.person_serial
WHERE mb.person_serial <> 0
  AND COALESCE(pc.service_count, 0) >= 3
  AND COALESCE(person.person_type, '') <> 'Vendor'
  AND NOT EXISTS (
      SELECT 1 FROM staging.ks_ac_note AS note
      WHERE note.person_serial = mb.person_serial
        AND note.note_type = 'Deceased Member'
        AND (note.expiration_date IS NULL OR note.expiration_date > (SELECT posting_date FROM staging.ks_env))
  )
