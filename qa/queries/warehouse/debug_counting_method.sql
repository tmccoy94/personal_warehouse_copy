-- Compare COUNT vs COUNT(DISTINCT) for service counting
-- If someone has 2 "Additional Savings" accounts:
--   COUNT(product) = 2 (DB2 method)
--   COUNT(DISTINCT product) = 1 (current warehouse method)
-- Show how many people have different service counts under each method
WITH PersonProducts AS (
    SELECT share.person_serial, share.share_type AS product
    FROM staging.ks_share AS share
    WHERE share.close_date IS NULL AND share.charge_off_date IS NULL AND share.share_type IS NOT NULL
    UNION ALL
    SELECT loan.person_serial, COALESCE(loan.loan_type_description, 'Unknown Loan') AS product
    FROM staging.ks_loan AS loan WHERE loan.close_date IS NULL AND loan.charge_off_date IS NULL
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
    WHERE monetary.source = 'B'
      AND txn.posting_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'
),
Counts AS (
    SELECT person_serial,
           COUNT(product)          AS count_all,
           COUNT(DISTINCT product) AS count_distinct
    FROM PersonProducts WHERE product IS NOT NULL
    GROUP BY person_serial
)
SELECT
    COUNT(*) FILTER (WHERE count_all >= 3)      AS active_count_all,
    COUNT(*) FILTER (WHERE count_distinct >= 3) AS active_count_distinct,
    COUNT(*) FILTER (WHERE count_all >= 3 AND count_distinct < 3) AS extra_via_count_all
FROM Counts
JOIN (
    SELECT DISTINCT ON (share.person_serial) share.person_serial
    FROM staging.ks_share AS share
    WHERE share.share_id = '0001' AND share.close_date IS NULL AND share.charge_off_date IS NULL
    ORDER BY share.person_serial, share.share_serial ASC
) AS gsholders ON gsholders.person_serial = Counts.person_serial
JOIN staging.ks_person AS person ON person.person_serial = Counts.person_serial
WHERE Counts.person_serial <> 0
  AND (
      EXISTS (SELECT 1 FROM staging.ks_account t WHERE t.primary_person_serial = Counts.person_serial
              AND t.close_date IS NULL AND TRIM(t.type_description) = 'Trust')
      OR (COALESCE(person.person_type,'') <> 'Vendor'
          AND NOT EXISTS (SELECT 1 FROM staging.ks_ac_note n WHERE n.person_serial = Counts.person_serial
                          AND n.note_type = 'Deceased Member'
                          AND (n.expiration_date IS NULL OR n.expiration_date > CURRENT_DATE)))
  )
