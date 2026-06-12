-- Warehouse: Same service breakdown for comparison
SELECT 'Open Shares (any type)' AS service,
       COUNT(DISTINCT person_serial) AS member_count
FROM staging.ks_share
WHERE close_date IS NULL AND charge_off_date IS NULL

UNION ALL

SELECT 'Open Loans (any type)',
       COUNT(DISTINCT loan.person_serial)
FROM staging.ks_loan AS loan
WHERE loan.close_date IS NULL AND loan.charge_off_date IS NULL


UNION ALL

SELECT 'Online Banking (Access_Softek)',
       COUNT(DISTINCT person_serial)
FROM staging.ks_login WHERE channel = 'Access_Softek'

UNION ALL

SELECT 'Direct Deposit (ACH >= 500, last 30 days)',
       COUNT(DISTINCT person_serial)
FROM staging.ks_share
WHERE close_date IS NULL AND charge_off_date IS NULL
  AND ach_deposit_last_amount >= 500.00
  AND ach_deposit_last_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'

UNION ALL

SELECT 'Bill Payment (SOURCE=B, last 30 days)',
       COUNT(DISTINCT txn.person_serial)
FROM staging.ks_transaction AS txn
JOIN staging.ks_monetary AS monetary ON monetary.transaction_serial = txn.transaction_serial
WHERE monetary.source = 'B'
  AND txn.posting_date > (SELECT posting_date FROM staging.ks_env) - INTERVAL '30 days'
