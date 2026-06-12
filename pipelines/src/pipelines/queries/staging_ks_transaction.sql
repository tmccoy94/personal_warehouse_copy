-- Transaction records rolling 90 days.
-- TRANSACTION has no SOURCE column — source lives on CORE.MONETARY (MONETARY.PARENT_SERIAL = TRANSACTION.SERIAL).
-- Join staging.ks_transaction → staging.ks_monetary on transaction_serial = monetary.transaction_serial
-- to filter by source (e.g. SOURCE = 'B' for bill pay).
-- :cutoff_date = today - 90 days (passed at runtime)
SELECT
    TRANSACTION.SERIAL          AS transaction_serial,
    TRANSACTION.PERSON_SERIAL   AS person_serial,
    TRANSACTION.POSTING_DATE    AS posting_date
FROM
    CORE.TRANSACTION AS TRANSACTION
WHERE
    TRANSACTION.POSTING_DATE >= :cutoff_date
