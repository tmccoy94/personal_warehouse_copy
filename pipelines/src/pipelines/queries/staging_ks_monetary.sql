-- Monetary records rolling 90 days.
-- NOTE: Bill pay detection via SOURCE = 'B' does not work in this system — transactions
-- are not marked correctly in Keystone. Bill pay members will be matched via CSV import
-- in a future pass. Do not use SOURCE = 'B' for product detection.
-- Person linkage: MONETARY.PARENT_SERIAL = TRANSACTION.SERIAL → TRANSACTION.PERSON_SERIAL
-- Join staging.ks_monetary → staging.ks_transaction on transaction_serial to get person.
-- :cutoff_date = today - 90 days (passed at runtime)
-- Monetary records rolling 90 days.
-- No AMOUNT column — money is split across PRINCIPAL, INTEREST, IMPOUND, LATE_FEE, OTHER_CHARGES.
-- CATEGORY: D=Deposit W=Withdrawal P=Payment A=Advance R=Refinance N=New Loan C=Comment
-- TARGET_CATEGORY: S=Share L=Loan
-- SOURCE values: B=Bill payment, a=ACH, P=Payroll, H=Online banking, M=Mobile banking, etc.
-- :cutoff_date = today - 90 days (passed at runtime)
SELECT
    MONETARY.SERIAL             AS monetary_serial,
    MONETARY.PARENT_SERIAL      AS transaction_serial,
    MONETARY.POSTING_DATE       AS posting_date,
    MONETARY.CATEGORY           AS category,
    MONETARY.SOURCE             AS source,
    MONETARY.TARGET_CATEGORY    AS target_category,
    MONETARY.TARGET_SERIAL      AS target_serial,
    MONETARY.PRINCIPAL          AS principal,
    MONETARY.INTEREST           AS interest,
    MONETARY.NEW_BALANCE        AS new_balance
FROM
    CORE.MONETARY AS MONETARY
WHERE
    MONETARY.POSTING_DATE >= :cutoff_date
