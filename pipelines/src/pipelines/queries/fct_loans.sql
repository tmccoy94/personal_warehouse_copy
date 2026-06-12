-- =============================================================================
-- fct_loans.sql
-- Extracts the active loan portfolio for warehouse.fct_loans.
-- Dagster asset TRUNCATES the current month's partition before loading.
-- Run against CORE DB2. Dagster asset maps source serials to warehouse keys:
--   SOURCE_BRANCH_SERIAL  → branch_key  (via dim_branch.source_branch_serial)
--   SOURCE_PERSON_SERIAL  → person_key  (via person_source_identifier)
-- Parameters:
--   :snapshot_date  = last day of month (e.g. 2026-01-31) — loans open on or before this date
--   :start_date     = first day of month (e.g. 2026-01-01) — exclude loans closed before this date
--
-- =============================================================================

SELECT
    LOAN.SERIAL                                                             AS SOURCE_LOAN_SERIAL,
    LOAN_REQUEST.SERIAL                                                     AS SOURCE_LOAN_REQUEST_SERIAL,
    ACCOUNT.PRIMARY_PERSON_SERIAL                                           AS SOURCE_PERSON_SERIAL,
    LOAN_REQUEST.BRANCH_SERIAL                                              AS SOURCE_BRANCH_SERIAL,
    LN_TYPE.DESCRIPTION                                                     AS LOAN_TYPE,
    CASE
        WHEN LOAN.CLOSE_DATE IS NOT NULL THEN 'closed'                      /* confirm: LOAN.CLOSE_DATE */
        ELSE 'open'
    END                                                                     AS LOAN_STATUS,
    LOAN_REQUEST.CREDIT_LIMIT                                               AS ORIGINAL_AMOUNT,
    LOAN.BALANCE,                                                           /* confirm: LOAN.BALANCE or LOAN.CURRENT_BALANCE */
    LOAN.OPEN_DATE,
    LOAN.CLOSE_DATE,                                                        /* confirm: LOAN.CLOSE_DATE */
    LOAN.MATURITY_DATE,                                                     /* confirm: LOAN.MATURITY_DATE */
    LOAN.INTEREST_RATE,                                                     /* confirm: LOAN.INTEREST_RATE */
    CASE
        WHEN (
            COALESCE(LOAN.PAYMENT_COUNT_DQ_UNDER_30, 0)
            + COALESCE(LOAN.PAYMENT_COUNT_DQ_30_TO_59, 0)
            + COALESCE(LOAN.PAYMENT_COUNT_DQ_60_TO_89, 0)
            + COALESCE(LOAN.PAYMENT_COUNT_DQ_90_TO_119, 0)
            + COALESCE(LOAN.PAYMENT_COUNT_DQ_120_AND_UP, 0)
        ) > 0 THEN 'Y'
        ELSE 'N'
    END                                                                      AS IS_DELINQUENT
    -- Python asset casts IS_DELINQUENT 'Y'/'N' to BOOLEAN before inserting into fct_loans
FROM CORE.LOAN AS LOAN
    JOIN CORE.ACCOUNT AS ACCOUNT
        ON LOAN.PARENT_SERIAL = ACCOUNT.SERIAL
    LEFT JOIN CORE.LOAN_REQUEST AS LOAN_REQUEST
        ON LOAN_REQUEST.APPROVED_LOAN_SERIAL = LOAN.SERIAL
    LEFT JOIN CORE.LN_TYPE AS LN_TYPE
        ON LOAN_REQUEST.TYPE_SERIAL = LN_TYPE.SERIAL
WHERE
    -- Active loans: opened before or during month end, not yet closed (or closed this month)
    LOAN.OPEN_DATE <= :snapshot_date
  AND (LOAN.CLOSE_DATE IS NULL OR LOAN.CLOSE_DATE >= :start_date)
ORDER BY
    LOAN.OPEN_DATE,
    LOAN.SERIAL;
