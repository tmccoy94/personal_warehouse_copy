-- =============================================================================
-- fct_loan_requests.sql
-- Extracts one row per loan request for warehouse.fct_loan_requests.
-- Run against CORE DB2. Dagster asset maps source serials to warehouse keys:
--   SOURCE_BRANCH_SERIAL          → branch_key         (via dim_branch.source_branch_serial)
--   SOURCE_DECISION_MAKER_SERIAL  → decision_maker_key  (via dim_employee.source_employee_serial)
--   SOURCE_PROCESSOR_SERIAL       → processor_key       (via dim_employee.source_employee_serial)
--   SOURCE_APP_SERIAL             → application_key     (via fct_loan_applications.source_app_serial)
-- Parameters: :start_date (first day of month), :end_date (last day of month)
-- =============================================================================

WITH LatestDecision AS (
    -- Most recent decision per loan request.
    -- Uses the same NOT EXISTS anti-join pattern as full_loan_analytics_query.sql:
    -- keeps the row where no other decision exists with a later date + higher serial.
    SELECT
        DECISION.PARENT_SERIAL                                              AS LOAN_REQUEST_SERIAL,
        DECISION.SERIAL                                                     AS DECISION_SERIAL,
        DECISION.TYPE_SERIAL                                                AS DECISION_TYPE_SERIAL,
        DECISION.DECISION_DATE,
        DECISION.DECISION_MAKER_USER_SERIAL
    FROM CORE.DECISION AS DECISION
    WHERE NOT EXISTS (
        SELECT 1
        FROM CORE.DECISION AS D2
        WHERE D2.PARENT_SERIAL = DECISION.PARENT_SERIAL
          AND D2.DECISION_DATE >= DECISION.DECISION_DATE
          AND D2.SERIAL > DECISION.SERIAL
    )
)
SELECT
    LOAN_REQUEST.SERIAL                                                     AS SOURCE_LOAN_REQUEST_SERIAL,
    APP.SERIAL                                                              AS SOURCE_APP_SERIAL,
    APP.APPLICATION_NUMBER,
    LN_TYPE.DESCRIPTION                                                     AS LOAN_TYPE,
    LOAN_REQUEST.CREDIT_LIMIT                                               AS REQUESTED_AMOUNT,
    LOAN_REQUEST.CREDIT_SCORE                                               AS LOAN_REQUEST_CREDIT_SCORE,
    LOAN_REQUEST.BRANCH_SERIAL                                              AS SOURCE_BRANCH_SERIAL,
    CASE DECISION_TYPE.CATEGORY
        WHEN 'P' THEN 'Pending'
        WHEN 'a' THEN 'Pending Additional Approval'
        WHEN 'A' THEN 'Approved'
        WHEN 'D' THEN 'Denied'
        WHEN 'C' THEN 'Countered'
        ELSE 'Unknown'
    END                                                                     AS DECISION_CATEGORY,
    CASE
        WHEN DECISION_TYPE.DESCRIPTION = 'System Approved' THEN 'Approved'
        ELSE DECISION_TYPE.DESCRIPTION
    END                                                                     AS DECISION_DESCRIPTION,
    LD.DECISION_DATE,
    LD.DECISION_MAKER_USER_SERIAL                                           AS SOURCE_DECISION_MAKER_SERIAL,
    LOAN_REQUEST.PROCESSOR_USER_SERIAL                                      AS SOURCE_PROCESSOR_SERIAL,
    LOAN_REQUEST.APPROVED_LOAN_SERIAL                                       AS SOURCE_APPROVED_LOAN_SERIAL,
    LOAN.OPEN_DATE                                                          AS LOAN_OPEN_DATE,
    CASE
        WHEN LN_TYPE.DESCRIPTION = 'Payday Alternative Loan' THEN 1
        ELSE 0
    END                                                                     AS IS_PAYDAY_ALT
FROM CORE.APP AS APP
    JOIN CORE.APP_TYPE AS APP_TYPE
        ON APP.TYPE_SERIAL = APP_TYPE.SERIAL
    JOIN CORE.LOAN_REQUEST AS LOAN_REQUEST
        ON LOAN_REQUEST.PARENT_SERIAL = APP.SERIAL
    LEFT JOIN CORE.LN_TYPE AS LN_TYPE
        ON LOAN_REQUEST.TYPE_SERIAL = LN_TYPE.SERIAL
    LEFT JOIN LatestDecision AS LD
        ON LOAN_REQUEST.SERIAL = LD.LOAN_REQUEST_SERIAL
    LEFT JOIN CORE.DECISION_TYPE AS DECISION_TYPE
        ON LD.DECISION_TYPE_SERIAL = DECISION_TYPE.SERIAL
    LEFT JOIN CORE.LOAN AS LOAN
        ON LOAN_REQUEST.APPROVED_LOAN_SERIAL = LOAN.SERIAL
WHERE APP_TYPE.DESCRIPTION LIKE '%New Loan App%'
  AND APP.APPLICATION_DATE BETWEEN :start_date AND :end_date
ORDER BY
    APP.APPLICATION_NUMBER,
    LOAN_REQUEST.SERIAL;
