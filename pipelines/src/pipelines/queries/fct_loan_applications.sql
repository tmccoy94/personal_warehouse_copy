-- =============================================================================
-- fct_loan_applications.sql
-- Extracts one row per loan application for warehouse.fct_loan_applications.
-- Run against CORE DB2. Dagster asset maps source serials to warehouse keys:
--   SOURCE_APP_PROCESSOR_SERIAL   → employee_key          (via dim_employee.source_employee_serial)
--   SOURCE_BRANCH_SERIAL          → branch_key            (via dim_branch.source_branch_serial)
--   SOURCE_PRIMARY_PERSON_SERIAL  → primary_person_key    (via person_source_identifier)
--   SOURCE_CO_APPLICANT_PERSON_SERIAL → co_applicant_person_key (nullable; NULL if not in warehouse)
-- Parameters: :start_date (first day of month), :end_date (last day of month)
-- =============================================================================

WITH PrimaryApplicant AS (
    -- Lowest ordinal applicant = primary borrower
    SELECT
        APPLICANT.PARENT_SERIAL                                             AS APP_SERIAL,
        PERSON.SERIAL                                                       AS PERSON_SERIAL,
        APPLICANT.SERIAL                                                    AS APPLICANT_SERIAL,
        ROW_NUMBER() OVER (
            PARTITION BY APPLICANT.PARENT_SERIAL
            ORDER BY APPLICANT.ORDINAL ASC
        )                                                                   AS RANK
    FROM CORE.APPLICANT AS APPLICANT
        JOIN CORE.PERSON AS PERSON
            ON APPLICANT.SOURCE_PERSON_SERIAL = PERSON.SERIAL
),
CoApplicant AS (
    -- Second-lowest ordinal = co-applicant; NULL if application has only one applicant
    SELECT
        APPLICANT.PARENT_SERIAL                                             AS APP_SERIAL,
        PERSON.SERIAL                                                       AS PERSON_SERIAL,
        COALESCE(PERSON.FIRST_NAME, '') || ' ' || PERSON.LAST_NAME         AS CO_APPLICANT_NAME,
        APPLICANT.SERIAL                                                    AS APPLICANT_SERIAL,
        ROW_NUMBER() OVER (
            PARTITION BY APPLICANT.PARENT_SERIAL
            ORDER BY APPLICANT.ORDINAL ASC
        )                                                                   AS RANK
    FROM CORE.APPLICANT AS APPLICANT
        JOIN CORE.PERSON AS PERSON
            ON APPLICANT.SOURCE_PERSON_SERIAL = PERSON.SERIAL
),
LatestApplicantScore AS (
    -- Most recent credit score per applicant
    SELECT
        APPLICANT_SCORE.PARENT_SERIAL                                       AS APPLICANT_SERIAL,
        APPLICANT_SCORE.SCORE,
        ROW_NUMBER() OVER (
            PARTITION BY APPLICANT_SCORE.PARENT_SERIAL
            ORDER BY APPLICANT_SCORE.SCORE_DATE DESC
        )                                                                   AS RN
    FROM CORE.APPLICANT_SCORE AS APPLICANT_SCORE
    WHERE APPLICANT_SCORE.SCORE_DATE IS NOT NULL
),
AppBranch AS (
    -- Branch from the first loan request on the application (apps can have multiple LRs
    -- but branch is on the LR, not the APP; take the earliest LR's branch)
    SELECT
        LOAN_REQUEST.PARENT_SERIAL                                          AS APP_SERIAL,
        LOAN_REQUEST.BRANCH_SERIAL,
        ROW_NUMBER() OVER (
            PARTITION BY LOAN_REQUEST.PARENT_SERIAL
            ORDER BY LOAN_REQUEST.SERIAL ASC
        )                                                                   AS RN
    FROM CORE.LOAN_REQUEST AS LOAN_REQUEST
)
SELECT
    APP.SERIAL                                                              AS SOURCE_APP_SERIAL,
    APP.APPLICATION_NUMBER,
    APP.APPLICATION_DATE,
    APP_TYPE.DESCRIPTION                                                    AS APP_TYPE,
    CASE APP.WITHDRAWN_INDICATOR
        WHEN 'A' THEN 'Withdrawn'
        WHEN 'D' THEN 'Duplicate'
        ELSE NULL
    END                                                                     AS WITHDRAWN_STATUS,
    PA.PERSON_SERIAL                                                        AS SOURCE_PRIMARY_PERSON_SERIAL,
    PRIMARY_SCORE.SCORE                                                     AS PRIMARY_CREDIT_SCORE,
    CA.PERSON_SERIAL                                                        AS SOURCE_CO_APPLICANT_PERSON_SERIAL,
    CA.CO_APPLICANT_NAME,
    CO_SCORE.SCORE                                                          AS CO_APPLICANT_CREDIT_SCORE,
    APP.PROCESSOR_USER_SERIAL                                               AS SOURCE_APP_PROCESSOR_SERIAL,
    AB.BRANCH_SERIAL                                                        AS SOURCE_BRANCH_SERIAL
FROM CORE.APP AS APP
    JOIN CORE.APP_TYPE AS APP_TYPE
        ON APP.TYPE_SERIAL = APP_TYPE.SERIAL
    LEFT JOIN PrimaryApplicant AS PA
        ON APP.SERIAL = PA.APP_SERIAL AND PA.RANK = 1
    LEFT JOIN LatestApplicantScore AS PRIMARY_SCORE
        ON PA.APPLICANT_SERIAL = PRIMARY_SCORE.APPLICANT_SERIAL AND PRIMARY_SCORE.RN = 1
    LEFT JOIN CoApplicant AS CA
        ON APP.SERIAL = CA.APP_SERIAL AND CA.RANK = 2
    LEFT JOIN LatestApplicantScore AS CO_SCORE
        ON CA.APPLICANT_SERIAL = CO_SCORE.APPLICANT_SERIAL AND CO_SCORE.RN = 1
    LEFT JOIN AppBranch AS AB
        ON APP.SERIAL = AB.APP_SERIAL AND AB.RN = 1
WHERE APP_TYPE.DESCRIPTION LIKE '%New Loan App%'
  AND APP.APPLICATION_DATE BETWEEN :start_date AND :end_date
ORDER BY
    APP.APPLICATION_DATE,
    APP.APPLICATION_NUMBER;
