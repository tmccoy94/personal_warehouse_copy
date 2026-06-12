-- DB2: New Loan App count by decision category (latest decision per loan request)
-- Date cutoff '2025-04-01' matches source query — eliminates legacy records.
-- Inner subquery resolves category per row; outer query counts distinct apps per category.
SELECT
    decision_category,
    COUNT(DISTINCT application_number) AS application_count
FROM (
    SELECT
        APP.APPLICATION_NUMBER AS application_number,
        CASE
            WHEN DECISION.SERIAL IS NULL       THEN 'No Decision'
            WHEN DECISION_TYPE.CATEGORY = 'A'  THEN 'Approved'
            WHEN DECISION_TYPE.CATEGORY = 'D'  THEN 'Denied'
            WHEN DECISION_TYPE.CATEGORY = 'P'  THEN 'Pending'
            WHEN DECISION_TYPE.CATEGORY = 'a'  THEN 'Pending additional approval'
            WHEN DECISION_TYPE.CATEGORY = 'C'  THEN 'Countered'
            ELSE                                    'Unknown'
        END AS decision_category
    FROM CORE.APP AS APP
        JOIN  CORE.APP_TYPE      AS APP_TYPE      ON APP.TYPE_SERIAL = APP_TYPE.SERIAL
        LEFT JOIN CORE.LOAN_REQUEST AS LOAN_REQUEST ON LOAN_REQUEST.PARENT_SERIAL = APP.SERIAL
        LEFT JOIN CORE.DECISION     AS DECISION     ON LOAN_REQUEST.SERIAL = DECISION.PARENT_SERIAL
            AND NOT EXISTS (
                SELECT 1 FROM CORE.DECISION AS later_dec
                WHERE later_dec.PARENT_SERIAL  = DECISION.PARENT_SERIAL
                  AND later_dec.DECISION_DATE >= DECISION.DECISION_DATE
                  AND later_dec.SERIAL         > DECISION.SERIAL
            )
        LEFT JOIN CORE.DECISION_TYPE AS DECISION_TYPE ON DECISION.TYPE_SERIAL = DECISION_TYPE.SERIAL
    WHERE APP_TYPE.DESCRIPTION LIKE '%New Loan App%'
      AND APP.APPLICATION_DATE > '2025-04-01'
) AS resolved
GROUP BY decision_category
ORDER BY application_count DESC
