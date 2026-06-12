-- Loan decisions — all decisions, not just latest.
-- DECISION.PARENT_SERIAL = LOAN_REQUEST.SERIAL.
-- Latest-per-loan-request derivation happens in the warehouse using MAX(decision_date) + serial tiebreaker.
-- Embeds decision category code and description from DECISION_TYPE.
SELECT
    DECISION.SERIAL                         AS decision_serial,
    DECISION.PARENT_SERIAL                  AS loan_request_serial,
    DECISION.TYPE_SERIAL                    AS type_serial,
    DECISION.DECISION_DATE                  AS decision_date,
    DECISION.DECISION_MAKER_USER_SERIAL     AS decision_maker_user_serial,
    DECISION_TYPE.CATEGORY                  AS decision_category_code,
    CASE DECISION_TYPE.CATEGORY
        WHEN 'P' THEN 'Pending'
        WHEN 'a' THEN 'Pending Additional Approval'
        WHEN 'A' THEN 'Approved'
        WHEN 'D' THEN 'Denied'
        WHEN 'C' THEN 'Countered'
        ELSE          'Unknown'
    END                                     AS decision_category,
    CASE
        WHEN DECISION_TYPE.DESCRIPTION = 'System Approved' THEN 'Approved'
        ELSE DECISION_TYPE.DESCRIPTION
    END                                     AS decision_description
FROM
    CORE.DECISION      AS DECISION LEFT JOIN
    CORE.DECISION_TYPE AS DECISION_TYPE ON DECISION_TYPE.SERIAL = DECISION.TYPE_SERIAL
