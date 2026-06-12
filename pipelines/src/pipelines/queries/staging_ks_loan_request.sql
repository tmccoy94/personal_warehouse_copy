-- All loan requests from CORE.LOAN_REQUEST.
-- LOAN_REQUEST.PARENT_SERIAL = APP.SERIAL — embeds loan type description from LN_TYPE.
SELECT
    LOAN_REQUEST.SERIAL                     AS loan_request_serial,
    LOAN_REQUEST.PARENT_SERIAL              AS app_serial,
    LOAN_REQUEST.TYPE_SERIAL                AS type_serial,
    LN_TYPE.DESCRIPTION                     AS loan_type,
    LOAN_REQUEST.BRANCH_SERIAL              AS branch_serial,
    LOAN_REQUEST.CREDIT_LIMIT               AS credit_limit,
    LOAN_REQUEST.CREDIT_SCORE               AS credit_score,
    LOAN_REQUEST.PROCESSOR_USER_SERIAL      AS processor_user_serial,
    LOAN_REQUEST.APPROVED_LOAN_SERIAL       AS approved_loan_serial
FROM
    CORE.LOAN_REQUEST AS LOAN_REQUEST LEFT JOIN
    CORE.LN_TYPE      AS LN_TYPE      ON LN_TYPE.SERIAL = LOAN_REQUEST.TYPE_SERIAL
