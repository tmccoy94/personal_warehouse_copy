-- All loans from CORE.LOAN with account/person link.
-- LOAN.PARENT_SERIAL = ACCOUNT.SERIAL — person comes from ACCOUNT.PRIMARY_PERSON_SERIAL.
-- Includes delinquency payment counts for is_delinquent derivation in the warehouse.
-- TO_RESOLVE: confirm LOAN.BALANCE vs LOAN.CURRENT_BALANCE column name.
-- TO_RESOLVE: confirm LOAN.CHARGE_OFF_DATE column name if different from SHARE.
SELECT
    LOAN.SERIAL                         AS loan_serial,
    LOAN.PARENT_SERIAL                  AS account_serial,
    ACCOUNT.PRIMARY_PERSON_SERIAL       AS person_serial,
    LOAN.OPEN_DATE                      AS open_date,
    LOAN.CLOSE_DATE                     AS close_date,
    LOAN.CHARGE_OFF_DATE                AS charge_off_date,
    LOAN.MATURITY_DATE                  AS maturity_date,
    LOAN.BALANCE                        AS balance,
    LOAN.INTEREST_RATE                  AS interest_rate,
    LOAN.BRANCH_SERIAL                  AS branch_serial,
    LN_TYPE.DESCRIPTION                    AS loan_type_description,
    COALESCE(LOAN.PAYMENT_COUNT_DQ_UNDER_30,   0) AS payment_count_dq_under_30,
    COALESCE(LOAN.PAYMENT_COUNT_DQ_30_TO_59,   0) AS payment_count_dq_30_to_59,
    COALESCE(LOAN.PAYMENT_COUNT_DQ_60_TO_89,   0) AS payment_count_dq_60_to_89,
    COALESCE(LOAN.PAYMENT_COUNT_DQ_90_TO_119,  0) AS payment_count_dq_90_to_119,
    COALESCE(LOAN.PAYMENT_COUNT_DQ_120_AND_UP, 0) AS payment_count_dq_120_and_up
FROM
    CORE.LOAN    AS LOAN JOIN
    CORE.ACCOUNT AS ACCOUNT ON ACCOUNT.SERIAL = LOAN.PARENT_SERIAL LEFT JOIN
    CORE.LN_TYPE AS LN_TYPE ON LN_TYPE.SERIAL  = LOAN.TYPE_SERIAL
