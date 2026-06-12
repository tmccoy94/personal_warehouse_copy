-- SHARE.PARENT_SERIAL = ACCOUNT.SERIAL (not person serial) — person is on the account.
-- share_id '0001' = General Share (membership indicator).
SELECT
    SHARE.SERIAL                    AS share_serial,
    ACCOUNT.PRIMARY_PERSON_SERIAL   AS person_serial,
    SHARE.PARENT_SERIAL             AS account_serial,
    SHARE.ID                        AS share_id,
    SHARE.TYPE_SERIAL               AS type_serial,
    SH_TYPE.DESCRIPTION             AS share_type,
    SHARE.BRANCH_SERIAL             AS branch_serial,
    SHARE.OPEN_DATE                 AS open_date,
    SHARE.CLOSE_DATE                AS close_date,
    SHARE.CHARGE_OFF_DATE           AS charge_off_date,
    SHARE.ACH_DEPOSIT_LAST_AMOUNT   AS ach_deposit_last_amount,
    SHARE.ACH_DEPOSIT_LAST_DATE     AS ach_deposit_last_date
FROM
    CORE.SHARE   AS SHARE INNER JOIN
    CORE.ACCOUNT AS ACCOUNT ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL LEFT JOIN
    CORE.SH_TYPE AS SH_TYPE ON SH_TYPE.SERIAL = SHARE.TYPE_SERIAL
