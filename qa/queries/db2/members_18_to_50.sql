-- DB2: Members aged 18-50 using ENV.POSTING_DATE for dynamic age calculation
-- Matches Member Count.sql 18-50 query with full exclusion logic
WITH memberBranch(personSerial, branchSerial) AS (
    SELECT PERSON_SERIAL, BRANCH_SERIAL
    FROM (
        SELECT SHARE.SERIAL                                        AS SHARE_SERIAL,
               SHARE.BRANCH_SERIAL                                 AS BRANCH_SERIAL,
               PERSON.SERIAL                                       AS PERSON_SERIAL,
               MIN(SHARE.SERIAL) OVER (PARTITION BY PERSON.SERIAL) AS MIN_SHARE_SERIAL
        FROM CORE.PERSON  AS PERSON
                 INNER JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL    = ACCOUNT.PRIMARY_PERSON_SERIAL
                 INNER JOIN CORE.SHARE   AS SHARE   ON ACCOUNT.SERIAL   = SHARE.PARENT_SERIAL
        WHERE ACCOUNT.CLOSE_DATE    IS NULL
          AND SHARE.ID              = '0001'
          AND SHARE.CLOSE_DATE      IS NULL
          AND SHARE.CHARGE_OFF_DATE IS NULL
    )
    WHERE SHARE_SERIAL = MIN_SHARE_SERIAL
)
SELECT COUNT(DISTINCT memberBranch.personSerial) AS members_18_to_50
FROM memberBranch
         JOIN CORE.PERSON AS PERSON ON memberBranch.personSerial = PERSON.SERIAL
         CROSS JOIN CORE.ENV AS ENV
WHERE (
    EXISTS (
        SELECT 1 FROM CORE.ACCOUNT_TYPE AS ACCOUNT_TYPE
                          INNER JOIN CORE.ACCOUNT AS ACCOUNT ON ACCOUNT_TYPE.SERIAL = ACCOUNT.TYPE_SERIAL
        WHERE memberBranch.personSerial = ACCOUNT.PRIMARY_PERSON_SERIAL
          AND ACCOUNT_TYPE.DESCRIPTION = 'Trust'
          AND ACCOUNT.CLOSE_DATE IS NULL
    )
    OR (
        NOT EXISTS (
            SELECT 1 FROM CORE.PERSON_TYPE AS PERSON_TYPE
                              INNER JOIN CORE.PERSON AS P2 ON PERSON_TYPE.SERIAL = P2.TYPE_SERIAL
            WHERE memberBranch.personSerial = P2.SERIAL AND PERSON_TYPE.DESCRIPTION = 'Vendor'
        )
        AND NOT EXISTS (
            SELECT 1 FROM CORE.NOTE_TYPE AS NOTE_TYPE
                              INNER JOIN CORE.AC_NOTE AS AC_NOTE ON NOTE_TYPE.SERIAL = AC_NOTE.TYPE_SERIAL
                              INNER JOIN CORE.ACCOUNT AS ACCOUNT  ON AC_NOTE.PARENT_SERIAL = ACCOUNT.SERIAL
                              CROSS JOIN CORE.ENV AS ENV2
            WHERE ACCOUNT.PRIMARY_PERSON_SERIAL = memberBranch.personSerial
              AND ACCOUNT.CLOSE_DATE IS NULL
              AND NOTE_TYPE.DESCRIPTION = 'Deceased Member'
              AND (AC_NOTE.EXPIRATION_DATE IS NULL OR AC_NOTE.EXPIRATION_DATE > ENV2.POSTING_DATE)
        )
        AND NOT EXISTS (
            SELECT 1 FROM CORE.ACCOUNT AS EXCL_ACCT
                              INNER JOIN CORE.ACCOUNT_TYPE AS EXCL_TYPE ON EXCL_TYPE.SERIAL = EXCL_ACCT.TYPE_SERIAL
            WHERE EXCL_ACCT.PRIMARY_PERSON_SERIAL = memberBranch.personSerial
              AND EXCL_ACCT.CLOSE_DATE IS NULL
              AND TRIM(EXCL_TYPE.DESCRIPTION) IN ('Reserves', '1-Choose One', 'Trust')
        )
        AND PERSON.BIRTH_DATE <= ENV.POSTING_DATE - 18 YEARS
        AND PERSON.BIRTH_DATE >  ENV.POSTING_DATE - 51 YEARS
        AND memberBranch.personSerial <> 0
    )
)
