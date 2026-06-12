-- Find the person_serials DB2 counts that our staging is missing
-- Returns persons who appear in the DB2 member count but NOT in staging.ks_share
-- with the same open-share criteria
WITH memberBranch(personSerial, branchSerial) AS (
    SELECT PERSON_SERIAL, BRANCH_SERIAL
    FROM (
        SELECT SHARE.SERIAL                                        AS SHARE_SERIAL,
               SHARE.BRANCH_SERIAL                                 AS BRANCH_SERIAL,
               PERSON.SERIAL                                       AS PERSON_SERIAL,
               MIN(SHARE.SERIAL) OVER (PARTITION BY PERSON.SERIAL) AS MIN_SHARE_SERIAL
        FROM CORE.PERSON AS PERSON
                 INNER JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
                 INNER JOIN CORE.SHARE   AS SHARE   ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
        WHERE ACCOUNT.CLOSE_DATE    IS NULL
          AND SHARE.ID              = '0001'
          AND SHARE.CLOSE_DATE      IS NULL
          AND SHARE.CHARGE_OFF_DATE IS NULL
    )
    WHERE SHARE_SERIAL = MIN_SHARE_SERIAL
)
SELECT
    memberBranch.personSerial AS person_serial,
    BRANCH.DESCRIPTION        AS branch
FROM CORE.BRANCH AS BRANCH
         LEFT OUTER JOIN memberBranch ON BRANCH.SERIAL = memberBranch.branchSerial
         JOIN CORE.PERSON AS PERSON ON memberBranch.personSerial = PERSON.SERIAL
WHERE (
    EXISTS (
        SELECT 1 FROM CORE.ACCOUNT_TYPE AS ACCOUNT_TYPE
                          INNER JOIN CORE.ACCOUNT AS ACCOUNT ON ACCOUNT_TYPE.SERIAL = ACCOUNT.TYPE_SERIAL
        WHERE memberBranch.personSerial = ACCOUNT.PRIMARY_PERSON_SERIAL
          AND ACCOUNT_TYPE.DESCRIPTION = 'Trust'
          AND ACCOUNT.CLOSE_DATE IS NULL
    )
    OR NOT EXISTS (
        SELECT 1 FROM CORE.PERSON_TYPE AS PERSON_TYPE
                          INNER JOIN CORE.PERSON AS PERSON ON PERSON_TYPE.SERIAL = PERSON.TYPE_SERIAL
        WHERE memberBranch.personSerial = PERSON.SERIAL
          AND PERSON_TYPE.DESCRIPTION  = 'Vendor'
    )
    AND NOT EXISTS (
        SELECT 1 FROM CORE.NOTE_TYPE AS NOTE_TYPE
                          INNER JOIN CORE.AC_NOTE  AS AC_NOTE  ON NOTE_TYPE.SERIAL = AC_NOTE.TYPE_SERIAL
                          INNER JOIN CORE.ACCOUNT  AS ACCOUNT  ON AC_NOTE.PARENT_SERIAL = ACCOUNT.SERIAL
                          CROSS JOIN CORE.ENV AS ENV
        WHERE ACCOUNT.PRIMARY_PERSON_SERIAL = memberBranch.personSerial
          AND ACCOUNT.CLOSE_DATE            IS NULL
          AND NOTE_TYPE.DESCRIPTION         = 'Deceased Member'
          AND (AC_NOTE.EXPIRATION_DATE IS NULL OR AC_NOTE.EXPIRATION_DATE > ENV.POSTING_DATE)
    )
    AND memberBranch.personSerial <> 0
)
AND BRANCH.DESCRIPTION IN ('Main Branch', 'North Branch', 'Overland Park Branch')
ORDER BY BRANCH.DESCRIPTION, memberBranch.personSerial
