-- Parameters: :begin_date (first day of month), :end_date (last day of month)
-- Returns one row per member who joined during the date range with their home branch and ZIP.
-- Used by ks_fct_home_branch_members_summary_table to count new member additions per branch territory.
WITH
CurrentAddress(personSerial, street, additionalAddressLine, city, state, postalCode, combinedAddress) AS (
    SELECT
        PERSON_SERIAL,
        COALESCE(ADDRESS.STREET, '') AS STREET,
        COALESCE(ADDRESS.ADDITIONAL_ADDRESS_LINE, '') AS ADDITIONAL_ADDRESS_LINE,
        COALESCE(ADDRESS.CITY, '') AS CITY,
        COALESCE(ADDRESS.STATE, '') AS STATE,
        COALESCE(ADDRESS.POSTAL_CODE, '') AS POSTAL_CODE,
        COALESCE(ADDRESS.STREET || ' ', '') || COALESCE(ADDRESS.ADDITIONAL_ADDRESS_LINE || ', ', '') ||
        COALESCE(ADDRESS.CITY || ' ', '') || COALESCE(ADDRESS.STATE, '') || COALESCE(', ' || ADDRESS.POSTAL_CODE, '') AS COMBINED_ADDRESS
    FROM
    (
        SELECT
            PAL.PARENT_SERIAL AS PERSON_SERIAL,
            PAL.ADDRESS_SERIAL AS ADDRESS_SERIAL,
            COALESCE(PAL.ORDINAL, 0) AS PAL_ORDINAL,
            MIN(COALESCE(PAL.ORDINAL, 0)) OVER (PARTITION BY PAL.PARENT_SERIAL) AS MIN_PAL_ORDINAL
        FROM
            CORE.PERSON_ADDRESS_LINK AS PAL LEFT OUTER JOIN
            CORE.ENV AS ENV ON
                ENV.SERIAL > 0
        WHERE
            (PAL.EFFECTIVE_DATE IS NULL OR
            PAL.EFFECTIVE_DATE <= ENV.POSTING_DATE) AND
            (PAL.EXPIRATION_DATE IS NULL OR
            PAL.EXPIRATION_DATE >= ENV.POSTING_DATE) AND
            PAL.BAD_ADDRESS <> 'Y' AND
            PAL.CATEGORY <> 'V'
    ) LEFT OUTER JOIN
        CORE.ADDRESS AS ADDRESS ON
            ADDRESS_SERIAL = ADDRESS.SERIAL
    WHERE
        PAL_ORDINAL = MIN_PAL_ORDINAL
),
ActiveOnBeginDate AS (
  SELECT DISTINCT
    A.PRIMARY_PERSON_SERIAL AS PERSON_SERIAL,
    1 AS IS_ACTIVE
  FROM CORE.ACCOUNT A
  JOIN CORE.SHARE S
    ON A.SERIAL = S.PARENT_SERIAL
   AND S.ID = '0001'
   AND S.OPEN_DATE <= :begin_date
   AND (S.CLOSE_DATE IS NULL OR S.CLOSE_DATE > DATE :begin_date)),
closedGeneralShares AS (SELECT
    PERSON.SERIAL AS PERSON_SERIAL,
    SHARE.OPEN_DATE AS SHARE_OPEN_DATE,
    SHARE.CLOSE_DATE AS SHARE_CLOSE_DATE
FROM
    CORE.ENV AS ENV CROSS JOIN
    CORE.PERSON AS PERSON
    JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
    JOIN CORE.SHARE AS SHARE ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
        AND SHARE.ID = '0001' AND SHARE.CLOSE_DATE IS NOT NULL
    LEFT JOIN CORE.PERSON_TYPE AS PERSON_TYPE ON PERSON.TYPE_SERIAL = PERSON_TYPE.SERIAL
WHERE
    (PERSON_TYPE.DESCRIPTION IS NULL OR PERSON_TYPE.DESCRIPTION != 'Vendor') AND
    SHARE.CLOSE_DATE BETWEEN :begin_date AND :end_date
ORDER BY
    PERSON.SERIAL, SHARE.CLOSE_DATE DESC),
allClosedMembers AS (SELECT
    PERSON.SERIAL AS PERSON_SERIAL,
    MAX( PERSON.LAST_NAME || ', ' || COALESCE( PERSON.FIRST_NAME, '') || COALESCE(' ' || PERSON.MIDDLE_NAME, '')) as PERSON_NAME,
    MAX(CASE
        WHEN ActiveOnBeginDate.PERSON_SERIAL IS NULL THEN 'NO'
        ELSE 'YES'
    END) AS ACTIVE_ON_BEGIN_DATE
FROM
    CORE.ENV AS ENV CROSS JOIN
    CORE.PERSON AS PERSON
    JOIN closedGeneralShares ON closedGeneralShares.PERSON_SERIAL = PERSON.SERIAL
    JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
    JOIN CORE.SHARE AS SHARE ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
        AND SHARE.ID = '0001'
    LEFT JOIN CORE.PERSON_TYPE AS PERSON_TYPE ON PERSON.TYPE_SERIAL = PERSON_TYPE.SERIAL
    LEFT JOIN CORE.AC_NOTE AS AC_NOTE ON ACCOUNT.SERIAL = AC_NOTE.PARENT_SERIAL
        AND (AC_NOTE.EXPIRATION_DATE IS NULL OR
            AC_NOTE.EXPIRATION_DATE > ENV.POSTING_DATE)
    LEFT JOIN CORE.NOTE_TYPE AS NOTE_TYPE ON AC_NOTE.TYPE_SERIAL = NOTE_TYPE.SERIAL
    LEFT JOIN CORE.ACCOUNT_CLOSE_REASON AS ACCOUNT_CLOSE_REASON ON ACCOUNT_CLOSE_REASON.SERIAL = ACCOUNT.CLOSE_REASON_SERIAL
    LEFT JOIN ActiveOnBeginDate ON ActiveOnBeginDate.PERSON_SERIAL = PERSON.SERIAL
WHERE
    (PERSON_TYPE.DESCRIPTION IS NULL OR PERSON_TYPE.DESCRIPTION != 'Vendor')
GROUP BY
    PERSON.SERIAL
HAVING
    COUNT(CASE WHEN SHARE.CLOSE_DATE IS NULL AND SHARE.ID = '0001' THEN SHARE.SERIAL END) = 0),
closedTable AS (SELECT
        PERSON_SERIAL
FROM allClosedMembers),
memberBranch(personSerial, branchSerial) AS (SELECT PERSON_SERIAL,
                                                         BRANCH_SERIAL
                                                  FROM (SELECT SHARE.SERIAL                                        AS SHARE_SERIAL,
                                                               SHARE.BRANCH_SERIAL                                 AS BRANCH_SERIAL,
                                                               PERSON.SERIAL                                       AS PERSON_SERIAL,
                                                               MIN(SHARE.SERIAL) OVER (PARTITION BY PERSON.SERIAL) AS MIN_SHARE_SERIAL
                                                        FROM CORE.PERSON AS PERSON
                                                                 INNER JOIN
                                                             CORE.ACCOUNT AS ACCOUNT ON
                                                                 PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
                                                                 INNER JOIN
                                                             CORE.SHARE AS SHARE ON
                                                                 ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
                                                        WHERE ACCOUNT.CLOSE_DATE IS NULL
                                                          AND SHARE.ID = '0001'
                                                          AND SHARE.CLOSE_DATE IS NULL)
                                                  WHERE SHARE_SERIAL = MIN_SHARE_SERIAL),
HomeBranch(cuHomeSerial, homeBranchName) AS (SELECT CU_HOME_BRANCH.PARENT_SERIAL AS CU_HOME_SERIAL,
                                                         HOME_BRANCH.DESCRIPTION      AS HOME_BRANCH_NAME
                                                  FROM CORE.PERSON AS HOME_PERSON
                                                           INNER JOIN
                                                       CORE.CU_HOME_BRANCH AS CU_HOME_BRANCH ON
                                                           HOME_PERSON.SERIAL = CU_HOME_BRANCH.PARENT_SERIAL
                                                           LEFT OUTER JOIN
                                                       CORE.BRANCH AS HOME_BRANCH ON
                                                           HOME_BRANCH.SERIAL = CU_HOME_BRANCH.BRANCH_SERIAL
                                                  WHERE NOT EXISTS (SELECT 1
                                                                    FROM CORE.PERSON AS HOME_PERSON_INNER
                                                                             INNER JOIN
                                                                         CORE.CU_HOME_BRANCH AS CU_HOME_BRANCH_INNER ON
                                                                             HOME_PERSON_INNER.SERIAL = CU_HOME_BRANCH_INNER.PARENT_SERIAL
                                                                    WHERE HOME_PERSON.SERIAL = HOME_PERSON_INNER.SERIAL
                                                                      AND CU_HOME_BRANCH.SERIAL < CU_HOME_BRANCH_INNER.SERIAL)),
closedHB AS(
    SELECT
        closedTable.PERSON_SERIAL AS PERSON_SERIAL,
        HomeBranch.homeBranchName AS HOME_BRANCH
    FROM
        closedTable JOIN HomeBranch ON HomeBranch.cuHomeSerial = closedTable.PERSON_SERIAL
),
openHB AS (
    SELECT
        memberBranch.personSerial AS PERSON_SERIAL,
        HomeBranch.homeBranchName AS HOME_BRANCH
    FROM CORE.BRANCH AS BRANCH
             INNER JOIN
         memberBranch ON
             BRANCH.SERIAL = memberBranch.branchSerial
             INNER JOIN
         HomeBranch ON
             memberBranch.personSerial = HomeBranch.cuHomeSerial
    WHERE EXISTS (SELECT 1
                  FROM CORE.ACCOUNT_TYPE AS ACCOUNT_TYPE
                           INNER JOIN
                       CORE.ACCOUNT AS ACCOUNT ON
                           ACCOUNT_TYPE.SERIAL = ACCOUNT.TYPE_SERIAL
                  WHERE memberBranch.personSerial = ACCOUNT.PRIMARY_PERSON_SERIAL
                    AND ACCOUNT_TYPE.DESCRIPTION = 'Trust'
                    AND ACCOUNT.CLOSE_DATE IS NULL)
       OR NOT EXISTS (SELECT 1
                      FROM CORE.PERSON_TYPE AS PERSON_TYPE
                               INNER JOIN
                           CORE.PERSON AS PERSON ON
                               PERSON_TYPE.SERIAL = PERSON.TYPE_SERIAL
                      WHERE memberBranch.personSerial = PERSON.SERIAL
                        AND PERSON_TYPE.DESCRIPTION = 'Vendor') AND
          NOT EXISTS (SELECT 1
                      FROM CORE.NOTE_TYPE AS NOTE_TYPE
                               INNER JOIN
                           CORE.AC_NOTE AS AC_NOTE ON
                               NOTE_TYPE.SERIAL = AC_NOTE.TYPE_SERIAL
                               INNER JOIN
                           CORE.ACCOUNT AS ACCOUNT ON
                               AC_NOTE.PARENT_SERIAL = ACCOUNT.SERIAL
                               CROSS JOIN
                           CORE.ENV AS ENV
                      WHERE ACCOUNT.PRIMARY_PERSON_SERIAL = memberBranch.personSerial
                        AND ACCOUNT.CLOSE_DATE IS NULL
                        AND NOTE_TYPE.DESCRIPTION = 'Deceased Member'
                        AND (AC_NOTE.EXPIRATION_DATE IS NULL OR
                             AC_NOTE.EXPIRATION_DATE > ENV.POSTING_DATE)) AND
          memberBranch.personSerial <> 0
),
allHB AS (
    SELECT *
    FROM openHB
        UNION ALL
    SELECT *
    FROM closedHB
),
newOpenedAccounts AS (SELECT  -- Use this CTE to get all currently active members who opened acct in time range specified
    PERSON.SERIAL AS PERSON_SERIAL,
    YEAR(MAX(ENV.POSTING_DATE)) - YEAR(MIN(ACCOUNT.OPEN_DATE)) -
            CASE
                WHEN (MONTH(MAX(ACCOUNT.CLOSE_DATE)) * 100 + DAY(MAX(ACCOUNT.CLOSE_DATE))) < (MONTH(MIN(ACCOUNT.OPEN_DATE)) * 100 + DAY(MIN(ACCOUNT.OPEN_DATE))) THEN 1
                ELSE 0
            END AS MEMBERSHIP_DURATION_YRS,
    MIN(ACCOUNT.OPEN_DATE) AS MIN_ACCT_OPEN_DATE
    FROM
        CORE.ENV AS ENV CROSS JOIN
        CORE.PERSON AS PERSON
        JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL -- must be a primary person on at least one account
            AND ACCOUNT.CLOSE_DATE IS NULL -- only active accounts
        JOIN CORE.SHARE AS SHARE ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
        LEFT JOIN CORE.PERSON_TYPE AS PERSON_TYPE ON PERSON.TYPE_SERIAL = PERSON_TYPE.SERIAL
    WHERE
        (PERSON_TYPE.DESCRIPTION IS NULL OR PERSON_TYPE.DESCRIPTION != 'Vendor') -- Is not a vendor
    GROUP BY
        PERSON.SERIAL
    HAVING
        MIN(ACCOUNT.OPEN_DATE) BETWEEN :begin_date AND :end_date),
completeMemberAdditions AS (SELECT -- use this to get accounts that were opened before the time range specified for those returning members
        PERSON.SERIAL AS PERSON_SERIAL,
        MAX( PERSON.LAST_NAME || ', ' || COALESCE( PERSON.FIRST_NAME, '') || COALESCE(' ' || PERSON.MIDDLE_NAME, '')) as PERSON_NAME,
        CASE WHEN MIN(ACCOUNT.OPEN_DATE) < :begin_date THEN 'YES' ELSE 'NO' END AS RETURNING_MEMBER,
        CASE
          WHEN MAX(CASE
                     WHEN SHARE.ID = '0001' AND SHARE.CLOSE_DATE IS NULL
                     THEN 1 ELSE 0
                   END) = 0 THEN 'YES'
          ELSE NULL
        END AS CLOSED_MEMBER,
        max(ActiveOnBeginDate.IS_ACTIVE) AS ACTIVE_BEGIN_DATE,
        MAX(newOpenedAccounts.MIN_ACCT_OPEN_DATE) AS MIN_ACCT_OPEN_DATE
    FROM
        newOpenedAccounts
        JOIN CORE.PERSON AS PERSON ON newOpenedAccounts.PERSON_SERIAL = PERSON.SERIAL
        JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
        JOIN CORE.SHARE AS SHARE ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
        LEFT JOIN ActiveOnBeginDate ON ActiveOnBeginDate.PERSON_SERIAL = PERSON.SERIAL
    GROUP BY
        PERSON.SERIAL

    UNION ALL
    -- use this to include members who closed their accounts since they opened
    SELECT *
    FROM
        (SELECT
            PERSON.SERIAL AS PERSON_SERIAL,
            MAX( PERSON.LAST_NAME || ', ' || COALESCE( PERSON.FIRST_NAME, '') || COALESCE(' ' || PERSON.MIDDLE_NAME, '')) as PERSON_NAME,
            CASE WHEN MIN(ACCOUNT.OPEN_DATE) < :begin_date THEN 'YES' ELSE 'NO' END AS RETURNING_MEMBER,
            CASE
              WHEN MAX(CASE
                         WHEN SHARE.ID = '0001' AND SHARE.CLOSE_DATE IS NULL
                         THEN 1 ELSE 0
                       END) = 0 THEN 'YES'
              ELSE NULL
            END AS CLOSED_MEMBER,
            max(ActiveOnBeginDate.IS_ACTIVE) AS ACTIVE_BEGIN_DATE,
            MIN(ACCOUNT.OPEN_DATE) AS MIN_ACCT_OPEN_DATE
        FROM
            CORE.PERSON AS PERSON
            JOIN CORE.ACCOUNT AS ACCOUNT ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
            JOIN CORE.SHARE AS SHARE ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
            LEFT JOIN CORE.PERSON_TYPE AS PERSON_TYPE ON PERSON.TYPE_SERIAL = PERSON_TYPE.SERIAL
            LEFT JOIN ActiveOnBeginDate ON ActiveOnBeginDate.PERSON_SERIAL = PERSON.SERIAL
        WHERE
            (PERSON_TYPE.DESCRIPTION IS NULL OR PERSON_TYPE.DESCRIPTION != 'Vendor') -- Is not a vendor
        GROUP BY
            PERSON.SERIAL
        HAVING
            MIN(ACCOUNT.OPEN_DATE) BETWEEN :begin_date AND :end_date)
    WHERE
        CLOSED_MEMBER = 'YES'),
openedTable AS (
    SELECT
        PERSON_SERIAL,
       'OPENED' AS MEMBER_TYPE,
        MIN_ACCT_OPEN_DATE
    FROM completeMemberAdditions
    WHERE ACTIVE_BEGIN_DATE IS NULL)
SELECT
    YEAR(MIN_ACCT_OPEN_DATE) AS YEAR_OPENED,
    MONTH(MIN_ACCT_OPEN_DATE) AS MONTH_OPENED,
    DAY(MIN_ACCT_OPEN_DATE) AS DAY_OPENED,
    HOME_BRANCH,
    openedTable.PERSON_SERIAL,
    LEFT(CurrentAddress.postalCode, 5) AS ZIP
FROM
    openedTable
    LEFT JOIN allHB ON openedTable.PERSON_SERIAL = allHB.PERSON_SERIAL
    LEFT JOIN CurrentAddress ON CurrentAddress.personSerial = openedTable.PERSON_SERIAL
ORDER BY MONTH_OPENED
;
