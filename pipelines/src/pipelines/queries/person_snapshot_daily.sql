-- PERSON_SNAPSHOT_DAILY — daily snapshot of all active members in CORE
-- No date parameters — runs against current DB2 state; snapshot_date assigned by the Python asset.
-- Outputs: biographic data + branch serials + credit score + account standing
-- Products are now derived in the warehouse from staging.ks_share and fct_loans.
-- Contact info is loaded separately by ks_person_contact_table.
-- nCino columns removed — not applicable to this system.
WITH MemberUniverse(personSerial) AS (
    -- All persons who hold at least one open General Share (active members).
    SELECT DISTINCT
        ACCOUNT.PRIMARY_PERSON_SERIAL   AS PERSON_SERIAL
    FROM
        CORE.ACCOUNT AS ACCOUNT INNER JOIN
        CORE.SHARE   AS SHARE   ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
    WHERE
        SHARE.ID               = '0001' AND
        SHARE.CLOSE_DATE       IS NULL  AND
        SHARE.CHARGE_OFF_DATE  IS NULL
),

PersonOldestShare(personSerial, branchDescription, branchSerial) AS (
    SELECT
        PERSON.SERIAL                AS PERSON_SERIAL,
        BRANCH.DESCRIPTION           AS BRANCH_DESCRIPTION,
        SHARE.BRANCH_SERIAL          AS BRANCH_SERIAL
    FROM
        CORE.ACCOUNT AS ACCOUNT INNER JOIN
        CORE.PERSON  AS PERSON  ON ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL INNER JOIN
        CORE.SHARE   AS SHARE   ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL LEFT OUTER JOIN
        CORE.BRANCH  AS BRANCH  ON SHARE.BRANCH_SERIAL = BRANCH.SERIAL
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM
                CORE.ACCOUNT AS ACCOUNT_INNER INNER JOIN
                CORE.SHARE   AS SHARE_INNER   ON ACCOUNT_INNER.SERIAL = SHARE_INNER.PARENT_SERIAL INNER JOIN
                CORE.PERSON  AS PERSON_INNER  ON ACCOUNT_INNER.PRIMARY_PERSON_SERIAL = PERSON_INNER.SERIAL
            WHERE
                PERSON.SERIAL = PERSON_INNER.SERIAL AND
                SHARE_INNER.SERIAL < SHARE.SERIAL
        )
),

mostRecentPersonScore(personSerial, score, scoreDate) AS (
    SELECT
        PERSON_SCORE.PARENT_SERIAL  AS PERSON_SERIAL,
        PERSON_SCORE.SCORE          AS SCORE,
        PERSON_SCORE.SCORE_DATE     AS SCORE_DATE
    FROM
        CORE.PERSON_SCORE AS PERSON_SCORE
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM CORE.PERSON_SCORE AS PERSON_SCORE_INNER
            WHERE
                PERSON_SCORE_INNER.PARENT_SERIAL = PERSON_SCORE.PARENT_SERIAL AND
                PERSON_SCORE_INNER.SCORE_DATE   >= PERSON_SCORE.SCORE_DATE    AND
                PERSON_SCORE_INNER.SERIAL        > PERSON_SCORE.SERIAL
        )
    ORDER BY PERSON_SCORE.PARENT_SERIAL
),

HomeBranchAssignment(personSerial, home_branch_serial) AS (
    SELECT
        home_branch.PARENT_SERIAL   AS PERSON_SERIAL,
        home_branch.BRANCH_SERIAL   AS HOME_BRANCH_SERIAL
    FROM
        CORE.CU_HOME_BRANCH AS home_branch
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM CORE.CU_HOME_BRANCH AS home_branch_inner
            WHERE
                home_branch_inner.PARENT_SERIAL = home_branch.PARENT_SERIAL AND
                home_branch_inner.SERIAL        > home_branch.SERIAL
        )
),

PersonAccountDetails(personSerial, account_eligibility, account_type) AS (
    SELECT
        PERSON.SERIAL                        AS PERSON_SERIAL,
        MAX(ACCOUNT_ELIGIBILITY.DESCRIPTION) AS ACCOUNT_ELIGIBILITY,
        MAX(ACCOUNT_TYPE.DESCRIPTION)        AS ACCOUNT_TYPE
    FROM
        CORE.PERSON               AS PERSON JOIN
        CORE.ACCOUNT              AS ACCOUNT              ON ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL JOIN
        CORE.ACCOUNT_ELIGIBILITY  AS ACCOUNT_ELIGIBILITY  ON ACCOUNT.ELIGIBILITY_SERIAL    = ACCOUNT_ELIGIBILITY.SERIAL INNER JOIN
        CORE.ACCOUNT_TYPE         AS ACCOUNT_TYPE         ON ACCOUNT.TYPE_SERIAL            = ACCOUNT_TYPE.SERIAL
    GROUP BY PERSON.SERIAL
),

relationshipStatus AS (
    SELECT
        PERSON.SERIAL   AS PERSON_SERIAL,
        MAX('Platinum') AS STATUS
    FROM
        CORE.PERSON                AS PERSON
        JOIN CORE.ACCOUNT          AS ACCOUNT          ON PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL
                                                      AND ACCOUNT.CLOSE_DATE          IS NULL
                                                      AND ACCOUNT.DORMANCY_RESTRICTION != 'Dormant'
        LEFT JOIN CORE.SHARE       AS SHARE            ON ACCOUNT.SERIAL = SHARE.PARENT_SERIAL
        LEFT JOIN CORE.SH_TYPE     AS SH_TYPE          ON SHARE.TYPE_SERIAL = SH_TYPE.SERIAL
        LEFT JOIN CORE.ACCOUNT_TYPE AS ACCT_TYPE       ON ACCOUNT.TYPE_SERIAL = ACCT_TYPE.SERIAL
        LEFT JOIN CORE.ACCOUNT_RELATIONSHIP AS ACCT_RELATIONSHIP ON ACCOUNT.RELATIONSHIP_SERIAL = ACCT_RELATIONSHIP.SERIAL
    GROUP BY PERSON.SERIAL
    HAVING LISTAGG(ACCT_RELATIONSHIP.DESCRIPTION, ', ') LIKE '%Platinum%'
)

SELECT
    PERSON.SERIAL                             AS person_serial,
    PERSON.FIRST_NAME                         AS first_name,
    PERSON.MIDDLE_NAME                        AS middle_name,
    PERSON.LAST_NAME                          AS last_name,
    PERSON.BIRTH_DATE                         AS birth_date,
    (YEAR(ENV.POSTING_DATE) - YEAR(PERSON.BIRTH_DATE))
      - CASE
          WHEN MONTH(ENV.POSTING_DATE) < MONTH(PERSON.BIRTH_DATE)
               OR (MONTH(ENV.POSTING_DATE) = MONTH(PERSON.BIRTH_DATE)
                   AND DAY(ENV.POSTING_DATE) < DAY(PERSON.BIRTH_DATE))
          THEN 1
          ELSE 0
        END                                   AS age,
    PERSON.GENDER                             AS gender,
    relationshipStatus.STATUS                 AS status,
    mostRecentPersonScore.score               AS cred_score,
    mostRecentPersonScore.scoreDate           AS score_date,
    PersonOldestShare.branchSerial            AS branch_serial,
    HomeBranchAssignment.home_branch_serial   AS home_branch_serial,
    PersonAccountDetails.ACCOUNT_ELIGIBILITY  AS account_eligibility,
    PersonAccountDetails.ACCOUNT_TYPE         AS account_type
FROM
    MemberUniverse
    CROSS JOIN CORE.ENV AS ENV
    JOIN  CORE.PERSON AS PERSON ON PERSON.SERIAL = MemberUniverse.personSerial
    LEFT JOIN mostRecentPersonScore ON mostRecentPersonScore.personSerial    = PERSON.SERIAL
    LEFT JOIN relationshipStatus    ON relationshipStatus.PERSON_SERIAL      = PERSON.SERIAL
    LEFT JOIN PersonOldestShare     ON PersonOldestShare.personSerial        = PERSON.SERIAL
    LEFT JOIN HomeBranchAssignment  ON HomeBranchAssignment.personSerial     = PERSON.SERIAL
    LEFT JOIN PersonAccountDetails  ON PersonAccountDetails.personSerial     = PERSON.SERIAL
ORDER BY
    PERSON.SERIAL
