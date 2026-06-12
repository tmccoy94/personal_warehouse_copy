-- Latest credit score per person.
-- Uses NOT EXISTS to keep only the most recent score record,
-- matching the mostRecentPersonScore CTE pattern in person_snapshot_daily.sql.
SELECT
    PERSON_SCORE.PARENT_SERIAL  AS person_serial,
    PERSON_SCORE.SCORE          AS credit_score,
    PERSON_SCORE.SCORE_DATE     AS score_date
FROM
    CORE.PERSON_SCORE AS PERSON_SCORE
WHERE NOT EXISTS (
    SELECT 1
    FROM CORE.PERSON_SCORE AS PERSON_SCORE_INNER
    WHERE
        PERSON_SCORE_INNER.PARENT_SERIAL = PERSON_SCORE.PARENT_SERIAL AND
        PERSON_SCORE_INNER.SERIAL        > PERSON_SCORE.SERIAL
)
