-- Latest home branch assignment per person.
-- Uses NOT EXISTS to keep only the most recent record per person,
-- matching the HomeBranchAssignment CTE pattern in person_snapshot_daily.sql.
SELECT
    HOME_BRANCH.PARENT_SERIAL   AS person_serial,
    HOME_BRANCH.BRANCH_SERIAL   AS branch_serial
FROM
    CORE.CU_HOME_BRANCH AS HOME_BRANCH
WHERE NOT EXISTS (
    SELECT 1
    FROM CORE.CU_HOME_BRANCH AS HOME_BRANCH_INNER
    WHERE
        HOME_BRANCH_INNER.PARENT_SERIAL = HOME_BRANCH.PARENT_SERIAL AND
        HOME_BRANCH_INNER.SERIAL        > HOME_BRANCH.SERIAL
)
