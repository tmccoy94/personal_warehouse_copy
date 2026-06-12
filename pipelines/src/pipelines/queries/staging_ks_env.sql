-- CORE.ENV — single-row system table holding the current business posting date.
-- ENV.POSTING_DATE is used in DB2 queries as "today" for date-window calculations
-- (e.g., direct deposit within last 30 days, deceased note expiration checks).
-- Mirroring it here ensures our PostgreSQL queries use the same reference date
-- as the source system instead of CURRENT_DATE.
SELECT
    ENV.POSTING_DATE AS posting_date
FROM
    CORE.ENV AS ENV
FETCH FIRST 1 ROW ONLY
