-- Account notes — used to identify deceased members.
-- Joins AC_NOTE → NOTE_TYPE for the description and → ACCOUNT for person linkage.
-- Only pulls notes on open accounts so the deceased check matches the original report logic.
-- CORE.ENV.POSTING_DATE is used in the source report to check note expiration;
-- we store expiration_date and compare against CURRENT_DATE at query time in PostgreSQL.
SELECT
    AC_NOTE.SERIAL                      AS ac_note_serial,
    ACCOUNT.PRIMARY_PERSON_SERIAL       AS person_serial,
    NOTE_TYPE.DESCRIPTION               AS note_type,
    AC_NOTE.EXPIRATION_DATE             AS expiration_date
FROM
    CORE.AC_NOTE AS AC_NOTE
JOIN CORE.NOTE_TYPE AS NOTE_TYPE ON NOTE_TYPE.SERIAL = AC_NOTE.TYPE_SERIAL
JOIN CORE.ACCOUNT   AS ACCOUNT   ON ACCOUNT.SERIAL   = AC_NOTE.PARENT_SERIAL
WHERE
    ACCOUNT.CLOSE_DATE IS NULL
