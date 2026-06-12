-- Applicants on loan applications.
-- APPLICANT.PARENT_SERIAL = APP.SERIAL.
-- Lowest ordinal = primary applicant; second = co-applicant.
SELECT
    APPLICANT.SERIAL                AS applicant_serial,
    APPLICANT.PARENT_SERIAL         AS app_serial,
    APPLICANT.SOURCE_PERSON_SERIAL  AS person_serial,
    APPLICANT.ORDINAL               AS ordinal
FROM
    CORE.APPLICANT AS APPLICANT
