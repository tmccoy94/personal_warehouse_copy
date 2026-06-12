-- Online banking login records with channel description.
-- Used to determine whether a person has an active online banking enrollment.
SELECT
    LOGIN.SERIAL                AS login_serial,
    LOGIN.PERSON_SERIAL         AS person_serial,
    LOGIN_CHANNEL.DESCRIPTION   AS channel
FROM
    CORE.LOGIN         AS LOGIN LEFT JOIN
    CORE.LOGIN_CHANNEL AS LOGIN_CHANNEL ON LOGIN_CHANNEL.SERIAL = LOGIN.CHANNEL_SERIAL
