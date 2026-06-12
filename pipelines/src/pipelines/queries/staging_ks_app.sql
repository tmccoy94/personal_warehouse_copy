-- All loan applications from CORE.APP — embeds app type description.
SELECT
    APP.SERIAL                      AS app_serial,
    APP.APPLICATION_NUMBER          AS application_number,
    APP.APPLICATION_DATE            AS application_date,
    APP_TYPE.DESCRIPTION            AS app_type,
    APP.WITHDRAWN_INDICATOR         AS withdrawn_indicator,
    APP.PROCESSOR_USER_SERIAL       AS processor_user_serial
FROM
    CORE.APP      AS APP JOIN
    CORE.APP_TYPE AS APP_TYPE ON APP_TYPE.SERIAL = APP.TYPE_SERIAL
