WITH UserRoles (UserName, UserSerial, TerminationDate, RoleSerial, RoleDescription, BaseRoleSerial, Level) AS (
    SELECT
        USER.USERNAME,
        USER.SERIAL as USER_SERIAL,
        USER.TERMINATION_DATE,
        ROLE.SERIAL,
        ROLE.DESCRIPTION,
        ROLE.BASE_ROLE_SERIAL,
        0
    FROM
        CORE.USER AS USER INNER JOIN
        CORE.ROLE AS ROLE ON
            USER.ROLE_SERIAL = ROLE.SERIAL

    UNION ALL

    SELECT
        UR.UserName,
        UR.UserSerial,
        UR.TerminationDate,
        ROLE.SERIAL,
        ROLE.DESCRIPTION,
        ROLE.BASE_ROLE_SERIAL,
        UR.Level + 1
    FROM
        CORE.ROLE AS ROLE,
        UserRoles AS UR
    WHERE
        UR.BaseRoleSerial = ROLE.SERIAL AND
        UR.Level < 1000
)
SELECT
    UserRoles.UserSerial as USER_SERIAL,
    MAX(UserRoles.UserName) AS USERNAME,
    LISTAGG(UserRoles.RoleDescription || COALESCE(CASE WHEN UserRoles.Level = 0 THEN ' (Primary)' END, ''), ', ') AS ROLE_DESCRIPTIONS,
    MAX(UserRoles.TerminationDate) as TERMINATION_DATE
FROM
    UserRoles
GROUP BY UserRoles.UserSerial
ORDER BY
    USERNAME;
