SELECT
    PERSON.SERIAL            AS person_serial,
    PERSON.FIRST_NAME        AS first_name,
    PERSON.MIDDLE_NAME       AS middle_name,
    PERSON.LAST_NAME         AS last_name,
    PERSON.BIRTH_DATE        AS birth_date,
    PERSON.GENDER            AS gender,
    PERSON_TYPE.DESCRIPTION  AS person_type
FROM
    CORE.PERSON AS PERSON
LEFT JOIN CORE.PERSON_TYPE AS PERSON_TYPE ON PERSON_TYPE.SERIAL = PERSON.TYPE_SERIAL
