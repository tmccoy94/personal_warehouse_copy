WITH GetPrimaryContacts AS (
    SELECT
        PERSON_CONTACT.PARENT_SERIAL AS PERSON_SERIAL,
        PERSON_CONTACT.VALUE,
        CASE
            WHEN PERSON_CONTACT.CATEGORY = 'HP' THEN 'Home phone'
            WHEN PERSON_CONTACT.CATEGORY = 'BP' THEN 'Business phone'
            WHEN PERSON_CONTACT.CATEGORY = 'HF' THEN 'Home fax'
            WHEN PERSON_CONTACT.CATEGORY = 'BF' THEN 'Business fax'
            WHEN PERSON_CONTACT.CATEGORY = 'PC' THEN 'Personal cell'
            WHEN PERSON_CONTACT.CATEGORY = 'BC' THEN 'Business cell'
            WHEN PERSON_CONTACT.CATEGORY = 'PP' THEN 'Personal pager'
            WHEN PERSON_CONTACT.CATEGORY = 'Bp' THEN 'Business pager'
            WHEN PERSON_CONTACT.CATEGORY = 'PE' THEN 'Personal email'
            WHEN PERSON_CONTACT.CATEGORY = 'BE' THEN 'Business email'
            WHEN PERSON_CONTACT.CATEGORY = 'PW' THEN 'Personal web page'
            WHEN PERSON_CONTACT.CATEGORY = 'BW' THEN 'Business web page'
            WHEN PERSON_CONTACT.CATEGORY = 'CU' THEN 'Custom'
        END AS CATEGORY,
        PERSON_CONTACT.ORDINAL,
        ROW_NUMBER() over (PARTITION BY PERSON_CONTACT.PARENT_SERIAL, PERSON_CONTACT.CATEGORY ORDER BY ORDINAL) AS RN,
        CASE WHEN EXPIRATION_DATE IS NULL THEN BAD_CONTACT ELSE 'Y' END AS BAD_CONTACT,
        PERSON_CONTACT.LAST_VERIFICATION_DATE,
        PERSON_CONTACT.EXPIRATION_DATE
    FROM
        CORE.PERSON_CONTACT AS PERSON_CONTACT
)
SELECT
    PERSON_SERIAL              AS source_identifier,
    CATEGORY                   AS type,
    VALUE                      AS value,
    LAST_VERIFICATION_DATE     AS last_verification_date,
    BAD_CONTACT                AS bad_contact,
    CASE WHEN RN = 1 THEN TRUE ELSE FALSE END AS is_primary,
    EXPIRATION_DATE            AS expiration_date
FROM
    GetPrimaryContacts
ORDER BY
    PERSON_SERIAL, TYPE