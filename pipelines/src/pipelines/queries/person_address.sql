-- Parameters: :posting_date (DATE, optional)
--   Normal runs: NULL → falls back to CORE.ENV.POSTING_DATE
--   Recovery runs: pass the bad partition date to recover addresses as-of that date
WITH PersonAddressLinkInfo AS (SELECT
    PAL.PARENT_SERIAL                                          AS PERSON_SERIAL,
    PAL.ADDRESS_SERIAL                                         AS ADDRESS_SERIAL,
    COALESCE(PAL.ORDINAL, 0)                                   AS PAL_ORDINAL,
    MIN(COALESCE(PAL.ORDINAL, 0)) OVER (PARTITION BY PAL.PARENT_SERIAL) AS MIN_PAL_ORDINAL,
    COALESCE(DATE :posting_date, ENV.POSTING_DATE)             AS POSTING_DATE,
    PAL.EFFECTIVE_DATE,
    CASE WHEN PAL.EXPIRATION_DATE IS NOT NULL THEN TRUE ELSE FALSE END AS IS_EXPIRED,
    CASE
        WHEN PAL.CATEGORY = 'R' THEN 'Residence'
        WHEN PAL.CATEGORY = 'B' THEN 'Business'
        WHEN PAL.CATEGORY = 'M' THEN 'Mailing'
        WHEN PAL.CATEGORY = 'V' THEN 'Previous'
    END AS CATEGORY,
    PAL.EXPIRATION_DATE
FROM
    CORE.PERSON_ADDRESS_LINK AS PAL CROSS JOIN CORE.ENV AS ENV
WHERE
    (PAL.EFFECTIVE_DATE IS NULL OR PAL.EFFECTIVE_DATE <= COALESCE(DATE :posting_date, ENV.POSTING_DATE)))
SELECT
    PersonAddressLinkInfo.PERSON_SERIAL AS source_identifier,
    CASE WHEN PersonAddressLinkInfo.PAL_ORDINAL = PersonAddressLinkInfo.MIN_PAL_ORDINAL THEN TRUE ELSE FALSE END AS is_primary,
    PersonAddressLinkInfo.IS_EXPIRED AS is_expired,
    PersonAddressLinkInfo.EXPIRATION_DATE AS expiration_date,
    PersonAddressLinkInfo.EFFECTIVE_DATE AS address_effective_date,
    PersonAddressLinkInfo.CATEGORY AS category,
    ADDRESS.STREET AS street,
    ADDRESS.ADDITIONAL_ADDRESS_LINE AS ad2,
    ADDRESS.CITY AS city,
    ADDRESS.STATE AS state,
    LEFT(ADDRESS.POSTAL_CODE, 5) AS zip
FROM
    PersonAddressLinkInfo
    JOIN CORE.ADDRESS AS ADDRESS ON ADDRESS.SERIAL = PersonAddressLinkInfo.ADDRESS_SERIAL