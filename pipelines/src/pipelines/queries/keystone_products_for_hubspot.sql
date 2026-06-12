-- Keystone "products for HubSpot pull": one row per person who holds at least one
-- product, with current + all past emails, birth date, address, phone, product list,
-- and a YES/NO member flag. Feeds the hubspot_live_import_csv asset (joined against the
-- HubSpot CRM export on email + name). Verbatim DB2 source query — uses CURRENT_DATE /
-- ENV.POSTING_DATE for "now", no hardcoded dates, no bind params.
WITH CurrentEmail(personSerial, email) AS (
    SELECT
        PERSON_CONTACT.PARENT_SERIAL AS PERSON_SERIAL,
        PERSON_CONTACT.VALUE
    FROM
        CORE.PERSON_CONTACT AS PERSON_CONTACT CROSS JOIN
        CORE.ENV AS ENV
    WHERE
        PERSON_CONTACT.CATEGORY IN ('PE','BE') AND
        PERSON_CONTACT.BAD_CONTACT = 'N' AND
        (PERSON_CONTACT.EXPIRATION_DATE IS NULL OR PERSON_CONTACT.EXPIRATION_DATE >= ENV.POSTING_DATE) AND
        NOT EXISTS (SELECT 1
                    FROM
                        CORE.PERSON_CONTACT AS PERSON_CONTACT_INNER
                    WHERE
                        PERSON_CONTACT_INNER.PARENT_SERIAL = PERSON_CONTACT.PARENT_SERIAL AND
                        PERSON_CONTACT_INNER.ORDINAL < PERSON_CONTACT.ORDINAL AND
                        PERSON_CONTACT_INNER.CATEGORY IN ('PE','BE') AND
                        PERSON_CONTACT_INNER.BAD_CONTACT = 'N')
),
     AllPastEmails(personSerial, allPastEmails) AS (
         SELECT
             PERSON_CONTACT.PARENT_SERIAL AS PERSON_SERIAL,
             LISTAGG(PERSON_CONTACT.VALUE, ', ') AS ALL_PAST_EMAILS
         FROM
             CORE.PERSON_CONTACT AS PERSON_CONTACT CROSS JOIN
             CORE.ENV AS ENV
         WHERE
             PERSON_CONTACT.CATEGORY IN ('PE','BE')
         GROUP BY PERSON_CONTACT.PARENT_SERIAL
     ),
     PersonOldestShare(personSerial,branchDescription) as(

         SELECT
             PERSON.SERIAL AS PERSON_SERIAL,
             BRANCH.DESCRIPTION AS BRANCH_DESCIRPTION
         FROM
             CORE.ACCOUNT AS ACCOUNT INNER JOIN
             CORE.PERSON AS PERSON ON
                 ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL INNER JOIN
             CORE.SHARE AS SHARE ON
                 ACCOUNT.SERIAL = SHARE.PARENT_SERIAL LEFT OUTER JOIN
             CORE.BRANCH AS BRANCH ON
                 SHARE.BRANCH_SERIAL = BRANCH.SERIAL
         WHERE
             NOT EXISTS (SELECT 1
                         FROM
                             CORE.ACCOUNT AS ACCOUNT_INNER INNER JOIN
                             CORE.SHARE AS SHARE_INNER ON
                                 ACCOUNT_INNER.SERIAL = SHARE_INNER.PARENT_SERIAL INNER JOIN
                             CORE.PERSON AS PERSON_INNER ON
                                 ACCOUNT_INNER.PRIMARY_PERSON_SERIAL = PERSON_INNER.SERIAL
                         WHERE
                             PERSON.SERIAL = PERSON_INNER.SERIAL AND
                             SHARE_INNER.SERIAL < SHARE.SERIAL)
     ),
     ProductCounts(personSerial, count) AS (
         SELECT
             PERSON_SERIAL,
             COUNT(PRODUCT) AS PROD_COUNT
         FROM(
                 SELECT
                     PERSON.SERIAL AS PERSON_SERIAL,
                     SH_TYPE.DESCRIPTION AS PRODUCT
                 FROM
                     CORE.ENV AS ENV CROSS JOIN
                     CORE.ACCOUNT AS ACCOUNT INNER JOIN
                     CORE.SHARE AS SHARE ON
                         ACCOUNT.SERIAL = SHARE.PARENT_SERIAL  INNER JOIN
                     CORE.SH_TYPE AS SH_TYPE ON
                         SHARE.TYPE_SERIAL = SH_TYPE.SERIAL INNER JOIN
                     CORE.PERSON AS PERSON ON
                         ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL
                 WHERE
                     SHARE.CLOSE_DATE IS NULL AND
                     SHARE.CHARGE_OFF_DATE IS NULL
                 UNION ALL

                 SELECT
                     PERSON.SERIAL AS PERSON_SERIAL,
                     LN_TYPE.DESCRIPTION AS PRODUCT
                 FROM
                     CORE.ENV AS ENV CROSS JOIN
                     CORE.ACCOUNT AS ACCOUNT INNER JOIN
                     CORE. LOAN AS LOAN ON
                         ACCOUNT.SERIAL = LOAN.PARENT_SERIAL INNER JOIN
                     CORE.LN_TYPE AS LN_TYPE ON
                         LOAN.TYPE_SERIAL = LN_TYPE.SERIAL INNER JOIN
                     CORE.PERSON AS PERSON ON
                         ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL
                 WHERE
                     LOAN.CLOSE_DATE IS NULL AND
                     LOAN.CHARGE_OFF_DATE IS NULL

                 UNION ALL

                 SELECT DISTINCT
                     PERSON.SERIAL AS PERSON_SERIAL,
                     CASE
                         WHEN LOGIN_CHANNEL.DESCRIPTION = 'Access_Softek' THEN 'Online Banking'
                         ELSE NULL
                         END AS PRODUCT
                 FROM
                     CORE.ACCOUNT AS ACCOUNT INNER JOIN
                     CORE.PERSON AS PERSON ON
                         ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL LEFT OUTER JOIN
                     CORE.LOGIN AS LOGIN ON
                         PERSON.SERIAL = LOGIN.PERSON_SERIAL LEFT OUTER JOIN
                     CORE.LOGIN_CHANNEL AS LOGIN_CHANNEL ON
                         LOGIN.CHANNEL_SERIAL = LOGIN_CHANNEL.SERIAL
                 WHERE
                     LOGIN_CHANNEL.DESCRIPTION = 'Access_Softek'

                 UNION ALL

                 SELECT
                     PERSON.SERIAL AS PERSON_SERIAL,
                     CASE
                         WHEN (MONETARY.SOURCE = 'B' ) AND (TRANSACTION.POSTING_DATE > ENV.POSTING_DATE - 30 DAYS) THEN 'Bill Payment'
                         END AS PRODUCT
                 FROM
                     CORE.ENV AS ENV CROSS JOIN
                     CORE.ACCOUNT AS ACCOUNT INNER JOIN
                     CORE.PERSON AS PERSON ON
                         ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL INNER JOIN
                     CORE.TRANSACTION AS TRANSACTION ON
                         PERSON.SERIAL = TRANSACTION.PERSON_SERIAL LEFT OUTER JOIN
                     CORE.MONETARY AS MONETARY ON
                         TRANSACTION.SERIAL = MONETARY.PARENT_SERIAL LEFT OUTER JOIN
                     CurrentEmail ON
                         PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                     PersonOldestShare ON
                         PERSON.SERIAL = PersonOldestShare.personSerial
                 WHERE
                     MONETARY.SOURCE = 'B' AND
                     TRANSACTION.POSTING_DATE > ENV.POSTING_DATE - 30 DAYS

                 UNION ALL

                 SELECT
                     PERSON.SERIAL AS PERSON_SERIAL,
                     CASE
                         WHEN (SHARE.ACH_DEPOSIT_LAST_AMOUNT >= 500.00) AND (SHARE.ACH_DEPOSIT_LAST_DATE > ENV.POSTING_DATE - 30) THEN 'Direct Deposit'
                         END AS PRODUCT
                 FROM
                     CORE.ENV AS ENV CROSS JOIN
                     CORE.ACCOUNT AS ACCOUNT INNER JOIN
                     CORE.PERSON AS PERSON ON
                         ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL INNER JOIN
                     CORE.SHARE AS SHARE ON
                         ACCOUNT.SERIAL = SHARE.PARENT_SERIAL LEFT OUTER JOIN
                     CORE.BRANCH AS BRANCH ON
                         SHARE.BRANCH_SERIAL = BRANCH.SERIAL LEFT OUTER JOIN
                     CurrentEmail ON
                         PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                     PersonOldestShare ON
                         PERSON.SERIAL = PersonOldestShare.personSerial
                 WHERE
                     SHARE.CLOSE_DATE IS NULL AND
                     SHARE.CHARGE_OFF_DATE IS NULL AND
                     SHARE.ACH_DEPOSIT_LAST_AMOUNT >= 500.00 AND
                     SHARE.ACH_DEPOSIT_LAST_DATE > ENV.POSTING_DATE - 30 DAYS
             )
         GROUP BY
             PERSON_SERIAL
     ),
     PERSON_SERVICE AS (SELECT
                            PERSON_SERIAL,
                            MAX(NAME) AS NAME,
                            MAX(NCINO_NAME) AS NCINO_NAME,
                            MAX(AGE) AS AGE,
                            MAX(EMAIL_ADDRESS) AS EMAIL,
                            COUNT(PRODUCT) AS PRODUCT_COUNT,
                            LISTAGG(PRODUCT, ', ') AS PRODUCTS
                        FROM (
                                 SELECT
                                     PERSON_SERIAL,
                                     NAME,
                                     NCINO_NAME,
                                     SSN,
                                     ACCOUNT_ELIGIBILITY,
                                     EMAIL_ADDRESS,
                                     AGE,
                                     DATE_OF_BIRTH,
                                     BRANCH,
                                     PRODUCT
                                 FROM(
                                         SELECT
                                             PERSON.SERIAL AS PERSON_SERIAL,
                                             PERSON.LAST_NAME||COALESCE ((',')|| PERSON.FIRST_NAME,'') AS NAME,
                                             COALESCE(PERSON.FIRST_NAME,'') || ' ' ||  COALESCE(PERSON.MIDDLE_NAME, '') || ' ' || PERSON.LAST_NAME AS NCINO_NAME,
                                             CASE
                                                 WHEN PERSON.TIN IS NULL THEN TAX_PERSON.TIN
                                                 WHEN TRIM(PERSON.TIN) = '' THEN TAX_PERSON.TIN
                                                 ELSE PERSON.TIN
                                                 END AS SSN,
                                             ACCOUNT_ELIGIBILITY.DESCRIPTION AS ACCOUNT_ELIGIBILITY,
                                             CurrentEmail.email AS EMAIL_ADDRESS,
                                             YEAR(ENV.POSTING_DATE - PERSON.BIRTH_DATE) AS AGE,
                                             PERSON.BIRTH_DATE AS DATE_OF_BIRTH,
                                             PersonOldestShare.branchDescription AS BRANCH,
                                             SH_TYPE.DESCRIPTION AS PRODUCT
                                         FROM
                                             CORE.ENV AS ENV CROSS JOIN
                                             CORE.ACCOUNT AS ACCOUNT INNER JOIN
                                             CORE.ACCOUNT_ELIGIBILITY AS ACCOUNT_ELIGIBILITY ON
                                                 ACCOUNT.ELIGIBILITY_SERIAL = ACCOUNT_ELIGIBILITY.SERIAL INNER JOIN
                                             CORE.SHARE AS SHARE ON
                                                 ACCOUNT.SERIAL = SHARE.PARENT_SERIAL INNER JOIN
                                             CORE.SH_TYPE AS SH_TYPE ON
                                                 SHARE.TYPE_SERIAL = SH_TYPE.SERIAL INNER JOIN
                                             CORE.PERSON AS PERSON ON
                                                 ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.PERSON AS TAX_PERSON ON
                                                 PERSON.TAX_PERSON_SERIAL = TAX_PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.BRANCH AS BRANCH ON
                                                 SHARE.BRANCH_SERIAL = BRANCH.SERIAL LEFT OUTER JOIN
                                             CurrentEmail ON
                                                 PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                                             PersonOldestShare ON
                                                 PERSON.SERIAL = PersonOldestShare.personSerial
                                         WHERE
                                             SHARE.CLOSE_DATE IS NULL AND
                                             SHARE.CHARGE_OFF_DATE IS NULL

                                         UNION ALL

                                         SELECT
                                             PERSON.SERIAL AS PERSON_SERIAL,
                                             PERSON.LAST_NAME||COALESCE ((',')|| PERSON.FIRST_NAME,'') AS NAME,
                                             COALESCE(PERSON.FIRST_NAME,'') || ' ' ||  COALESCE(PERSON.MIDDLE_NAME, '') || ' ' || PERSON.LAST_NAME AS NCINO_NAME,
                                             CASE
                                                 WHEN PERSON.TIN IS NULL THEN TAX_PERSON.TIN
                                                 WHEN TRIM(PERSON.TIN) = '' THEN TAX_PERSON.TIN
                                                 ELSE PERSON.TIN
                                                 END AS SSN,
                                             ACCOUNT_ELIGIBILITY.DESCRIPTION AS ACCOUNT_ELIGIBILITY,
                                             CurrentEmail.email AS EMAIL_ADDRESS,
                                             YEAR(ENV.POSTING_DATE - PERSON.BIRTH_DATE) AS AGE,
                                             PERSON.BIRTH_DATE AS DATE_OF_BIRTH,
                                             PersonOldestShare.branchDescription AS BRANCH,
                                             LN_TYPE.DESCRIPTION AS PRODUCT
                                         FROM
                                             CORE.ENV AS ENV CROSS JOIN
                                             CORE.ACCOUNT AS ACCOUNT INNER JOIN
                                             CORE.ACCOUNT_ELIGIBILITY AS ACCOUNT_ELIGIBILITY ON
                                                 ACCOUNT.ELIGIBILITY_SERIAL = ACCOUNT_ELIGIBILITY.SERIAL INNER JOIN
                                             CORE. LOAN AS LOAN ON
                                                 ACCOUNT.SERIAL = LOAN.PARENT_SERIAL INNER JOIN
                                             CORE.LN_TYPE AS LN_TYPE ON
                                                 LOAN.TYPE_SERIAL = LN_TYPE.SERIAL INNER JOIN
                                             CORE.PERSON AS PERSON ON
                                                 ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.PERSON AS TAX_PERSON ON
                                                 PERSON.TAX_PERSON_SERIAL = TAX_PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.BRANCH AS BRANCH ON
                                                 LOAN.BRANCH_SERIAL = BRANCH.SERIAL LEFT OUTER JOIN
                                             CurrentEmail ON
                                                 PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                                             PersonOldestShare ON
                                                 PERSON.SERIAL = PersonOldestShare.personSerial
                                         WHERE
                                             LOAN.CLOSE_DATE IS NULL AND
                                             LOAN.CHARGE_OFF_DATE IS NULL

                                         UNION ALL

                                         SELECT DISTINCT
                                             PERSON.SERIAL AS PERSON_SERIAL,
                                             PERSON.LAST_NAME||COALESCE ((',')|| PERSON.FIRST_NAME,'') AS NAME,
                                             COALESCE(PERSON.FIRST_NAME,'') || ' ' ||  COALESCE(PERSON.MIDDLE_NAME, '') || ' ' || PERSON.LAST_NAME AS NCINO_NAME,
                                             CASE
                                                 WHEN PERSON.TIN IS NULL THEN TAX_PERSON.TIN
                                                 WHEN TRIM(PERSON.TIN) = '' THEN TAX_PERSON.TIN
                                                 ELSE PERSON.TIN
                                                 END AS SSN,
                                             ACCOUNT_ELIGIBILITY.DESCRIPTION AS ACCOUNT_ELIGIBILITY,
                                             CurrentEmail.email AS EMAIL_ADDRESS,
                                             YEAR(ENV.POSTING_DATE - PERSON.BIRTH_DATE) AS AGE,
                                             PERSON.BIRTH_DATE AS DATE_OF_BIRTH,
                                             PersonOldestShare.branchDescription AS BRANCH,
                                             CASE
                                                 WHEN LOGIN_CHANNEL.DESCRIPTION = 'Access_Softek' THEN 'Online Banking'
                                                 ELSE NULL
                                                 END AS PRODUCT
                                         FROM
                                             CORE.ENV AS ENV CROSS JOIN
                                             CORE.ACCOUNT AS ACCOUNT INNER JOIN
                                             CORE.ACCOUNT_ELIGIBILITY AS ACCOUNT_ELIGIBILITY ON
                                                 ACCOUNT.ELIGIBILITY_SERIAL = ACCOUNT_ELIGIBILITY.SERIAL INNER JOIN
                                             CORE.PERSON AS PERSON ON
                                                 ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.PERSON AS TAX_PERSON ON
                                                 PERSON.TAX_PERSON_SERIAL = TAX_PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.LOGIN AS LOGIN ON
                                                 PERSON.SERIAL = LOGIN.PERSON_SERIAL LEFT OUTER JOIN
                                             CORE.LOGIN_CHANNEL AS LOGIN_CHANNEL ON
                                                 LOGIN.CHANNEL_SERIAL = LOGIN_CHANNEL.SERIAL
                                                             LEFT OUTER JOIN
                                             CurrentEmail ON
                                                 PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                                             PersonOldestShare ON
                                                 PERSON.SERIAL = PersonOldestShare.personSerial
                                         WHERE
                                             LOGIN_CHANNEL.DESCRIPTION = 'Access_Softek'

                                         UNION ALL

                                         SELECT
                                             PERSON.SERIAL AS PERSON_SERIAL,
                                             PERSON.LAST_NAME||COALESCE ((',')|| PERSON.FIRST_NAME,'') AS NAME,
                                             COALESCE(PERSON.FIRST_NAME,'') || ' ' ||  COALESCE(PERSON.MIDDLE_NAME, '') || ' ' || PERSON.LAST_NAME AS NCINO_NAME,
                                             CASE
                                                 WHEN PERSON.TIN IS NULL THEN TAX_PERSON.TIN
                                                 WHEN TRIM(PERSON.TIN) = '' THEN TAX_PERSON.TIN
                                                 ELSE PERSON.TIN
                                                 END AS SSN,
                                             ACCOUNT_ELIGIBILITY.DESCRIPTION AS ACCOUNT_ELIGIBILITY,
                                             CurrentEmail.email AS EMAIL_ADDRESS,
                                             YEAR(ENV.POSTING_DATE - PERSON.BIRTH_DATE) AS AGE,
                                             PERSON.BIRTH_DATE AS DATE_OF_BIRTH,
                                             PersonOldestShare.branchDescription AS BRANCH,
                                             CASE
                                                 WHEN (MONETARY.SOURCE = 'B' ) AND (TRANSACTION.POSTING_DATE > ENV.POSTING_DATE - 30 DAYS) THEN 'Bill Payment'
                                                 END AS PRODUCT
                                         FROM
                                             CORE.ENV AS ENV CROSS JOIN
                                             CORE.ACCOUNT AS ACCOUNT INNER JOIN
                                             CORE.ACCOUNT_ELIGIBILITY AS ACCOUNT_ELIGIBILITY ON
                                                 ACCOUNT.ELIGIBILITY_SERIAL = ACCOUNT_ELIGIBILITY.SERIAL INNER JOIN
                                             CORE.PERSON AS PERSON ON
                                                 ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.PERSON AS TAX_PERSON ON
                                                 PERSON.TAX_PERSON_SERIAL = TAX_PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.TRANSACTION AS TRANSACTION ON
                                                 PERSON.SERIAL = TRANSACTION.PERSON_SERIAL LEFT OUTER JOIN
                                             CORE.MONETARY AS MONETARY ON
                                                 TRANSACTION.SERIAL = MONETARY.PARENT_SERIAL LEFT OUTER JOIN
                                             CurrentEmail ON
                                                 PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                                             PersonOldestShare ON
                                                 PERSON.SERIAL = PersonOldestShare.personSerial
                                         WHERE
                                             MONETARY.SOURCE = 'B' AND
                                             TRANSACTION.POSTING_DATE > ENV.POSTING_DATE - 30 DAYS

                                         UNION ALL

                                         SELECT
                                             PERSON.SERIAL AS PERSON_SERIAL,
                                             PERSON.LAST_NAME||COALESCE ((',')|| PERSON.FIRST_NAME,'') AS NAME,
                                             COALESCE(PERSON.FIRST_NAME,'') || ' ' ||  COALESCE(PERSON.MIDDLE_NAME, '') || ' ' || PERSON.LAST_NAME AS NCINO_NAME,
                                             CASE
                                                 WHEN PERSON.TIN IS NULL THEN TAX_PERSON.TIN
                                                 WHEN TRIM(PERSON.TIN) = '' THEN TAX_PERSON.TIN
                                                 ELSE PERSON.TIN
                                                 END AS SSN,
                                             ACCOUNT_ELIGIBILITY.DESCRIPTION AS ACCOUNT_ELIGIBILITY,
                                             CurrentEmail.email AS EMAIL_ADDRESS,
                                             YEAR(ENV.POSTING_DATE - PERSON.BIRTH_DATE) AS AGE,
                                             PERSON.BIRTH_DATE AS DATE_OF_BIRTH,
                                             PersonOldestShare.branchDescription AS BRANCH,
                                             CASE
                                                 WHEN (SHARE.ACH_DEPOSIT_LAST_AMOUNT >= 500.00) AND (SHARE.ACH_DEPOSIT_LAST_DATE > ENV.POSTING_DATE - 30 DAYS) THEN 'Direct Deposit'
                                                 END AS PRODUCT
                                         FROM
                                             CORE.ENV AS ENV CROSS JOIN
                                             CORE.ACCOUNT AS ACCOUNT INNER JOIN
                                             CORE.ACCOUNT_ELIGIBILITY AS ACCOUNT_ELIGIBILITY ON
                                                 ACCOUNT.ELIGIBILITY_SERIAL = ACCOUNT_ELIGIBILITY.SERIAL INNER JOIN
                                             CORE.SHARE AS SHARE ON
                                                 ACCOUNT.SERIAL = SHARE.PARENT_SERIAL INNER JOIN
                                             CORE.SH_TYPE AS SH_TYPE ON
                                                 SHARE.TYPE_SERIAL = SH_TYPE.SERIAL INNER JOIN
                                             CORE.BRANCH AS BRANCH ON
                                                 SHARE.BRANCH_SERIAL = BRANCH.SERIAL LEFT OUTER JOIN
                                             CORE.PERSON AS PERSON ON
                                                 ACCOUNT.PRIMARY_PERSON_SERIAL = PERSON.SERIAL LEFT OUTER JOIN
                                             CORE.PERSON AS TAX_PERSON ON
                                                 PERSON.TAX_PERSON_SERIAL = TAX_PERSON.SERIAL LEFT OUTER JOIN
                                             CurrentEmail ON
                                                 PERSON.SERIAL = CurrentEmail.personSerial LEFT OUTER JOIN
                                             PersonOldestShare ON
                                                 PERSON.SERIAL = PersonOldestShare.personSerial
                                         WHERE
                                             SHARE.CLOSE_DATE IS NULL AND
                                             SHARE.CHARGE_OFF_DATE IS NULL AND
                                             SHARE.ACH_DEPOSIT_LAST_AMOUNT >= 500.00 AND
                                             SHARE.ACH_DEPOSIT_LAST_DATE > ENV.POSTING_DATE - 30 DAYS
                                     )
                             ) a
                                 INNER JOIN
                             ProductCounts ON
                                 a.PERSON_SERIAL = ProductCounts.personSerial
                        GROUP BY PERSON_SERIAL),
     CurrentAddress(personSerial, street, additionalAddressLine, city, state, postalCode, combinedAddress) AS (
         SELECT
             PERSON_SERIAL,
             COALESCE(ADDRESS.STREET, '') AS STREET,
             COALESCE(ADDRESS.ADDITIONAL_ADDRESS_LINE, '') AS ADDITIONAL_ADDRESS_LINE,
             COALESCE(ADDRESS.CITY, '') AS CITY,
             COALESCE(ADDRESS.STATE, '') AS STATE,
             COALESCE(ADDRESS.POSTAL_CODE, '') AS POSTAL_CODE,
             COALESCE(ADDRESS.STREET || ' ', '') || COALESCE(ADDRESS.ADDITIONAL_ADDRESS_LINE || ', ', '') ||
             COALESCE(ADDRESS.CITY || ' ', '') || COALESCE(ADDRESS.STATE, '') || COALESCE(', ' || ADDRESS.POSTAL_CODE, '') AS COMBINED_ADDRESS
         FROM
             (
                 SELECT
                     PAL.PARENT_SERIAL AS PERSON_SERIAL,
                     PAL.ADDRESS_SERIAL AS ADDRESS_SERIAL,
                     COALESCE(PAL.ORDINAL, 0) AS PAL_ORDINAL,
                     MIN(COALESCE(PAL.ORDINAL, 0)) OVER (PARTITION BY PAL.PARENT_SERIAL) AS MIN_PAL_ORDINAL
                 FROM
                     CORE.PERSON_ADDRESS_LINK AS PAL LEFT OUTER JOIN
                     CORE.ENV AS ENV ON
                         ENV.SERIAL > 0
                 WHERE
                     (PAL.EFFECTIVE_DATE IS NULL OR
                      PAL.EFFECTIVE_DATE <= ENV.POSTING_DATE) AND
                     (PAL.EXPIRATION_DATE IS NULL OR
                      PAL.EXPIRATION_DATE >= ENV.POSTING_DATE) AND
                     PAL.BAD_ADDRESS <> 'Y' AND
                     PAL.CATEGORY <> 'V'
             ) LEFT OUTER JOIN
             CORE.ADDRESS AS ADDRESS ON
                 ADDRESS_SERIAL = ADDRESS.SERIAL
         WHERE
             PAL_ORDINAL = MIN_PAL_ORDINAL
     ),
     CurrentPhone(personSerial, phone) AS (
         SELECT
             PERSON_CONTACT.PARENT_SERIAL AS PERSON_SERIAL,
             PERSON_CONTACT.VALUE
         FROM
             CORE.PERSON_CONTACT AS PERSON_CONTACT
         WHERE
             PERSON_CONTACT.CATEGORY IN ('HP','BP','PC','BC') AND
             PERSON_CONTACT.BAD_CONTACT = 'N' AND
             NOT EXISTS (SELECT 1 FROM CORE.PERSON_CONTACT AS PERSON_CONTACT_INNER
                         WHERE
                             PERSON_CONTACT_INNER.PARENT_SERIAL = PERSON_CONTACT.PARENT_SERIAL AND
                             PERSON_CONTACT_INNER.ORDINAL < PERSON_CONTACT.ORDINAL AND
                             PERSON_CONTACT_INNER.CATEGORY IN ('HP','BP','PC','BC') AND
                             PERSON_CONTACT_INNER.BAD_CONTACT = 'N')
     )
SELECT
    PERSON.SERIAL AS PERSON_SERIAL,
    PERSON.FIRST_NAME AS FIRST_NAME,
    PERSON.MIDDLE_NAME AS MIDDLE_NAME,
    PERSON.LAST_NAME AS LAST_NAME,
    PERSON.SUFFIX AS SUFFIX,
    CurrentEmail.email AS CURRENT_EMAIL,
    AllPastEmails.allPastEmails AS ALL_PAST_EMAILS,
    PERSON.BIRTH_DATE AS BIRTH_DATE,
    CurrentAddress.street AS STREET,
    COALESCE(CurrentAddress.additionalAddressLine, NULL) AS AD2,
    CurrentAddress.city AS CITY,
    CurrentAddress.state AS STATE,
    LEFT(CurrentAddress.postalCode, 5) AS ZIP,
    CurrentPhone.phone AS PHONE,
    PERSON_SERVICE.PRODUCT_COUNT AS PRODUCT_COUNT,
    PERSON_SERVICE.PRODUCTS AS PRODUCTS,
    CASE
        WHEN PERSON_SERVICE.PRODUCTS LIKE '%General Share%' THEN 'YES'
        ELSE 'NO'
        END AS MEMBER
FROM
    PERSON_SERVICE
    JOIN CORE.PERSON AS PERSON ON PERSON_SERVICE.PERSON_SERIAL = PERSON.SERIAL
    LEFT JOIN CurrentAddress ON CurrentAddress.personSerial = PERSON_SERVICE.PERSON_SERIAL
    LEFT JOIN CurrentPhone ON CurrentPhone.personSerial = PERSON.SERIAL
    LEFT JOIN CurrentEmail ON CurrentEmail.personSerial = PERSON.SERIAL
    LEFT JOIN AllPastEmails ON AllPastEmails.personSerial = PERSON.SERIAL
