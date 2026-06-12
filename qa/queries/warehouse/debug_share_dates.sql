-- Show the actual share dates for the 10 missing persons in staging
SELECT
    s.share_serial,
    s.person_serial,
    s.share_id,
    s.open_date,
    s.close_date,
    s.charge_off_date
FROM staging.ks_share AS s
WHERE s.person_serial IN (5945, 10782, 14928, 27496, 29851, 30969, 31413, 34622, 49142, 138648)
  AND s.share_id = '0001'
ORDER BY s.person_serial
