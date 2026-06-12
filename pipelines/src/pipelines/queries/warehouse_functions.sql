-- Warehouse utility functions.
-- Loaded by warehouse_create_functions Dagster asset after warehouse_create_tables.
-- Re-runnable: uses CREATE OR REPLACE.

CREATE OR REPLACE FUNCTION calc_age(birth_date DATE, as_of DATE DEFAULT CURRENT_DATE)
RETURNS SMALLINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT (
        EXTRACT(YEAR FROM as_of)::INT
        - EXTRACT(YEAR FROM birth_date)::INT
        - CASE
            WHEN (EXTRACT(MONTH FROM as_of), EXTRACT(DAY FROM as_of))
               < (EXTRACT(MONTH FROM birth_date), EXTRACT(DAY FROM birth_date))
            THEN 1
            ELSE 0
          END
    )::SMALLINT
$$;

CREATE OR REPLACE FUNCTION age_group(age SMALLINT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN age IS NULL THEN NULL
        WHEN age < 18    THEN '<18'
        WHEN age <= 24   THEN '18-24'
        WHEN age <= 34   THEN '25-34'
        WHEN age <= 44   THEN '35-44'
        WHEN age <= 54   THEN '45-54'
        WHEN age <= 64   THEN '55-64'
        WHEN age <= 74   THEN '65-74'
        ELSE '75+'
    END
$$;
