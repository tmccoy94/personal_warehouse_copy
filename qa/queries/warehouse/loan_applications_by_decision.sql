-- Warehouse: New Loan App count by decision category (latest decision per loan request)
-- Matches DB2 loan_applications_by_decision.sql logic against staging tables.
SELECT
    COALESCE(decision.decision_category, 'No Decision') AS decision_category,
    COUNT(DISTINCT app.application_number) AS application_count
FROM staging.ks_app               AS app
LEFT JOIN staging.ks_loan_request AS loan_request ON loan_request.app_serial = app.app_serial
LEFT JOIN staging.ks_decision AS decision ON decision.loan_request_serial = loan_request.loan_request_serial
    AND NOT EXISTS (
        SELECT 1 FROM staging.ks_decision AS later_dec
        WHERE later_dec.loan_request_serial = decision.loan_request_serial
          AND later_dec.decision_date       >= decision.decision_date
          AND later_dec.decision_serial      > decision.decision_serial
    )
WHERE app.app_type LIKE '%New Loan App%'
  AND app.application_date > '2025-04-01'
GROUP BY decision.decision_category
ORDER BY application_count DESC
