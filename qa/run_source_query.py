import ibm_db, os, sys
import psycopg2

sys.path.insert(0, "/opt/dagster/app/qa/src")
from qa.connections import LIVE

db_name = os.environ.get("KEYSTONE_DB_OVERRIDE") or LIVE
host = os.environ["SOURCE_DB_HOST"]
port = os.environ.get("SOURCE_DB_PORT", "50000")
user = os.environ["SOURCE_DB_USER"]
pwd  = os.environ["SOURCE_DB_PASSWORD"]

conn_str = f"DATABASE={db_name};HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={pwd};"
db2_conn = ibm_db.connect(conn_str, "", "")

pg_conn = psycopg2.connect(
    host=os.environ["DAGSTER_POSTGRES_HOST"],
    port=int(os.environ.get("DAGSTER_POSTGRES_PORT", "5432")),
    dbname=os.environ["DAGSTER_POSTGRES_DB"],
    user=os.environ["DAGSTER_POSTGRES_USER"],
    password=os.environ["DAGSTER_POSTGRES_PASSWORD"],
)
pg_cur = pg_conn.cursor()

# 1. How many decisions have a null decision_date?
stmt = ibm_db.exec_immediate(db2_conn, """
SELECT COUNT(*) AS cnt
FROM CORE.APP AS APP
    JOIN CORE.APP_TYPE AS APP_TYPE ON APP.TYPE_SERIAL = APP_TYPE.SERIAL
    JOIN CORE.LOAN_REQUEST AS LOAN_REQUEST ON LOAN_REQUEST.PARENT_SERIAL = APP.SERIAL
    JOIN CORE.DECISION AS DECISION ON LOAN_REQUEST.SERIAL = DECISION.PARENT_SERIAL
WHERE APP_TYPE.DESCRIPTION LIKE '%New Loan App%'
  AND APP.APPLICATION_DATE > '2025-04-01'
  AND DECISION.DECISION_DATE IS NULL
""")
row = ibm_db.fetch_assoc(stmt)
print(f"DB2 decisions with NULL decision_date: {row['CNT']}")

pg_cur.execute("""
SELECT COUNT(*) FROM staging.ks_decision AS d
JOIN staging.ks_loan_request AS lr ON lr.loan_request_serial = d.loan_request_serial
JOIN staging.ks_app AS app ON app.app_serial = lr.app_serial
WHERE app.app_type LIKE '%New Loan App%'
  AND app.application_date > '2025-04-01'
  AND d.decision_date IS NULL
""")
print(f"Warehouse decisions with NULL decision_date: {pg_cur.fetchone()[0]}")

# 2. How many distinct apps appear if we do COUNT(DISTINCT app_number) -- both systems
stmt = ibm_db.exec_immediate(db2_conn, """
SELECT COUNT(DISTINCT APP.APPLICATION_NUMBER) AS app_count
FROM CORE.APP AS APP
    JOIN CORE.APP_TYPE AS APP_TYPE ON APP.TYPE_SERIAL = APP_TYPE.SERIAL
    JOIN CORE.LOAN_REQUEST AS LOAN_REQUEST ON LOAN_REQUEST.PARENT_SERIAL = APP.SERIAL
WHERE APP_TYPE.DESCRIPTION LIKE '%New Loan App%'
  AND APP.APPLICATION_DATE > '2025-04-01'
""")
row = ibm_db.fetch_assoc(stmt)
print(f"\nDB2 distinct application count (no decision filter): {row['APP_COUNT']}")

pg_cur.execute("""
SELECT COUNT(DISTINCT app.application_number)
FROM staging.ks_app AS app
JOIN staging.ks_loan_request AS lr ON lr.app_serial = app.app_serial
WHERE app.app_type LIKE '%New Loan App%'
  AND app.application_date > '2025-04-01'
""")
print(f"Warehouse distinct application count (no decision filter): {pg_cur.fetchone()[0]}")

# 3. Latest-decision check: how many loan_requests have >1 row surviving NOT EXISTS?
stmt = ibm_db.exec_immediate(db2_conn, """
SELECT COUNT(*) AS multi_decision_lrs
FROM (
    SELECT LOAN_REQUEST.SERIAL, COUNT(*) AS surviving_decisions
    FROM CORE.APP AS APP
        JOIN CORE.APP_TYPE AS APP_TYPE ON APP.TYPE_SERIAL = APP_TYPE.SERIAL
        JOIN CORE.LOAN_REQUEST AS LOAN_REQUEST ON LOAN_REQUEST.PARENT_SERIAL = APP.SERIAL
        JOIN CORE.DECISION AS DECISION ON LOAN_REQUEST.SERIAL = DECISION.PARENT_SERIAL
            AND NOT EXISTS (
                SELECT 1 FROM CORE.DECISION AS later_dec
                WHERE later_dec.PARENT_SERIAL = DECISION.PARENT_SERIAL
                  AND later_dec.DECISION_DATE >= DECISION.DECISION_DATE
                  AND later_dec.SERIAL > DECISION.SERIAL
            )
    WHERE APP_TYPE.DESCRIPTION LIKE '%New Loan App%'
      AND APP.APPLICATION_DATE > '2025-04-01'
    GROUP BY LOAN_REQUEST.SERIAL
    HAVING COUNT(*) > 1
) x
""")
row = ibm_db.fetch_assoc(stmt)
print(f"\nDB2 loan_requests with >1 decision surviving NOT EXISTS: {row['MULTI_DECISION_LRS']}")

pg_cur.execute("""
SELECT COUNT(*) FROM (
    SELECT lr.loan_request_serial, COUNT(*) AS surviving
    FROM staging.ks_app AS app
    JOIN staging.ks_loan_request AS lr ON lr.app_serial = app.app_serial
    JOIN staging.ks_decision AS d ON d.loan_request_serial = lr.loan_request_serial
        AND NOT EXISTS (
            SELECT 1 FROM staging.ks_decision AS later_dec
            WHERE later_dec.loan_request_serial = d.loan_request_serial
              AND later_dec.decision_date >= d.decision_date
              AND later_dec.decision_serial > d.decision_serial
        )
    WHERE app.app_type LIKE '%New Loan App%%'
      AND app.application_date > '2025-04-01'
    GROUP BY lr.loan_request_serial
    HAVING COUNT(*) > 1
) x
""")
print(f"Warehouse loan_requests with >1 decision surviving NOT EXISTS: {pg_cur.fetchone()[0]}")

ibm_db.close(db2_conn)
pg_conn.close()
