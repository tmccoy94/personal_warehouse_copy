"""Database connection helpers for the QA tool.

Reads credentials from the project root .env file.
DB2 database name comes from ks_db_connections constants — not from .env.
"""
import os
from pathlib import Path

import ibm_db
import psycopg
from dotenv import load_dotenv

# Load .env from project root (one level up from qa/)
_ENV_PATH = Path(__file__).parents[3] / ".env"
load_dotenv(_ENV_PATH)

# ── DB2 database constants ────────────────────────────────────────────────────
LIVE        = "D0001LIV"
DEFAULT_DB  = os.environ.get("KEYSTONE_DB_OVERRIDE") or LIVE
MONTH_END_1 = "D0001T31"   # Jan / Apr / Jul / Oct
MONTH_END_2 = "D0001T32"   # Feb / May / Aug / Nov
QUARTER_END = "D0001T33"   # Mar / Jun / Sep / Dec


def get_db2_connection(db_name: str = MONTH_END_2):
    """Connect to a Keystone DB2 database.

    Defaults to MONTH_END_2 (the frozen snapshot used for QA).
    Change to LIVE for live comparisons.
    """
    host     = os.environ["SOURCE_DB_HOST"]
    port     = os.environ["SOURCE_DB_PORT"]
    user     = os.environ["SOURCE_DB_USER"]
    password = os.environ["SOURCE_DB_PASSWORD"]

    conn_str = (
        f"DATABASE={db_name};"
        f"HOSTNAME={host};"
        f"PORT={port};"
        "PROTOCOL=TCPIP;"
        f"UID={user};"
        f"PWD={password};"
    )
    return ibm_db.connect(conn_str, "", "")


def get_pg_connection():
    """Connect to the warehouse PostgreSQL database on localhost."""
    host     = "localhost"
    port     = os.environ.get("POSTGRES_PORT", "5433")
    database = os.environ.get("POSTGRES_DB", "warehouse")
    user     = os.environ.get("POSTGRES_USER", "warehouse_admin")
    password = os.environ["DAGSTER_POSTGRES_PASSWORD"]

    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={user} password={password}"
    )
