import os
import re

from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_schema.mode_select import warehouse_schema_mode_select
from pipelines.defs.resources import WarehousePgResource
from pipelines.queries import load


def _split_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements.

    Strips line comments (-- to end of line) BEFORE splitting on ';' so that a
    semicolon inside a comment doesn't break a statement in two. The DDL files
    have no '--' inside string literals, so this is safe.
    """
    sql_no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in sql_no_comments.split(";") if s.strip()]


@asset(group_name="warehouse_schema", deps=[warehouse_schema_mode_select])
def warehouse_tables(warehouse_pg: WarehousePgResource):
    schema = os.environ.get("WAREHOUSE_SCHEMA", "dev")
    sql = load("warehouse_schema.sql")
    engine = warehouse_pg.get_engine()
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        for stmt in _split_statements(sql):
            conn.execute(text(stmt))
        conn.commit()
    return {"status": "ok", "warehouse_schema": schema}


@asset(group_name="warehouse_schema", deps=[warehouse_tables])
def warehouse_funcs(warehouse_pg: WarehousePgResource):
    schema = os.environ.get("WAREHOUSE_SCHEMA", "dev")
    sql = load("warehouse_functions.sql")
    engine = warehouse_pg.get_engine()
    with engine.connect() as conn:
        for stmt in _split_statements(sql):
            conn.execute(text(stmt))
        conn.commit()
    return {"status": "ok", "warehouse_schema": schema}
