import ibm_db
import dagster
from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource

# Keys must match CORE.BRANCH.DESCRIPTION exactly.
# Add an entry here whenever a new branch appears — the asset will raise if one is missing.
_BRANCH_ABBREVIATIONS = {
    "Main Branch":          "MAIN",
    "North Branch":         "NORTH",
    "East Branch":          "EAST",
    "South Branch":         "SOUTH",
    "Overland Park Branch": "OP",
    "LSI Branch - Online Applications": "LSI",
}

# CORE.BRANCH.DESCRIPTION → warehouse dim_branch.branch_name.
# OP Branch and Overland Park Branch are the same physical branch — CORE stores
# it as "Overland Park Branch", the warehouse renamed it to "OP Branch" so the
# label matches how staff refer to it. Anywhere we read a branch name from CORE
# and write it to (or match it against) dim_branch, normalize through this map.
CORE_TO_WAREHOUSE_BRANCH_NAME = {
    "Overland Park Branch": "OP Branch",
}

_BRANCH_QUERY = """
    SELECT
        'keystone'              AS source_system,
        SERIAL                  AS source_branch_serial,
        DESCRIPTION             AS branch_name,
        CODE                    AS keystone_code,
        STREET                  AS street,
        CITY                    AS city,
        STATE                   AS state,
        LEFT(POSTAL_CODE, 5)    AS zip,
        TIME_ZONE               AS time_zone
    FROM CORE.BRANCH
"""


@asset(group_name="ks_daily_loaders", tags={"recovery": "staging_mirror"}, deps=[warehouse_schema_ready])
def dim_branch_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    conn = keystone_database.get_connection(context=context)
    try:
        stmt = ibm_db.exec_immediate(conn, _BRANCH_QUERY)
        rows = []
        row = ibm_db.fetch_assoc(stmt)
        while row:
            rows.append({k.lower(): v for k, v in row.items()})
            row = ibm_db.fetch_assoc(stmt)
    finally:
        ibm_db.close(conn)

    missing = [row["branch_name"] for row in rows if row["branch_name"] not in _BRANCH_ABBREVIATIONS]
    if missing:
        raise Exception(
            f"Missing abbreviation mapping for branch(es): {missing}. "
            "Add them to _BRANCH_ABBREVIATIONS in warehouse_loaders/branch.py."
        )

    for row in rows:
        row["abbreviation"] = _BRANCH_ABBREVIATIONS[row["branch_name"]]
        row["branch_name"] = CORE_TO_WAREHOUSE_BRANCH_NAME.get(row["branch_name"], row["branch_name"])

    engine = warehouse_pg.get_engine()
    upserted = 0
    with engine.connect() as pg_conn:
        for row in rows:
            result = pg_conn.execute(
                text("""
                    INSERT INTO dim_branch as branch
                        (source_system, source_branch_serial, branch_name, abbreviation,
                         keystone_code, street, city, state, zip, time_zone)
                    VALUES
                        (:source_system, :source_branch_serial, :branch_name, :abbreviation,
                         :keystone_code, :street, :city, :state, :zip, :time_zone)
                    ON CONFLICT (source_system, source_branch_serial) DO UPDATE SET
                        branch_name   = EXCLUDED.branch_name,
                        abbreviation  = EXCLUDED.abbreviation,
                        keystone_code = EXCLUDED.keystone_code,
                        street        = EXCLUDED.street,
                        city          = EXCLUDED.city,
                        state         = EXCLUDED.state,
                        zip           = EXCLUDED.zip,
                        time_zone     = EXCLUDED.time_zone
                     WHERE (
                        branch.branch_name   IS DISTINCT FROM EXCLUDED.branch_name   OR
                        branch.abbreviation  IS DISTINCT FROM EXCLUDED.abbreviation  OR
                        branch.keystone_code IS DISTINCT FROM EXCLUDED.keystone_code OR
                        branch.street        IS DISTINCT FROM EXCLUDED.street        OR
                        branch.city          IS DISTINCT FROM EXCLUDED.city          OR
                        branch.state         IS DISTINCT FROM EXCLUDED.state         OR
                        branch.zip           IS DISTINCT FROM EXCLUDED.zip           OR
                        branch.time_zone     IS DISTINCT FROM EXCLUDED.time_zone
                    )
                """),
                row,
            )
            upserted += result.rowcount

        pg_conn.commit()

    return {"status": "ok", "branches_upserted": upserted}
