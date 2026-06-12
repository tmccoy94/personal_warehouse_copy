import dagster
from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import KeystoneDatabaseResource, WarehousePgResource
from pipelines.loading import audit_load, db2_fetch_all
from pipelines.queries import load


@asset(group_name="ks_daily_loaders", tags={"recovery": "staging_mirror"}, deps=[warehouse_schema_ready])
def dim_employee_table(
    context: dagster.AssetExecutionContext,
    keystone_database: KeystoneDatabaseResource,
    warehouse_pg: WarehousePgResource,
):
    conn = keystone_database.get_connection(context=context)
    try:
        rows = db2_fetch_all(conn, load("user_roles.sql"))
    finally:
        import ibm_db
        ibm_db.close(conn)

    if not rows:
        raise Exception("user_roles.sql returned zero rows — expected at least one active user")

    engine = warehouse_pg.get_engine()
    upserted = 0

    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="keystone",
    ) as stats:
        with engine.begin() as pg_conn:
            for row in rows:
                result = pg_conn.execute(
                    text("""
                        INSERT INTO dim_employee AS employee
                            (source_system, source_employee_serial, teller_user,
                             full_name, role_descriptions, termination_date)
                        VALUES
                            ('keystone', :source_employee_serial, :teller_user,
                             :full_name, :role_descriptions, :termination_date)
                        ON CONFLICT (source_system, source_employee_serial) DO UPDATE SET
                            teller_user       = EXCLUDED.teller_user,
                            full_name         = EXCLUDED.full_name,
                            role_descriptions = EXCLUDED.role_descriptions,
                            termination_date  = EXCLUDED.termination_date,
                            updated_at        = NOW()
                        WHERE (
                            employee.teller_user       IS DISTINCT FROM EXCLUDED.teller_user       OR
                            employee.full_name         IS DISTINCT FROM EXCLUDED.full_name         OR
                            employee.role_descriptions IS DISTINCT FROM EXCLUDED.role_descriptions OR
                            employee.termination_date  IS DISTINCT FROM EXCLUDED.termination_date
                        )
                    """),
                    {
                        "source_employee_serial": row["user_serial"],
                        "teller_user":            row["username"],
                        "full_name":              row["username"],
                        "role_descriptions":      row["role_descriptions"],
                        "termination_date":       row["termination_date"],
                    },
                )
                upserted += result.rowcount

        stats["row_count"] = upserted

    return {"status": "ok", "employees_upserted": upserted}
