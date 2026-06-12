from datetime import date
from io import StringIO
from typing import Iterable

import dagster
import pandas as pd
from dagster import MonthlyPartitionsDefinition, asset
from sqlalchemy import Engine, text

from pipelines.defs.assets.warehouse_loaders.branch import dim_branch_table
from pipelines.defs.assets.warehouse_loaders.employee import dim_employee_table
from pipelines.defs.resources import JasperserverReportingApiResource, WarehousePgResource
from pipelines.loading import audit_load, resolve_branch_keys
from pipelines.utils import last_day_of_month


def _resolve_employee_keys_by_teller_user(engine: Engine, teller_users: Iterable[str]) -> dict[str, int]:
    """Return {teller_user: employee_key} for every username in the input."""
    users = list(teller_users)
    if not users:
        return {}
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT teller_user, employee_key "
                "FROM dim_employee "
                "WHERE teller_user = ANY(:users)"
            ),
            {"users": users},
        )
        return {r.teller_user: int(r.employee_key) for r in result}


@asset(
    group_name="jasperserver_loaders",
    deps=[dim_employee_table, dim_branch_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def fct_teller_activity_table(
    context: dagster.AssetExecutionContext,
    jasperserver_reporting_api: JasperserverReportingApiResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date = last_day_of_month(partition_date)

    # Transactions_By_Role_Type — monthly total transaction count per teller.
    # Columns confirmed: User, Role, Transaction Count (no date per row — one row per teller).
    csv_text = jasperserver_reporting_api.fetch_report_csv(
        "organizations/d0001liv/Reports/Operations/Transactions_By_Role_Type",
        params={
            "StartDate": str(start_date),
            "EndDate": str(end_date),
            "RoleDescriptionList": "Teller",
        },
    )
    df = pd.read_csv(StringIO(csv_text))

    if df.empty:
        context.log.warning(f"Transactions_By_Role_Type returned 0 rows for {partition_date}")
        return {"status": "ok", "partition": str(partition_date), "rows_upserted": 0}

    context.log.info(f"Transactions_By_Role_Type columns: {list(df.columns)}")

    engine = warehouse_pg.get_engine()
    teller_users = df["User"].dropna().unique().tolist()
    employee_key_map = _resolve_employee_keys_by_teller_user(engine, teller_users)

    upserted = 0
    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="jasperserver",
    ) as stats:
        with engine.begin() as pg_conn:
            for _, row in df.iterrows():
                teller_user = row.get("User")
                employee_key = employee_key_map.get(teller_user) if teller_user else None
                if employee_key is None:
                    context.log.warning(f"No employee_key for teller_user={teller_user!r} — skipping row")
                    continue

                transaction_count = row.get("Transaction Count")

                result = pg_conn.execute(
                    text("""
                        INSERT INTO fct_teller_activity AS teller_act
                            (activity_date, employee_key, branch_key, transaction_count)
                        VALUES
                            (:activity_date, :employee_key, :branch_key, :transaction_count)
                        ON CONFLICT (activity_date, employee_key) DO UPDATE SET
                            transaction_count = EXCLUDED.transaction_count
                        WHERE teller_act.transaction_count IS DISTINCT FROM EXCLUDED.transaction_count
                    """),
                    {
                        "activity_date":     end_date,   # monthly total keyed to last day of month
                        "employee_key":      employee_key,
                        "branch_key":        None,
                        "transaction_count": int(transaction_count) if pd.notna(transaction_count) else None,
                    },
                )
                upserted += result.rowcount

        stats["row_count"] = upserted

    return {
        "status": "ok",
        "partition": str(partition_date),
        "rows_upserted": upserted,
    }


@asset(
    group_name="jasperserver_loaders",
    deps=[dim_employee_table],
    partitions_def=MonthlyPartitionsDefinition(start_date="2026-05-01", timezone="America/Chicago"),
)
def fct_teller_out_of_balance_table(
    context: dagster.AssetExecutionContext,
    jasperserver_reporting_api: JasperserverReportingApiResource,
    warehouse_pg: WarehousePgResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date
    end_date = last_day_of_month(partition_date)

    # TellerDrawerOver-UnderMonthly — per-day cash drawer over/under events.
    # No header row in this report — 4 columns: teller name, date, prev_cash, new_cash.
    # Cash values come back as currency strings ($0.00, $-10.00) — stripped before insert.
    csv_text = jasperserver_reporting_api.fetch_report_csv(
        "organizations/d0001liv/Reports/Operations/TellerDrawerOver-UnderMonthly",
        params={
            "StartDate": str(start_date),
            "EndDate": str(end_date),
        },
    )
    df = pd.read_csv(
        StringIO(csv_text),
        header=None,
        names=["teller", "date", "prev_cash", "new_cash"],
    )

    if df.empty:
        context.log.warning(f"TellerDrawerOver-UnderMonthly returned 0 rows for {partition_date}")
        return {"status": "ok", "partition": str(partition_date), "rows_inserted": 0}

    context.log.info(f"TellerDrawerOver-UnderMonthly rows: {len(df)}")

    def parse_currency(val) -> float | None:
        if pd.isna(val):
            return None
        return float(str(val).replace("$", "").replace(",", ""))

    engine = warehouse_pg.get_engine()
    teller_users = df["teller"].dropna().unique().tolist()
    employee_key_map = _resolve_employee_keys_by_teller_user(engine, teller_users)

    insert_rows = []
    for _, row in df.iterrows():
        teller_user = row.get("teller")
        employee_key = employee_key_map.get(teller_user) if teller_user else None
        if employee_key is None:
            context.log.warning(f"No employee_key for teller_user={teller_user!r} — skipping row")
            continue

        insert_rows.append(
            {
                "employee_key":     employee_key,
                "transaction_date": row.get("date"),
                "prev_cash":        parse_currency(row.get("prev_cash")),
                "new_cash":         parse_currency(row.get("new_cash")),
            }
        )

    inserted = 0
    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="jasperserver",
    ) as stats:
        with engine.begin() as pg_conn:
            # Truncate this partition window before loading — out_of_balance has no surrogate key
            # suitable for ON CONFLICT, so truncate+insert is the idempotency mechanism.
            pg_conn.execute(
                text(
                    "DELETE FROM fct_teller_out_of_balance "
                    "WHERE transaction_date BETWEEN :start_date AND :end_date"
                ),
                {"start_date": start_date, "end_date": end_date},
            )

            if insert_rows:
                result = pg_conn.execute(
                    text("""
                        INSERT INTO fct_teller_out_of_balance
                            (employee_key, transaction_date, prev_cash, new_cash)
                        VALUES
                            (:employee_key, :transaction_date, :prev_cash, :new_cash)
                    """),
                    insert_rows,
                )
                inserted = result.rowcount

        stats["row_count"] = inserted

    return {
        "status": "ok",
        "partition": str(partition_date),
        "rows_inserted": inserted,
    }
