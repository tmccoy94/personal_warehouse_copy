import os
from pathlib import Path

from dagster import AssetExecutionContext, asset

_VALID_SCHEMAS  = {"dev", "test", "prod"}
_INTENT_FILE    = Path(__file__).parents[3] / ".schema_intent"


@asset(
    group_name="warehouse_schema",
    description=(
        "Schema environment validation gate — no loaders or DDL assets can run without this "
        "having materialized successfully.\n\n"
        "Runs two checks:\n"
        "  1. The running WAREHOUSE_SCHEMA matches what switch_schema.py last set "
        "(intent file).\n"
        "  2. The schema is a recognised warehouse schema (dev, test, prod).\n\n"
        "To switch schemas, run switch_schema.py from the project root:\n"
        "    python switch_schema.py\n\n"
        "That opens a window to pick dev / test / prod, restarts the containers, and opens "
        "this asset in the browser so you can click Materialize to confirm."
    ),
)
def warehouse_schema_mode_select(context: AssetExecutionContext) -> str:
    running = os.environ.get("WAREHOUSE_SCHEMA", "").strip()

    # Check 1 — intent file exists and matches the running env var
    if not _INTENT_FILE.exists():
        raise Exception(
            "No schema intent file found. switch_schema.py has never been run, or the "
            "file was deleted.\n\n"
            "Run switch_schema.py from the project root and select a schema."
        )

    intended = _INTENT_FILE.read_text().strip()

    if intended != running:
        raise Exception(
            f"Schema mismatch.\n\n"
            f"  switch_schema.py last set:  {intended}\n"
            f"  WAREHOUSE_SCHEMA running:   {running}\n\n"
            f"These must match before any data can be loaded. Either:\n"
            f"  - Run switch_schema.py and select '{running}' to update the intent, or\n"
            f"  - Update .env to WAREHOUSE_SCHEMA={intended} and restart containers."
        )

    # Check 2 — the schema is a recognised warehouse schema
    if running not in _VALID_SCHEMAS:
        raise Exception(
            f"WAREHOUSE_SCHEMA='{running}' is not a recognised warehouse schema.\n\n"
            f"Valid options: {', '.join(sorted(_VALID_SCHEMAS))}\n\n"
            f"Run switch_schema.py and select one of the valid options."
        )

    context.log.info(f"Schema validated — warehouse is running in '{running}' mode")
    context.add_output_metadata({"warehouse_schema": running})
    return running
