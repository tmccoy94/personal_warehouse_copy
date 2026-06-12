import json
import os
from pathlib import Path

import dagster
from dagster import (
    AssetKey,
    AssetSelection,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    define_asset_job,
    sensor,
)


# ── Job ───────────────────────────────────────────────────────────────────────

# One job covers all drop-folder CSV loaders. The sensor yields RunRequests
# with asset_selection narrowed to the specific asset that has a new file.
csv_file_loader_job = define_asset_job(
    name="csv_file_loader_job",
    selection=AssetSelection.groups("file_loaders"),
)


# ── Sensor ────────────────────────────────────────────────────────────────────

# Maps csv_drop subdirectory name → warehouse asset key.
# Add a new entry here whenever a new drop-folder table is added.
_CSV_DROP_ASSET_MAP: dict[str, AssetKey] = {
    "fct_digital_enrollments": AssetKey("fct_digital_enrollments_table"),
    "fct_survey_responses":    AssetKey("fct_survey_responses_table"),
    # fct_atm_by_machine_summary and fct_branch_profitability_summary moved to
    # manual_entries.csv (Decisions 42–43); no longer drop-folder watched.
}


@sensor(
    job=csv_file_loader_job,
    minimum_interval_seconds=300,  # poll every 5 minutes
)
def csv_drop_sensor(context: SensorEvaluationContext):
    """Watches data/csv_drop/ subdirectories and triggers the matching file loader
    asset whenever a new CSV file appears.

    Cursor is a JSON dict mapping {subdir_name: "<filename>:<mtime>"} so that
    re-dropping the same file doesn't retrigger, but replacing it with a new file
    (different name or different mtime) does.

    Configure drop root via CSV_DROP_PATH env var; defaults to /data/csv_drop.
    """
    csv_drop_root = Path(os.environ.get("CSV_DROP_PATH", "/data/csv_drop"))

    if not csv_drop_root.exists():
        return SkipReason(f"csv_drop directory not found at {csv_drop_root}")

    cursor: dict[str, str] = json.loads(context.cursor) if context.cursor else {}
    new_cursor = dict(cursor)
    run_requests: list[RunRequest] = []

    for subdir_name, asset_key in _CSV_DROP_ASSET_MAP.items():
        subdir = csv_drop_root / subdir_name
        if not subdir.exists():
            continue

        csv_files = sorted(subdir.glob("*.csv"))
        if not csv_files:
            continue

        # Use the most-recently-modified file as the trigger signal
        latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
        file_fingerprint = f"{latest_file.name}:{latest_file.stat().st_mtime}"

        if cursor.get(subdir_name) == file_fingerprint:
            continue  # same file as last run — nothing new

        run_requests.append(
            RunRequest(
                run_key=f"{subdir_name}:{file_fingerprint}",
                asset_selection=[asset_key],
            )
        )
        new_cursor[subdir_name] = file_fingerprint

    return SensorResult(
        run_requests=run_requests,
        cursor=json.dumps(new_cursor),
    )


# ── Registration ──────────────────────────────────────────────────────────────

@dagster.definitions
def sensor_defs():
    return dagster.Definitions(
        jobs=[csv_file_loader_job],
        sensors=[csv_drop_sensor],
    )
