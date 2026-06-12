"""
Materialize a single Dagster asset with a specific partition.

Usage:
    python materialize_asset.py <asset_key> <partition>

Examples:
    python materialize_asset.py ks_daily_repair 2026-06-03
    python materialize_asset.py ks_monthly_repair 2026-05-01
    python materialize_asset.py ks_fct_members_summary_table 2026-05-01

Fails immediately if:
  - Not exactly 2 arguments provided
  - Asset key does not exist in the repository
  - Asset has no job to run through (can't be materialised standalone)
"""
import sys
import json
import requests

GRAPHQL = "http://localhost:3000/graphql"


def gql(query: str, variables: dict = None) -> dict:
    resp = requests.post(GRAPHQL, json={"query": query, "variables": variables or {}})
    resp.raise_for_status()
    return resp.json()


# ── Argument validation ───────────────────────────────────────────────────────

if len(sys.argv) != 3:
    print("ERROR: exactly 2 arguments required: <asset_key> <partition>")
    print(f"       got {len(sys.argv) - 1}: {sys.argv[1:]}")
    sys.exit(1)

asset_key, partition = sys.argv[1], sys.argv[2]

# ── Verify asset exists and find its job ─────────────────────────────────────

jobs_query = gql("""
query AssetJobs($path: [String!]!) {
  assetOrError(assetKey: {path: $path}) {
    __typename
    ... on Asset {
      definition { jobNames }
    }
  }
}
""", {"path": [asset_key]})

asset_data = jobs_query["data"]["assetOrError"]
definition = asset_data.get("definition") if asset_data.get("__typename") == "Asset" else None
job_names = definition.get("jobNames", []) if definition else []

if not job_names:
    print(f"ERROR: asset '{asset_key}' not found or has no associated job.")
    print("       Check spelling. Asset names are visible in the Dagster UI under Assets.")
    sys.exit(1)

# Prefer a named job over auto-generated ones (auto-generated start with '__')
named = [j for j in job_names if not j.startswith("__")]
job_name = named[0] if named else job_names[0]
print(f"Asset '{asset_key}' found. Job: {job_name}. Launching partition '{partition}'...")

# ── Launch the run ────────────────────────────────────────────────────────────

mutation = """
mutation Launch($jobName: String!, $assetKey: [String!]!, $partition: String!) {
  launchRun(executionParams: {
    selector: {
      repositoryLocationName: "pipelines",
      repositoryName: "__repository__",
      jobName: $jobName,
      assetSelection: [{path: $assetKey}]
    },
    runConfigData: {},
    executionMetadata: {
      tags: [{key: "dagster/partition", value: $partition}]
    },
    mode: "default"
  }) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
    ... on InvalidSubsetError { message }
    ... on RunConfigValidationInvalid { errors { message } }
  }
}
"""

launch = gql(mutation, {
    "jobName": job_name,
    "assetKey": [asset_key],
    "partition": partition,
})

launch_result = launch["data"]["launchRun"]
if launch_result["__typename"] != "LaunchRunSuccess":
    print(f"ERROR: failed to launch run: {json.dumps(launch_result, indent=2)}")
    sys.exit(1)

run_id = launch_result["run"]["runId"]
print(f"Run launched: {run_id}")

# ── Poll until complete ───────────────────────────────────────────────────────

import time

poll_query = """
query PollRun($runId: ID!) {
  runOrError(runId: $runId) {
    ... on Run {
      status
      stepStats { stepKey status }
    }
    ... on PythonError { message }
  }
}
"""

for i in range(120):
    time.sleep(5)
    poll = gql(poll_query, {"runId": run_id})
    run_data = poll["data"]["runOrError"]
    status = run_data.get("status", "UNKNOWN")
    print(f"  [{i*5}s] {status}", flush=True)
    if status in ("SUCCESS", "FAILURE", "CANCELED"):
        for step in run_data.get("stepStats", []):
            marker = "✓" if step["status"] == "SUCCESS" else "✗"
            print(f"    {marker} {step['stepKey']} — {step['status']}")
        if status != "SUCCESS":
            print(f"\nRun {status}. Check Dagster UI for logs: run {run_id}")
            sys.exit(1)
        print(f"\n✓ {asset_key} [{partition}] completed successfully.")
        sys.exit(0)

print("Timed out waiting for run to complete.")
sys.exit(1)
