"""Launch a single asset materialisation with a partition.
Usage: python run_asset.py <asset_key> <partition>
"""
import requests, json, sys

asset_key = sys.argv[1] if len(sys.argv) > 1 else "ks_daily_repair"
partition = sys.argv[2] if len(sys.argv) > 2 else "2026-06-03"

mutation = """
mutation {
  launchPipelineExecution(executionParams: {
    selector: {
      repositoryLocationName: "pipelines",
      repositoryName: "__repository__",
      assetSelection: [{path: ["%s"]}],
      partitionsByAssets: [{assetKey: {path: ["%s"]}, partitions: {range: {start: "%s", end: "%s"}}}]
    },
    mode: "default",
    runConfigData: "{}"
  }) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
  }
}
""" % (asset_key, asset_key, partition, partition)

resp = requests.post("http://localhost:3000/graphql", json={"query": mutation})
data = resp.json()
print(json.dumps(data, indent=2))
