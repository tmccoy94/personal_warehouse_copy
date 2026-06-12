"""Launch ks_monthly_job for 2026-05-01 partition via Dagster GraphQL API."""
import requests
import json

mutation = """
mutation {
  launchRun(executionParams: {
    selector: {
      repositoryLocationName: "pipelines",
      repositoryName: "__repository__",
      jobName: "ks_monthly_job"
    },
    runConfigData: {},
    executionMetadata: {
      tags: [{key: "dagster/partition", value: "2026-05-01"}]
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

resp = requests.post("http://localhost:3000/graphql", json={"query": mutation})
print(json.dumps(resp.json(), indent=2))
