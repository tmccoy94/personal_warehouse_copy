"""Launch a Dagster job with a specific partition. Usage: python run_job.py <job_name> <partition>"""
import requests, json, sys

job_name  = sys.argv[1] if len(sys.argv) > 1 else "ks_daily_job"
partition = sys.argv[2] if len(sys.argv) > 2 else "2026-06-05"

mutation = """
mutation {
  launchRun(executionParams: {
    selector: {
      repositoryLocationName: "pipelines",
      repositoryName: "__repository__",
      jobName: "%s"
    },
    runConfigData: {},
    executionMetadata: {
      tags: [{key: "dagster/partition", value: "%s"}]
    },
    mode: "default"
  }) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
    ... on InvalidSubsetError { message }
  }
}
""" % (job_name, partition)

resp = requests.post("http://localhost:3000/graphql", json={"query": mutation})
result = resp.json()
print(json.dumps(result, indent=2))
