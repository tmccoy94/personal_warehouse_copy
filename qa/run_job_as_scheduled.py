"""Launch a job WITH a schedule tag (simulates a scheduled run for sensor testing)."""
import requests, json, sys

job_name      = sys.argv[1] if len(sys.argv) > 1 else "ks_daily_job"
partition     = sys.argv[2] if len(sys.argv) > 2 else "2026-06-05"
schedule_name = sys.argv[3] if len(sys.argv) > 3 else "nightly_ks_schedule"

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
      tags: [
        {key: "dagster/partition", value: "%s"},
        {key: "dagster/schedule_name", value: "%s"}
      ]
    },
    mode: "default"
  }) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
  }
}
""" % (job_name, partition, schedule_name)

resp = requests.post("http://localhost:3000/graphql", json={"query": mutation})
print(json.dumps(resp.json(), indent=2))
