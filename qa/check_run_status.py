"""Poll a Dagster run until it finishes."""
import requests, json, time, sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "025d0bdc-88aa-4136-ab86-7ab351629c2b"

query = """
{
  runOrError(runId: "%s") {
    ... on Run {
      runId status
      stats {
        ... on RunStatsSnapshot {
          startTime endTime
          numSucceeded numFailed numSkipped
        }
      }
    }
    ... on RunNotFoundError { message }
    ... on PythonError { message }
  }
}
""" % run_id

for i in range(60):
    resp = requests.post("http://localhost:3000/graphql", json={"query": query})
    result = resp.json()
    if result.get("data"):
        data = result["data"]["runOrError"]
        status = data.get("status", "UNKNOWN")
        print(f"[{i*10}s] status={status}", flush=True)
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            print(json.dumps(data, indent=2))
            break
    else:
        print(f"[{i*10}s] error: {result.get('errors')}", flush=True)
    time.sleep(10)
