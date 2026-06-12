import requests, json, time, sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "c9842d84-9499-4906-aad6-50fc3896d810"

for i in range(90):
    resp = requests.post("http://localhost:3000/graphql", json={"query": """
{ runOrError(runId: "%s") {
    __typename
    ... on Run { status stepStats { stepKey status } }
    ... on PythonError { message }
} }""" % run_id})
    result = resp.json()
    if result.get("data") and result["data"]["runOrError"]:
        run = result["data"]["runOrError"]
        status = run.get("status", "UNKNOWN")
        print(f"[{i*5}s] {status}", flush=True)
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            for s in run.get("stepStats", []):
                print(f"  {s['stepKey']} -> {s['status']}")
            break
    else:
        print(f"[{i*5}s] waiting...", flush=True)
    time.sleep(5)
