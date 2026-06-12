import requests, json

run_id = "025d0bdc-88aa-4136-ab86-7ab351629c2b"

resp = requests.post("http://localhost:3000/graphql", json={"query": """
{
  runOrError(runId: "%s") {
    __typename
    ... on Run {
      status
      stepStats { stepKey status }
    }
    ... on PythonError { message }
  }
}
""" % run_id})

result = resp.json()
if result.get("data"):
    run = result["data"]["runOrError"]
    print("Run type:", run["__typename"])
    if "stepStats" in run:
        for s in run["stepStats"]:
            print(f"  {s['stepKey']} -> {s['status']}")
    else:
        print(json.dumps(run, indent=2))
else:
    print(json.dumps(result, indent=2))
