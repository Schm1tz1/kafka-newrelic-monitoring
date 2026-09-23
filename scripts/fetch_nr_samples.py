#!/usr/bin/env python3
"""Fetch sample log records from New Relic and write them to testdata/."""
import json
import os
import sys
import urllib.request

API_KEY = os.environ.get("NEW_RELIC_API_KEY", "")
ACCOUNT_ID = int(os.environ.get("NEW_RELIC_ACCOUNT_ID", "0"))
OTLP_ENDPOINT = os.environ.get("NEW_RELIC_OTLP_ENDPOINT", "")

NR_URL = (
    "https://api.eu.newrelic.com/graphql"
    if "eu" in OTLP_ENDPOINT
    else "https://api.newrelic.com/graphql"
)

if not API_KEY or not ACCOUNT_ID:
    print("ERROR: NEW_RELIC_API_KEY and NEW_RELIC_ACCOUNT_ID must be set", file=sys.stderr)
    sys.exit(1)


def nrql(query: str) -> list:
    gql = {
        "query": (
            f"{{ actor {{ account(id: {ACCOUNT_ID}) "
            f"{{ nrql(query: {json.dumps(query)}) {{ results }} }} }} }}"
        )
    }
    req = urllib.request.Request(
        NR_URL,
        data=json.dumps(gql).encode(),
        headers={"Api-Key": API_KEY, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())
    return body["data"]["actor"]["account"]["nrql"]["results"]


def fetch(label: str, query: str, path: str) -> None:
    print(f"  {label} ...", end=" ", flush=True)
    results = nrql(query)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"{len(results)} records → {path}")


fetch(
    "audit logs (1000 newest records, last 1 hour)",
    "SELECT * FROM Log WHERE `confluent.log.source` = 'confluent-audit' "
    "SINCE 1 hour ago ORDER BY timestamp DESC LIMIT 1000",
    "testdata/newrelic-audit-sample.json",
)

fetch(
    "connector logs (1000 newest records, last 1 hour)",
    "SELECT * FROM Log WHERE `confluent.log.source` = 'confluent-connector' "
    "SINCE 1 hour ago ORDER BY timestamp DESC LIMIT 1000",
    "testdata/newrelic-connector-sample.json",
)

print("Done.")
