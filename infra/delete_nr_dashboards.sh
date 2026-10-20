#!/usr/bin/env bash
# Delete New Relic dashboards in NEW_RELIC_ACCOUNT_ID.
#
# Default: only the dashboards this bundle deploys (one per infra/dashboards/*.json.tftpl), matched by
# name. With NEW_RELIC_TEAM_NAME set, the "<name> [<team>]" variant is deleted as well as the bare name.
# With -r: every dashboard in the account whose name matches the (jq/Oniguruma) regexp instead.
#
#   ./delete_nr_dashboards.sh                  # bundle dashboards, asks for confirmation
#   ./delete_nr_dashboards.sh -y               # no prompt
#   ./delete_nr_dashboards.sh -r '^Kafka .*'   # any dashboards matching the regexp
#   ./delete_nr_dashboards.sh -r '(?i)test' -y
#
# Prerequisites: jq, NEW_RELIC_API_KEY (User key, NRAK-...), NEW_RELIC_ACCOUNT_ID (load from .env).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARDS_DIR="$SCRIPT_DIR/dashboards"

YES=false
REGEX=""
while getopts "yr:" opt; do
  case "$opt" in
    y) YES=true ;;
    r) REGEX="$OPTARG" ;;
    *) echo "Usage: $0 [-y] [-r regexp]" >&2; exit 2 ;;
  esac
done

command -v jq >/dev/null || { echo "ERROR: jq is required." >&2; exit 1; }
: "${NEW_RELIC_API_KEY:?Set NEW_RELIC_API_KEY (a User API key, starts with NRAK-...) in .env first}"
: "${NEW_RELIC_ACCOUNT_ID:?Set NEW_RELIC_ACCOUNT_ID in .env first}"

NERDGRAPH_URL="https://api.newrelic.com/graphql"
case "${NEW_RELIC_OTLP_ENDPOINT:-}" in
  *eu*) NERDGRAPH_URL="https://api.eu.newrelic.com/graphql" ;;
esac

nerdgraph() {
  curl -sS "$NERDGRAPH_URL" \
    -H "Api-Key: $NEW_RELIC_API_KEY" \
    -H "Content-Type: application/json" \
    -d "$1"
}

# Print "guid<TAB>name" for dashboards matching entitySearch query $1 (follows pagination).
search() {
  local cursor="" resp
  while :; do
    resp=$(nerdgraph "$(jq -n --arg q "$1" --arg c "$cursor" '{
      query: "query($q: String!, $c: String) { actor { entitySearch(query: $q) { results(cursor: $c) { entities { guid name } nextCursor } } } }",
      variables: { q: $q, c: (if $c == "" then null else $c end) }
    }')")
    if echo "$resp" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
      echo "ERROR: search failed:" >&2; echo "$resp" | jq . >&2; exit 1
    fi
    echo "$resp" | jq -r '.data.actor.entitySearch.results.entities[] | [.guid, .name] | @tsv'
    cursor=$(echo "$resp" | jq -r '.data.actor.entitySearch.results.nextCursor // empty')
    [[ -n "$cursor" ]] || break
  done
}

BASE_QUERY="domainType IN ('VIZ-DASHBOARD') AND accountId = ${NEW_RELIC_ACCOUNT_ID}"

# ── Collect targets ───────────────────────────────────────────────────────────
if [[ -n "$REGEX" ]]; then
  # Fail early on a bad regexp rather than mid-pipeline.
  jq -n --arg re "$REGEX" '"" | test($re)' >/dev/null
  TARGETS=$(search "$BASE_QUERY" | while IFS=$'\t' read -r guid name; do
    jq -n -e --arg re "$REGEX" --arg n "$name" '$n | test($re)' >/dev/null && printf '%s\t%s\n' "$guid" "$name"
  done || true)
else
  NAMES=()
  for f in "$DASHBOARDS_DIR"/*.json.tftpl; do
    base=$(sed "s/\${account_id}/$NEW_RELIC_ACCOUNT_ID/g" "$f" | jq -r '.name')
    NAMES+=("$base")
    [[ -n "${NEW_RELIC_TEAM_NAME:-}" ]] && NAMES+=("$base [$NEW_RELIC_TEAM_NAME]")
  done
  TARGETS=""
  for name in "${NAMES[@]}"; do
    # entitySearch name = is case-insensitive, so re-check for an exact match
    found=$(search "$BASE_QUERY AND name = '${name//\'/\'\'}'" | awk -F'\t' -v n="$name" '$2 == n')
    [[ -n "$found" ]] && TARGETS+="$found"$'\n'
  done
  TARGETS="${TARGETS%$'\n'}"
fi

if [[ -z "$TARGETS" ]]; then
  echo "No matching dashboards in account $NEW_RELIC_ACCOUNT_ID."
  exit 0
fi

echo "Dashboards to delete from account $NEW_RELIC_ACCOUNT_ID:"
echo "$TARGETS" | awk -F'\t' '{printf "  - %s (%s)\n", $2, $1}'

if [[ "$YES" != true ]]; then
  read -rp "Delete them? [y/N] " ANSWER
  [[ "$ANSWER" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
fi

# ── Delete ────────────────────────────────────────────────────────────────────
while IFS=$'\t' read -r guid name; do
  R=$(nerdgraph "$(jq -n --arg guid "$guid" '{
    query: "mutation($guid: EntityGuid!) { dashboardDelete(guid: $guid) { status errors { description type } } }",
    variables: { guid: $guid }
  }')")
  if echo "$R" | jq -e '(.errors // [] | length > 0) or (.data.dashboardDelete.status != "SUCCESS")' >/dev/null 2>&1; then
    echo "ERROR: delete failed for \"$name\" ($guid):" >&2; echo "$R" | jq . >&2; exit 1
  fi
  echo "    deleted: $name ($guid)"
done <<< "$TARGETS"
