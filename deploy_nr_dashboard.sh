#!/usr/bin/env bash
# Deploy newrelic-dashboard.json via the NerdGraph API, using credentials from .env.
# See README "New Relic dashboard" for what this creates and prerequisites (jq, a
# NEW_RELIC_API_KEY User API key, NEW_RELIC_ACCOUNT_ID).
#
# Usage: ./deploy_nr_dashboard.sh [path/to/dashboard.json]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_FILE="${1:-$SCRIPT_DIR/newrelic-dashboard.json}"
ENV_FILE="$SCRIPT_DIR/.env"

command -v jq >/dev/null || { echo "jq is required to run this script." >&2; exit 1; }
[[ -f "$DASHBOARD_FILE" ]] || { echo "Dashboard file not found: $DASHBOARD_FILE" >&2; exit 1; }

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

: "${NEW_RELIC_API_KEY:?Set NEW_RELIC_API_KEY (a User API key, starts with NRAK-...) in .env first}"
NEW_RELIC_ACCOUNT_ID="${NEW_RELIC_ACCOUNT_ID:-123456789}"

NERDGRAPH_URL="https://api.newrelic.com/graphql"
case "${NEW_RELIC_OTLP_ENDPOINT:-}" in
  *eu*) NERDGRAPH_URL="https://api.eu.newrelic.com/graphql" ;;
esac

# Rewrite every widget's accountId placeholder to the real account before sending.
DASHBOARD=$(jq --argjson acct "$NEW_RELIC_ACCOUNT_ID" \
  'walk(if type == "object" and has("accountId") then .accountId = $acct else . end)' \
  "$DASHBOARD_FILE")

PAYLOAD=$(jq -n --argjson dashboard "$DASHBOARD" --argjson acct "$NEW_RELIC_ACCOUNT_ID" '{
    query: "mutation($accountId: Int!, $dashboard: DashboardInput!) { dashboardCreate(accountId: $accountId, dashboard: $dashboard) { entityResult { guid name } errors { description type } } }",
    variables: { accountId: $acct, dashboard: $dashboard }
  }')

RESPONSE=$(curl -sS "$NERDGRAPH_URL" \
  -H "Api-Key: $NEW_RELIC_API_KEY" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

echo "$RESPONSE" | jq .

if echo "$RESPONSE" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
  echo "GraphQL request failed (e.g. bad NEW_RELIC_API_KEY), see errors above." >&2
  exit 1
fi

if ! echo "$RESPONSE" | jq -e '.data.dashboardCreate.errors // [] | length == 0' >/dev/null 2>&1; then
  echo "dashboardCreate returned errors, see above." >&2
  exit 1
fi

GUID=$(echo "$RESPONSE" | jq -r '.data.dashboardCreate.entityResult.guid // empty')
if [[ -z "$GUID" ]]; then
  echo "No entityResult returned — check the response above." >&2
  exit 1
fi

echo "Dashboard created: guid=$GUID"
