#!/usr/bin/env bash
# Deploy a New Relic dashboard via the NerdGraph API — idempotent.
#
# Searches for an existing dashboard with the same name in the target account.
# If found, updates it in place (dashboardUpdate). If not, creates a new one
# (dashboardCreate). Re-running the script never produces duplicates.
#
# When called with no arguments the script lists every JSON file in dashboards/
# and prompts you to pick one. Pass a path directly to skip the prompt:
#
#   ./deploy_nr_dashboard.sh                              # interactive picker
#   ./deploy_nr_dashboard.sh dashboards/newrelic-dashboard-python.json
#
# Prerequisites:
#   jq, NEW_RELIC_API_KEY (User API key, starts with NRAK-...), NEW_RELIC_ACCOUNT_ID
#   (both can live in .env — the script loads it automatically).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARDS_DIR="$SCRIPT_DIR/dashboards"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Load .env if present ──────────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

# ── Prerequisites ─────────────────────────────────────────────────────────────
command -v jq >/dev/null || { echo "ERROR: jq is required." >&2; exit 1; }
: "${NEW_RELIC_API_KEY:?Set NEW_RELIC_API_KEY (a User API key, starts with NRAK-...) in .env first}"
NEW_RELIC_ACCOUNT_ID="${NEW_RELIC_ACCOUNT_ID:-0}"

# ── Resolve dashboard file ────────────────────────────────────────────────────
if [[ $# -ge 1 ]]; then
  DASHBOARD_FILE="$1"
else
  if [[ ! -d "$DASHBOARDS_DIR" ]]; then
    echo "ERROR: dashboards/ directory not found at $DASHBOARDS_DIR" >&2
    exit 1
  fi

  mapfile -t FILES < <(find "$DASHBOARDS_DIR" -maxdepth 1 -name "*.json" | sort)

  if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "ERROR: No JSON files found in $DASHBOARDS_DIR" >&2
    exit 1
  fi

  echo ""
  echo "Available dashboards:"
  for i in "${!FILES[@]}"; do
    printf "  [%d] %s\n" "$((i+1))" "$(basename "${FILES[$i]}")"
  done
  echo ""

  while true; do
    read -rp "Pick a dashboard [1-${#FILES[@]}]: " CHOICE
    if [[ "$CHOICE" =~ ^[0-9]+$ ]] && (( CHOICE >= 1 && CHOICE <= ${#FILES[@]} )); then
      DASHBOARD_FILE="${FILES[$((CHOICE-1))]}"
      break
    fi
    echo "  Please enter a number between 1 and ${#FILES[@]}."
  done
fi

[[ -f "$DASHBOARD_FILE" ]] || { echo "ERROR: Dashboard file not found: $DASHBOARD_FILE" >&2; exit 1; }

# ── NerdGraph endpoint (US vs EU) ─────────────────────────────────────────────
NERDGRAPH_URL="https://api.newrelic.com/graphql"
case "${NEW_RELIC_OTLP_ENDPOINT:-}" in
  *eu*) NERDGRAPH_URL="https://api.eu.newrelic.com/graphql" ;;
esac

# ── Helper: POST a NerdGraph query ────────────────────────────────────────────
nerdgraph() {
  curl -sS "$NERDGRAPH_URL" \
    -H "Api-Key: $NEW_RELIC_API_KEY" \
    -H "Content-Type: application/json" \
    -d "$1"
}

# ── Normalise dashboard JSON ──────────────────────────────────────────────────
# Handles dashboards from multiple sources with slightly different shapes:
#  1. permissions: null              → "PUBLIC_READ_WRITE"
#  2. variables[].nrqlQuery.accountId (singular) → accountIds: [$acct] array
#  3. widgets[].linkedEntityGuids: null → []
#  4. accountId  (singular) → $acct
#  5. accountIds (plural array) → [$acct]
DASHBOARD=$(jq --argjson acct "$NEW_RELIC_ACCOUNT_ID" '
  if .permissions == null then .permissions = "PUBLIC_READ_WRITE" else . end |
  if .variables then
    .variables = [
      .variables[] |
      if .nrqlQuery and (.nrqlQuery | has("accountId")) and (.nrqlQuery | has("accountIds") | not) then
        .nrqlQuery = { accountIds: [$acct], query: .nrqlQuery.query }
      else . end
    ]
  else . end |
  walk(
    if type == "object" and has("linkedEntityGuids") and .linkedEntityGuids == null then
      .linkedEntityGuids = []
    elif type == "object" and has("accountId") then
      .accountId = $acct
    elif type == "object" and has("accountIds") then
      .accountIds = [$acct]
    else . end
  )
' "$DASHBOARD_FILE")

DASHBOARD_NAME=$(echo "$DASHBOARD" | jq -r '.name')
echo "==> Deploying: \"$DASHBOARD_NAME\" (account $NEW_RELIC_ACCOUNT_ID)"

# ── Search for an existing dashboard with the same name ───────────────────────
echo "    Checking for existing dashboard..."
SEARCH_PAYLOAD=$(jq -n --arg name "$DASHBOARD_NAME" --argjson acct "$NEW_RELIC_ACCOUNT_ID" '{
  query: "query($acct: Int!, $name: String!) { actor { account(id: $acct) { dashboards(query: {title: $name}) { results { guid name } } } } }",
  variables: { acct: $acct, name: $name }
}')

SEARCH_RESPONSE=$(nerdgraph "$SEARCH_PAYLOAD")

if echo "$SEARCH_RESPONSE" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
  echo "ERROR: Search query failed:" >&2
  echo "$SEARCH_RESPONSE" | jq . >&2
  exit 1
fi

EXISTING_GUID=$(echo "$SEARCH_RESPONSE" | jq -r '
  .data.actor.account.dashboards.results[]
  | select(.name == "'"$DASHBOARD_NAME"'")
  | .guid' 2>/dev/null | head -1)

# ── Create or update ──────────────────────────────────────────────────────────
if [[ -n "$EXISTING_GUID" ]]; then
  echo "    Found existing dashboard (guid=$EXISTING_GUID) — updating..."
  UPDATE_PAYLOAD=$(jq -n \
    --arg guid "$EXISTING_GUID" \
    --argjson dashboard "$DASHBOARD" '{
      query: "mutation($guid: EntityGuid!, $dashboard: DashboardInput!) { dashboardUpdate(guid: $guid, dashboard: $dashboard) { entityResult { guid name } errors { description type } } }",
      variables: { guid: $guid, dashboard: $dashboard }
    }')

  RESPONSE=$(nerdgraph "$UPDATE_PAYLOAD")
  echo "$RESPONSE" | jq .

  if echo "$RESPONSE" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
    echo "ERROR: GraphQL request failed, see above." >&2; exit 1
  fi
  if ! echo "$RESPONSE" | jq -e '.data.dashboardUpdate.errors // [] | length == 0' >/dev/null 2>&1; then
    echo "ERROR: dashboardUpdate returned errors, see above." >&2; exit 1
  fi

  GUID=$(echo "$RESPONSE" | jq -r '.data.dashboardUpdate.entityResult.guid // empty')
  echo "Dashboard updated: guid=$GUID"

else
  echo "    No existing dashboard found — creating..."
  CREATE_PAYLOAD=$(jq -n \
    --argjson dashboard "$DASHBOARD" \
    --argjson acct "$NEW_RELIC_ACCOUNT_ID" '{
      query: "mutation($accountId: Int!, $dashboard: DashboardInput!) { dashboardCreate(accountId: $accountId, dashboard: $dashboard) { entityResult { guid name } errors { description type } } }",
      variables: { accountId: $acct, dashboard: $dashboard }
    }')

  RESPONSE=$(nerdgraph "$CREATE_PAYLOAD")
  echo "$RESPONSE" | jq .

  if echo "$RESPONSE" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
    echo "ERROR: GraphQL request failed, see above." >&2; exit 1
  fi
  if ! echo "$RESPONSE" | jq -e '.data.dashboardCreate.errors // [] | length == 0' >/dev/null 2>&1; then
    echo "ERROR: dashboardCreate returned errors, see above." >&2; exit 1
  fi

  GUID=$(echo "$RESPONSE" | jq -r '.data.dashboardCreate.entityResult.guid // empty')
  if [[ -z "$GUID" ]]; then
    echo "ERROR: No entityResult returned — check the response above." >&2; exit 1
  fi
  echo "Dashboard created: guid=$GUID"
fi
