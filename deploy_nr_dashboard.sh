#!/usr/bin/env bash
# Deploy a New Relic dashboard via the NerdGraph API.
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
  # Build list of JSON files in dashboards/
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
echo "==> Deploying: $(basename "$DASHBOARD_FILE") (account $NEW_RELIC_ACCOUNT_ID)"

# ── NerdGraph endpoint (US vs EU) ─────────────────────────────────────────────
NERDGRAPH_URL="https://api.newrelic.com/graphql"
case "${NEW_RELIC_OTLP_ENDPOINT:-}" in
  *eu*) NERDGRAPH_URL="https://api.eu.newrelic.com/graphql" ;;
esac

# ── Normalise + rewrite before POST ──────────────────────────────────────────
# Handles dashboards from multiple sources that have slightly different shapes:
#
#  1. permissions: null              → "PUBLIC_READ_WRITE"   (NerdGraph requires non-null)
#  2. variables[].nrqlQuery.accountId (singular, some sources use this)
#                                    → accountIds: [$acct]   (API requires the array form;
#                                                              drop the singular key)
#  3. widgets[].linkedEntityGuids: null → []                 (API rejects null here)
#  4. accountId  (singular, widget nrqlQueries) → $acct
#  5. accountIds (plural array)      → [$acct]
DASHBOARD=$(jq --argjson acct "$NEW_RELIC_ACCOUNT_ID" '
  # 1. Ensure permissions is set
  if .permissions == null then .permissions = "PUBLIC_READ_WRITE" else . end |

  # 2. Fix variable nrqlQuery: rename accountId → accountIds array, drop singular key
  if .variables then
    .variables = [
      .variables[] |
      if .nrqlQuery and (.nrqlQuery | has("accountId")) and (.nrqlQuery | has("accountIds") | not) then
        .nrqlQuery = { accountIds: [$acct], query: .nrqlQuery.query }
      else . end
    ]
  else . end |

  # 3 + 4 + 5. Fix linkedEntityGuids null and rewrite all accountId/accountIds
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
