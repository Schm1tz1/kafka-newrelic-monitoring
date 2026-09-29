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

BASE_NAME=$(echo "$DASHBOARD" | jq -r '.name')

# Append team name as a suffix so dashboard names are unique across teams.
if [[ -n "${NEW_RELIC_TEAM_NAME:-}" ]]; then
  DASHBOARD_NAME="${BASE_NAME} [${NEW_RELIC_TEAM_NAME}]"
  DASHBOARD=$(echo "$DASHBOARD" | jq --arg name "$DASHBOARD_NAME" '.name = $name')
else
  DASHBOARD_NAME="$BASE_NAME"
fi

echo "==> Deploying: \"$DASHBOARD_NAME\" (account $NEW_RELIC_ACCOUNT_ID)"

# ── Search for an existing dashboard with the same name ───────────────────────
# When NEW_RELIC_TEAM_NAME is set we search for BOTH the suffixed name and the
# bare base name. This handles migration: a dashboard previously deployed without
# a team suffix is found and updated (renamed) rather than duplicated.
echo "    Checking for existing dashboard..."
# entitySearch is the correct API for finding dashboards by name.
# domainType = 'VIZ-DASHBOARD' scopes results to dashboards only.
# accountId filter ensures we only match within the target account.
SEARCH_PAYLOAD=$(jq -n \
  --arg query "domainType IN ('VIZ-DASHBOARD') AND name = '${DASHBOARD_NAME//\'/\'\'}' AND accountId = ${NEW_RELIC_ACCOUNT_ID}" '{
  query: "query($q: String!) { actor { entitySearch(query: $q) { results { entities { guid name } } } } }",
  variables: { q: $query }
}')

SEARCH_RESPONSE=$(nerdgraph "$SEARCH_PAYLOAD")

if echo "$SEARCH_RESPONSE" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
  echo "ERROR: Search query failed:" >&2
  echo "$SEARCH_RESPONSE" | jq . >&2
  exit 1
fi

EXISTING_GUID=$(echo "$SEARCH_RESPONSE" | jq -r '
  .data.actor.entitySearch.results.entities[]
  | select(.name == "'"$DASHBOARD_NAME"'")
  | .guid' 2>/dev/null | head -1)

# If not found by suffixed name, try the bare base name (migration from un-suffixed deploy).
if [[ -z "$EXISTING_GUID" ]] && [[ "$DASHBOARD_NAME" != "$BASE_NAME" ]]; then
  SEARCH_PAYLOAD_BASE=$(jq -n \
    --arg query "domainType IN ('VIZ-DASHBOARD') AND name = '${BASE_NAME//\'/\'\'}' AND accountId = ${NEW_RELIC_ACCOUNT_ID}" '{
    query: "query($q: String!) { actor { entitySearch(query: $q) { results { entities { guid name } } } } }",
    variables: { q: $query }
  }')
  SEARCH_RESPONSE_BASE=$(nerdgraph "$SEARCH_PAYLOAD_BASE")
  EXISTING_GUID=$(echo "$SEARCH_RESPONSE_BASE" | jq -r '
    .data.actor.entitySearch.results.entities[]
    | select(.name == "'"$BASE_NAME"'")
    | .guid' 2>/dev/null | head -1)
  if [[ -n "$EXISTING_GUID" ]]; then
    echo "    Found existing dashboard under base name \"$BASE_NAME\" (guid=$EXISTING_GUID) — will rename to \"$DASHBOARD_NAME\"."
  fi
fi

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
  # Newly created entities are not immediately visible to entityManagement —
  # wait briefly for the entity index to catch up before attempting team assignment.
  DASHBOARD_JUST_CREATED=true
fi

# ── Assign to New Relic team (optional) ──────────────────────────────────────
# If NEW_RELIC_TEAM_NAME is set, find the team's ownership collection and add
# the dashboard entity to it. This causes the nr.team tag to appear on the entity.
if [[ -n "${NEW_RELIC_TEAM_NAME:-}" ]]; then
  echo "==> Assigning dashboard to team \"$NEW_RELIC_TEAM_NAME\"..."

  if [[ "${DASHBOARD_JUST_CREATED:-false}" == "true" ]]; then
    echo "    Waiting for entity index to catch up after create..."
    sleep 10
  fi

  TEAMS_PAYLOAD=$(jq -n '{
    query: "{ actor { entityManagement { entitySearch(query: \"type = '"'"'TEAM'"'"'\") { entities { id name ... on EntityManagementTeamEntity { ownership { id } } } } } } }"
  }')

  TEAMS_RESPONSE=$(nerdgraph "$TEAMS_PAYLOAD")

  if echo "$TEAMS_RESPONSE" | jq -e '.errors // [] | length > 0' >/dev/null 2>&1; then
    echo "WARNING: Team search failed — skipping team assignment:" >&2
    echo "$TEAMS_RESPONSE" | jq . >&2
  else
    OWNERSHIP_COLLECTION_ID=$(echo "$TEAMS_RESPONSE" | jq -r \
      --arg name "$NEW_RELIC_TEAM_NAME" \
      '.data.actor.entityManagement.entitySearch.entities[]
       | select(.name == $name)
       | .ownership.id // empty' | head -1)

    if [[ -z "$OWNERSHIP_COLLECTION_ID" ]]; then
      echo "WARNING: Team \"$NEW_RELIC_TEAM_NAME\" not found or has no ownership collection — skipping." >&2
    else
      echo "    Found ownership collection: $OWNERSHIP_COLLECTION_ID"
      ASSIGN_PAYLOAD=$(jq -n \
        --arg collectionId "$OWNERSHIP_COLLECTION_ID" \
        --arg entityId "$GUID" '{
          query: "mutation($collectionId: ID!, $ids: [ID!]!) { entityManagementAddCollectionMembers(collectionId: $collectionId, ids: $ids) }",
          variables: { collectionId: $collectionId, ids: [$entityId] }
        }')

      ASSIGN_RESPONSE=$(nerdgraph "$ASSIGN_PAYLOAD")

      # NOT_FOUND on a freshly created dashboard means the entity index still hasn't
      # caught up — retry once after a further wait.
      if echo "$ASSIGN_RESPONSE" | jq -e '
          [.errors // [] | .[] | select(.extensions.errorClass == "NOT_FOUND")] | length > 0
        ' >/dev/null 2>&1; then
        echo "    Entity not yet indexed — retrying in 15 s..."
        sleep 15
        ASSIGN_RESPONSE=$(nerdgraph "$ASSIGN_PAYLOAD")
      fi

      # COLLECTION_DUPLICATE_MEMBER means the entity is already in the team — idempotent, not an error.
      NON_DUPE_ERRORS=$(echo "$ASSIGN_RESPONSE" | jq '
        [.errors // [] | .[] | select(.extensions.errorClass != "COLLECTION_DUPLICATE_MEMBER")]
      ')
      if echo "$NON_DUPE_ERRORS" | jq -e 'length > 0' >/dev/null 2>&1; then
        echo "WARNING: Team assignment request failed:" >&2
        echo "$ASSIGN_RESPONSE" | jq . >&2
      else
        echo "Dashboard assigned to team \"$NEW_RELIC_TEAM_NAME\"."
      fi
    fi
  fi
fi
