#!/usr/bin/env bash
# confluent-connector-logs.sh
#
# Polls connector logs using `confluent connect logs <id>` and writes one JSON
# line per log entry to a named pipe for the OTel Collector filelog receiver.
#
# The Confluent CLI `connect event consume` command no longer exists; the
# replacement is a windowed polling API: `confluent connect logs <id>
# --start-time <t> --end-time <t>`. This script runs a poll loop per connector
# that advances the time window forward on each iteration.
#
# Authentication uses a mounted config.json (token auto-refresh) or
# `confluent login --save` with CONFLUENT_CLOUD_EMAIL + CONFLUENT_CLOUD_PASSWORD.
#
# Required environment variables:
#   CONFLUENT_CLUSTER_ID         — Kafka cluster ID  (lkc-...)
#   CONFLUENT_ENVIRONMENT_ID     — Environment ID    (env-...)
#
# One of:
#   CONFLUENT_CONNECTOR_IDS      — * (auto-discover all) or comma-separated lcc-... list
#   CONFLUENT_CONNECTOR_ID       — single connector ID (legacy)
#
# Optional:
#   CONNECTOR_LOGS_FIFO          — path to named pipe  (default: /fifo/connector-logs.fifo)
#   CONNECTOR_POLL_SECS          — seconds between poll windows  (default: 60)
#   CONNECTOR_LOG_LEVEL          — log level filter    (default: ERROR|WARN|INFO)
#   CONNECTOR_REDISCOVER_SECS    — re-check interval for new connectors when using *
#                                   (default: 300 — 5 minutes)
#
# Usage (Docker — see docker-compose.ccloud.yml --profile connector-logs-cli):
#   docker compose -f infra/docker-compose.ccloud.yml \
#     --profile connector-logs-cli up -d

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
: "${CONFLUENT_CLUSTER_ID:?Set CONFLUENT_CLUSTER_ID (lkc-...)}"
: "${CONFLUENT_ENVIRONMENT_ID:?Set CONFLUENT_ENVIRONMENT_ID (env-...)}"

FIFO="${CONNECTOR_LOGS_FIFO:-/fifo/connector-logs.fifo}"
POLL_SECS="${CONNECTOR_POLL_SECS:-60}"
LOG_LEVEL="${CONNECTOR_LOG_LEVEL:-ERROR|WARN|INFO}"
REDISCOVER_SECS="${CONNECTOR_REDISCOVER_SECS:-300}"

# CONFLUENT_CONNECTOR_IDS takes precedence over the legacy CONFLUENT_CONNECTOR_ID.
CONNECTOR_IDS_RAW="${CONFLUENT_CONNECTOR_IDS:-${CONFLUENT_CONNECTOR_ID:-*}}"

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [connector-logs] $*" >&2; }

# List all connector IDs on the cluster via the CLI.
# jq is available in the confluentinc/confluent-cli image; python3 is not.
list_connectors() {
  confluent connect cluster list \
    --cluster     "$CONFLUENT_CLUSTER_ID" \
    --environment "$CONFLUENT_ENVIRONMENT_ID" \
    --output json \
  | jq -r '.[] | (.id // .connector_id // .connectorId // "") | select(startswith("lcc-"))' \
  2>/dev/null
}

# Resolve CONNECTOR_IDS_RAW → newline-separated list of connector IDs.
resolve_connectors() {
  if [[ "$CONNECTOR_IDS_RAW" == "*" ]]; then
    list_connectors
  else
    echo "$CONNECTOR_IDS_RAW" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$'
  fi
}

# ── Named pipe setup ──────────────────────────────────────────────────────────
# In Docker the FIFO is created by connector-logs-init before this container
# starts. For standalone use we create it here if needed.
# mkfifo fails if the file already exists (e.g. on container restart with a
# persistent volume), so guard with -p and suppress the error either way.
if [[ ! -p "$FIFO" ]]; then
  mkfifo -m 0666 "$FIFO" 2>/dev/null || true
  log "Created FIFO at $FIFO"
fi

# Hold the FIFO write-end open in a persistent file descriptor so the filelog
# receiver can open/close its read end without leaving the FIFO writerless
# (which would block subsequent writes).
exec 3>"$FIFO"

# ── Confluent CLI authentication ──────────────────────────────────────────────
CLI_CONFIG="${HOME:-/home/confluent}/.confluent/config.json"
if [[ -f "$CLI_CONFIG" ]]; then
  log "Found existing CLI credentials at $CLI_CONFIG — skipping login."
else
  : "${CONFLUENT_CLOUD_EMAIL:?Neither $CLI_CONFIG nor CONFLUENT_CLOUD_EMAIL is set.}"
  : "${CONFLUENT_CLOUD_PASSWORD:?Neither $CLI_CONFIG nor CONFLUENT_CLOUD_PASSWORD is set.}"
  log "Logging in to Confluent Cloud (email: ${CONFLUENT_CLOUD_EMAIL})..."
  confluent login --save 2>&1 | while IFS= read -r line; do log "login: $line"; done
  log "Login complete — credentials saved for auto-refresh."
fi

confluent environment use "$CONFLUENT_ENVIRONMENT_ID" >/dev/null
log "Active environment: $CONFLUENT_ENVIRONMENT_ID  Cluster: $CONFLUENT_CLUSTER_ID"

if [[ "$CONNECTOR_IDS_RAW" == "*" ]]; then
  log "Connector discovery: auto (all connectors, re-check every ${REDISCOVER_SECS}s)"
else
  log "Connector discovery: explicit list — $CONNECTOR_IDS_RAW"
fi

# ── Per-connector poll function ───────────────────────────────────────────────
# Polls logs for one connector in a sliding time window, writing NDJSON to fd 3
# (the FIFO). Each output line is a JSON object with the original API fields
# plus `connector_id` injected so the OTel filelog receiver can attribute it.
#
# The window advances: after each successful poll, start_time moves to end_time.
# Pagination is handled via --next until the CLI prints "No logs found".
poll_connector() {
  local cid="$1"
  # Start one poll-window back so we don't miss logs on first boot.
  # Use epoch arithmetic so this works on BusyBox date (Alpine) as well as
  # GNU date (Debian/Ubuntu) and BSD date (macOS).
  local window_start
  window_start=$(date -u -d "@$(( $(date -u +%s) - POLL_SECS ))" +%Y-%m-%dT%H:%M:%SZ)

  log "[$cid] Starting poll loop (window=${POLL_SECS}s, level=${LOG_LEVEL})"

  while true; do
    local window_end
    window_end=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # Drain all pages for this window.
    local page=0
    while true; do
      local flags=(
        --cluster     "$CONFLUENT_CLUSTER_ID"
        --environment "$CONFLUENT_ENVIRONMENT_ID"
        --level       "$LOG_LEVEL"
        --start-time  "$window_start"
        --end-time    "$window_end"
        --output      json
      )
      # --next advances the cursor for subsequent pages.
      [[ $page -gt 0 ]] && flags+=(--next)

      local raw
      raw=$(confluent connect logs "$cid" "${flags[@]}" 2>&1) || true

      # CLI prints this literal string when pagination is exhausted.
      if echo "$raw" | grep -q "No logs found for the current query"; then
        break
      fi

      # Skip non-JSON output (error messages, warnings from the CLI).
      if ! echo "$raw" | jq -e '. | arrays' >/dev/null 2>&1; then
        if [[ -n "$raw" ]]; then
          log "[$cid] Unexpected output (page $page): $(echo "$raw" | head -3)"
        fi
        break
      fi

      # Emit one NDJSON line per log entry — write to fd 3 (the FIFO) and
      # mirror each line to stderr so it appears in `docker logs`.
      local lines
      lines=$(echo "$raw" | jq -r --arg cid "$cid" \
        '.[] | . + {connector_id: $cid} | tojson') || true
      if [[ -n "$lines" ]]; then
        echo "$lines" >&2          # mirror to container stdout / docker logs
        echo "$lines" >&3          # send to FIFO for OTel collector
      fi
      local nlines
      nlines=$(echo "$lines" | grep -c '^') || true
      log "[$cid] Emitted ${nlines} log line(s) to FIFO (page $page, window ${window_start}→${window_end})"

      page=$(( page + 1 ))
    done

    # Advance the window start to where we just ended.
    window_start="$window_end"

    sleep "$POLL_SECS"
  done
}

# ── Main loop ─────────────────────────────────────────────────────────────────
# Re-discovers connectors every REDISCOVER_SECS. Each connector gets its own
# background poll subshell; PIDs are tracked so stale polls can be reaped when
# a connector is deleted.
declare -A PIDS=()

cleanup() {
  log "Shutting down all connector polls..."
  for cid in "${!PIDS[@]}"; do
    kill "${PIDS[$cid]}" 2>/dev/null || true
  done
  wait
  exit 0
}
trap cleanup SIGTERM SIGINT

while true; do
  mapfile -t CONNECTORS < <(resolve_connectors)

  if [[ ${#CONNECTORS[@]} -eq 0 ]]; then
    log "No connectors found. Retrying in ${REDISCOVER_SECS}s..."
    sleep "$REDISCOVER_SECS"
    continue
  fi

  log "Active connectors (${#CONNECTORS[@]}): ${CONNECTORS[*]}"

  # Start a poll subprocess for any connector not already running.
  for cid in "${CONNECTORS[@]}"; do
    if [[ -n "${PIDS[$cid]:-}" ]] && kill -0 "${PIDS[$cid]}" 2>/dev/null; then
      continue
    fi
    poll_connector "$cid" &
    PIDS["$cid"]=$!
    log "[$cid] Spawned poll PID ${PIDS[$cid]}"
  done

  # Reap PIDs for connectors no longer in the discovered list (connector deleted).
  for cid in "${!PIDS[@]}"; do
    if ! printf '%s\n' "${CONNECTORS[@]}" | grep -qx "$cid"; then
      log "[$cid] Connector no longer present — stopping (PID ${PIDS[$cid]})..."
      kill "${PIDS[$cid]}" 2>/dev/null || true
      unset 'PIDS[$cid]'
    fi
  done

  # Sleep until next re-discovery, waking early if any child exits.
  sleep "$REDISCOVER_SECS" &
  SLEEP_PID=$!
  wait -n 2>/dev/null || true
  kill "$SLEEP_PID" 2>/dev/null || true
  wait "$SLEEP_PID" 2>/dev/null || true
done
