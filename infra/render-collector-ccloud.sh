#!/usr/bin/env bash
# render-collector-ccloud.sh — fill the Confluent Cloud Collector template and write
# collector/otel-collector-ccloud.resolved.yaml. Does not touch Docker.
#
# Reads from the environment:
#   CONFLUENT_CLUSTER_ID                  required; single ID or comma-separated list
#   CONFLUENT_AUDIT_BOOTSTRAP_SERVERS,
#   CONFLUENT_AUDIT_SASL_USERNAME,
#   CONFLUENT_AUDIT_SASL_PASSWORD         all three set -> audit + connector (Plan A) pipelines
#   CONFLUENT_ENVIRONMENT_ID plus CLI auth (~/.confluent/config.json, or
#   CONFLUENT_CLOUD_EMAIL + CONFLUENT_CLOUD_PASSWORD)
#                                         -> CLI connector log pipeline (Plan B)
#   CONFLUENT_CONNECTOR_IDS               optional, defaults to * (auto-discover)
#
# Usage:
#   set -a; . ./.env; set +a
#   ./infra/render-collector-ccloud.sh
#
# start-infra-ccloud.sh sources this file, so AUDIT_LOGS_ENABLED,
# CLI_CONNECTOR_LOGS_ENABLED and _CLI_IDS are available to it afterwards.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${CONFLUENT_CLUSTER_ID:-}" ]]; then
  echo "ERROR: CONFLUENT_CLUSTER_ID is not set." >&2
  echo "  Copy env.example to .env, fill in the values, then:" >&2
  echo "  set -a; . ./.env; set +a" >&2
  exit 1
fi

# Detect whether audit log vars are all set — enables the Kafka-receiver log pipelines.
AUDIT_LOGS_ENABLED=false
if [[ -n "${CONFLUENT_AUDIT_BOOTSTRAP_SERVERS:-}" ]] \
   && [[ -n "${CONFLUENT_AUDIT_SASL_USERNAME:-}" ]] \
   && [[ -n "${CONFLUENT_AUDIT_SASL_PASSWORD:-}" ]]; then
  AUDIT_LOGS_ENABLED=true
fi

# Detect whether CLI wrapper connector log vars are set — enables the filelog pipeline.
# Requires CONFLUENT_ENVIRONMENT_ID plus ONE of:
#   a) ~/.confluent/config.json present on the host (mounted into the container), OR
#   b) CONFLUENT_CLOUD_EMAIL + CONFLUENT_CLOUD_PASSWORD set in env.
# CONFLUENT_CONNECTOR_IDS defaults to * (auto-discover) so it need not be explicit.
CLI_CONNECTOR_LOGS_ENABLED=false
_CLI_IDS="${CONFLUENT_CONNECTOR_IDS:-${CONFLUENT_CONNECTOR_ID:-*}}"
_HAS_CLI_AUTH=false
[[ -f "${HOME}/.confluent/config.json" ]] && _HAS_CLI_AUTH=true
[[ -n "${CONFLUENT_CLOUD_EMAIL:-}" ]] && [[ -n "${CONFLUENT_CLOUD_PASSWORD:-}" ]] && _HAS_CLI_AUTH=true
if [[ "$_HAS_CLI_AUTH" == "true" ]] && [[ -n "${CONFLUENT_ENVIRONMENT_ID:-}" ]]; then
  CLI_CONNECTOR_LOGS_ENABLED=true
fi

# CONFLUENT_CLUSTER_ID may be a single ID or a comma-separated list of IDs.
# The OTel Collector cannot expand env vars into YAML lists, so we generate a
# resolved config file with the list entries written out explicitly.
COLLECTOR_TEMPLATE="$SCRIPT_DIR/collector/otel-collector-ccloud.yaml"
COLLECTOR_RESOLVED="$SCRIPT_DIR/collector/otel-collector-ccloud.resolved.yaml"

# Three transformations run over the template in a single awk pass:
#  1. Replace PLACEHOLDER_CLUSTER_ID with one "- lkc-xxx" per cluster ID.
#  2. Strip [AUDIT_LOGS] ... [/AUDIT_LOGS] blocks when audit log vars are not set.
#  3. Strip [CLI_CONNECTOR_LOGS] ... [/CLI_CONNECTOR_LOGS] blocks when CLI vars are not set.
# awk is used instead of sed because the replacements span multiple lines, and
# BSD sed (macOS) does not support multi-line replacements in a portable way.
awk -v ids="${CONFLUENT_CLUSTER_ID}" \
    -v audit="${AUDIT_LOGS_ENABLED}" \
    -v cli="${CLI_CONNECTOR_LOGS_ENABLED}" '
  /^# \[AUDIT_LOGS\]$/           { skip = (audit != "true"); next }
  /^# \[\/AUDIT_LOGS\]$/         { skip = 0; next }
  /^# \[CLI_CONNECTOR_LOGS\]$/   { skip = (cli != "true"); next }
  /^# \[\/CLI_CONNECTOR_LOGS\]$/ { skip = 0; next }
  skip { next }
  /^              - PLACEHOLDER_CLUSTER_ID$/ {
    n = split(ids, arr, /,/)
    for (i = 1; i <= n; i++) {
      id = arr[i]
      gsub(/^ +| +$/, "", id)   # trim spaces around commas
      if (id != "") print "              - " id
    }
    next
  }
  { print }
' "$COLLECTOR_TEMPLATE" > "$COLLECTOR_RESOLVED"

echo "==> Resolved collector config written to collector/otel-collector-ccloud.resolved.yaml"
echo "    Cluster IDs: ${CONFLUENT_CLUSTER_ID}"
if [[ "$AUDIT_LOGS_ENABLED" == "true" ]]; then
  echo "    Audit log pipelines (Kafka receiver):      ENABLED"
else
  echo "    Audit log pipelines (Kafka receiver):      DISABLED (set CONFLUENT_AUDIT_* vars to enable)"
fi
if [[ "$CLI_CONNECTOR_LOGS_ENABLED" == "true" ]]; then
  echo "    Connector log pipeline (CLI wrapper):      ENABLED  (connectors: ${_CLI_IDS})"
else
  echo "    Connector log pipeline (CLI wrapper):      DISABLED (set CONFLUENT_ENVIRONMENT_ID + CONFLUENT_CONNECTOR_IDS to enable)"
fi
