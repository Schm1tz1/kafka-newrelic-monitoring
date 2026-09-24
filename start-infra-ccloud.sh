#!/usr/bin/env bash
# start-infra-ccloud.sh — spin up the OTel Collector for Confluent Cloud, create the
# demo topic on the remote cluster, and deploy the Confluent Cloud New Relic dashboard.
#
# No local Kafka is started; the Python app connects directly to Confluent Cloud.
# The Collector always runs:
#   • prometheus/confluent_metrics  — scrapes the Confluent Metrics API every 60 s
#   • metrics/app_metrics           — receives OTLP push metrics from the Python app
# When CONFLUENT_AUDIT_BOOTSTRAP_SERVERS / _SASL_USERNAME / _SASL_PASSWORD are set,
# two additional log pipelines are enabled:
#   • logs/audit          — consumes confluent-audit-log-events
#   • logs/connector      — consumes confluent-connect-log-events
#
# Required environment variables (copy env.example → .env and fill in):
#   NEW_RELIC_LICENSE_KEY       — New Relic ingest license key
#   NEW_RELIC_OTLP_ENDPOINT     — e.g. https://otlp.eu01.nr-data.net
#   CONFLUENT_API_KEY           — Confluent Cloud API key (MetricsViewer role)
#   CONFLUENT_API_SECRET        — Confluent Cloud API secret
#   CONFLUENT_CLUSTER_ID        — Confluent Cloud cluster ID  (e.g. lkc-abc123)
#   KAFKA_BOOTSTRAP_SERVERS     — Confluent Cloud bootstrap URL
#   KAFKA_SASL_USERNAME         — Confluent Cloud Cluster API key
#   KAFKA_SASL_PASSWORD         — Confluent Cloud Cluster API secret
#
# Required for dashboard deploy:
#   NEW_RELIC_API_KEY           — User API key (starts with NRAK-...)
#   NEW_RELIC_ACCOUNT_ID        — Your New Relic account ID
#
# Optional — enables audit + connector log pipelines when all three are set:
#   CONFLUENT_AUDIT_BOOTSTRAP_SERVERS, CONFLUENT_AUDIT_SASL_USERNAME, CONFLUENT_AUDIT_SASL_PASSWORD
# Optional (uncomment matching params blocks in collector/otel-collector-ccloud.yaml):
#   CONFLUENT_SCHEMA_REGISTRY_ID
#   CONFLUENT_CONNECTOR_ID
#
# Usage:
#   set -a; . ./.env; set +a
#   ./start-infra-ccloud.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── 1. Guard: required variables ─────────────────────────────────────────────
MISSING=()
for var in NEW_RELIC_LICENSE_KEY NEW_RELIC_OTLP_ENDPOINT \
           CONFLUENT_API_KEY CONFLUENT_API_SECRET CONFLUENT_CLUSTER_ID \
           KAFKA_BOOTSTRAP_SERVERS KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD; do
  [[ -z "${!var:-}" ]] && MISSING+=("$var")
done

if [[ ${#MISSING[@]} -gt 0 ]]; then
  echo "ERROR: The following required variables are not set:" >&2
  for v in "${MISSING[@]}"; do echo "  $v" >&2; done
  echo "" >&2
  echo "  Copy env.example to .env, fill in the values, then:" >&2
  echo "  set -a; . ./.env; set +a" >&2
  exit 1
fi

# Detect whether audit log vars are all set — enables the two log pipelines.
AUDIT_LOGS_ENABLED=false
if [[ -n "${CONFLUENT_AUDIT_BOOTSTRAP_SERVERS:-}" ]] \
   && [[ -n "${CONFLUENT_AUDIT_SASL_USERNAME:-}" ]] \
   && [[ -n "${CONFLUENT_AUDIT_SASL_PASSWORD:-}" ]]; then
  AUDIT_LOGS_ENABLED=true
fi

# ── 2. Resolve collector config — expand CONFLUENT_CLUSTER_ID list ───────────
# CONFLUENT_CLUSTER_ID may be a single ID or a comma-separated list of IDs.
# The OTel Collector cannot expand env vars into YAML lists, so we generate a
# resolved config file (collector/otel-collector-ccloud.resolved.yaml) with the
# list entries written out explicitly before starting the container.
COLLECTOR_TEMPLATE="$SCRIPT_DIR/collector/otel-collector-ccloud.yaml"
COLLECTOR_RESOLVED="$SCRIPT_DIR/collector/otel-collector-ccloud.resolved.yaml"

# Two transformations run over the template in a single awk pass:
#  1. Replace the PLACEHOLDER_CLUSTER_ID line with one "- lkc-xxx" per cluster ID.
#  2. Strip [AUDIT_LOGS] ... [/AUDIT_LOGS] blocks when audit log vars are not set.
# awk is used instead of sed because the replacements span multiple lines, and
# BSD sed (macOS) does not support multi-line replacements in a portable way.
awk -v ids="${CONFLUENT_CLUSTER_ID}" -v audit="${AUDIT_LOGS_ENABLED}" '
  /^# \[AUDIT_LOGS\]$/  { skip = (audit != "true"); next }
  /^# \[\/AUDIT_LOGS\]$/ { skip = 0; next }
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
  echo "    Audit/connector log pipelines: ENABLED"
else
  echo "    Audit/connector log pipelines: DISABLED (set CONFLUENT_AUDIT_* vars to enable)"
fi

# ── 3. Start the OTel Collector only (no local Kafka) ────────────────────────
echo "==> Starting OTel Collector (Confluent Cloud mode)..."
docker compose -f "$SCRIPT_DIR/docker-compose.ccloud.yml" up -d otel-collector-ccloud

# ── 4. Create the demo topic on Confluent Cloud ───────────────────────────────
TOPIC="${KAFKA_TOPIC:-demo-events}"
echo "==> Creating topic '${TOPIC}' on Confluent Cloud (if it doesn't exist)..."

# Use kafka-topics.sh via a temporary cp-kafka container — self-contained,
# requires no confluent CLI login or environment context.
# Write the client config to a temp file so Docker can mount it
# (process substitution /dev/fd paths are not accessible inside containers).
_KAFKA_CFG=$(mktemp)
trap 'rm -f "$_KAFKA_CFG"' EXIT
printf 'security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";\n' \
  "${KAFKA_SASL_USERNAME}" "${KAFKA_SASL_PASSWORD}" > "$_KAFKA_CFG"

docker run --rm \
  -v "${_KAFKA_CFG}:/tmp/client.properties:ro" \
  confluentinc/cp-kafka:8.3.1 \
  kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --command-config /tmp/client.properties \
    --create --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 3 \
    --replication-factor 3
echo "   Topic '${TOPIC}' is ready."

# ── 5. Deploy New Relic dashboards ───────────────────────────────────────────
if command -v jq &>/dev/null \
   && [[ -n "${NEW_RELIC_API_KEY:-}" ]] && [[ -n "${NEW_RELIC_ACCOUNT_ID:-}" ]]; then
  for DASHBOARD_FILE in \
    "$SCRIPT_DIR/dashboards/newrelic-dashboard-ccloud.json" \
    "$SCRIPT_DIR/dashboards/newrelic-dashboard-ccloud-logs.json"; do
    if [[ -f "$DASHBOARD_FILE" ]]; then
      echo "==> Deploying $(basename "$DASHBOARD_FILE")..."
      "$SCRIPT_DIR/deploy_nr_dashboard.sh" "$DASHBOARD_FILE" && \
        echo "   Deployed." || \
        echo "   Deploy failed — check credentials, continuing anyway."
    fi
  done
else
  echo "==> Skipping dashboard deploy (missing: jq, NEW_RELIC_API_KEY, or NEW_RELIC_ACCOUNT_ID)."
  echo "    Run deploy_nr_dashboard.sh manually later."
fi

# ── 6. Done ───────────────────────────────────────────────────────────────────
cat <<EOF

Infrastructure is up (Confluent Cloud mode).

The Collector is:
  • Scraping the Confluent Metrics API every 60 s → New Relic
  • Listening on :4317 (gRPC) / :4318 (HTTP) for app OTLP push metrics

Next steps — run producer/consumer against Confluent Cloud:
  set -a; . ./.env; set +a
  python -m python.confluent_kafka.consumer   # terminal 1
  python -m python.confluent_kafka.producer   # terminal 2

To stop:
  docker compose -f docker-compose.ccloud.yml down
EOF
