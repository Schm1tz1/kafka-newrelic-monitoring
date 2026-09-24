#!/usr/bin/env bash
# start-infra-cp.sh — spin up local Confluent Platform Kafka + OpenTelemetry Collector
# + nri-prometheus (JMX broker metrics), create the demo topic, and deploy the
# New Relic dashboard.
#
# Metrics pipelines:
#   • Python app  --OTLP/gRPC--> OTel Collector --OTLP/HTTP--> New Relic
#   • Kafka broker JMX --jmx_prometheus_javaagent:1234--> nri-prometheus --> New Relic
#
# Usage:
#   NEW_RELIC_LICENSE_KEY=<your-key> ./start-infra-cp.sh
#
# Or load your .env first:
#   cp env.example .env  # (once) fill in NEW_RELIC_LICENSE_KEY
#   set -a; . ./.env; set +a
#   ./start-infra-cp.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Load .env if present ──────────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

# ── 1. Guard: NEW_RELIC_LICENSE_KEY must be set ──────────────────────────────
if [[ -z "${NEW_RELIC_LICENSE_KEY:-}" ]]; then
  echo "ERROR: NEW_RELIC_LICENSE_KEY is not set." >&2
  echo "  Export it directly or load your .env:  set -a; . ./.env; set +a" >&2
  exit 1
fi

# ── 2. Start Kafka (with JMX agent), OTel Collector, and nri-prometheus ──────
echo "==> Starting Kafka (+ JMX agent), OTel Collector, and nri-prometheus..."
docker compose -f "$SCRIPT_DIR/docker-compose.cp.yml" up -d kafka otel-collector nri-prometheus

# ── 3. Wait for Kafka to become ready ────────────────────────────────────────
echo "==> Waiting for Kafka broker to be ready..."
RETRIES=30
until docker compose -f "$SCRIPT_DIR/docker-compose.cp.yml" \
      exec -T kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --list &>/dev/null; do
  RETRIES=$((RETRIES - 1))
  if [[ $RETRIES -eq 0 ]]; then
    echo "ERROR: Kafka did not become ready in time." >&2
    docker compose -f "$SCRIPT_DIR/docker-compose.cp.yml" logs kafka | tail -20 >&2
    exit 1
  fi
  echo "   ...not ready yet, retrying in 2 s ($RETRIES attempts left)"
  sleep 2
done
echo "   Kafka is ready."

# ── 4. Create the demo topic ─────────────────────────────────────────────────
TOPIC="${KAFKA_TOPIC:-demo-events}"
echo "==> Creating topic '${TOPIC}' (if it doesn't exist)..."
docker compose -f "$SCRIPT_DIR/docker-compose.cp.yml" \
  exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 3 \
    --replication-factor 1
echo "   Topic '${TOPIC}' is ready."

# ── 5. Deploy New Relic dashboard ────────────────────────────────────────────
DASHBOARD_FILE="$SCRIPT_DIR/dashboards/newrelic-dashboard-cp.json"
if [[ -f "$DASHBOARD_FILE" ]] && command -v jq &>/dev/null \
   && [[ -n "${NEW_RELIC_API_KEY:-}" ]] && [[ -n "${NEW_RELIC_ACCOUNT_ID:-}" ]]; then
  echo "==> Deploying New Relic dashboard..."
  "$SCRIPT_DIR/deploy_nr_dashboard.sh" "$DASHBOARD_FILE" && \
    echo "   Dashboard deployed." || \
    echo "   Dashboard deploy failed — check credentials, continuing anyway."
else
  echo "==> Skipping dashboard deploy (missing: jq, NEW_RELIC_API_KEY, NEW_RELIC_ACCOUNT_ID,"
  echo "    or dashboards/newrelic-dashboard-python.json). Run deploy_nr_dashboard.sh manually later."
fi

# ── 6. Done ───────────────────────────────────────────────────────────────────
cat <<EOF

Infrastructure is up (Confluent Platform local mode).

Metrics pipelines:
  • Python app OTLP  :4317/:4318  --> OTel Collector --> New Relic
  • Kafka broker JMX :1234        --> nri-prometheus  --> New Relic

Next steps:
  set -a; . ./.env; set +a
  python -m python.confluent_kafka.consumer   # terminal 1
  python -m python.confluent_kafka.producer   # terminal 2

To stop:
  docker compose -f docker-compose.cp.yml down
EOF
