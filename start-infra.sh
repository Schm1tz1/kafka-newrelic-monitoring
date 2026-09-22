#!/usr/bin/env bash
# start-infra.sh — spin up local Kafka + OpenTelemetry Collector, then create the demo topic.
#
# Usage:
#   NEW_RELIC_LICENSE_KEY=<your-key> ./start-infra.sh
#
# Or load your .env first:
#   cp env.example .env  # (once) fill in NEW_RELIC_LICENSE_KEY
#   set -a; . ./.env; set +a
#   ./start-infra.sh

set -euo pipefail

# ── 1. Guard: NEW_RELIC_LICENSE_KEY must be set ──────────────────────────────
if [[ -z "${NEW_RELIC_LICENSE_KEY:-}" ]]; then
  echo "ERROR: NEW_RELIC_LICENSE_KEY is not set." >&2
  echo "  Export it directly or load your .env:  set -a; . ./.env; set +a" >&2
  exit 1
fi

# ── 2. Start Kafka and the OTel Collector ────────────────────────────────────
echo "==> Starting Kafka and OTel Collector..."
docker compose up -d kafka otel-collector

# ── 3. Wait for Kafka to become ready ────────────────────────────────────────
echo "==> Waiting for Kafka broker to be ready..."
RETRIES=30
until docker compose exec -T kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --list &>/dev/null; do
  RETRIES=$((RETRIES - 1))
  if [[ $RETRIES -eq 0 ]]; then
    echo "ERROR: Kafka did not become ready in time." >&2
    docker compose logs kafka | tail -20 >&2
    exit 1
  fi
  echo "   ...not ready yet, retrying in 2 s ($RETRIES attempts left)"
  sleep 2
done
echo "   Kafka is ready."

# ── 4. Create the demo topic ─────────────────────────────────────────────────
TOPIC="${KAFKA_TOPIC:-demo-events}"
echo "==> Creating topic '${TOPIC}' (if it doesn't exist)..."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions 3 \
  --replication-factor 1
echo "   Topic '${TOPIC}' is ready."

# ── 5. Done ───────────────────────────────────────────────────────────────────
echo ""
echo "Infrastructure is up. Next steps:"
echo "  set -a; . ./.env; set +a"
echo "  python -m python.confluent_kafka.consumer   # terminal 1"
echo "  python -m python.confluent_kafka.producer   # terminal 2"
