#!/usr/bin/env bash
# stop-infra-ccloud.sh — stop and remove the Confluent Cloud OTel Collector container.
#
# Usage: ./stop-infra-ccloud.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Stopping Confluent Cloud infrastructure..."
docker compose -f "$SCRIPT_DIR/docker-compose.ccloud.yml" down
echo "Done."
