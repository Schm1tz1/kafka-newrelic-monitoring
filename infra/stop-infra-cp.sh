#!/usr/bin/env bash
# stop-infra-cp.sh — stop and remove all Confluent Platform containers.
#
# Usage: ./stop-infra-cp.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Stopping Confluent Platform infrastructure..."
docker compose -f "$SCRIPT_DIR/docker-compose.cp.yml" down
echo "Done."
