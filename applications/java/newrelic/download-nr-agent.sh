#!/usr/bin/env bash
# download-nr-agent.sh — download the New Relic Java agent into this directory.
#
# Usage:
#   ./applications/java/newrelic/download-nr-agent.sh
#
# After downloading, run the producer or consumer with:
#   java -javaagent:applications/java/newrelic/newrelic.jar \
#        -Dnewrelic.config.file=applications/java/newrelic/newrelic.yml \
#        -cp applications/java/target/kafka-monitoring-1.0-SNAPSHOT.jar \
#        com.example.kafka.Producer

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NR_AGENT_VERSION="8.21.0"
NR_AGENT_JAR="$SCRIPT_DIR/newrelic.jar"

if [[ -f "$NR_AGENT_JAR" ]]; then
  echo "newrelic.jar already present at $NR_AGENT_JAR — skipping download."
  exit 0
fi

echo "==> Downloading New Relic Java agent v${NR_AGENT_VERSION}..."
curl -fsSL \
  "https://download.newrelic.com/newrelic/java-agent/newrelic-agent/${NR_AGENT_VERSION}/newrelic-agent-${NR_AGENT_VERSION}.jar" \
  -o "$NR_AGENT_JAR"

echo "    Saved to $NR_AGENT_JAR"
echo ""
echo "Run with agent:"
echo "  java -javaagent:$NR_AGENT_JAR \\"
echo "       -Dnewrelic.config.file=$SCRIPT_DIR/newrelic.yml \\"
echo "       -cp applications/java/target/kafka-monitoring-1.0-SNAPSHOT.jar \\"
echo "       com.example.kafka.Producer"
