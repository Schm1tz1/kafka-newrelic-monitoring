# Monitoring Options

This project supports multiple monitoring strategies for both Python and Java applications. They are not mutually exclusive — you can combine them.

## Overview

| Option | Language | What it covers | Code change? |
|---|---|---|---|
| [OTel SDK (built-in)](#1-opentelemetry-sdk-built-in) | Python + Java | Custom app metrics (lag, latency, errors) | Already instrumented |
| [New Relic Java Agent](#2-new-relic-java-agent) | Java | APM traces, Kafka client spans, JVM metrics | None — `-javaagent` flag |
| [New Relic Python Agent](#3-new-relic-python-agent) | Python | APM traces, Kafka client spans | None — `newrelic-admin` wrapper |
| [nri-jmx (New Relic JMX integration)](#4-nri-jmx) | Java | JMX MBeans (JVM + Kafka client internals) | None — sidecar container |
| [OTel Collector JMX receiver](#5-otel-collector-jmx-receiver) | Java | JMX MBeans via existing OTel Collector | None — collector config |
| [nri-prometheus (existing)](#6-nri-prometheus-existing) | Kafka broker | Broker JMX via Prometheus → New Relic | Already wired (broker only) |

---

## 1. OpenTelemetry SDK (built-in)

Already instrumented in both applications. Emits 8 custom metrics via OTLP to the OTel Collector (or directly to New Relic).

**Python**: [`applications/python/confluent_kafka/metrics.py`](applications/python/confluent_kafka/metrics.py)
**Java**: [`applications/java/src/main/java/com/example/kafka/KafkaMetrics.java`](applications/java/src/main/java/com/example/kafka/KafkaMetrics.java)

No further setup needed — this runs whenever you start the apps.

---

## 2. New Relic Java Agent

The New Relic APM agent attaches as a `-javaagent` and auto-instruments:
- Kafka `producer.send()` and `consumer.poll()` calls as distributed trace spans
- JVM metrics: heap usage, GC time/count, thread count, class loading
- External service calls, SQL queries (if any), error rates

**No code changes required.**

### Setup

```bash
# 1. Download the agent (once)
./applications/java/newrelic/download-nr-agent.sh

# 2. Build the app
cd applications/java && mvn package -q && cd ../..

# 3. Run with agent
NEW_RELIC_LICENSE_KEY=your-key \
java -javaagent:applications/java/newrelic/newrelic.jar \
     -Dnewrelic.config.file=applications/java/newrelic/newrelic.yml \
     -cp applications/java/target/kafka-monitoring-1.0-SNAPSHOT.jar \
     com.example.kafka.Consumer
```

Set `NEW_RELIC_APP_NAME` to override the application name shown in the New Relic APM UI:

```bash
NEW_RELIC_APP_NAME="my-kafka-consumer" \
NEW_RELIC_LICENSE_KEY=your-key \
java -javaagent:applications/java/newrelic/newrelic.jar ...
```

### What you see in New Relic

- **APM → Services**: the app appears as a service with transaction throughput, response time, and error rate
- **Distributed tracing**: producer and consumer spans linked across service boundaries
- **JVM runtime**: heap, GC, thread panels in the APM service view

### Combining with OTel SDK

Both can run simultaneously. The OTel SDK emits custom metrics (`kafka_app.*`) and the NR agent emits APM spans and JVM metrics — they appear in different parts of the New Relic UI and complement each other.

---

## 3. New Relic Python Agent

The New Relic APM agent for Python auto-instruments:
- `confluent-kafka` producer/consumer calls as transaction segments
- External HTTP/database calls
- Error rates and stack traces

**No code changes required.**

### Setup

```bash
# 1. Install
pip install newrelic

# 2. Generate config
newrelic-admin generate-config $NEW_RELIC_LICENSE_KEY newrelic.ini
# Edit newrelic.ini: set app_name = python-kafka-app (or preferred name)

# 3. Run with agent
newrelic-admin run-program python -m applications.python.confluent_kafka.consumer
newrelic-admin run-program python -m applications.python.confluent_kafka.producer
```

Or set via environment (no `newrelic.ini` needed):

```bash
NEW_RELIC_LICENSE_KEY=your-key \
NEW_RELIC_APP_NAME=python-kafka-consumer \
newrelic-admin run-program python -m applications.python.confluent_kafka.consumer
```

### Combining with OTel SDK

Both run simultaneously. The Python agent handles APM traces; the OTel SDK handles the custom `kafka_app.*` metrics. `newrelic.ini` is in `.gitignore` — do not commit it.

---

## 4. nri-jmx

New Relic's native JMX integration scrapes JMX MBeans from any JVM and ships them to New Relic Infrastructure.

Best for: JVM internals (memory pools, GC details, class loader, Kafka client MBeans like `kafka.consumer:type=consumer-fetch-manager-metrics`).

### Setup

**Step 1** — Start the Java app with JMX enabled (local dev, no auth):

```bash
java -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9999 \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -Dcom.sun.management.jmxremote.ssl=false \
     -cp applications/java/target/kafka-monitoring-1.0-SNAPSHOT.jar \
     com.example.kafka.Consumer
```

**Step 2** — Uncomment the `nri-jmx` service in [`infra/docker-compose.cp.yml`](infra/docker-compose.cp.yml) and start it:

```bash
docker compose -f infra/docker-compose.cp.yml up -d nri-jmx
```

**Step 3** — Create `infra/jmx-exporter/nri-jmx-app-config.yml` to define which MBeans to collect:

```yaml
integrations:
  - name: nri-jmx
    env:
      JMX_HOST: localhost
      JMX_PORT: "9999"
      COLLECTION_FILES: /etc/newrelic-infra/integrations.d/jvm-metrics.yml
```

Find metrics in New Relic under **Infrastructure → Integrations → JMX**.

---

## 5. OTel Collector JMX receiver

The OTel Collector contrib distribution includes a `jmxreceiver` that connects to a JMX endpoint via a metrics-gathering jar and converts MBeans to OTel metrics. These flow through the existing collector pipeline to New Relic.

Best for: teams already using the OTel Collector who want JVM metrics in the same pipeline as app metrics, visible in the same NRQL queries.

### Setup

**Step 1** — Start the Java app with JMX enabled (same as nri-jmx above).

**Step 2** — Download the OTel JMX metrics jar (needed by the receiver):

```bash
curl -fsSL https://github.com/open-telemetry/opentelemetry-java-contrib/releases/download/v2.14.0/opentelemetry-jmx-metrics.jar \
  -o infra/collector/opentelemetry-jmx-metrics.jar
```

**Step 3** — Mount the jar into the collector container in [`infra/docker-compose.cp.yml`](infra/docker-compose.cp.yml):

```yaml
otel-collector:
  volumes:
    - ./collector/otel-collector.yaml:/etc/otelcol-contrib/otel-collector.yaml:ro
    - ./collector/opentelemetry-jmx-metrics.jar:/opt/opentelemetry-jmx-metrics.jar:ro
    - /var/lib/docker/containers:/var/log/containers:ro
```

**Step 4** — Uncomment the `jmx` receiver and `metrics/jmx` pipeline in [`infra/collector/otel-collector.yaml`](infra/collector/otel-collector.yaml).

Metrics appear in New Relic under the `jvm.*` namespace alongside `kafka_app.*`.

---

## 6. nri-prometheus (existing)

Already wired for the **Kafka broker** in [`infra/docker-compose.cp.yml`](infra/docker-compose.cp.yml). Scrapes the `jmx_prometheus_javaagent` running on port `1234` of the broker container and ships directly to New Relic.

This covers broker-side metrics only (topics, partitions, replication, network). For application-side metrics use one of the options above.

To extend to additional components (Schema Registry, Connect, ksqlDB), add targets to [`infra/jmx-exporter/nri-prometheus-config.yaml`](infra/jmx-exporter/nri-prometheus-config.yaml).

---

## Choosing between options

```
Need APM traces (call graphs, latency breakdowns)?
  → New Relic Java/Python Agent

Need JVM internals (GC pressure, heap pools, thread states)?
  → nri-jmx  (simpler, native NR UI)
  → OTel JMX receiver  (if you want everything in one OTLP pipeline)

Need custom application metrics (lag, processing errors, delivery latency)?
  → OTel SDK (already built-in, no further setup)

Need Kafka broker cluster metrics?
  → nri-prometheus (already running for local CP)
  → Confluent Metrics API via OTel Collector (already running for Confluent Cloud)
```
