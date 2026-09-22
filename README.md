# Python Kafka Monitoring with New Relic

This bundle provides runnable producer and consumer examples for monitoring Python Kafka applications and exporting application metrics to New Relic through a local OpenTelemetry Collector — the architecture most production Kafka monitoring setups use, since it gives you a single place to hold the license key and batch/buffer metrics for many services. Sending OTLP straight to New Relic, skipping the Collector, is documented as an alternative below for simpler setups.

The examples use `confluent-kafka`, which is backed by `librdkafka`. They enable the `statistics.interval.ms` callback for client diagnostics, and emit normalized application metrics such as throughput, delivery errors, processing latency, rebalances, and consumer lag.

## Architecture

```text
Python producer / consumer
        |
        | OTLP/gRPC metrics
        v
OpenTelemetry Collector
        |
        | OTLP/HTTP metrics
        v
New Relic

Kafka client statistics callback -> structured application logs
```

See [Alternative: direct to New Relic](#alternative-direct-to-new-relic) to skip the Collector.

## Contents

* `python/confluent_kafka/producer.py` — runnable Kafka producer.
* `python/confluent_kafka/consumer.py` — runnable Kafka consumer with lag and processing metrics.
* `python/confluent_kafka/metrics.py` — OpenTelemetry metric instruments and OTLP export setup, shared by every example in this bundle.
* `python/confluent_kafka/kafka_config.py` — environment-based Kafka configuration for the plain `confluent-kafka` example.
* `python/faststream_confluent/producer.py` / `consumer.py` — the same producer/consumer example built on [FastStream](https://faststream.ag2.ai/)'s `faststream.confluent` broker (still backed by `confluent-kafka`/`librdkafka`).
* `python/faststream_aiokafka/producer.py` / `consumer.py` — the same example built on FastStream's `faststream.kafka` broker (backed by `aiokafka` instead of `librdkafka`).
* `collector/otel-collector.yaml` — Collector pipeline forwarding metrics to New Relic.
* `docker-compose.cp.yml` — Confluent Platform (local): Kafka broker + OTel Collector + nri-prometheus (JMX).
* `docker-compose.ccloud.yml` — Confluent Cloud: OTel Collector only (no local Kafka).
* `env.example` — configuration template for local Kafka or Confluent Cloud.
* `dashboards/newrelic-dashboard-python.json` — importable New Relic dashboard covering all Python app metrics below.
* `dashboards/newrelic-dashboard-cp.json` — Confluent Platform (local) dashboard: 7 pages covering broker overview, Kafka cluster, throughput, Zookeeper, producer/consumer, fetch follower (sourced from [confluentinc/jmx-monitoring-stacks — jmxexporter-newrelic](https://github.com/confluentinc/jmx-monitoring-stacks/blob/main/jmxexporter-newrelic/assets/newrelic/cpdashboard.json)).
* `dashboards/newrelic-dashboard-ccloud.json` — importable Confluent Cloud cluster dashboard (sourced from [newrelic/newrelic-quickstarts — confluent-cloud](https://github.com/newrelic/newrelic-quickstarts/blob/main/dashboards/confluent-cloud/confluent-cloud.json)).
* `deploy_nr_dashboard.sh` — interactive script to deploy any dashboard from `dashboards/` via the NerdGraph API.
* `requirements.txt` — Python dependencies.

**Always run examples as `python -m python.<package>.<module>` from the repository root**, exactly as shown below — never `cd python && python -m confluent_kafka.consumer`. `python/confluent_kafka/` shares its name with the real `confluent_kafka` PyPI package; running it from inside `python/` puts that directory on `sys.path` and shadows the real library, breaking `from confluent_kafka import Producer` with a confusing `ImportError` that appears to come from `confluent_kafka` itself.

## Prerequisites

* Python 3.9 or later.
* Docker and Docker Compose if using the included local Kafka broker and Collector.
* A New Relic license/ingest key for metric export.
* Network access from the Collector to the selected New Relic OTLP endpoint (or from the application directly, if using the [direct alternative](#alternative-direct-to-new-relic)).
* `jq`, only if deploying a dashboard from `dashboards/` via `deploy_nr_dashboard.sh` instead of the UI (see [New Relic dashboard](#new-relic-dashboard)).

## Quick start with local Kafka

Create a working copy and install dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp env.example .env
```

Set the New Relic values. For an EU New Relic account:

```bash
export NEW_RELIC_LICENSE_KEY='replace-with-your-new-relic-ingest-key'
export NEW_RELIC_OTLP_ENDPOINT='https://otlp.eu01.nr-data.net'
```

For a US account, use:

```bash
export NEW_RELIC_OTLP_ENDPOINT='https://otlp.nr-data.net'
```

Start Kafka and the Collector:

```bash
docker compose up -d kafka otel-collector
```

Create the example topic:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic demo-events \
  --partitions 3 \
  --replication-factor 1
```

In a first terminal, start the consumer:

```bash
. .venv/bin/activate
set -a; . ./.env; set +a
python -m python.confluent_kafka.consumer
```

In a second terminal, start the producer:

```bash
. .venv/bin/activate
set -a; . ./.env; set +a
python -m python.confluent_kafka.producer
```

The producer sends a finite batch by default. Override it with `PRODUCER_MESSAGES`, for example:

```bash
PRODUCER_MESSAGES=10000 python -m python.confluent_kafka.producer
```

The consumer continuously polls until interrupted. Stop it with `Ctrl+C`; it closes the consumer cleanly and leaves committed offsets intact.

By default `KAFKA_GROUP_ID` is blank in `env.example`, so each consumer generates its own group as `<client_id>-<8 random hex chars>` (logged at startup as `consumer_starting`) — this keeps unrelated runs against a shared broker from colliding into the same group and splitting partitions unexpectedly. Because the group is regenerated on every start, restarting the consumer this way joins a **new**, empty-offset group rather than resuming. Set `KAFKA_GROUP_ID` explicitly in `.env` to a fixed value if you want restarts to resume from committed offsets.

## Quick start with Confluent Cloud

Do not start the local Kafka service. Create a Confluent Cloud API key and secret for the Kafka cluster, then export the connection settings:

```bash
export KAFKA_BOOTSTRAP_SERVERS='pkc-xxxxx.region.provider.confluent.cloud:9092'
export KAFKA_SECURITY_PROTOCOL='SASL_SSL'
export KAFKA_SASL_MECHANISMS='PLAIN'
export KAFKA_SASL_USERNAME='your-kafka-api-key'
export KAFKA_SASL_PASSWORD='your-kafka-api-secret'
export KAFKA_TOPIC='your-topic'
export KAFKA_GROUP_ID='python-monitoring-example'
```

Run the same producer and consumer commands shown above. The application metrics still go to the local Collector, which forwards them to New Relic.

## FastStream examples

`python/faststream_confluent/` and `python/faststream_aiokafka/` are self-contained ports of the same producer/consumer example built on [FastStream](https://faststream.ag2.ai/), a higher-level async framework that layers typed pub/sub handlers, an AsyncAPI schema, and lifecycle management on top of a Kafka client. They emit the same metric names and attributes documented below, so the NRQL queries and dashboards work unchanged against either example.

* `faststream_confluent` (`python/faststream_confluent/`) uses `faststream.confluent.KafkaBroker`, which is still backed by `confluent-kafka`/`librdkafka`. It keeps the same `statistics.interval.ms` / `stats_cb` JSON logging as the plain example, passed through FastStream's `config=` option using the same dot-separated `librdkafka` property names as `python/confluent_kafka/kafka_config.py`.
* `faststream_aiokafka` (`python/faststream_aiokafka/`) uses `faststream.kafka.KafkaBroker`, which is backed by `aiokafka` (a pure-Python client) instead of `librdkafka`. There is no `stats_cb` equivalent for this backend — `aiokafka` doesn't expose `librdkafka`-style client statistics. It also connects eagerly: if the broker is unreachable, `async with broker:` raises immediately, whereas the `librdkafka`-backed examples retry deliveries in the background per `librdkafka`'s own retry/timeout settings.

Both reuse `python/confluent_kafka/metrics.py` for OpenTelemetry setup and the same environment variables as the plain example (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_TOPIC`, `PRODUCER_MESSAGES`, etc. — see `env.example`). Each defaults `KAFKA_CLIENT_ID`/`OTEL_SERVICE_NAME` to its own example-specific name, and — same as the plain example — generates its own random `KAFKA_GROUP_ID` when that's left unset, so you can run any of the three example pairs against the same topic without their consumer groups colliding.

Install the extra dependencies (already included in `requirements.txt`) and run the same way as the plain example, from separate terminals:

```bash
set -a; . ./.env; set +a
python -m python.faststream_confluent.consumer
python -m python.faststream_confluent.producer
```

```bash
set -a; . ./.env; set +a
python -m python.faststream_aiokafka.consumer
python -m python.faststream_aiokafka.producer
```

Both go through the local Collector by default too, since they reuse `python/confluent_kafka/metrics.py` unchanged. See [Alternative: direct to New Relic](#alternative-direct-to-new-relic) to skip it.

## Alternative: direct to New Relic

By default every example in this bundle sends OTLP metrics to a local Collector, which forwards them to New Relic (see `python/confluent_kafka/metrics.py`). Skipping the Collector and exporting straight to New Relic's own OTLP endpoint is a documented alternative, useful for simpler setups where you don't need:

* A single place holding the license key, instead of setting it directly in every application/service's environment.
* The Collector's `memory_limiter`/`batch` processors, or additional processors (redaction, sampling, multi-backend fan-out) without touching application code.
* The `debug` exporter's stdout visibility into exactly what's being sent, useful while developing.

To use it, point `OTEL_EXPORTER_OTLP_ENDPOINT` at New Relic's own OTLP host instead of `localhost:4317` — `python/confluent_kafka/metrics.py`'s `_otlp_target()` detects the `nr-data.net` host, switches to TLS, and adds `NEW_RELIC_LICENSE_KEY` as the `api-key` auth header automatically:

```bash
# In .env, replacing the default OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.eu01.nr-data.net   # or https://otlp.nr-data.net for a US account
```

You no longer need to start the Collector — `docker compose up -d kafka` is enough. Run the producer/consumer commands from the quick start sections unchanged; only the OTLP hop is different.

## Metrics emitted

`python/confluent_kafka/metrics.py`'s `KafkaMetrics` class is the single place all eight instruments are created, and it's shared unchanged by every example in this bundle (`python/confluent_kafka/`, `python/faststream_confluent/`, `python/faststream_aiokafka/`). Creating an instrument there makes it available everywhere; a given script only actually emits it if that script calls `.add()`/`.record()`/`.set_lag()` on it.

| Metric | Type | Meaning |
|---|---|---|
| `kafka_app.produced_records` | Counter | Records accepted by the producer client |
| `kafka_app.producer_delivery_errors` | Counter | Delivery callbacks reporting an error |
| `kafka_app.producer_delivery_latency_ms` | Histogram | Time from `produce()` to delivery callback |
| `kafka_app.consumed_records` | Counter | Records returned by `poll()` |
| `kafka_app.consumer_processing_errors` | Counter | Application processing failures |
| `kafka_app.consumer_processing_latency_ms` | Histogram | Application processing duration |
| `kafka_app.consumer_lag_records` | Observable gauge | Latest estimated lag for the consumed partition |
| `kafka_app.consumer_rebalances` | Counter | Consumer group assignment events |

Actually used, per script:

| Metric | `python/confluent_kafka/producer.py` | `python/confluent_kafka/consumer.py` | `python/faststream_confluent/producer.py` | `python/faststream_confluent/consumer.py` | `python/faststream_aiokafka/producer.py` | `python/faststream_aiokafka/consumer.py` |
|---|---|---|---|---|---|---|
| `produced_records` | ✅ | | ✅ | | ✅ | |
| `producer_delivery_errors` | ✅ | | ✅ | | ✅ | |
| `producer_delivery_latency_ms` | ✅ | | ✅ | | ✅ | |
| `consumed_records` | | ✅ | | ✅ | | ✅ |
| `consumer_processing_errors` | | ✅ | | ✅ | | ✅ |
| `consumer_processing_latency_ms` | | ✅ | | ✅ | | ✅ |
| `consumer_lag_records` | | ✅ | | ✅ | | ✅ |
| `consumer_rebalances` | | ✅ | | ✅ | | ✅ |

Every consumer wires up `consumer_rebalances` via its native rebalance-callback mechanism: `on_assign`/`on_revoke` passed to `consumer.subscribe()` in `python/confluent_kafka/consumer.py` and `python/faststream_confluent/consumer.py` (both `confluent-kafka`-backed), and a `ConsumerRebalanceListener` passed to `broker.subscriber(...)` in `python/faststream_aiokafka/consumer.py` (`aiokafka`).

Metrics carry low-cardinality attributes such as `service.name`, `deployment.environment`, `client.id`, `topic`, and `consumer.group`. Do not add message IDs, offsets, payloads, or unbounded user identifiers as metric attributes.

## Useful New Relic queries

The exact attribute names can vary with the Collector and New Relic account configuration. Start with these NRQL examples:

```sql
FROM Metric
SELECT rate(sum(kafka_app.produced_records), 1 minute)
FACET service.name, topic
TIMESERIES
```

```sql
FROM Metric
SELECT sum(kafka_app.producer_delivery_errors), sum(kafka_app.consumer_processing_errors)
FACET service.name
TIMESERIES
```

```sql
FROM Metric
SELECT average(kafka_app.consumer_processing_latency_ms), max(kafka_app.consumer_lag_records)
FACET service.name, topic, consumer.group
TIMESERIES
```

For an alerting starting point, alert on any producer delivery error, a sustained upward trend in consumer lag, and processing latency above the application SLO. Establish thresholds from a normal baseline rather than copying a universal value.

## New Relic dashboard

All dashboards live under `dashboards/`. Each uses `"accountId": 0` as a placeholder; the deploy script replaces it with your real account ID before sending.

`dashboards/newrelic-dashboard-python.json` is a ready-to-import dashboard built from the queries above, covering all eight Python app metrics: billboards for delivery/processing errors, rebalances, and max lag, plus timeseries for throughput, latency (avg/p95), lag trend, and rebalance history, and a table breaking down errors by `service.name`.

To import any dashboard through the UI: replace every `"accountId": 0` with your New Relic account ID, then in New Relic go to **Dashboards → Import dashboard** and paste the file's contents.

To deploy via the NerdGraph API, add two variables to `.env` — a User API key (`NEW_RELIC_API_KEY`, starts with `NRAK-...`, **not** the ingest `NEW_RELIC_LICENSE_KEY`) and `NEW_RELIC_ACCOUNT_ID` — then run:

```bash
chmod +x deploy_nr_dashboard.sh   # first time only
./deploy_nr_dashboard.sh
```

With no arguments the script lists all JSON files in `dashboards/` and prompts you to pick one. Pass a path directly to skip the prompt:

```bash
./deploy_nr_dashboard.sh dashboards/newrelic-dashboard-python.json
./deploy_nr_dashboard.sh dashboards/newrelic-dashboard-ccloud.json
```

It loads `.env`, picks the right NerdGraph endpoint (US vs EU) from `NEW_RELIC_OTLP_ENDPOINT`, rewrites every widget's `accountId` placeholder to your real `NEW_RELIC_ACCOUNT_ID`, and `curl`s the `dashboardCreate` mutation. On success it prints `guid=...` for the new dashboard; on failure (bad API key, invalid NRQL, etc.) it prints the full GraphQL response and exits non-zero.

The dashboard reads whichever metrics are actually reaching New Relic, so it works unchanged whether you're using the default Collector path or the [direct alternative](#alternative-direct-to-new-relic), and against any of the three example pairs (`python/confluent_kafka/`, `python/faststream_confluent/`, `python/faststream_aiokafka/`) — see the usage matrix above for which script emits which metric.

## Client statistics callback

Both examples configure `statistics.interval.ms` and `stats_cb`. The callback logs a compact JSON summary of the `librdkafka` statistics document. This is intentionally kept in logs rather than converted into hundreds of metric dimensions. If detailed client statistics are required in New Relic, forward the structured logs using the New Relic infrastructure/logging integration and build log-based dashboards or parsing rules.

The callback is useful for troubleshooting queue depth, broker connectivity, request rates, byte rates, and other client internals. Consult the [`librdkafka` statistics reference](https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md) before mapping additional fields.

## Production recommendations

* Run the OpenTelemetry Collector as a separate deployment or DaemonSet rather than inside the application process.
* Store `NEW_RELIC_LICENSE_KEY` and Kafka credentials in a secret manager.
* Use a dedicated `client.id` and `consumer.group` for every application workload.
* Keep metric labels bounded. Avoid partition-level metrics unless the partition count is small and operationally necessary.
* Monitor application processing latency separately from Kafka fetch latency.
* Treat producer delivery errors as a high-priority signal; a non-zero value means messages were not delivered successfully.
* Monitor consumer lag as a trend and correlate it with processing latency and incoming throughput.
* Use manual offset handling when processing must complete before the offset is committed.
* For Confluent Cloud cluster-side metrics, use the Confluent Cloud Metrics API or its documented New Relic OpenTelemetry integration in addition to application-side metrics.
* Do not use the legacy Confluent Monitoring Interceptor for new Python deployments. For librdkafka-based Python clients, the built-in statistics callback is the supported application-side path.

## Troubleshooting

### No metrics in New Relic

Collector path (default):

* Confirm the Collector is running: `docker compose logs otel-collector`.
* Verify `NEW_RELIC_LICENSE_KEY` is set in the environment passed to Docker Compose.
* Verify the OTLP endpoint matches the New Relic region.
* Confirm outbound TCP/443 access from the Collector.
* Check that the application can reach `localhost:4317`.

Direct path (`OTEL_EXPORTER_OTLP_ENDPOINT` pointed at a `nr-data.net` host):

* Verify `NEW_RELIC_LICENSE_KEY` and `NEW_RELIC_OTLP_ENDPOINT`/`OTEL_EXPORTER_OTLP_ENDPOINT` are set in the application's environment.
* Verify the OTLP endpoint matches the New Relic region (`otlp.nr-data.net` vs `otlp.eu01.nr-data.net`) — a license key from one region will fail against the other's endpoint.
* Confirm outbound TCP/4317 access from wherever the application runs.

### Kafka connection failures

* For local Kafka, verify `docker compose ps` and use `localhost:29092` from the host.
* For Confluent Cloud, verify `SASL_SSL`, `PLAIN`, API key, API secret, and bootstrap hostname.
* Confirm the topic exists and the Kafka principal can read or write it.

### Consumer lag is missing

The example estimates lag from the high watermark for each consumed partition. It is an application-side estimate and can be absent while partitions are being assigned or when no record has yet been received. For group-level lag across all partitions and consumers, use the Kafka admin/consumer-group tooling or the Confluent Cloud monitoring interfaces as a complementary signal.

## Source documentation

* [Confluent Python client overview](https://docs.confluent.io/kafka-clients/python/current/overview.html)
* [Confluent monitoring clients guidance](https://support.confluent.io/hc/en-us/articles/4410843405204-Monitoring-Clients)
* [Confluent Cloud client monitoring](https://docs.confluent.io/cloud/current/client-apps/monitoring.html)
* [Confluent Cloud Metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)
* [Confluent Cloud New Relic OpenTelemetry example](https://github.com/confluentinc/jmx-monitoring-stacks/tree/main/ccloud-opentelemetry-newrelic)
* [OpenTelemetry Collector OTLP exporter](https://opentelemetry.io/docs/collector/configuration/)
* [New Relic OTLP endpoint documentation](https://docs.newrelic.com/docs/opentelemetry/best-practices/opentelemetry-otlp/)
