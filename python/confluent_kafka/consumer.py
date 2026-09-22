import json
import logging
import os
import secrets
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition

from python.confluent_kafka.kafka_config import kafka_config
from python.confluent_kafka.metrics import KafkaMetrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kafka-consumer")


def main():
    topic = os.getenv("KAFKA_TOPIC", "demo-events")
    client_id = os.getenv("KAFKA_CLIENT_ID", "python-monitoring-consumer")
    # No KAFKA_GROUP_ID set: pick a group unique to this run so unrelated runs against a
    # shared broker don't collide. Set KAFKA_GROUP_ID explicitly to resume the same group
    # (and its committed offsets) across restarts.
    group_id = os.getenv("KAFKA_GROUP_ID") or f"{client_id}-{secrets.token_hex(4)}"
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "dev")
    processing_ms = int(os.getenv("PROCESSING_MS", "10"))
    metrics = KafkaMetrics(os.getenv("OTEL_SERVICE_NAME", "python-kafka-consumer"), environment)
    log.info("consumer_starting client_id=%s group_id=%s topic=%s", client_id, group_id, topic)

    def stats_cb(stats_json: str):
        stats = json.loads(stats_json)
        log.info("librdkafka_stats %s", json.dumps({
            "name": stats.get("name"),
            "txmsgs": stats.get("txmsgs"),
            "rxmsgs": stats.get("rxmsgs"),
            "tx": stats.get("tx"),
            "rx": stats.get("rx"),
            "msg_cnt": stats.get("msg_cnt"),
            "msg_size": stats.get("msg_size"),
            "brokers": len(stats.get("brokers", {})),
        }, separators=(",", ":")))

    config = kafka_config(client_id, group_id)
    config.update({
        "enable.auto.commit": False,
        "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        "stats_cb": stats_cb,
    })
    consumer = Consumer(config)
    common_attrs = {"client.id": client_id, "consumer.group": group_id, "topic": topic}

    def on_assign(consumer, partitions):
        metrics.rebalances.add(1, common_attrs)
        log.info("partitions_assigned partitions=%s", [p.partition for p in partitions])

    def on_revoke(consumer, partitions):
        metrics.rebalances.add(1, common_attrs)
        log.info("partitions_revoked partitions=%s", [p.partition for p in partitions])

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("consumer_error error=%s", msg.error())
                metrics.processing_errors.add(1, common_attrs)
                continue

            attrs = {**common_attrs, "partition": msg.partition()}
            metrics.consumed.add(1, attrs)
            started = time.perf_counter()
            try:
                json.loads(msg.value().decode("utf-8"))
                if processing_ms:
                    time.sleep(processing_ms / 1000)
            except Exception:
                metrics.processing_errors.add(1, attrs)
                log.exception("record_processing_failed topic=%s partition=%s offset=%s", msg.topic(), msg.partition(), msg.offset())
                continue
            finally:
                metrics.processing_latency.record((time.perf_counter() - started) * 1000, attrs)

            try:
                _, high = consumer.get_watermark_offsets(TopicPartition(msg.topic(), msg.partition()), timeout=2.0, cached=False)
                metrics.set_lag(
                    high - msg.offset() - 1,
                    {**common_attrs, "partition": msg.partition()},
                )
            except Exception:
                log.debug("lag_estimate_failed", exc_info=True)

            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        log.info("shutdown_requested")
    finally:
        consumer.close()
        metrics.shutdown()


if __name__ == "__main__":
    main()
