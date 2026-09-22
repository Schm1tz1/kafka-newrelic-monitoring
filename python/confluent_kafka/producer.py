import json
import logging
import os
import time

from confluent_kafka import Producer

from python.confluent_kafka.kafka_config import kafka_config
from python.confluent_kafka.metrics import KafkaMetrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kafka-producer")


def main():
    topic = os.getenv("KAFKA_TOPIC", "demo-events")
    client_id = os.getenv("KAFKA_CLIENT_ID", "python-monitoring-producer")
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "dev")
    metrics = KafkaMetrics(os.getenv("OTEL_SERVICE_NAME", "python-kafka-producer"), environment)

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

    config = kafka_config(client_id)
    config["stats_cb"] = stats_cb
    producer = Producer(config)
    count = int(os.getenv("PRODUCER_MESSAGES", "100"))
    interval_ms = int(os.getenv("PRODUCER_INTERVAL_MS", "100"))
    common_attrs = {"client.id": client_id, "topic": topic}

    def delivery_report(err, msg, started):
        latency_ms = (time.perf_counter() - started) * 1000
        attrs = {**common_attrs, "partition": msg.partition()}
        metrics.delivery_latency.record(latency_ms, attrs)
        if err is not None:
            metrics.delivery_errors.add(1, attrs)
            log.error("delivery_failed topic=%s partition=%s error=%s", msg.topic(), msg.partition(), err)
        else:
            log.debug("delivery_succeeded topic=%s partition=%s offset=%s", msg.topic(), msg.partition(), msg.offset())

    try:
        for index in range(count):
            payload = json.dumps({"sequence": index, "created_at": time.time()}).encode()
            started = time.perf_counter()
            while True:
                try:
                    producer.produce(
                        topic,
                        value=payload,
                        on_delivery=lambda err, msg, started=started: delivery_report(err, msg, started),
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            metrics.produced.add(1, common_attrs)
            producer.poll(0)
            if interval_ms:
                time.sleep(interval_ms / 1000)
        remaining = producer.flush(30)
        if remaining:
            log.error("producer_flush_incomplete remaining=%s", remaining)
    finally:
        metrics.shutdown()


if __name__ == "__main__":
    main()
