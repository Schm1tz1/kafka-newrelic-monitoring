import asyncio
import json
import logging
import os
import time

from faststream.confluent import ConfluentConfig, KafkaBroker
from pydantic import BaseModel

from python.confluent_kafka.metrics import KafkaMetrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("faststream-confluent-producer")


class DemoEvent(BaseModel):
    sequence: int
    created_at: float


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


def confluent_config() -> ConfluentConfig:
    config: ConfluentConfig = {
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").lower(),
        "statistics.interval.ms": int(os.getenv("KAFKA_STATS_INTERVAL_MS", "30000")),
        "stats_cb": stats_cb,
    }
    sasl_mechanisms = os.getenv("KAFKA_SASL_MECHANISMS", "").strip()
    if sasl_mechanisms:
        config["sasl.mechanisms"] = sasl_mechanisms
        config["sasl.username"] = os.getenv("KAFKA_SASL_USERNAME", "")
        config["sasl.password"] = os.getenv("KAFKA_SASL_PASSWORD", "")
    ssl_ca_location = os.getenv("KAFKA_SSL_CA_LOCATION", "").strip()
    if ssl_ca_location:
        config["ssl.ca.location"] = ssl_ca_location
    return config


async def main():
    topic = os.getenv("KAFKA_TOPIC", "demo-events")
    client_id = os.getenv("KAFKA_CLIENT_ID", "faststream-confluent-producer")
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "dev")
    metrics = KafkaMetrics(os.getenv("OTEL_SERVICE_NAME", "faststream-confluent-producer"), environment)

    broker = KafkaBroker(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        client_id=client_id,
        allow_auto_create_topics=False,
        config=confluent_config(),
    )

    count = int(os.getenv("PRODUCER_MESSAGES", "100"))
    interval_ms = int(os.getenv("PRODUCER_INTERVAL_MS", "100"))
    common_attrs = {"client.id": client_id, "topic": topic}

    try:
        async with broker:
            for index in range(count):
                event = DemoEvent(sequence=index, created_at=time.time())
                started = time.perf_counter()
                result = None
                while True:
                    try:
                        result = await broker.publish(event, topic)
                        break
                    except BufferError:
                        await asyncio.sleep(0.1)
                    except Exception as exc:
                        metrics.delivery_errors.add(1, common_attrs)
                        log.error("delivery_failed topic=%s error=%s", topic, exc)
                        break
                if result is not None:
                    latency_ms = (time.perf_counter() - started) * 1000
                    attrs = {**common_attrs, "partition": result.partition()}
                    metrics.delivery_latency.record(latency_ms, attrs)
                    metrics.produced.add(1, attrs)
                if interval_ms:
                    await asyncio.sleep(interval_ms / 1000)
    finally:
        metrics.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
