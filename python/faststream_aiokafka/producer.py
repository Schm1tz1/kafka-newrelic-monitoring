import asyncio
import logging
import os
import ssl
import time

from faststream.kafka import KafkaBroker
from faststream.security import BaseSecurity, SASLPlaintext
from pydantic import BaseModel

from python.confluent_kafka.metrics import KafkaMetrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("faststream-aiokafka-producer")


class DemoEvent(BaseModel):
    sequence: int
    created_at: float


def build_security():
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    use_ssl = "SSL" in protocol
    ssl_context = None
    if use_ssl:
        ssl_context = ssl.create_default_context()
        ca_location = os.getenv("KAFKA_SSL_CA_LOCATION", "").strip()
        if ca_location:
            ssl_context.load_verify_locations(cafile=ca_location)

    username = os.getenv("KAFKA_SASL_USERNAME", "").strip()
    if "SASL" in protocol and username:
        return SASLPlaintext(
            username=username,
            password=os.getenv("KAFKA_SASL_PASSWORD", ""),
            use_ssl=use_ssl,
            ssl_context=ssl_context,
        )
    if use_ssl:
        return BaseSecurity(ssl_context=ssl_context, use_ssl=True)
    return None


async def main():
    topic = os.getenv("KAFKA_TOPIC", "demo-events")
    client_id = os.getenv("KAFKA_CLIENT_ID", "faststream-aiokafka-producer")
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "dev")
    metrics = KafkaMetrics(os.getenv("OTEL_SERVICE_NAME", "faststream-aiokafka-producer"), environment)

    broker = KafkaBroker(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        client_id=client_id,
        security=build_security(),
    )

    count = int(os.getenv("PRODUCER_MESSAGES", "100"))
    interval_ms = int(os.getenv("PRODUCER_INTERVAL_MS", "100"))
    common_attrs = {"client.id": client_id, "topic": topic}

    try:
        async with broker:
            for index in range(count):
                event = DemoEvent(sequence=index, created_at=time.time())
                started = time.perf_counter()
                try:
                    result = await broker.publish(event, topic)
                except Exception as exc:
                    metrics.delivery_errors.add(1, common_attrs)
                    log.error("delivery_failed topic=%s error=%s", topic, exc)
                else:
                    latency_ms = (time.perf_counter() - started) * 1000
                    attrs = {**common_attrs, "partition": result.partition}
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
