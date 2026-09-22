import asyncio
import logging
import os
import secrets
import ssl
import time

from aiokafka import TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from faststream import AckPolicy
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import Consumer, KafkaMessage
from faststream.security import BaseSecurity, SASLPlaintext
from pydantic import BaseModel

from python.confluent_kafka.metrics import KafkaMetrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("faststream-aiokafka-consumer")


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


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, metrics: KafkaMetrics, attrs: dict):
        self._metrics = metrics
        self._attrs = attrs

    async def on_partitions_revoked(self, revoked):
        self._metrics.rebalances.add(1, self._attrs)
        log.info("partitions_revoked partitions=%s", [tp.partition for tp in revoked])

    async def on_partitions_assigned(self, assigned):
        self._metrics.rebalances.add(1, self._attrs)
        log.info("partitions_assigned partitions=%s", [tp.partition for tp in assigned])


async def main():
    topic = os.getenv("KAFKA_TOPIC", "demo-events")
    client_id = os.getenv("KAFKA_CLIENT_ID", "faststream-aiokafka-consumer")
    # No KAFKA_GROUP_ID set: pick a group unique to this run so unrelated runs against a
    # shared broker don't collide. Set KAFKA_GROUP_ID explicitly to resume the same group
    # (and its committed offsets) across restarts.
    group_id = os.getenv("KAFKA_GROUP_ID") or f"{client_id}-{secrets.token_hex(4)}"
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "dev")
    processing_ms = int(os.getenv("PROCESSING_MS", "10"))
    metrics = KafkaMetrics(os.getenv("OTEL_SERVICE_NAME", "faststream-aiokafka-consumer"), environment)
    log.info("consumer_starting client_id=%s group_id=%s topic=%s", client_id, group_id, topic)

    broker = KafkaBroker(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        client_id=client_id,
        security=build_security(),
    )
    common_attrs = {"client.id": client_id, "consumer.group": group_id, "topic": topic}

    @broker.subscriber(
        topic,
        group_id=group_id,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        ack_policy=AckPolicy.MANUAL,
        listener=RebalanceListener(metrics, common_attrs),
    )
    async def handle_event(event: DemoEvent, msg: KafkaMessage, consumer: Consumer):
        raw = msg.raw_message
        attrs = {**common_attrs, "partition": raw.partition}
        metrics.consumed.add(1, attrs)

        # `event` is already validated/decoded by FastStream before this handler runs, so
        # processing_errors here covers application-level failures, not malformed payloads.
        started = time.perf_counter()
        try:
            if processing_ms:
                await asyncio.sleep(processing_ms / 1000)
        except Exception:
            metrics.processing_errors.add(1, attrs)
            log.exception("record_processing_failed topic=%s partition=%s offset=%s", raw.topic, raw.partition, raw.offset)
            await msg.nack()
            return
        finally:
            metrics.processing_latency.record((time.perf_counter() - started) * 1000, attrs)

        high = consumer.highwater(TopicPartition(raw.topic, raw.partition))
        if high is not None:
            metrics.set_lag(high - raw.offset - 1, attrs)

        await msg.ack()

    try:
        async with broker:
            await broker.start()
            await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("shutdown_requested")
    finally:
        metrics.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
