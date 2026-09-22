import asyncio
import functools
import json
import logging
import os
import secrets
import time

from confluent_kafka import TopicPartition
from faststream import AckPolicy
from faststream.confluent import ConfluentConfig, KafkaBroker
from faststream.confluent.annotations import Consumer, KafkaMessage
from pydantic import BaseModel

from python.confluent_kafka.metrics import KafkaMetrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("faststream-confluent-consumer")


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
    client_id = os.getenv("KAFKA_CLIENT_ID", "faststream-confluent-consumer")
    # No KAFKA_GROUP_ID set: pick a group unique to this run so unrelated runs against a
    # shared broker don't collide. Set KAFKA_GROUP_ID explicitly to resume the same group
    # (and its committed offsets) across restarts.
    group_id = os.getenv("KAFKA_GROUP_ID") or f"{client_id}-{secrets.token_hex(4)}"
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "dev")
    processing_ms = int(os.getenv("PROCESSING_MS", "10"))
    metrics = KafkaMetrics(os.getenv("OTEL_SERVICE_NAME", "faststream-confluent-consumer"), environment)
    log.info("consumer_starting client_id=%s group_id=%s topic=%s", client_id, group_id, topic)

    broker = KafkaBroker(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        client_id=client_id,
        allow_auto_create_topics=False,
        config=confluent_config(),
    )
    common_attrs = {"client.id": client_id, "consumer.group": group_id, "topic": topic}

    def on_assign(consumer, partitions):
        metrics.rebalances.add(1, common_attrs)
        log.info("partitions_assigned partitions=%s", [p.partition for p in partitions])

    def on_revoke(consumer, partitions):
        metrics.rebalances.add(1, common_attrs)
        log.info("partitions_revoked partitions=%s", [p.partition for p in partitions])

    @broker.subscriber(
        topic,
        group_id=group_id,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        ack_policy=AckPolicy.MANUAL,
        on_assign=on_assign,
        on_revoke=on_revoke,
    )
    async def handle_event(event: DemoEvent, msg: KafkaMessage, consumer: Consumer):
        raw = msg.raw_message
        attrs = {**common_attrs, "partition": raw.partition()}
        metrics.consumed.add(1, attrs)

        # `event` is already validated/decoded by FastStream before this handler runs, so
        # processing_errors here covers application-level failures, not malformed payloads.
        started = time.perf_counter()
        try:
            if processing_ms:
                await asyncio.sleep(processing_ms / 1000)
        except Exception:
            metrics.processing_errors.add(1, attrs)
            log.exception("record_processing_failed topic=%s partition=%s offset=%s", raw.topic(), raw.partition(), raw.offset())
            await msg.nack()
            return
        finally:
            metrics.processing_latency.record((time.perf_counter() - started) * 1000, attrs)

        try:
            # FastStream's consumer wrapper doesn't expose watermark lookups, so this reaches
            # into its underlying confluent_kafka.Consumer, same as the plain confluent-kafka example.
            loop = asyncio.get_running_loop()
            _, high = await loop.run_in_executor(
                None,
                functools.partial(
                    consumer.consumer.get_watermark_offsets,
                    TopicPartition(raw.topic(), raw.partition()),
                    timeout=2.0,
                    cached=False,
                ),
            )
            metrics.set_lag(high - raw.offset() - 1, attrs)
        except Exception:
            log.debug("lag_estimate_failed", exc_info=True)

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
