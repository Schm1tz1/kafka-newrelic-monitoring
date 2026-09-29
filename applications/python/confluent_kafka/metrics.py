import os
import threading
from typing import Dict, Iterable, Tuple

from opentelemetry import metrics
from opentelemetry.metrics import Observation
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource


Attributes = Iterable[Tuple[str, str]]


def _otlp_target() -> tuple[str, bool, Dict[str, str] | None]:
    # Default: send OTLP to a local Collector, which forwards to New Relic (see README).
    # Point OTEL_EXPORTER_OTLP_ENDPOINT straight at New Relic's own OTLP host to skip the
    # Collector instead; the api-key auth header is added automatically for that host.
    raw = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317").strip()
    insecure = raw.startswith("http://")
    endpoint = raw.removeprefix("http://").removeprefix("https://").rstrip("/")

    headers = None
    if "nr-data.net" in endpoint:
        insecure = False
        if ":" not in endpoint:
            endpoint = f"{endpoint}:4317"
        headers = {"api-key": os.getenv("NEW_RELIC_LICENSE_KEY", "")}

    return endpoint, insecure, headers


class KafkaMetrics:
    def __init__(self, service_name: str, environment: str):
        endpoint, insecure, headers = _otlp_target()
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=insecure, headers=headers)
        reader = PeriodicExportingMetricReader(
            exporter,
            export_interval_millis=int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL_MS", "10000")),
        )
        resource = Resource.create({
            "service.name": service_name,
            "service.version": os.getenv("SERVICE_VERSION", "dev"),
            "deployment.environment": environment,
        })
        self.provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(self.provider)
        self.meter = self.provider.get_meter("python-kafka-monitoring")

        self.produced = self.meter.create_counter(
            "kafka_app.produced_records", unit="{record}", description="Records accepted by a producer"
        )
        self.delivery_errors = self.meter.create_counter(
            "kafka_app.producer_delivery_errors", unit="{error}", description="Producer delivery errors"
        )
        self.delivery_latency = self.meter.create_histogram(
            "kafka_app.producer_delivery_latency_ms", unit="ms", description="Producer delivery latency"
        )
        self.consumed = self.meter.create_counter(
            "kafka_app.consumed_records", unit="{record}", description="Records returned by consumer poll"
        )
        self.processing_errors = self.meter.create_counter(
            "kafka_app.consumer_processing_errors", unit="{error}", description="Consumer processing errors"
        )
        self.processing_latency = self.meter.create_histogram(
            "kafka_app.consumer_processing_latency_ms", unit="ms", description="Application processing latency"
        )
        self.rebalances = self.meter.create_counter(
            "kafka_app.consumer_rebalances", unit="{rebalance}", description="Consumer assignment events"
        )
        self._lag_lock = threading.Lock()
        self._lag: Dict[Tuple[Tuple[str, str], ...], int] = {}
        self.lag = self.meter.create_observable_gauge(
            "kafka_app.consumer_lag_records",
            callbacks=[self._observe_lag],
            unit="{record}",
            description="Estimated consumer lag by topic and partition",
        )

    @staticmethod
    def _attrs(attrs: dict) -> dict:
        return {str(k): str(v) for k, v in attrs.items() if v is not None}

    def _observe_lag(self, _options):
        with self._lag_lock:
            values = list(self._lag.items())
        return [Observation(value, dict(attrs)) for attrs, value in values]

    def set_lag(self, lag: int, attrs: dict):
        clean = self._attrs(attrs)
        key = tuple(sorted(clean.items()))
        with self._lag_lock:
            self._lag[key] = max(0, int(lag))

    def shutdown(self):
        self.provider.shutdown()
