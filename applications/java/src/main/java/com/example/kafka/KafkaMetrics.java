package com.example.kafka;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ServiceAttributes;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OTel metrics instrumentation for the Java Kafka example.
 * Emits the same eight instruments as python/confluent_kafka/metrics.py
 * so all existing NRQL queries and dashboards work unchanged.
 */
public class KafkaMetrics implements AutoCloseable {

    private final SdkMeterProvider meterProvider;

    // Producer instruments
    public final LongCounter produced;
    public final LongCounter deliveryErrors;
    public final DoubleHistogram deliveryLatency;

    // Consumer instruments
    public final LongCounter consumed;
    public final LongCounter processingErrors;
    public final DoubleHistogram processingLatency;
    public final LongCounter rebalances;

    // Consumer lag — observable gauge backed by a concurrent map
    private final ConcurrentHashMap<Attributes, Long> lagValues = new ConcurrentHashMap<>();
    @SuppressWarnings("unused")
    private final ObservableLongGauge lagGauge; // held to prevent GC

    public KafkaMetrics(String serviceName, String environment) {
        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.of(
                        ServiceAttributes.SERVICE_NAME, serviceName,
                        ServiceAttributes.SERVICE_VERSION, env("SERVICE_VERSION", "dev")
                )).merge(Resource.create(Attributes.builder()
                        .put("deployment.environment", environment)
                        .build()))
        );

        OtlpGrpcMetricExporterBuilder exporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(otlpEndpoint());

        String licenseKey = System.getenv("NEW_RELIC_LICENSE_KEY");
        String endpoint = otlpEndpoint();
        if (endpoint.contains("nr-data.net") && licenseKey != null && !licenseKey.isBlank()) {
            exporterBuilder.addHeader("api-key", licenseKey);
        }

        OtlpGrpcMetricExporter exporter = exporterBuilder.build();

        long exportIntervalMs = Long.parseLong(env("OTEL_METRIC_EXPORT_INTERVAL_MS", "10000"));
        PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
                .setInterval(Duration.ofMillis(exportIntervalMs))
                .build();

        meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(reader)
                .build();

        Meter meter = meterProvider.get("java-kafka-monitoring");

        produced = meter.counterBuilder("kafka_app.produced_records")
                .setUnit("{record}").setDescription("Records accepted by a producer").build();
        deliveryErrors = meter.counterBuilder("kafka_app.producer_delivery_errors")
                .setUnit("{error}").setDescription("Producer delivery errors").build();
        deliveryLatency = meter.histogramBuilder("kafka_app.producer_delivery_latency_ms")
                .setUnit("ms").setDescription("Producer delivery latency").build();

        consumed = meter.counterBuilder("kafka_app.consumed_records")
                .setUnit("{record}").setDescription("Records returned by consumer poll").build();
        processingErrors = meter.counterBuilder("kafka_app.consumer_processing_errors")
                .setUnit("{error}").setDescription("Consumer processing errors").build();
        processingLatency = meter.histogramBuilder("kafka_app.consumer_processing_latency_ms")
                .setUnit("ms").setDescription("Application processing latency").build();
        rebalances = meter.counterBuilder("kafka_app.consumer_rebalances")
                .setUnit("{rebalance}").setDescription("Consumer assignment events").build();

        lagGauge = meter.gaugeBuilder("kafka_app.consumer_lag_records")
                .ofLongs()
                .setUnit("{record}").setDescription("Estimated consumer lag by topic and partition")
                .buildWithCallback(measurement -> lagValues.forEach((attrs, value) -> measurement.record(value, attrs)));
    }

    public void setLag(long lag, Attributes attrs) {
        lagValues.put(attrs, Math.max(0, lag));
    }

    @Override
    public void close() {
        meterProvider.shutdown();
    }

    private static String otlpEndpoint() {
        String raw = env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317").trim();
        // OtlpGrpcMetricExporter expects a full URI including scheme
        return raw;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }
}
