package com.example.kafka;

import io.opentelemetry.api.common.Attributes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Minimal Kafka producer with OTel metrics.
 * Mirrors python/confluent_kafka/producer.py.
 *
 * Env vars (see env.example at repo root):
 *   KAFKA_TOPIC              (default: demo-events)
 *   KAFKA_CLIENT_ID          (default: java-monitoring-producer)
 *   DEPLOYMENT_ENVIRONMENT   (default: dev)
 *   PRODUCER_MESSAGES        (default: 100)
 *   PRODUCER_INTERVAL_MS     (default: 100)
 *   OTEL_SERVICE_NAME        (default: java-kafka-producer)
 */
public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws InterruptedException {
        String topic = env("KAFKA_TOPIC", "demo-events");
        String clientId = env("KAFKA_CLIENT_ID", "java-monitoring-producer");
        String environment = env("DEPLOYMENT_ENVIRONMENT", "dev");
        int count = Integer.parseInt(env("PRODUCER_MESSAGES", "100"));
        long intervalMs = Long.parseLong(env("PRODUCER_INTERVAL_MS", "100"));

        KafkaMetrics metrics = new KafkaMetrics(env("OTEL_SERVICE_NAME", "java-kafka-producer"), environment);

        Map<String, Object> config = KafkaConfig.build(clientId);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        Attributes commonAttrs = Attributes.builder()
                .put("client.id", clientId)
                .put("topic", topic)
                .build();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < count; i++) {
                String payload = "{\"sequence\":" + i + ",\"created_at\":" + (System.currentTimeMillis() / 1000.0) + "}";
                long startNs = System.nanoTime();

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);
                producer.send(record, (metadata, exception) -> {
                    double latencyMs = (System.nanoTime() - startNs) / 1_000_000.0;
                    if (exception != null) {
                        metrics.deliveryErrors.add(1, commonAttrs);
                        log.error("delivery_failed topic={} error={}", topic, exception.getMessage());
                    } else {
                        Attributes attrs = Attributes.builder()
                                .putAll(commonAttrs)
                                .put("partition", metadata.partition())
                                .build();
                        metrics.deliveryLatency.record(latencyMs, attrs);
                        log.debug("delivery_succeeded topic={} partition={} offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });

                metrics.produced.add(1, commonAttrs);
                if (intervalMs > 0) {
                    Thread.sleep(intervalMs);
                }
            }
            producer.flush();
            log.info("producer_done sent={}", count);
        } catch (Exception e) {
            log.error("producer_error", e);
        } finally {
            metrics.close();
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }
}
