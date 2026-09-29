package com.example.kafka;

import io.opentelemetry.api.common.Attributes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Minimal Kafka consumer with OTel metrics.
 * Mirrors python/confluent_kafka/consumer.py.
 *
 * Env vars (see env.example at repo root):
 *   KAFKA_TOPIC              (default: demo-events)
 *   KAFKA_CLIENT_ID          (default: java-monitoring-consumer)
 *   KAFKA_GROUP_ID           (default: <client_id>-<random UUID prefix>)
 *   KAFKA_AUTO_OFFSET_RESET  (default: earliest)
 *   DEPLOYMENT_ENVIRONMENT   (default: dev)
 *   PROCESSING_MS            (default: 10)
 *   OTEL_SERVICE_NAME        (default: java-kafka-consumer)
 */
public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        String topic = env("KAFKA_TOPIC", "demo-events");
        String clientId = env("KAFKA_CLIENT_ID", "java-monitoring-consumer");
        // No KAFKA_GROUP_ID set: pick a group unique to this run so unrelated runs against a
        // shared broker don't collide. Set KAFKA_GROUP_ID explicitly to resume the same group
        // (and its committed offsets) across restarts.
        String groupId = envOrElse("KAFKA_GROUP_ID", clientId + "-" + UUID.randomUUID().toString().substring(0, 8));
        String environment = env("DEPLOYMENT_ENVIRONMENT", "dev");
        long processingMs = Long.parseLong(env("PROCESSING_MS", "10"));

        KafkaMetrics metrics = new KafkaMetrics(env("OTEL_SERVICE_NAME", "java-kafka-consumer"), environment);
        log.info("consumer_starting client_id={} group_id={} topic={}", clientId, groupId, topic);

        Map<String, Object> config = KafkaConfig.build(clientId, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env("KAFKA_AUTO_OFFSET_RESET", "earliest"));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Attributes commonAttrs = Attributes.builder()
                .put("client.id", clientId)
                .put("consumer.group", groupId)
                .put("topic", topic)
                .build();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);

        // Shutdown hook: wakeup unblocks the poll loop cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown_requested");
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Collections.singletonList(topic), new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
                    metrics.rebalances.add(1, commonAttrs);
                    log.info("partitions_revoked partitions={}", partitions);
                }

                @Override
                public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
                    metrics.rebalances.add(1, commonAttrs);
                    log.info("partitions_assigned partitions={}", partitions);
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    Attributes attrs = Attributes.builder()
                            .putAll(commonAttrs)
                            .put("partition", record.partition())
                            .build();
                    metrics.consumed.add(1, attrs);

                    long startNs = System.nanoTime();
                    try {
                        // Simulate application processing
                        if (processingMs > 0) {
                            Thread.sleep(processingMs);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        metrics.processingErrors.add(1, attrs);
                        log.error("record_processing_interrupted topic={} partition={} offset={}",
                                record.topic(), record.partition(), record.offset());
                        continue;
                    } catch (Exception e) {
                        metrics.processingErrors.add(1, attrs);
                        log.error("record_processing_failed topic={} partition={} offset={}",
                                record.topic(), record.partition(), record.offset(), e);
                        continue;
                    } finally {
                        metrics.processingLatency.record((System.nanoTime() - startNs) / 1_000_000.0, attrs);
                    }

                    // Estimate lag: high watermark − (current offset + 1)
                    try {
                        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(
                                Collections.singletonList(new TopicPartition(record.topic(), record.partition())));
                        Long high = endOffsets.get(new TopicPartition(record.topic(), record.partition()));
                        if (high != null) {
                            metrics.setLag(high - record.offset() - 1, attrs);
                        }
                    } catch (Exception e) {
                        log.debug("lag_estimate_failed", e);
                    }

                    consumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)));
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown
        } finally {
            consumer.close();
            metrics.close();
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }

    private static String envOrElse(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }
}
