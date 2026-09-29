package com.example.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Builds a Kafka client config map from environment variables.
 * Mirrors python/confluent_kafka/kafka_config.py exactly.
 *
 * Required env vars:
 *   KAFKA_BOOTSTRAP_SERVERS   (default: localhost:29092)
 *   KAFKA_SECURITY_PROTOCOL   (default: PLAINTEXT)
 *
 * Optional env vars (omitted when blank):
 *   KAFKA_SASL_MECHANISMS
 *   KAFKA_SASL_USERNAME
 *   KAFKA_SASL_PASSWORD
 *   KAFKA_SSL_CA_LOCATION
 */
public class KafkaConfig {

    public static Map<String, Object> build(String clientId) {
        return build(clientId, null);
    }

    public static Map<String, Object> build(String clientId, String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"));
        config.put("security.protocol", env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"));
        config.put("client.id", clientId);

        if (groupId != null) {
            config.put("group.id", groupId);
        }

        optional("KAFKA_SASL_MECHANISMS").ifPresent(v -> config.put("sasl.mechanism", v));
        optional("KAFKA_SASL_USERNAME").ifPresent(v -> config.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"" + v + "\" "
                        + "password=\"" + env("KAFKA_SASL_PASSWORD", "") + "\";"));
        optional("KAFKA_SSL_CA_LOCATION").ifPresent(v -> config.put("ssl.truststore.location", v));

        return config;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value.trim() : defaultValue;
    }

    private static Optional<String> optional(String name) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? Optional.of(value.trim()) : Optional.empty();
    }
}
