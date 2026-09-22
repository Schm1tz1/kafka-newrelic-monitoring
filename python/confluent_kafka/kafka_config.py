import os
from typing import Dict, Optional


def _optional(name: str):
    value = os.getenv(name, "").strip()
    return value or None


def kafka_config(client_id: str, group_id: Optional[str] = None) -> Dict[str, str]:
    config: Dict[str, str] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "client.id": client_id,
        "statistics.interval.ms": int(os.getenv("KAFKA_STATS_INTERVAL_MS", "30000")),
    }

    if group_id:
        config["group.id"] = group_id

    optional_mapping = {
        "sasl.mechanisms": "KAFKA_SASL_MECHANISMS",
        "sasl.username": "KAFKA_SASL_USERNAME",
        "sasl.password": "KAFKA_SASL_PASSWORD",
        "ssl.ca.location": "KAFKA_SSL_CA_LOCATION",
    }
    for kafka_name, env_name in optional_mapping.items():
        value = _optional(env_name)
        if value:
            config[kafka_name] = value

    return config
