import os
import json
import socket
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from app.utility.logging_config import get_logger

logger = get_logger("kafka_utility")

# Load .env file
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# POC-1 Topics
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "Food Orders")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "poc-consumer-group")

# POC-2 Topics & Groups
KAFKA_POC_2_ORDERS_TOPIC = os.getenv("KAFKA_POC_2_ORDERS_TOPIC", "poc-2-orders")
KAFKA_POC_2_DLQ_TOPIC = os.getenv("KAFKA_POC_2_DLQ_TOPIC", "poc-2-dlq")
KAFKA_POC_2_GROUP = os.getenv("KAFKA_POC_2_GROUP", "poc-2-order-group")


def check_kafka_connection(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> bool:
    """Checks if Kafka broker host and port are reachable."""
    for server in bootstrap_servers.split(","):
        try:
            host, port = server.strip().split(":")
            with socket.create_connection((host, int(port)), timeout=2):
                logger.info(f"Kafka broker connection verified: {server}")
                return True
        except Exception as err:
            logger.warning(f"Kafka broker connection check failed for {server}: {err}")
    return False


def create_producer() -> KafkaProducer:
    """Factory function creating a Kafka Producer instance."""
    logger.info(f"Creating KafkaProducer for broker: {KAFKA_BOOTSTRAP_SERVERS}")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, (dict, list)) else str(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        request_timeout_ms=30_000,
    )


def create_consumer(topic: str = KAFKA_TOPIC, group_id: str = KAFKA_CONSUMER_GROUP) -> KafkaConsumer:
    """Factory function creating a Kafka Consumer instance with manual commit."""
    logger.info(f"Creating KafkaConsumer for topic: {topic} group: {group_id}")
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Explicit manual offset commit
        consumer_timeout_ms=1_000,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )
