import uuid
import json
from datetime import datetime, timezone
from kafka.errors import KafkaError
from app.utility.kafka import create_producer, KAFKA_POC_2_ORDERS_TOPIC
from app.utility.logging_config import get_logger
from app.kafka_poc_2.models import ValidOrderRequest, PoisonPillSimulateRequest, OrderResponse

logger = get_logger("poc_2_producer")

_producer = None


def get_producer_instance():
    global _producer
    if _producer is None:
        _producer = create_producer()
    return _producer


def publish_valid_order(request: ValidOrderRequest) -> OrderResponse:
    """Publishes a valid order payload to Kafka topic 'poc-2-orders'."""
    message_id = str(uuid.uuid4())
    payload = {
        "message_id": message_id,
        "order_id": request.order_id,
        "customer_id": request.customer_id,
        "amount": request.amount,
        "item_name": request.item_name,
        "produced_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(f"Publishing Valid Order: order_id={request.order_id} amount=${request.amount}")

    try:
        producer = get_producer_instance()
        future = producer.send(
            topic=KAFKA_POC_2_ORDERS_TOPIC,
            key=request.customer_id,
            value=payload,
        )
        metadata = future.get(timeout=10)
        producer.flush()

        return OrderResponse(
            message_id=message_id,
            status="valid_order_published",
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except KafkaError as exc:
        logger.error(f"Failed to publish valid order: {exc}")
        raise exc


def publish_poison_pill(request: PoisonPillSimulateRequest) -> OrderResponse:
    """Publishes a Poison Pill payload (bad data) to Kafka topic 'poc-2-orders' to test consumer resilience."""
    message_id = str(uuid.uuid4())

    if request.pill_type == "corrupt_json":
        # Raw malformed non-JSON string
        payload = request.custom_payload or "POISON_PILL_BAD_RAW_STRING_{corrupt_json: true"
    elif request.pill_type == "negative_amount":
        # Valid JSON structure but invalid business payload (negative amount)
        payload = {
            "message_id": message_id,
            "order_id": "ORD-POISON-001",
            "customer_id": "BAD-CUST",
            "amount": -50.00,  # Poison Pill: Negative amount!
            "item_name": "Corrupt Negative Payment Order",
            "produced_at": datetime.now(timezone.utc).isoformat(),
        }
    else:  # missing_order_id
        # Invalid schema: missing required 'order_id'
        payload = {
            "message_id": message_id,
            "customer_id": "BAD-CUST",
            "amount": 10.00,
            "item_name": "Missing Order ID Payload",
            "produced_at": datetime.now(timezone.utc).isoformat(),
        }

    logger.warning(f"[POISON PILL PRODUCER] Publishing poison pill type={request.pill_type} payload={payload}")

    try:
        producer = get_producer_instance()
        future = producer.send(
            topic=KAFKA_POC_2_ORDERS_TOPIC,
            key="POISON_PILL_KEY",
            value=payload,
        )
        metadata = future.get(timeout=10)
        producer.flush()

        return OrderResponse(
            message_id=message_id,
            status=f"poison_pill_published ({request.pill_type})",
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except KafkaError as exc:
        logger.error(f"Failed to publish poison pill: {exc}")
        raise exc
