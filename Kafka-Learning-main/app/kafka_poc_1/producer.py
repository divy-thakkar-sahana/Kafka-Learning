import uuid
from datetime import datetime, timezone
from kafka.errors import KafkaError
from app.utility.kafka import create_producer, KAFKA_TOPIC
from app.utility.logging_config import get_logger
from app.kafka_poc_1.models import OrderDeliveryRequest, OrderDeliveryResponse

logger = get_logger("poc_1_producer")

_producer = None


def get_producer_instance():
    global _producer
    if _producer is None:
        _producer = create_producer()
    return _producer


def publish_delivery_event(request: OrderDeliveryRequest) -> OrderDeliveryResponse:
    """
    Publish an Order Delivery event to Kafka topic with producer partitioning options:
    - If explicit_partition is provided: Sends directly to that partition (0, 1, or 2).
    - Else if use_key is True: Uses customer_id as key for hashing (MurmurHash2).
    - Else: Sends without key (Kafka auto-partitioning).
    """
    message_id = str(uuid.uuid4())
    partition_key = request.customer_id if request.use_key else None

    payload = {
        "message_id": message_id,
        "order_id": request.order_id,
        "customer_id": request.customer_id,
        "delivery_partner_id": request.delivery_partner_id,
        "item_name": request.item_name,
        "produced_at": datetime.now(timezone.utc).isoformat(),
    }

    if request.explicit_partition is not None:
        routing_mode = "explicit_partition"
    elif request.use_key:
        routing_mode = "customer_key_hash"
    else:
        routing_mode = "unkeyed_auto"

    logger.info(
        f"Publishing Order Delivery event: order_id={request.order_id} mode={routing_mode} "
        f"explicit_partition={request.explicit_partition} key={partition_key}"
    )

    try:
        producer = get_producer_instance()

        send_args = {
            "topic": KAFKA_TOPIC,
            "value": payload,
        }

        if request.explicit_partition is not None:
            send_args["partition"] = request.explicit_partition
        elif partition_key:
            send_args["key"] = partition_key

        future = producer.send(**send_args)
        record_metadata = future.get(timeout=10)
        producer.flush()

        logger.info(
            f"Successfully published order_id={request.order_id} "
            f"partition={record_metadata.partition} offset={record_metadata.offset}"
        )

        return OrderDeliveryResponse(
            message_id=message_id,
            order_id=request.order_id,
            customer_id=request.customer_id,
            delivery_partner_id=request.delivery_partner_id,
            item_name=request.item_name,
            routing_mode=routing_mode,
            partition_key=partition_key,
            topic=record_metadata.topic,
            partition=record_metadata.partition,
            offset=record_metadata.offset,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except KafkaError as exc:
        logger.error(f"Kafka send failed for order_id={request.order_id}: {exc}")
        raise exc
