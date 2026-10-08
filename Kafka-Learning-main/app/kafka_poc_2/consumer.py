import json
import threading
from datetime import datetime, timezone
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from app.utility.kafka import (
    create_consumer,
    create_producer,
    KAFKA_POC_2_ORDERS_TOPIC,
    KAFKA_POC_2_DLQ_TOPIC,
    KAFKA_POC_2_GROUP,
)
from app.utility.logging_config import get_logger

logger = get_logger("poc_2_consumer")

# In-memory ring buffers for inspectability
processed_orders_store: deque[dict] = deque(maxlen=100)
dlq_orders_store: deque[dict] = deque(maxlen=100)

store_lock = threading.Lock()

_consumer_thread: threading.Thread | None = None
_stop_event = threading.Event()


def send_to_dlq(producer: KafkaProducer, raw_payload: str, error_reason: str, partition: int, offset: int):
    """Routes a Poison Pill message to the Dead Letter Queue topic ('poc-2-dlq')."""
    dlq_record = {
        "dlq_status": "POISON_PILL_DIVERTED",
        "error_reason": error_reason,
        "original_topic": KAFKA_POC_2_ORDERS_TOPIC,
        "original_partition": partition,
        "original_offset": offset,
        "diverted_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload": raw_payload,
    }

    try:
        future = producer.send(
            topic=KAFKA_POC_2_DLQ_TOPIC,
            value=dlq_record,
        )
        future.get(timeout=5)
        producer.flush()
        logger.warning(f"[DLQ ROUTER] Diverted poison pill from offset {offset} to topic '{KAFKA_POC_2_DLQ_TOPIC}'")
    except Exception as exc:
        logger.error(f"Failed to forward message to DLQ: {exc}")

    with store_lock:
        dlq_orders_store.append(dlq_record)


def consume_loop():

    import time
    
    logger.info(f"Starting POC-2 Consumer loop on topic='{KAFKA_POC_2_ORDERS_TOPIC}' group='{KAFKA_POC_2_GROUP}'")
    
    # Retry loop to wait for Kafka to be ready
    while not _stop_event.is_set():
        try:
            consumer = create_consumer(topic=KAFKA_POC_2_ORDERS_TOPIC, group_id=KAFKA_POC_2_GROUP)
            dlq_producer = create_producer()
            logger.info("Successfully connected to Kafka brokers!")
            break
        except Exception as exc:
            logger.error(f"Failed to initialize POC-2 consumer (Kafka might not be ready yet): {exc}. Retrying in 5 seconds...")
            time.sleep(5)
            
    if _stop_event.is_set():
        return

    try:
        while not _stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    raw_val = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value)
                    processed_at = datetime.now(timezone.utc).isoformat()

                    try:
                        # 1. Attempt JSON Deserialization
                        payload = json.loads(raw_val)

                        # 2. Business Logic Validation
                        if not isinstance(payload, dict):
                            raise ValueError(f"Payload is not a valid JSON object: {payload!r}")

                        order_id = payload.get("order_id")
                        if not order_id:
                            raise ValueError("Missing mandatory field 'order_id'")

                        amount = payload.get("amount", 0)
                        if amount <= 0:
                            raise ValueError(f"Poison Pill: Invalid non-positive amount ${amount}")

                        # --- SUCCESSFUL VALID ORDER PROCESSING ---
                        record = {
                            "order_id": order_id,
                            "customer_id": payload.get("customer_id"),
                            "amount": amount,
                            "item_name": payload.get("item_name"),
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "consumed_at": processed_at,
                            "status": "PROCESSED_SUCCESSFULLY",
                        }

                        # Commit offset for valid order
                        consumer.commit()
                        logger.info(f"[ORDER PROCESSED] order_id={order_id} amount=${amount} offset={msg.offset}")

                        with store_lock:
                            processed_orders_store.append(record)

                    except Exception as err:
                        # --- POISON PILL DETECTED! DLQ REDIRECTION ---
                        error_reason = str(err)
                        logger.error(
                            f"[POISON PILL DETECTED] at partition={msg.partition} offset={msg.offset}: {error_reason}. "
                            f"Diverting to DLQ..."
                        )

                        # 1. Forward corrupt payload to DLQ
                        send_to_dlq(
                            producer=dlq_producer,
                            raw_payload=raw_val,
                            error_reason=error_reason,
                            partition=msg.partition,
                            offset=msg.offset,
                        )

                        # 2. EXPLICIT MANUAL COMMIT on main topic offset to skip poison pill!
                        consumer.commit()
                        logger.info(f"[POISON PILL SKIPPED] Successfully committed offset {msg.offset} on '{KAFKA_POC_2_ORDERS_TOPIC}'")

    except KafkaError as exc:
        logger.error(f"KafkaError in POC-2 consumer: {exc}")
    except Exception as exc:
        logger.error(f"Unexpected error in POC-2 consumer: {exc}")
    finally:
        consumer.close()
        logger.info("POC-2 Consumer stopped.")


def start_poc2_consumers() -> dict:
    global _consumer_thread, _stop_event
    if _consumer_thread and _consumer_thread.is_alive():
        return {"status": "already_running"}

    _stop_event.clear()
    _consumer_thread = threading.Thread(target=consume_loop, daemon=True)
    _consumer_thread.start()
    logger.info("POC-2 Poison Pill / DLQ Consumer thread started.")
    return {"status": "started", "topic": KAFKA_POC_2_ORDERS_TOPIC, "dlq_topic": KAFKA_POC_2_DLQ_TOPIC}


def get_processed_orders(limit: int = 50) -> list[dict]:
    with store_lock:
        return list(processed_orders_store)[-limit:]


def get_dlq_orders(limit: int = 50) -> list[dict]:
    with store_lock:
        return list(dlq_orders_store)[-limit:]
