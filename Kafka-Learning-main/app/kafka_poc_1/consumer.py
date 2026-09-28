import threading
from datetime import datetime, timezone
from collections import deque
from kafka.errors import KafkaError
from app.utility.kafka import create_consumer, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP
from app.utility.logging_config import get_logger

logger = get_logger("poc_1_consumer")

message_store: deque[dict] = deque(maxlen=100)
store_lock = threading.Lock()

_consumer_thread: threading.Thread | None = None
_stop_event = threading.Event()


def consume_loop():
    """
    Background polling loop consuming Order Delivery events with manual offset commit (ACK).
    """
    import time
    
    logger.info(f"Starting Order Delivery Consumer loop on topic={KAFKA_TOPIC} group={KAFKA_CONSUMER_GROUP}")
    
    while not _stop_event.is_set():
        try:
            consumer = create_consumer()
            logger.info("Successfully connected to Kafka brokers!")
            break
        except Exception as exc:
            logger.error(f"Failed to initialize Kafka consumer (Kafka might not be ready yet): {exc}. Retrying in 5 seconds...")
            time.sleep(5)
            
    if _stop_event.is_set():
        return

    try:
        while not _stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    payload = msg.value  # json deserialized
                    processed_at = datetime.now(timezone.utc).isoformat()

                    record = {
                        "message_id": payload.get("message_id", "unknown"),
                        "order_id": payload.get("order_id", "unknown"),
                        "customer_id": payload.get("customer_id", "unknown"),
                        "delivery_partner_id": payload.get("delivery_partner_id", "unknown"),
                        "item_name": payload.get("item_name", ""),
                        "produced_at": payload.get("produced_at"),
                        "consumed_at": processed_at,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "ack_status": "pending",
                    }
                    logger.info(
                        f"Processing order_id={record['order_id']} customer={record['customer_id']} "
                        f"partition={msg.partition} offset={msg.offset}"
                    )

                    # Explicit manual commit (acknowledgement)
                    consumer.commit()
                    record["ack_status"] = "acknowledged"
                    logger.info(f"ACKed order_id={record['order_id']} offset={msg.offset}")

                    with store_lock:
                        message_store.append(record)

    except KafkaError as exc:
        logger.error(f"KafkaError in order consumer poll loop: {exc}")
    except Exception as exc:
        logger.error(f"Unexpected error in order consumer poll loop: {exc}")
    finally:
        consumer.close()
        logger.info("Order Delivery Consumer loop stopped.")


def start_consumer_thread() -> dict:
    global _consumer_thread, _stop_event
    if _consumer_thread and _consumer_thread.is_alive():
        return {"status": "already_running"}

    _stop_event.clear()
    _consumer_thread = threading.Thread(target=consume_loop, daemon=True)
    _consumer_thread.start()
    logger.info("Background Order Delivery consumer thread started.")
    return {"status": "started", "topic": KAFKA_TOPIC, "group": KAFKA_CONSUMER_GROUP}


def get_buffered_orders(limit: int = 50) -> list[dict]:
    with store_lock:
        return list(message_store)[-limit:]
