import json
import time
import threading
from datetime import datetime, timezone
from collections import deque
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from app.utility.kafka import create_consumer
from app.utility.logging_config import get_logger

logger = get_logger("poc_3_consumer")

DEBEZIUM_TOPIC = "dbserver1.public.users"
CONSUMER_GROUP = "poc-3-cdc-group"

cdc_events_store: deque[dict] = deque(maxlen=100)
store_lock = threading.Lock()

_consumer_thread: threading.Thread | None = None
_stop_event = threading.Event()


def consume_cdc_loop():
    """Background loop listening to Debezium CDC events on topic 'dbserver1.public.users'."""
    logger.info(f"Starting POC-3 Debezium CDC Consumer loop on topic='{DEBEZIUM_TOPIC}' group='{CONSUMER_GROUP}'")

    # Retry loop to wait for Kafka to be ready
    while not _stop_event.is_set():
        try:
            consumer = create_consumer(topic=DEBEZIUM_TOPIC, group_id=CONSUMER_GROUP)
            logger.info("Successfully connected to Kafka for POC-3 CDC events!")
            break
        except Exception as exc:
            logger.error(f"Failed to initialize POC-3 CDC consumer (Topic/Broker not ready yet): {exc}. Retrying in 5s...")
            time.sleep(5)

    if _stop_event.is_set():
        return

    try:
        while not _stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    raw_val = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value)
                    
                    try:
                        payload = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
                        
                        # Handle cases where payload might be wrapped inside a 'payload' object
                        if isinstance(payload, dict) and "payload" in payload:
                            payload = payload["payload"]

                        op_code = payload.get("op") if isinstance(payload, dict) else None
                        
                        op_name = {
                            "c": "CREATE (INSERT)",
                            "u": "UPDATE",
                            "d": "DELETE",
                            "r": "SNAPSHOT (READ)"
                        }.get(op_code, f"UNKNOWN ({op_code})")

                        cdc_record = {
                            "operation": op_name,
                            "op_code": op_code,
                            "before_state": payload.get("before") if isinstance(payload, dict) else None,
                            "after_state": payload.get("after") if isinstance(payload, dict) else None,
                            "timestamp_ms": payload.get("ts_ms") if isinstance(payload, dict) else None,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "captured_at": datetime.now(timezone.utc).isoformat(),
                        }

                        logger.info(f"[CDC EVENT CAPTURED] op={op_name} partition={msg.partition} offset={msg.offset}")

                        with store_lock:
                            cdc_events_store.append(cdc_record)

                        # Explicit manual commit
                        consumer.commit()

                    except Exception as err:
                        logger.error(f"Error parsing Debezium CDC message at offset {msg.offset}: {err}")
                        consumer.commit()

    except KafkaError as exc:
        logger.error(f"KafkaError in POC-3 CDC consumer: {exc}")
    except Exception as exc:
        logger.error(f"Unexpected error in POC-3 CDC consumer: {exc}")
    finally:
        consumer.close()
        logger.info("POC-3 CDC Consumer stopped.")


def start_poc3_consumer() -> dict:
    global _consumer_thread, _stop_event
    if _consumer_thread and _consumer_thread.is_alive():
        return {"status": "already_running"}

    _stop_event.clear()
    _consumer_thread = threading.Thread(target=consume_cdc_loop, daemon=True)
    _consumer_thread.start()
    logger.info("POC-3 Debezium CDC Consumer thread started.")
    return {"status": "started", "topic": DEBEZIUM_TOPIC, "group": CONSUMER_GROUP}


def get_cdc_events(limit: int = 50) -> list[dict]:
    with store_lock:
        return list(cdc_events_store)[-limit:]
