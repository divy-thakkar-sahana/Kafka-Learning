import json
import time
import threading
from datetime import datetime, timezone
from kafka.errors import KafkaError
from app.utility.kafka import create_consumer
from app.utility.logging_config import get_logger
from app.kafka_poc_3.es_client import get_es_client, INDEX_NAME

logger = get_logger("poc_3_es_sync_consumer")

PRODUCTS_CDC_TOPIC = "dbserver1.public.products"
CONSUMER_GROUP = "poc-3-es-syncer-group"

_sync_thread: threading.Thread | None = None
_stop_event = threading.Event()


def consume_products_cdc_loop():
    """Background polling loop reading Products CDC events and syncing to Elasticsearch."""
    logger.info(f"Starting Elasticsearch Sync Consumer on topic='{PRODUCTS_CDC_TOPIC}' group='{CONSUMER_GROUP}'")

    # Retry loop to wait for Kafka to be ready
    while not _stop_event.is_set():
        try:
            consumer = create_consumer(topic=PRODUCTS_CDC_TOPIC, group_id=CONSUMER_GROUP)
            logger.info("Elasticsearch Sync Consumer connected to Kafka successfully!")
            break
        except Exception as exc:
            logger.error(f"Failed to initialize ES sync consumer: {exc}. Retrying in 5s...")
            time.sleep(5)

    if _stop_event.is_set():
        return

    es = get_es_client()

    try:
        while not _stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    raw_val = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value)
                    
                    try:
                        payload = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
                        if isinstance(payload, dict) and "payload" in payload:
                            payload = payload["payload"]

                        op_code = payload.get("op") if isinstance(payload, dict) else None

                        if op_code in ("c", "u", "r"):
                            after_state = payload.get("after")
                            if after_state and "id" in after_state:
                                doc_id = str(after_state["id"])
                                doc = {
                                    "id": after_state["id"],
                                    "name": after_state.get("name"),
                                    "description": after_state.get("description"),
                                    "category": after_state.get("category"),
                                    "price": float(after_state.get("price", 0.0)),
                                    "stock_quantity": int(after_state.get("stock_quantity", 0)),
                                    "updated_at": datetime.now(timezone.utc).isoformat()
                                }
                                es.index(index=INDEX_NAME, id=doc_id, document=doc)
                                logger.info(f"[ES SYNC INDEXED] Product ID={doc_id} ('{doc['name']}') op={op_code}")

                        elif op_code == "d":
                            before_state = payload.get("before")
                            if before_state and "id" in before_state:
                                doc_id = str(before_state["id"])
                                try:
                                    es.delete(index=INDEX_NAME, id=doc_id)
                                    logger.info(f"[ES SYNC DELETED] Product ID={doc_id} from Elasticsearch index")
                                except Exception as es_err:
                                    logger.warning(f"Could not delete product ID={doc_id} from ES: {es_err}")

                        consumer.commit()

                    except Exception as err:
                        logger.error(f"Error syncing product CDC message at offset {msg.offset} to ES: {err}")
                        consumer.commit()

    except KafkaError as exc:
        logger.error(f"KafkaError in ES sync consumer: {exc}")
    except Exception as exc:
        logger.error(f"Unexpected error in ES sync consumer: {exc}")
    finally:
        consumer.close()
        logger.info("Elasticsearch Sync Consumer stopped.")


def start_es_sync_consumer() -> dict:
    global _sync_thread, _stop_event
    if _sync_thread and _sync_thread.is_alive():
        return {"status": "already_running"}

    _stop_event.clear()
    _sync_thread = threading.Thread(target=consume_products_cdc_loop, daemon=True)
    _sync_thread.start()
    logger.info("Elasticsearch CDC Sync Consumer thread started.")
    return {"status": "started", "topic": PRODUCTS_CDC_TOPIC, "group": CONSUMER_GROUP}
