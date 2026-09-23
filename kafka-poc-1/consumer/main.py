"""
Kafka POC — Consumer API
========================
GET  /health              → liveness check
GET  /messages            → return buffered (already-consumed) messages
POST /start               → start background consumer loop
POST /stop                → stop background consumer loop
GET  /ack/{message_id}    → manually acknowledge a specific message (for demo)
"""

import os
import json
import threading
from datetime import datetime, timezone
from collections import deque

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load .env from the project root (one level up from consumer/)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'poc-messages')
KAFKA_CONSUMER_GROUP    = os.getenv('KAFKA_CONSUMER_GROUP', 'poc-consumer-group')

app = FastAPI(title='Kafka POC — Consumer', version='1.0.0')

# ── In-memory message store (deque as a ring buffer of last 100 messages) ─────
message_store: deque[dict] = deque(maxlen=100)
store_lock = threading.Lock()

# ── Consumer loop state ───────────────────────────────────────────────────────
_consumer_thread: threading.Thread | None = None
_stop_event = threading.Event()


# ── Consumer loop (runs in background thread) ─────────────────────────────────
def consume_loop():
    """
    Polls Kafka with manual commit (enable_auto_commit=False).
    After processing each message we call consumer.commit() — this is the
    explicit ACKNOWLEDGEMENT that the offset has been processed successfully.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='earliest',       # start from beginning if no committed offset
        enable_auto_commit=False,           # WE control when to commit (acknowledge)
        value_deserializer=lambda b: json.loads(b.decode('utf-8')),
        consumer_timeout_ms=1_000,          # poll returns after 1s if no messages
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )
    print(f'[Consumer] Started. Listening on topic: {KAFKA_TOPIC}', flush=True)

    try:
        while not _stop_event.is_set():
            # poll() returns a dict of {TopicPartition: [ConsumerRecord, ...]}
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    payload = msg.value  # already deserialized by value_deserializer

                    # ── PROCESSING ────────────────────────────────────────────
                    processed_at = datetime.now(timezone.utc).isoformat()
                    record = {
                        'message_id':   payload.get('message_id', 'unknown'),
                        'content':      payload.get('content', ''),
                        'metadata':     payload.get('metadata', {}),
                        'produced_at':  payload.get('produced_at'),
                        'consumed_at':  processed_at,
                        'partition':    msg.partition,
                        'offset':       msg.offset,
                        'ack_status':   'pending',
                    }
                    print(f'[Consumer] Processing message_id={record["message_id"]} '
                          f'offset={msg.offset} partition={msg.partition}', flush=True)

                    # Simulate processing work here (e.g. DB write, transform, etc.)
                    # ...

                    # ── ACKNOWLEDGEMENT ───────────────────────────────────────
                    # Commit offset for this specific message so Kafka knows it
                    # has been successfully consumed. If we crash before this
                    # line, the message is re-delivered (at-least-once semantics).
                    consumer.commit()
                    record['ack_status'] = 'acknowledged'
                    print(f'[Consumer] ACKed message_id={record["message_id"]}', flush=True)

                    with store_lock:
                        message_store.append(record)

    except KafkaError as exc:
        print(f'[Consumer] KafkaError: {exc}', flush=True)
    finally:
        consumer.close()
        print('[Consumer] Stopped.', flush=True)


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get('/health')
def health():
    return {
        'status': 'ok',
        'service': 'consumer',
        'running': _consumer_thread is not None and _consumer_thread.is_alive(),
        'broker': KAFKA_BOOTSTRAP_SERVERS,
        'topic': KAFKA_TOPIC,
        'group': KAFKA_CONSUMER_GROUP,
    }


@app.post('/start')
def start_consumer():
    """Start the background Kafka consumer loop."""
    global _consumer_thread, _stop_event

    if _consumer_thread and _consumer_thread.is_alive():
        return {'status': 'already_running'}

    _stop_event.clear()
    _consumer_thread = threading.Thread(target=consume_loop, daemon=True)
    _consumer_thread.start()
    return {'status': 'started', 'topic': KAFKA_TOPIC, 'group': KAFKA_CONSUMER_GROUP}


@app.post('/stop')
def stop_consumer():
    """Signal the background consumer loop to stop."""
    global _stop_event
    _stop_event.set()
    return {'status': 'stopping'}


@app.get('/messages')
def get_messages(limit: int = 50):
    """Return the last N messages that have been consumed and acknowledged."""
    with store_lock:
        items = list(message_store)[-limit:]
    return {
        'count': len(items),
        'messages': items,
    }


@app.get('/ack/{message_id}')
def get_ack_status(message_id: str):
    """Look up the ack status of a specific message by its ID."""
    with store_lock:
        for record in message_store:
            if record['message_id'] == message_id:
                return record
    raise HTTPException(status_code=404, detail=f'message_id {message_id!r} not found in buffer')


@app.on_event('startup')
def startup_event():
    """Auto-start the consumer when the FastAPI app boots."""
    start_consumer()
