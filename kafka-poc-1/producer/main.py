import os
import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load .env from the project root (one level up from producer/)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'poc-messages')

app = FastAPI(title='Kafka POC — Producer', version='1.0.0')

# ── Kafka producer (singleton) ────────────────────────────────────────────────
_producer: KafkaProducer | None = None

def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',            # wait for all replicas to ack
            retries=3,
            request_timeout_ms=30_000,
        )
    return _producer


# ── Request / Response models ─────────────────────────────────────────────────
class MessageRequest(BaseModel):
    content: str = Field(..., min_length=1, description='Message body to send')
    key: str | None = Field(None, description='Optional Kafka partition key')
    metadata: dict | None = Field(None, description='Optional extra metadata')


class MessageResponse(BaseModel):
    message_id: str
    topic: str
    partition: int
    offset: int
    timestamp: str
    status: str


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get('/health')
def health():
    return {'status': 'ok', 'service': 'producer', 'broker': KAFKA_BOOTSTRAP_SERVERS}


@app.post('/send', response_model=MessageResponse)
def send_message(request: MessageRequest):
    """
    Publish a message to the Kafka topic.
    Returns partition + offset confirming delivery.
    """
    message_id = str(uuid.uuid4())
    payload = {
        'message_id': message_id,
        'content':    request.content,
        'metadata':   request.metadata or {},
        'produced_at': datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer = get_producer()
        future = producer.send(
            topic=KAFKA_TOPIC,
            key=request.key or message_id,
            value=payload,
        )
        # Block until broker acknowledges (or raises on failure)
        record_metadata = future.get(timeout=10)
        producer.flush()

    except KafkaError as exc:
        raise HTTPException(status_code=503, detail=f'Kafka send failed: {exc}')

    return MessageResponse(
        message_id=message_id,
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
        timestamp=datetime.now(timezone.utc).isoformat(),
        status='delivered',
    )
