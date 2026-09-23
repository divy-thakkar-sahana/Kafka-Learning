# Kafka POC — Producer + Consumer with Acknowledgement

A minimal, well-commented FastAPI + kafka-python proof-of-concept showing:
- A **Producer API** that publishes messages to a Kafka topic
- A **Consumer API** that consumes messages with **manual commit (explicit ACK)**

---

## Project structure

```
kafka-poc/
├── .env                   # Shared config (broker, topic, group)
├── producer/
│   ├── main.py            # FastAPI Producer API  (port 8001)
│   └── requirements.txt
└── consumer/
    ├── main.py            # FastAPI Consumer API  (port 8002)
    └── requirements.txt
```

---

## 1. Configure

Edit `.env` and point `KAFKA_BOOTSTRAP_SERVERS` at your Kafka broker:

```env
KAFKA_BOOTSTRAP_SERVERS=<your-kafka-host>:9092
KAFKA_TOPIC=poc-messages
KAFKA_CONSUMER_GROUP=poc-consumer-group
```

---

## 2. Install dependencies

```bash
# Producer
cd producer
pip install -r requirements.txt

# Consumer (new terminal)
cd consumer
pip install -r requirements.txt
```

---

## 3. Run

```bash
# Terminal 1 — Producer (port 8001)
cd producer
uvicorn main:app --host 0.0.0.0 --port 8001 --reload

# Terminal 2 — Consumer (port 8002)
cd consumer
uvicorn main:app --host 0.0.0.0 --port 8002 --reload
```

---

## 4. Test the flow

### Step 1 — Send a message (Producer)
```bash
curl -X POST http://localhost:8001/send \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello Kafka!", "key": "test-1", "metadata": {"source": "poc"}}'
```

Response:
```json
{
  "message_id": "a1b2c3...",
  "topic": "poc-messages",
  "partition": 0,
  "offset": 0,
  "timestamp": "2026-09-23T...",
  "status": "delivered"
}
```

### Step 2 — Check messages consumed (Consumer)
```bash
curl http://localhost:8002/messages
```

Response:
```json
{
  "count": 1,
  "messages": [
    {
      "message_id": "a1b2c3...",
      "content": "Hello Kafka!",
      "produced_at": "...",
      "consumed_at": "...",
      "partition": 0,
      "offset": 0,
      "ack_status": "acknowledged"   <-- explicit commit was called
    }
  ]
}
```

### Step 3 — Check ACK status for a specific message
```bash
curl http://localhost:8002/ack/a1b2c3...
```

---

## How the ACK works

| Step | Code | Kafka effect |
|------|------|--------------|
| `enable_auto_commit=False` | `consumer/main.py` | Kafka will NOT auto-advance the offset |
| Message arrives | `consume_loop()` | Message is processed in Python |
| `consumer.commit()` | after processing | Kafka is told: "this offset is done" |
| Crash before commit | — | Message is **re-delivered** (at-least-once) |

---

## API Reference

### Producer (port 8001)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness check |
| POST | `/send` | Publish a message |

### Consumer (port 8002)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness + consumer thread status |
| POST | `/start` | Start consumer loop |
| POST | `/stop` | Stop consumer loop |
| GET | `/messages` | List consumed messages |
| GET | `/ack/{message_id}` | Check ACK status of a message |
