# Enterprise Kafka POC-02: Resiliency, Error Isolation & Dead Letter Queues (DLQ)

Apache Kafka 4.0.0 | Docker Compose | Python | FastAPI | kafka-python-ng

## Overview
This proof of concept demonstrates a production-grade **Error Handling & Isolation Pipeline** using Apache Kafka. It specifically addresses the **Poison Pill Anti-Pattern**, where malformed or semantically invalid payloads cause consumer threads to crash, blocking partition advancement and causing systemic lag.

## Architecture Highlights
- **Primary Streaming Topic (`poc-2-orders`)**: Ingests high-throughput business events.
- **Dead Letter Queue Topic (`poc-2-dlq`)**: A dedicated telemetry and isolation topic. Corrupted payloads are redirected here alongside robust metadata (original partition, offset, and Python exception stacktraces) for asynchronous debugging without impacting live traffic.
- **Resilient Consumption Mechanics**:
  - The background consumer implements a strict `try...except` isolation boundary around deserialization and business logic validation.
  - When a Poison Pill is intercepted, the consumer logs a threshold alert, publishes the raw payload to the DLQ, and crucially executes an **explicit `consumer.commit()` on the primary topic**. This deterministic advancement unblocks the partition and guarantees systemic resilience.

## System Interfaces (`/poc-2`)
- `POST /poc-2/send-valid-order` – Ingest a strictly valid business event.
- `POST /poc-2/send-poison-pill` – Ingest a simulated Poison Pill (supports `corrupt_json`, `negative_amount`, and `missing_order_id` variants).
- `GET /poc-2/processed-orders` – Retrieve system state of successfully processed events.
- `GET /poc-2/dlq-orders` – Retrieve system state of isolated errors and DLQ telemetry.

---

## Testing & Verification

### Scenario A: Successful Execution Path
Ingest a valid order that strictly adheres to the schema contract.

```bash
curl -X POST http://localhost:8000/poc-2/send-valid-order \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-7001",
    "customer_id": "CUST-303",
    "amount": 25.50,
    "item_name": "Burger + Fries Combo"
  }'
```

### Scenario B: Triggering DLQ Isolation (Poison Pill)
Inject a malformed payload. The consumer will intercept the crash, route the byte-string to `poc-2-dlq`, and advance the offset.

```bash
curl -X POST http://localhost:8000/poc-2/send-poison-pill \
  -H "Content-Type: application/json" \
  -d '{
    "pill_type": "negative_amount"
  }'
```
*Verify the isolated payload by invoking `GET /poc-2/dlq-orders`.*

For an in-depth architectural breakdown, fault-tolerance sequence matrices, and DLQ implementation details, consult the [Kafka_POC_2_Workflow_Guide.md](./Kafka_POC_2_Workflow_Guide.md).
