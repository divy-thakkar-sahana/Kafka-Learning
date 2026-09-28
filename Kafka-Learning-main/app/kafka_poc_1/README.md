# Enterprise Kafka POC-01: Core Streaming & Delivery Semantics

Apache Kafka 4.0.0 | Docker Compose | Python | FastAPI | kafka-python-ng

## Overview
This proof of concept demonstrates a production-grade **Event Streaming Pipeline** tailored for high-throughput microservices. It focuses on deterministic partition routing, stateful manual offset management, and At-Least-Once delivery guarantees.

## Architecture Highlights
- **Producer Edge**: A FastAPI interface exposing endpoints to dispatch events with granular control over partition assignment.
- **Routing Algorithms**:
  1. **Semantic Key Hashing (`use_key: true`)**: Utilizes MurmurHash2 on `customer_id` to guarantee strict chronological ordering per entity.
  2. **Unkeyed Auto-Routing (`use_key: false`)**: Leverages Kafka's default Round-Robin / Sticky assignment for stateless, highly concurrent processing.
  3. **Strict Assignment (`explicit_partition`)**: Bypasses the partitioner for specialized routing or remediation workflows.
- **Resilient Consumption**: A background worker thread polling events with `enable_auto_commit=False`, utilizing explicit `consumer.commit()` calls only after successful business logic execution to prevent data loss.

## System Interfaces (`/poc-1`)
- `POST /poc-1/send` – Ingest event with configurable routing strategies.
- `GET /poc-1/orders` – Retrieve system state (successfully consumed and acknowledged events).

---

## Testing & Verification

### Scenario A: Semantic Key Routing (Strict Ordering)
Ensures all events for `CUST-402` deterministically route to the exact same partition.

```bash
curl -X POST http://localhost:8000/poc-1/send \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-9822",
    "customer_id": "CUST-402",
    "delivery_partner_id": "DRIVER-77",
    "item_name": "Garlic Naan",
    "use_key": true
  }'
```

### Scenario B: Strict Partition Override
Forces the broker to write the event explicitly to `Partition 2`.

```bash
curl -X POST http://localhost:8000/poc-1/send \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-9821",
    "customer_id": "CUST-402",
    "delivery_partner_id": "DRIVER-77",
    "item_name": "Paneer Butter Masala + Butter Naan",
    "use_key": false,
    "explicit_partition": 2
  }'
```

For an in-depth architectural breakdown, sequence diagrams, and offset lifecycle mechanics, consult the [Kafka_POC_1_Workflow_Guide.md](./Kafka_POC_1_Workflow_Guide.md).
