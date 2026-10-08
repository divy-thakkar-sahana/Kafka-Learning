# Enterprise CDC Architecture: Debezium + PostgreSQL + Kafka

This guide explains the complete Change Data Capture (CDC) workflow in **Kafka POC-3**, including how Debezium connects to PostgreSQL, monitors database changes, and streams them into Apache Kafka automatically.

---

## 🛑 1. The Problem: The Dual-Write Anti-Pattern

In traditional systems, applications that need to both persist data and emit events face a **Dual-Write problem**.

```mermaid
flowchart TD
    App["Application Code"] --> DB_Write["1. Write to Database"]
    App --> Kafka_Write["2. Write to Kafka"]

    DB_Write --> DB_Success{"DB Write OK?"}
    DB_Success -->|Yes| Kafka_Write
    DB_Success -->|No - App crashes| Lost["❌ Data inconsistency! DB and Kafka are now OUT OF SYNC."]
    
    Kafka_Write --> Kafka_Success{"Kafka Write OK?"}
    Kafka_Success -->|Yes| AllGood["✅ Both systems in sync"]
    Kafka_Success -->|No - Kafka is down| DBOnly["❌ DB has the record but Kafka never got the event!"]
```

**The Risk:** If the application crashes between step 1 and step 2, or if Kafka is temporarily unavailable, the database and the event stream are permanently out of sync with no way to recover.

---

## ✅ 2. The Solution: Debezium CDC Architecture

Debezium solves this by making the **database transaction log (WAL)** the single source of truth. The application only writes to PostgreSQL. Debezium watches the log and guarantees that every committed database change eventually reaches Kafka.

```mermaid
graph TD
    API["FastAPI Application (/poc-3)"]

    subgraph PostgreSQL["PostgreSQL 15 (Source of Truth)"]
        UsersTable["public.users Table"]
        WAL["Write-Ahead Log (WAL)"]
    end

    subgraph KafkaConnect["Kafka Connect Worker"]
        Debezium["Debezium PostgreSQL Connector\n(plugin.name: pgoutput)\n(topic.prefix: dbserver1)"]
    end

    subgraph Kafka_Cluster["Apache Kafka Cluster"]
        CDCTopic["Topic: dbserver1.public.users"]
    end

    subgraph FastAPI_Consumer["FastAPI Background Thread"]
        CDCConsumer["POC-3 CDC Consumer\n(group: poc-3-cdc-group)"]
        EventStore["CDC Event State Store\n(deque maxlen=100)"]
    end

    API -->|"SQL INSERT / UPDATE / DELETE"| UsersTable
    UsersTable -.->|"Every committed change is appended"| WAL
    WAL -->|"Debezium reads via pgoutput replication slot"| Debezium
    Debezium -->|"Publishes structured JSON event"| CDCTopic
    CDCTopic -.->|"poll() + consumer.commit()"| CDCConsumer
    CDCConsumer --> EventStore
    API -->|"GET /poc-3/cdc-events"| EventStore
```

---

## 🔄 3. Debezium CDC Event Anatomy

Every event Debezium publishes to Kafka contains a structured JSON payload with critical metadata. For an `UPDATE`, both the row's old and new state are captured.

```mermaid
block-beta
    columns 1
    A["🗂️ Debezium CDC Event (JSON Payload)"]
    B["op: 'u' ← Operation Type (c=Create, u=Update, d=Delete, r=Snapshot)"]
    C["before: { id: 1, email: 'old@mail.com' } ← Row state BEFORE the change"]
    D["after:  { id: 1, email: 'new@mail.com' } ← Row state AFTER the change"]
    E["ts_ms: 1728026112000 ← Unix timestamp of the database transaction"]
    F["source.db: 'inventory' | source.table: 'users' ← Origin metadata"]
```

---

## 🔢 4. End-to-End Sequence Diagram (CREATE Example)

```mermaid
sequenceDiagram
    autonumber
    participant Client as Client
    participant API as FastAPI /poc-3
    participant PSQL as PostgreSQL (WAL)
    participant Debezium as Debezium Connect
    participant Kafka as Kafka Broker
    participant Consumer as CDC Consumer Thread

    Client->>API: POST /poc-3/users {"email": "test@example.com"}
    API->>PSQL: SQL INSERT INTO users VALUES (...)
    PSQL-->>API: Row saved, id=1
    API-->>Client: HTTP 201 Created

    Note over PSQL, Debezium: No direct application involvement below this line!
    
    PSQL-->>Debezium: WAL log entry: INSERT op on public.users row id=1
    Debezium->>Kafka: Publish CDC event to 'dbserver1.public.users'

    Note over Kafka, Consumer: Async processing
    Consumer->>Kafka: poll(timeout_ms=1000)
    Kafka-->>Consumer: Yield CDC record {op='c', after={id:1, email:'test@example.com'}}
    Consumer->>Consumer: Parse and store in EventStore
    Consumer->>Kafka: consumer.commit()

    Client->>API: GET /poc-3/cdc-events
    API-->>Client: Returns [{op: 'CREATE (INSERT)', after_state: {...}}]
```

---

## 🧪 5. Step-by-Step Testing Guide

### Step 1: Start the System
```bash
docker compose up --build -d
```

### Step 2: Create a User (Triggers CDC INSERT)
```bash
curl -X POST http://localhost:8000/poc-3/users \
  -H "Content-Type: application/json" \
  -d '{"first_name": "Sahana", "last_name": "Thakkar", "email": "sahana@example.com"}'
```

### Step 3: Update the User (Triggers CDC UPDATE with before/after states)
```bash
curl -X PUT http://localhost:8000/poc-3/users/1 \
  -H "Content-Type: application/json" \
  -d '{"email": "sahana.updated@example.com"}'
```

### Step 4: Delete the User (Triggers CDC DELETE)
```bash
curl -X DELETE http://localhost:8000/poc-3/users/1
```

### Step 5: Inspect all CDC Events
```bash
curl http://localhost:8000/poc-3/cdc-events
```

Observe the `operation`, `before_state`, and `after_state` fields for each event!
