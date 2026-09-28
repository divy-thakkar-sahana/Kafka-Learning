# Enterprise Event Streaming: Kafka POC-1 Architecture Guide

This comprehensive technical guide outlines the architecture, partition routing strategies, and message delivery guarantees implemented in **Kafka POC-1**. It serves as a production-grade reference for building resilient, event-driven microservices.

---

## 🏛️ 1. End-to-End System Architecture

The following diagram illustrates the complete lifecycle of a message, from ingestion via the FastAPI edge layer to storage in the Kafka broker, and finally to consumption and manual acknowledgment by the background worker thread.

```mermaid
graph TD
    Client["Edge Client / API Gateway"]
    
    subgraph FastAPI_Service["FastAPI Application Node"]
        Router["HTTP Router (router.py)"]
        Validation["Schema Validation (models.py)"]
        Producer["Producer Client (producer.py)"]
        Consumer["Consumer Thread (consumer.py)"]
        StateStore["In-Memory State Store (Thread-Safe deque)"]
    end
    
    subgraph Kafka_Cluster["Apache Kafka Cluster (KRaft Mode)"]
        Topic["Topic: poc-messages (3 Partitions)"]
        P0["Partition-0 (Leader)"]
        P1["Partition-1 (Leader)"]
        P2["Partition-2 (Leader)"]
        OffsetTopic["Internal Topic: __consumer_offsets"]
    end

    Client -->|1. POST JSON Payload| Router
    Router -->|2. Enforce Data Contract| Validation
    Validation -->|3. Dispatch| Producer
    
    Producer -->|4. Publish (acks='all')| Topic
    Topic -.-> P0
    Topic -.-> P1
    Topic -.-> P2

    P0 -.->|5. Poll Record Batch| Consumer
    P1 -.->|5. Poll Record Batch| Consumer
    P2 -.->|5. Poll Record Batch| Consumer
    
    Consumer -->|6. Execute Business Logic| StateStore
    Consumer -->|7. Manual ACK (consumer.commit)| OffsetTopic

    Client -->|8. GET State| StateStore
```

---

## 🔀 2. Partition Routing Strategies

In distributed systems, data locality and ordering guarantees are paramount. POC-1 implements three distinct routing algorithms to demonstrate how producers can govern data distribution across partitions.

```mermaid
flowchart TD
    Req["Incoming Event Payload"] --> ModeSelect{"Determine Routing Mode"}
    
    ModeSelect -->|explicit_partition != null| Explicit["Strict Routing: Explicit Assignment"]
    Explicit --> ExplicitDesc["Bypasses partitioner. Directly assigns event to specified partition (0, 1, or 2).<br/>Use Case: Specialized partition processing or hot-fixing."]
    
    ModeSelect -->|use_key == true| Keyed["Semantic Routing: Keyed Hashing"]
    Keyed --> KeyedDesc["Applies MurmurHash2 to 'customer_id' modulo partition count.<br/>Use Case: Strict chronological ordering per entity (e.g., all events for Customer A go to Partition 1)."]
    
    ModeSelect -->|use_key == false| Unkeyed["Load Balanced: Unkeyed Auto-Routing"]
    Unkeyed --> UnkeyedDesc["Default partitioner uses Round-Robin / Sticky assignment.<br/>Use Case: High-throughput stateless processing where order does not matter."]

    ExplicitDesc --> Broker["Kafka Broker"]
    KeyedDesc --> Broker
    UnkeyedDesc --> Broker
```

---

## 🔄 3. Delivery Guarantees & Offset Management (Sequence Diagram)

To achieve **At-Least-Once Delivery** and prevent message loss during pod restarts or network failures, POC-1 explicitly disables auto-commits (`enable_auto_commit=False`) and relies on deterministic manual offset commits.

```mermaid
sequenceDiagram
    autonumber
    participant Client as Client Service
    participant API as FastAPI Edge
    participant Producer as Kafka Producer
    participant Broker as Kafka Broker
    participant Consumer as Consumer Thread

    Note over API, Consumer: System Initialization Phase
    API->>Consumer: Spawn daemon thread on @app.on_event("startup")
    Consumer->>Broker: Join group 'poc-consumer-group', fetch offsets

    Note over Client, Broker: Ingestion Phase (Synchronous)
    Client->>API: POST /poc-1/send
    API->>Producer: Execute publish_delivery_event()
    Producer->>Broker: send(topic, key, payload) [acks='all']
    Broker-->>Producer: Ack (Partition, Offset metadata)
    Producer-->>API: Yield Success Response
    API-->>Client: HTTP 200 OK

    Note over Broker, Consumer: Processing & Acknowledgment Phase (Asynchronous)
    Consumer->>Broker: poll(timeout_ms=1000)
    Broker-->>Consumer: Yield RecordBatch
    
    alt Processing Succeeds
        Consumer->>Consumer: Deserialize JSON & Execute Domain Logic
        Consumer->>Broker: consumer.commit() (Update __consumer_offsets)
        Note over Consumer, Broker: Offset durably committed. Message will not be re-delivered.
    else Processing Fails / Node Crashes
        Note over Consumer, Broker: Crash occurs before consumer.commit()
        Consumer->>Broker: Restart & Re-join group
        Broker-->>Consumer: Re-delivers uncommitted RecordBatch
        Note over Consumer: System guarantees no message loss (At-Least-Once semantics)
    end
```

---

## ⚙️ 4. Technical Specifications & Configuration

| Subsystem | Parameter | Value | Production Rationale |
| :--- | :--- | :--- | :--- |
| **Kafka Broker** | `KAFKA_NUM_PARTITIONS` | `3` | Enables parallel consumption. Partitions are the fundamental unit of scaling in Kafka. |
| **Producer** | `acks` | `"all"` | Highest durability. The leader broker will wait for all in-sync replicas (ISRs) to acknowledge the message before responding to the producer. |
| **Producer** | `retries` | `3` | Mitigates transient network failures. Prevents immediate application failure during brief broker unavailability. |
| **Consumer** | `enable_auto_commit` | `False` | Prevents the consumer from automatically committing offsets in the background, which can lead to data loss if the application crashes before processing is complete. |
| **Consumer** | `auto_offset_reset` | `"earliest"` | If no previous offset exists for the consumer group, start reading from the very beginning of the partition log to avoid missing historical data. |
