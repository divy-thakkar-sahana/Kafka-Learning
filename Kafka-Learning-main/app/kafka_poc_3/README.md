# Enterprise Kafka POC-03: Real-Time Change Data Capture (CDC) with Debezium & PostgreSQL

Apache Kafka 4.0.0 | Debezium 2.5 | PostgreSQL 15 | Python | FastAPI | SQLAlchemy

## Overview
This proof of concept demonstrates **Change Data Capture (CDC)** using **Debezium**. Instead of double-writing from the application to both a database and Kafka, the application writes *only* to a PostgreSQL database. Debezium silently monitors PostgreSQL's Write-Ahead Log (WAL) and automatically streams row-level changes (`INSERT`, `UPDATE`, `DELETE`) into Kafka topics in real-time.

## Architecture Highlights
- **Single Source of Truth**: Application endpoints only execute standard SQL operations on PostgreSQL.
- **Debezium Engine**: Connected via Kafka Connect REST API. Monitors PostgreSQL WAL using the `pgoutput` logical replication plugin.
- **Auto-Generated Kafka Topic**: `dbserver1.public.users` receives JSON event streams containing `before` and `after` row states along with operation codes (`c`, `u`, `d`).
- **CDC Consumer**: A background worker polling the CDC topic to drive downstream business logic (e.g. cache invalidation, search indexing, event notifications).

## System Interfaces (`/poc-3`)
- `POST /poc-3/users` – Insert a user in PostgreSQL (Triggers CDC `CREATE`).
- `GET /poc-3/users` – Query users directly from PostgreSQL.
- `PUT /poc-3/users/{id}` – Update user in PostgreSQL (Triggers CDC `UPDATE` showing before/after states).
- `DELETE /poc-3/users/{id}` – Delete user from PostgreSQL (Triggers CDC `DELETE`).
- `GET /poc-3/cdc-events` – Retrieve real-time CDC events captured from the Kafka topic.
