# Kafka Docker Setup

A lightweight, single-node Apache Kafka setup running in KRaft mode (no ZooKeeper required) along with Kafka UI for cluster management.

## Features
- **Apache Kafka (KRaft mode)**: Modern Kafka setup without ZooKeeper dependency.
- **Kafka UI**: Beautiful web interface available at `http://localhost:8080`.
- **Dual Listeners**:
  - `kafka:9092` for internal Docker container communication.
  - `localhost:9094` for host applications (Python, Java, Node, etc.).

## Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.

## Getting Started

### 1. Start the Kafka Server & Kafka UI
```bash
docker compose up -d
```

### 2. Verify Running Containers
```bash
docker ps
```

### 3. Access Kafka UI Dashboard
Open your web browser and navigate to:
```text
http://localhost:8080
```

### 4. Connection Details
- **Kafka Bootstrap Server (Host App / Python)**: `localhost:9094`
- **Kafka Bootstrap Server (Docker Containers)**: `kafka:9092`

### 5. Stop the Containers
```bash
docker compose down
```
To stop containers and delete stored volumes:
```bash
docker compose down -v
```
