import threading
from fastapi import FastAPI
from app.kafka_poc_1.router import router as kafka_poc_1_router
from app.kafka_poc_1.consumer import start_consumer_thread as start_poc1_consumers
from app.kafka_poc_2.router import router as kafka_poc_2_router
from app.kafka_poc_2.consumer import start_poc2_consumers
from app.kafka_poc_3.router import router as kafka_poc_3_router
from app.kafka_poc_3.database import init_db_with_retry
from app.kafka_poc_3.connector_setup import register_debezium_connector
from app.kafka_poc_3.consumer import start_poc3_consumer
from app.kafka_poc_3.es_client import init_es_index_with_retry
from app.kafka_poc_3.es_sync_consumer import start_es_sync_consumer
from app.utility.logging_config import get_logger

logger = get_logger("app_main")

app = FastAPI(
    title="Apache Kafka Learning POCs",
    description="FastAPI service demonstrating Apache Kafka architectural patterns including Core Streaming, Poison Pill/DLQ, and Debezium CDC with Elasticsearch Sync",
    version="3.1.0",
)

# Mount Routers
app.include_router(kafka_poc_1_router, prefix="/poc-1", tags=["Kafka POC 1 (Basic & Partition Control)"])
app.include_router(kafka_poc_2_router, prefix="/poc-2", tags=["Kafka POC 2 (Poison Pill & Dead Letter Queue)"])
app.include_router(kafka_poc_3_router, prefix="/poc-3", tags=["Kafka POC 3 (Debezium CDC & Elasticsearch Search)"])


def init_poc3_bg():
    """Background task to initialize DB, Debezium Connector, and Elasticsearch Index without blocking FastAPI startup."""
    logger.info("Initializing POC-3 Database tables...")
    init_db_with_retry()
    
    logger.info("Initializing Elasticsearch Index...")
    init_es_index_with_retry()

    logger.info("Initializing POC-3 Debezium Connector...")
    register_debezium_connector()
    
    logger.info("Starting POC-3 CDC Consumers...")
    start_poc3_consumer()
    start_es_sync_consumer()


@app.on_event("startup")
def startup_event():
    logger.info("Initializing application startup...")
    start_poc1_consumers()
    start_poc2_consumers()
    
    # Run POC-3 setup in daemon thread to avoid blocking fast container boot
    threading.Thread(target=init_poc3_bg, daemon=True).start()


@app.get("/", include_in_schema=False)
def root():
    return {
        "message": "Apache Kafka Learning Service is running",
        "docs": "/docs",
        "search_ui": "/poc-3/ui"
    }
