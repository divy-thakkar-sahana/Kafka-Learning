from fastapi import FastAPI
from app.kafka_poc_1.router import router as kafka_poc_1_router
from app.kafka_poc_1.consumer import start_consumer_thread as start_poc1_consumers
from app.kafka_poc_2.router import router as kafka_poc_2_router
from app.kafka_poc_2.consumer import start_poc2_consumers
from app.utility.logging_config import get_logger

logger = get_logger("app_main")

app = FastAPI(
    title="Apache Kafka Learning POCs",
    description="FastAPI service demonstrating Apache Kafka architectural patterns",
    version="2.0.0",
)

# Mount Routers
app.include_router(kafka_poc_1_router, prefix="/poc-1", tags=["Kafka POC 1 (Basic & Partition Control)"])
app.include_router(kafka_poc_2_router, prefix="/poc-2", tags=["Kafka POC 2 (Poison Pill & Dead Letter Queue)"])


@app.on_event("startup")
def startup_event():
    logger.info("Initializing application startup...")
    start_poc1_consumers()
    start_poc2_consumers()


@app.get("/", include_in_schema=False)
def root():
    return {
        "message": "Apache Kafka Learning Service is running",
        "docs": "/docs",
    }
