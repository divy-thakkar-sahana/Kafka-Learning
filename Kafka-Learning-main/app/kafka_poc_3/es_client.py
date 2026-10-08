import os
import time
from elasticsearch import Elasticsearch
from app.utility.logging_config import get_logger

logger = get_logger("poc_3_es_client")

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX_NAME = "products"

es_client: Elasticsearch | None = None


def get_es_client() -> Elasticsearch:
    global es_client
    if es_client is None:
        es_client = Elasticsearch(ES_URL)
    return es_client


def init_es_index_with_retry(max_retries=15, delay=4) -> bool:
    """Initializes the Elasticsearch 'products' index with proper mappings and retry loop."""
    es = get_es_client()
    
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                },
                "description": {"type": "text"},
                "category": {"type": "keyword"},
                "price": {"type": "float"},
                "stock_quantity": {"type": "integer"},
                "updated_at": {"type": "date"}
            }
        }
    }

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Checking Elasticsearch connection at {ES_URL} (Attempt {attempt}/{max_retries})...")
            if es.ping():
                if not es.indices.exists(index=INDEX_NAME):
                    logger.info(f"Creating Elasticsearch index '{INDEX_NAME}'...")
                    es.indices.create(index=INDEX_NAME, body=mapping)
                    logger.info(f"Elasticsearch index '{INDEX_NAME}' created successfully!")
                else:
                    logger.info(f"Elasticsearch index '{INDEX_NAME}' already exists.")
                return True
            else:
                logger.warning(f"Elasticsearch ping failed. Retrying in {delay}s...")
        except Exception as exc:
            logger.warning(f"Elasticsearch not ready yet: {exc}. Retrying in {delay}s...")
        
        time.sleep(delay)

    logger.error("Failed to initialize Elasticsearch index after maximum retries.")
    return False
