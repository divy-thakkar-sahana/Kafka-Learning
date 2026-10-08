import os
import time
import requests
from app.utility.logging_config import get_logger

logger = get_logger("poc_3_debezium_setup")

DEBEZIUM_CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "inventory-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "tasks.max": "1",
        "database.hostname": os.getenv("POSTGRES_HOST", "postgres"),
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "inventory",
        "topic.prefix": "dbserver1",
        "table.include.list": "public.users,public.products",
        "decimal.handling.mode": "double",
    }
}


def register_debezium_connector(max_retries=15, delay=4):
    """Registers or updates the Debezium PostgreSQL connector with Kafka Connect REST API."""
    connect_api_url = f"{DEBEZIUM_CONNECT_URL}/connectors"

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Checking Kafka Connect status at {DEBEZIUM_CONNECT_URL} (Attempt {attempt}/{max_retries})...")
            resp = requests.get(connect_api_url, timeout=3)
            
            if resp.status_code == 200:
                existing_connectors = resp.json()
                
                if CONNECTOR_NAME in existing_connectors:
                    # Check if existing config contains 'public.products'
                    cfg_resp = requests.get(f"{connect_api_url}/{CONNECTOR_NAME}/config", timeout=3)
                    current_tables = ""
                    if cfg_resp.status_code == 200:
                        current_tables = cfg_resp.json().get("table.include.list", "")
                    
                    if "public.products" not in current_tables:
                        logger.info(f"Existing connector lacks 'public.products'. Re-creating connector '{CONNECTOR_NAME}' to trigger full snapshot...")
                        requests.delete(f"{connect_api_url}/{CONNECTOR_NAME}", timeout=5)
                        time.sleep(2)
                        post_resp = requests.post(connect_api_url, json=CONNECTOR_CONFIG, timeout=5)
                        if post_resp.status_code in (200, 201):
                            logger.info(f"Successfully re-created Debezium connector '{CONNECTOR_NAME}' with products table!")
                            return True
                    else:
                        logger.info(f"Updating configuration for existing Debezium connector '{CONNECTOR_NAME}'...")
                        put_url = f"{connect_api_url}/{CONNECTOR_NAME}/config"
                        put_resp = requests.put(put_url, json=CONNECTOR_CONFIG["config"], timeout=5)
                        if put_resp.status_code in (200, 201):
                            logger.info(f"Successfully updated Debezium connector '{CONNECTOR_NAME}' config!")
                            return True
                else:
                    logger.info(f"Registering new Debezium connector '{CONNECTOR_NAME}'...")
                    post_resp = requests.post(connect_api_url, json=CONNECTOR_CONFIG, timeout=5)
                    if post_resp.status_code in (200, 201):
                        logger.info(f"Successfully registered Debezium connector '{CONNECTOR_NAME}'!")
                        return True
            else:
                logger.warning(f"Kafka Connect returned status {resp.status_code}. Retrying in {delay}s...")
        except Exception as exc:
            logger.warning(f"Kafka Connect not ready yet: {exc}. Retrying in {delay}s...")
        
        time.sleep(delay)

    logger.error("Failed to setup Debezium connector after maximum attempts.")
    return False
