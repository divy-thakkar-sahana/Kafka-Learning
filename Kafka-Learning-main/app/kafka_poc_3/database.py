import os
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.utility.logging_config import get_logger

logger = get_logger("poc_3_database")

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://postgres:postgres@localhost:5432/inventory"
)

engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db_with_retry(max_retries=10, delay=3):
    """Initializes Database tables with retry loop for startup wait."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempting PostgreSQL database connection (Attempt {attempt}/{max_retries})...")
            Base.metadata.create_all(bind=engine)
            logger.info("PostgreSQL database tables created successfully!")
            return True
        except Exception as exc:
            logger.warning(f"PostgreSQL connection failed: {exc}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.error("Could not connect to PostgreSQL database after multiple attempts.")
    return False
