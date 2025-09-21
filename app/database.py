# app/database.py
import logging
import json
import time
import redis
from typing import Optional
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

from .config import settings

# --------------------------------
# Logging Config
# --------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------
# Database Configuration
# --------------------------------
# Support both settings.DATABASE_URL and settings.database_url
db_url = getattr(settings, "DATABASE_URL", None) or getattr(settings, "database_url", None)

if db_url is None:
    raise ValueError("❌ No DATABASE_URL / database_url found in settings")

if db_url.startswith("sqlite"):
    engine = create_engine(
        db_url,
        connect_args={"check_same_thread": False},
        echo=False
    )
else:
    engine = create_engine(
        db_url,
        echo=False,
        pool_size=20,        # Increase from default 5
        max_overflow=30,     # Increase from default 10
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={
            'sslmode': 'require',
            # Add the path to your server's CA certificate here
            # 'sslrootcert': '/path/to/server-ca.pem',
            'connect_timeout': 10
        }
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
metadata = MetaData()

# --------------------------------
# DB Helpers
# --------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    except OperationalError as e:
        logger.error(f"❌ Database connection lost: {e}. Retrying...")
        db.close()
        time.sleep(1)  # Wait for 1 second before retrying
        db = SessionLocal()
        yield db
    finally:
        db.close()

def execute_sql_file(file_path: str):
    try:
        with engine.connect() as connection:
            with open(file_path, "r") as file:
                sql_script = file.read()
                connection.execute(text(sql_script))
        logger.info(f"✅ SQL script '{file_path}' executed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Error executing SQL script '{file_path}': {e}")
        return False

def create_tables():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created successfully (SQLAlchemy create_all)")
        return True
    except OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}", exc_info=True)
        logger.info("💡 Verify DB server & credentials")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error creating tables: {e}")
        return False

def test_connection():
    logger.info(f"Attempting to connect to database using URL: {db_url}")
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful")
        return True
    except OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}", exc_info=True)
        logger.info("💡 Verify DB server & credentials, and ensure it's reachable from this environment.")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected database error: {e}")
        return False