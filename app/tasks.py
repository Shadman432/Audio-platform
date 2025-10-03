# app/tasks.py

from . import models
from .celery_app import celery_app
from .database import SessionLocal
from .models.comments import Comment
from sqlalchemy.exc import IntegrityError
import logging
from redis import Redis
from .config import settings
import json
import asyncio
from .services.cache_service import cache_service

logger = logging.getLogger(__name__)

@celery_app.task
def save_comment_to_db(comment_data: dict):
    """Celery task to save a single comment to the database."""
    db = SessionLocal()
    try:
        comment = Comment(**comment_data)
        db.add(comment)
        db.commit()
        logger.info(f"Successfully saved comment {comment_data.get('comment_id')} to DB.")
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Database integrity error for comment {comment_data.get('comment_id')}: {e}")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save comment {comment_data.get('comment_id')} to DB: {e}")
    finally:
        db.close()

@celery_app.task
def batch_save_comments_to_db():
    """Celery task to save a batch of comments to the database from Redis."""
    logger.info("Starting batch save of comments to DB.")
    redis_client = Redis.from_url(settings.get_redis_url(), decode_responses=True)
    db = SessionLocal()
    
    try:
        # Retrieve all comments from the Redis list
        comment_data_list_str = redis_client.lrange("comments:db_queue", 0, -1)
        if not comment_data_list_str:
            logger.info("No comments in queue to save.")
            return

        # Deserialize the comment data
        comment_data_list = [json.loads(item) for item in comment_data_list_str]

        # Batch insert into the database
        db.bulk_insert_mappings(Comment, comment_data_list)
        db.commit()
        
        # Clear the Redis list
        redis_client.ltrim("comments:db_queue", len(comment_data_list), -1)
        
        logger.info(f"Successfully saved {len(comment_data_list)} comments to DB.")

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to batch save comments to DB: {e}")
    finally:
        db.close()


@celery_app.task
def refresh_cache_task():
    """
    Celery task to periodically refresh all registered 'hot keys' in the cache
    to prevent them from expiring.
    """
    logger.info("Starting scheduled cache refresh task.")
    try:
        # Since the cache service functions are async, we need to run them in an event loop.
        # Celery workers may not have a running loop, so we manage it manually.
        asyncio.run(cache_service.warm_up())
        logger.info("Scheduled cache refresh task completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during the scheduled cache refresh: {e}")