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
from sqlalchemy.dialects.postgresql import insert
from collections import Counter

from .models.comment_likes import CommentLike
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

@celery_app.task
def handle_task_error(task_name: str, exception_str: str):
    """Logs exceptions from other tasks."""
    logger.error(f"Error in task {task_name}: {exception_str}")

@celery_app.task
def batch_update_edited_comments():
    """Periodically updates edited comments from a Redis queue."""
    logger.info("Starting batch update of edited comments.")
    redis_client = Redis.from_url(settings.get_redis_url(), decode_responses=True)
    db = SessionLocal()
    queue_name = "comments:edit_queue"

    try:
        # Get all comment IDs from the queue
        comment_ids = redis_client.lrange(queue_name, 0, -1)
        if not comment_ids:
            logger.info("No edited comments to update.")
            return

        # Use a pipeline to fetch all comment data from Redis at once
        pipe = redis_client.pipeline()
        for comment_id in comment_ids:
            pipe.hgetall(f"comment:{comment_id}")
        
        updates_from_redis = pipe.execute()

        # Prepare updates for the database
        updates_to_apply = []
        for i, comment_data in enumerate(updates_from_redis):
            if comment_data:
                updates_to_apply.append({
                    "comment_id": comment_ids[i],
                    "comment_text": comment_data.get("comment_text"),
                    "updated_at": datetime.fromisoformat(comment_data.get("updated_at")),
                    "is_edited": True,
                })

        # Perform bulk update
        if updates_to_apply:
            db.bulk_update_mappings(Comment, updates_to_apply)
            db.commit()
            logger.info(f"Successfully updated {len(updates_to_apply)} edited comments.")

        # Clear the processed items from the Redis list
        redis_client.ltrim(queue_name, len(comment_ids), -1)

    except Exception as e:
        db.rollback()
        handle_task_error.delay('batch_update_edited_comments', str(e))
    finally:
        db.close()


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
        handle_task_error.delay('save_comment_to_db', f"Database integrity error: {e}")
    except Exception as e:
        db.rollback()
        handle_task_error.delay('save_comment_to_db', str(e))
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

        # Use on_conflict_do_nothing to prevent errors if a comment already exists
        if comment_data_list:
            stmt = insert(Comment).values(comment_data_list)
            stmt = stmt.on_conflict_do_nothing(index_elements=['comment_id'])
            db.execute(stmt)
            db.commit()
        
        # Clear the Redis list
        redis_client.ltrim("comments:db_queue", len(comment_data_list), -1)
        
        logger.info(f"Successfully saved {len(comment_data_list)} comments to DB.")

    except Exception as e:
        db.rollback()
        handle_task_error.delay('batch_save_comments_to_db', str(e))
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
        handle_task_error.delay('refresh_cache_task', str(e))

@celery_app.task
def batch_sync_comment_likes():
    """Sync comment likes from Redis to DB and update comment_like_count."""
    logger.info("Starting batch sync of comment likes")
    redis_client = Redis.from_url(settings.get_redis_url(), decode_responses=True)
    db = SessionLocal()
    like_count_changes = Counter()

    try:
        # Handle insertions
        insert_queue = redis_client.lrange("comment_likes:insert_queue", 0, -1)
        if insert_queue:
            insert_data = [json.loads(item) for item in insert_queue]
            comment_ids_to_check = {item['comment_id'] for item in insert_data}
            
            existing_comment_ids = {
                str(c.comment_id) for c in db.query(Comment).filter(Comment.comment_id.in_(comment_ids_to_check))
            }
            
            valid_insert_data = [
                item for item in insert_data if item['comment_id'] in existing_comment_ids
            ]
            
            if valid_insert_data:
                # Use RETURNING to get the IDs of successfully inserted rows
                stmt = insert(CommentLike).values(valid_insert_data)
                stmt = stmt.on_conflict_do_nothing(index_elements=['comment_id', 'user_id'])
                # The returning() clause is PostgreSQL specific
                stmt = stmt.returning(CommentLike.comment_id)
                inserted_rows = db.execute(stmt).fetchall()
                
                # Track successful insertions for count update
                for row in inserted_rows:
                    like_count_changes[str(row[0])] += 1

            redis_client.ltrim("comment_likes:insert_queue", len(insert_queue), -1)
            logger.info(f"Processed {len(insert_data)} like insertions. Skipped {len(insert_data) - len(valid_insert_data)}.")

        # Handle deletions
        delete_queue = redis_client.lrange("comment_likes:delete_queue", 0, -1)
        if delete_queue:
            delete_data = [json.loads(item) for item in delete_queue]
            for item in delete_data:
                result = db.query(CommentLike).filter(
                    CommentLike.comment_id == item["comment_id"],
                    CommentLike.user_id == item["user_id"]
                ).delete(synchronize_session=False)
                
                # Track successful deletions for count update
                if result > 0:
                    like_count_changes[item['comment_id']] -= 1

            redis_client.ltrim("comment_likes:delete_queue", len(delete_queue), -1)
            logger.info(f"Processed {len(delete_queue)} like deletions.")

        # Update comment_like_count in the Comment table
        if like_count_changes:
            for comment_id, change in like_count_changes.items():
                if change != 0:
                    db.query(Comment).filter(Comment.comment_id == comment_id).update(
                        {Comment.comment_like_count: Comment.comment_like_count + change},
                        synchronize_session=False
                    )
                    logger.info(f"Updated like count for comment {comment_id} by {change}.")
        
        db.commit()
        
    except Exception as e:
        db.rollback()
        handle_task_error.delay('batch_sync_comment_likes', str(e))
    finally:
        db.close()

@celery_app.task
def batch_update_reply_counts():
    """Periodically updates reply counts for comments from a Redis queue."""
    logger.info("Starting batch update of reply counts.")
    redis_client = Redis.from_url(settings.get_redis_url(), decode_responses=True)
    db = SessionLocal()
    queue_name = "comments:reply_count_updates"

    try:
        # Get all parent comment IDs from the queue
        parent_ids = redis_client.lrange(queue_name, 0, -1)
        if not parent_ids:
            logger.info("No reply counts to update.")
            return

        # Count the number of new replies for each parent
        id_counts = Counter(parent_ids)

        # Update the counts in the database
        for comment_id, count in id_counts.items():
            try:
                db.query(Comment).filter(Comment.comment_id == comment_id).update(
                    {Comment.reply_count: Comment.reply_count + count},
                    synchronize_session=False
                )
                logger.info(f"Incremented reply_count for comment {comment_id} by {count}.")
            except Exception as e:
                logger.error(f"Failed to update reply_count for comment {comment_id}: {e}")

        db.commit()

        # Clear the processed items from the Redis list
        redis_client.ltrim(queue_name, len(parent_ids), -1)
        logger.info(f"Successfully processed {len(parent_ids)} reply count updates.")

    except Exception as e:
        db.rollback()
        handle_task_error.delay('batch_update_reply_counts', str(e))
    finally:
        db.close()

@celery_app.task
def batch_update_comment_visibility():
    """Periodically updates comment visibility from a Redis queue."""
    logger.info("Starting batch update of comment visibility.")
    redis_client = Redis.from_url(settings.get_redis_url(), decode_responses=True)
    db = SessionLocal()
    queue_name = "comments:visibility_updates"

    try:
        # Get all visibility updates from the queue
        visibility_updates_str = redis_client.lrange(queue_name, 0, -1)
        if not visibility_updates_str:
            logger.info("No comment visibility updates to process.")
            return

        # Deserialize the update data
        visibility_updates = [json.loads(item) for item in visibility_updates_str]

        # Group updates by comment_id to handle multiple updates to the same comment
        # The last update for each comment_id will be used
        updates_to_apply = {item['comment_id']: item['is_visible'] for item in visibility_updates}

        # Update the visibility in the database
        for comment_id, is_visible in updates_to_apply.items():
            try:
                db.query(Comment).filter(Comment.comment_id == comment_id).update(
                    {Comment.is_visible: is_visible},
                    synchronize_session=False
                )
                logger.info(f"Updated visibility for comment {comment_id} to {is_visible}.")
            except Exception as e:
                logger.error(f"Failed to update visibility for comment {comment_id}: {e}")

        db.commit()

        # Clear the processed items from the Redis list
        redis_client.ltrim(queue_name, len(visibility_updates_str), -1)
        logger.info(f"Successfully processed {len(visibility_updates)} comment visibility updates.")

    except Exception as e:
        db.rollback()
        handle_task_error.delay('batch_update_comment_visibility', str(e))
    finally:
        db.close()