import asyncio
import logging
import json
import uuid
from typing import Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from redis.asyncio import Redis
from redis.exceptions import RedisError
from datetime import datetime

from ..config import settings
from ..database import SessionLocal
from ..models.comments import Comment

logger = logging.getLogger(__name__)

class SyncService:
    @staticmethod
    async def sync_redis_counters_to_db():
        """Sync counters every 5 minutes with error handling"""
        while True:
            try:
                logger.info("Starting counter sync from Redis to DB...")
                
                # Get Redis client
                from .cache_service import cache_service
                if not cache_service._redis_client:
                    logger.warning("Redis client not available for counter sync")
                    await asyncio.sleep(settings.counter_sync_interval)
                    continue
                    
                with SessionLocal() as db:
                    await SyncService._sync_entity_counters(
                        db, cache_service._redis_client,
                        entity_name="story",
                        table_name="stories", 
                        id_column="story_id",
                        counter_keys=["likes_count", "comments_count", "views_count", "shares_count"]
                    )
                    
                    await SyncService._sync_entity_counters(
                        db, cache_service._redis_client,
                        entity_name="episode",
                        table_name="episodes",
                        id_column="episode_id", 
                        counter_keys=["likes_count", "comments_count", "views_count", "shares_count"]
                    )
                    
                    await SyncService._sync_entity_counters(
                        db, cache_service._redis_client,
                        entity_name="comment",
                        table_name="comments",
                        id_column="comment_id",
                        counter_keys=["comment_like_count"]
                    )
                    
                logger.info("Counter sync completed successfully")
                
            except Exception as e:
                logger.error(f"Counter sync error: {e}", exc_info=True)
            
            await asyncio.sleep(settings.counter_sync_interval)

    @staticmethod
    async def sync_comments_to_db(redis: Redis):
        """Syncs comments from Redis to the database."""
        while True:
            try:
                comment_ids = await redis.smembers("comments:to_sync")
                if not comment_ids:
                    await asyncio.sleep(60) # Wait for a minute if no comments to sync
                    continue

                with SessionLocal() as db:
                    for comment_id_bytes in comment_ids:
                        comment_id = comment_id_bytes.decode('utf-8')
                        redis_key = f"comment:{comment_id}"
                        comment_data_json = await redis.get(redis_key)
                        if comment_data_json:
                            comment_data = json.loads(comment_data_json)
                            # Convert string IDs to UUIDs
                            for key in ['comment_id', 'story_id', 'episode_id', 'user_id', 'parent_comment_id']:
                                if key in comment_data and isinstance(comment_data[key], str):
                                    try:
                                        comment_data[key] = uuid.UUID(comment_data[key])
                                    except (ValueError, TypeError):
                                        comment_data[key] = None
                            
                            # Ensure created_at and updated_at are datetime objects
                            if 'created_at' in comment_data and isinstance(comment_data['created_at'], str):
                                comment_data['created_at'] = datetime.fromisoformat(comment_data['created_at'])
                            if 'updated_at' in comment_data and isinstance(comment_data['updated_at'], str):
                                comment_data['updated_at'] = datetime.fromisoformat(comment_data['updated_at'])

                            comment = Comment(**comment_data)
                            db.merge(comment)
                            await redis.srem("comments:to_sync", comment_id)
                    db.commit()
                    logger.info(f"Synced {len(comment_ids)} comments to the database.")
            except Exception as e:
                logger.error(f"Error during comment sync: {e}", exc_info=True)
            await asyncio.sleep(120) # Sync every 2 minutes

    @staticmethod
    def start_sync_comments_job(redis: Redis):
        """Starts the background sync comments job."""
        asyncio.create_task(SyncService.sync_comments_to_db(redis))

    @staticmethod
    async def _sync_entity_counters(
        db: Session,
        redis_client,
        entity_name: str,
        table_name: str, 
        id_column: str,
        counter_keys: list[str]
    ):
        """Sync entity counters from Redis to DB then to OpenSearch"""
        logger.info(f"Syncing {entity_name} counters...")
        
        # 1. Read from Redis
        all_updates = {}
        try:
            for counter_name in counter_keys:
                pattern = f"{entity_name}:*:{counter_name}"
                async for key_bytes in redis_client.scan_iter(match=pattern):
                    key = key_bytes.decode('utf-8')
                    parts = key.split(':')
                    if len(parts) == 3:
                        entity_id = parts[1]
                        try:
                            redis_value = await redis_client.get(key_bytes)
                            if redis_value:
                                count = int(redis_value.decode('utf-8'))
                                if entity_id not in all_updates:
                                    all_updates[entity_id] = {id_column: entity_id}
                                all_updates[entity_id][counter_name] = count
                        except (ValueError, RedisError) as e:
                            logger.error(f"Error reading Redis key {key}: {e}")
            
            if not all_updates:
                logger.info(f"No {entity_name} counters to sync")
                return
                
            # 2. Bulk update DB
            values_list = []
            for entity_id, updates in all_updates.items():
                counter_values = [str(updates.get(cn, 0)) for cn in counter_keys]
                values_list.append(f"('{entity_id}', {', '.join(counter_values)})")
            
            set_clauses = [f"{cn} = updates.{cn}" for cn in counter_keys]
            
            update_query = f"""
            UPDATE {table_name} AS t
            SET {', '.join(set_clauses)}
            FROM (VALUES {', '.join(values_list)}) AS updates({id_column}, {', '.join(counter_keys)})
            WHERE t.{id_column} = CAST(updates.{id_column} AS UUID)
            """
            
            result = db.execute(text(update_query))
            db.commit()
            logger.info(f"Updated {result.rowcount} {entity_name} counters in DB")
            
            # 3. Sync to OpenSearch
            from .search import SearchService
            for entity_id, counters in all_updates.items():
                counter_data = {k: v for k, v in counters.items() if k != id_column}
                await SearchService.update_counters_in_opensearch(entity_name, entity_id, counter_data)
            
            logger.info(f"Synced {len(all_updates)} {entity_name} counters to OpenSearch")
            
        except Exception as e:
            logger.error(f"Error syncing {entity_name} counters: {e}", exc_info=True)
            db.rollback()



    @staticmethod
    def start_sync_counters_job():
        """Starts the background sync counters job."""
        asyncio.create_task(SyncService.sync_redis_counters_to_db())