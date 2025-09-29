# app/services/sync_service.py - Replace entire content

import asyncio
import logging
from typing import Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..config import settings
from ..database import SessionLocal
from .cache_service import cache_service

logger = logging.getLogger(__name__)

class SyncService:
    _sync_tasks: Dict[str, asyncio.Task] = {}
    _is_shutting_down = False

    @staticmethod
    def start_sync_counters_job():
        """Start background job to sync Redis counters to DB every 5 minutes"""
        logger.info("Starting counter sync job (every 5 minutes)")
        
        async def sync_loop():
            while not SyncService._is_shutting_down:
                try:
                    await asyncio.sleep(settings.counter_sync_interval)  # 300 seconds = 5 minutes
                    await SyncService._sync_counters_to_db()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Counter sync error: {e}")
        
        task = asyncio.create_task(sync_loop())
        SyncService._sync_tasks['counter_sync'] = task

    @staticmethod
    async def _sync_counters_to_db():
        """Sync all Redis counters to database"""
        logger.info("Starting counter sync from Redis to DB...")
        
        if not cache_service._redis_client:
            logger.warning("Redis not available, skipping sync")
            return
        
        db = SessionLocal()
        try:
            # Sync story counters
            await SyncService._sync_story_counters(db)
            
            # Sync episode counters
            await SyncService._sync_episode_counters(db)
            
            # Sync comment like counters
            await SyncService._sync_comment_counters(db)
            
            logger.info("✅ Counter sync completed successfully")
            
        except Exception as e:
            logger.error(f"Counter sync failed: {e}")
            db.rollback()
        finally:
            db.close()

    @staticmethod
    async def _sync_story_counters(db: Session):
        """Sync story counters from Redis to DB"""
        cursor = '0'
        synced_count = 0
        
        while cursor != 0:
            cursor, keys = await cache_service._redis_client.scan(
                cursor, match="story:*:*_count", count=100
            )
            
            for key in keys:
                key_str = key.decode('utf-8')
                parts = key_str.split(':')
                
                if len(parts) == 3:
                    story_id = parts[1]
                    counter_type = parts[2]  # likes_count, views_count, etc.
                    
                    redis_value = await cache_service._redis_client.get(key)
                    if redis_value:
                        count = int(redis_value.decode('utf-8'))
                        
                        # Update DB
                        db.execute(
                            text(f"UPDATE stories SET {counter_type} = :count WHERE story_id = :story_id"),
                            {"count": count, "story_id": story_id}
                        )
                        synced_count += 1
        
        db.commit()
        logger.info(f"Synced {synced_count} story counters")

    @staticmethod
    async def _sync_episode_counters(db: Session):
        """Sync episode counters from Redis to DB"""
        cursor = '0'
        synced_count = 0
        
        while cursor != 0:
            cursor, keys = await cache_service._redis_client.scan(
                cursor, match="episode:*:*_count", count=100
            )
            
            for key in keys:
                key_str = key.decode('utf-8')
                parts = key_str.split(':')
                
                if len(parts) == 3:
                    episode_id = parts[1]
                    counter_type = parts[2]
                    
                    redis_value = await cache_service._redis_client.get(key)
                    if redis_value:
                        count = int(redis_value.decode('utf-8'))
                        
                        db.execute(
                            text(f"UPDATE episodes SET {counter_type} = :count WHERE episode_id = :episode_id"),
                            {"count": count, "episode_id": episode_id}
                        )
                        synced_count += 1
        
        db.commit()
        logger.info(f"Synced {synced_count} episode counters")

    @staticmethod
    async def _sync_comment_counters(db: Session):
        """Sync comment like counters from Redis to DB"""
        cursor = '0'
        synced_count = 0
        
        while cursor != 0:
            cursor, keys = await cache_service._redis_client.scan(
                cursor, match="comment:*:comment_like_count", count=100
            )
            
            for key in keys:
                key_str = key.decode('utf-8')
                parts = key_str.split(':')
                
                if len(parts) == 3:
                    comment_id = parts[1]
                    
                    redis_value = await cache_service._redis_client.get(key)
                    if redis_value:
                        count = int(redis_value.decode('utf-8'))
                        
                        db.execute(
                            text("UPDATE comments SET comment_like_count = :count WHERE comment_id = :comment_id"),
                            {"count": count, "comment_id": comment_id}
                        )
                        synced_count += 1
        
        db.commit()
        logger.info(f"Synced {synced_count} comment counters")

    @staticmethod
    async def shutdown():
        """Gracefully shutdown sync service"""
        logger.info("Shutting down sync service...")
        SyncService._is_shutting_down = True
        
        for task in SyncService._sync_tasks.values():
            if not task.done():
                task.cancel()
        
        logger.info("Sync service shutdown completed")

    @staticmethod
    def get_sync_stats() -> Dict[str, Any]:
        """Get sync service statistics"""
        return {
            "counter_sync": {
                "active": not SyncService._is_shutting_down,
                "interval_seconds": settings.counter_sync_interval,
                "tasks_running": len([t for t in SyncService._sync_tasks.values() if not t.done()])
            }
        }