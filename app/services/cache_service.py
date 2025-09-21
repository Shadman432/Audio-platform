import asyncio
import json
import gzip
import time
import logging
from typing import Any, Optional, Dict, Callable, Coroutine
import redis.asyncio as redis
from redis.exceptions import RedisError

from ..config import settings
from ..database import SessionLocal

from ..models.stories_authors import StoriesAuthors
from ..models.episode_authors import EpisodeAuthors
from .serializers import (
    story_to_dict,
    episode_to_dict,
    home_content_to_dict,
    home_content_series_to_dict,
    home_slideshow_to_dict,
    stories_authors_to_dict,
    episode_authors_to_dict,
)

# Configure logging
logger = logging.getLogger(__name__)


class CacheMetrics:
    """Track cache performance metrics"""
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.redis_hits = 0
        self.redis_misses = 0
        self.db_queries = 0
        self.refresh_count = 0
        self.error_count = 0
        self.stale_serves = 0
        self.background_refreshes = 0

    def redis_hit_rate(self) -> float:
        total = self.redis_hits + self.redis_misses
        return (self.redis_hits / total * 100) if total > 0 else 0


class MultiTierCacheService:
    """
    A simplified Redis-based caching service with stale-while-revalidate.
    Flow: DB -> Redis
    """

    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self._background_tasks_started = False
        self._refresh_tasks: Dict[str, asyncio.Task] = {}
        self._write_queue: asyncio.Queue = asyncio.Queue()
        self._write_worker_task: Optional[asyncio.Task] = None
        self.metrics = CacheMetrics()
        self._hot_keys: Dict[str, Callable[[], Coroutine[Any, Any, Any]]] = {}
        self._write_lock = asyncio.Lock()  # Prevent race conditions
        self._batch_size = 100     # Process writes in batches

        # Initialize Redis connection (safe, non-blocking)
        # _init_redis is now async, so it needs to be called in an async context or awaited later

    def register_hot_key(self, key: str, refresh_function: Callable[[], Coroutine[Any, Any, Any]]):
        self._hot_keys[key] = refresh_function

    async def _init_redis(self):
        """Initialize Redis connection with error handling and retries"""
        if not settings.USE_REDIS:
            logger.info("Redis disabled in configuration")
            return

        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self._redis_client = redis.from_url(
                    settings.REDIS_URL,
                    socket_timeout=10,
                    socket_connect_timeout=10,
                    retry_on_timeout=True,
                    health_check_interval=30,
                    decode_responses=False,
                    max_connections=20,
                )

                # Test connection with timeout
                await asyncio.wait_for(self._redis_client.ping(), timeout=5.0)
                logger.info(f"✅ Redis connected: {settings.REDIS_URL}")
                return

            except asyncio.TimeoutError:
                logger.warning(f"Redis connection timeout on attempt {attempt + 1}/{max_retries}")
            except Exception as e:
                logger.error(f"Redis connection failed on attempt {attempt + 1}/{max_retries}: {e}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying Redis connection in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

        logger.error("❌ Redis connection failed after all retries - running in degraded mode")
        self._redis_client = None

    async def warm_up(self):
        """Warm up the cache by pre-loading essential data."""
        logger.info("🔥 Warming up cache...")
        for key, refresh_func in self._hot_keys.items():
            try:
                logger.info(f"Pre-loading {key} into cache...")
                # Handle both sync and async functions
                if asyncio.iscoroutinefunction(refresh_func):
                    data = await refresh_func()
                else:
                    data = refresh_func()  # Call sync function directly
                await self.set(key, data)
                logger.info(f"✅ Pre-loaded {key} into cache.")
            except Exception as e:
                logger.error(f"🔥 Cache warm-up for key {key} failed: {e}")
                self.metrics.error_count += 1

    async def startup(self):
        """Initialize cache service on app startup"""
        from .sync_service import SyncService # Local import
        await self._init_redis() # Await the async init_redis
        await self.start_background_tasks()
        SyncService.start_sync_counters_job()
        
        self._write_worker_task = asyncio.create_task(self._write_worker()) # Start the write worker
        logger.info("🚀 Cache service started successfully")

    async def _write_worker(self):
        """Background worker to process Redis write operations from the queue."""
        while True:
            operation, key, data, ttl = await self._write_queue.get()
            try:
                if operation == "set":
                    if self._redis_client:
                        final_ttl = ttl if ttl is not None else settings.redis_cache_ttl
                        redis_expiry_ttl = final_ttl + settings.STALE_CACHE_EXTENSION
                        compressed_data = self._compress_data(data)
                        await self._redis_client.set(key, compressed_data, ex=redis_expiry_ttl)
                        logger.info(f"Redis: Stored {len(compressed_data)} bytes for key '{key}' with TTL {redis_expiry_ttl}s via queue.")
                elif operation == "delete":
                    if self._redis_client:
                        await self._redis_client.delete(key)
                        logger.info(f"Redis: Deleted key '{key}' via queue.")
            except RedisError as e:
                logger.error(f"Redis write error for key {key} (operation: {operation}): {e}")
                self.metrics.error_count += 1
            finally:
                self._write_queue.task_done()
                await asyncio.sleep(0.01) # Small delay to throttle writes (10ms)

    def _compress_data(self, data: Any) -> bytes:
        if isinstance(data, bytes):
            return data
            
        if not settings.ENABLE_COMPRESSION:
            if isinstance(data, str):
                return data.encode("utf-8")
            return json.dumps(data, separators=(",", ":"), default=str).encode("utf-8")

        if isinstance(data, str):
            json_str = data
        else:
            json_str = json.dumps(data, separators=(",", ":"), default=str)
        
        return gzip.compress(json_str.encode("utf-8"))

    def _decompress_data(self, data: bytes) -> Any:
        if not settings.ENABLE_COMPRESSION:
            return json.loads(data.decode("utf-8"))

        try:
            decompressed = gzip.decompress(data)
            return json.loads(decompressed.decode("utf-8"))
        except (gzip.BadGzipFile, json.JSONDecodeError, OSError):
            return json.loads(data.decode("utf-8"))

    def _get_current_time(self) -> float:
        return time.time()

    async def get(self, key: str, db_fallback: Callable = None, ttl: int = None) -> Any:
        """Get from Redis with stale-while-revalidate support"""
        if settings.CACHE_DEBUG_MODE:
            start_time = time.monotonic()

        if self._redis_client:
            try:
                # Get data and TTL in pipeline
                pipe = self._redis_client.pipeline()
                pipe.get(key)
                pipe.ttl(key)
                cached_data, item_ttl = await pipe.execute()

                if cached_data is not None:
                    self.metrics.redis_hits += 1
                    data = self._decompress_data(cached_data)
                    
                    # Check if stale (TTL < extension time)
                    if settings.ENABLE_STALE_WHILE_REVALIDATE and item_ttl < settings.STALE_CACHE_EXTENSION and item_ttl > 0:
                        self.metrics.stale_serves += 1
                        # Trigger background refresh
                        if key not in self._refresh_tasks or self._refresh_tasks[key].done():
                            if db_fallback:
                                self.metrics.background_refreshes += 1
                                task = asyncio.create_task(self._refresh_cache(key, db_fallback, ttl))
                                self._refresh_tasks[key] = task
                    
                    return data

            except RedisError as e:
                logger.error(f"Redis error for key {key}: {e}")

        # Fallback to DB if provided
        if db_fallback:
            self.metrics.db_queries += 1
            # Handle both sync and async functions
            if asyncio.iscoroutinefunction(db_fallback):
                fresh_data = await db_fallback()
            else:
                fresh_data = db_fallback()  # Call sync function directly
            await self.set(key, fresh_data, ttl)
            return fresh_data
        
        return None

    async def _refresh_cache(self, key: str, db_fallback: Callable, ttl: int = None):
        """Background task to refresh cache from DB."""
        try:
            self.metrics.refresh_count += 1
            if asyncio.iscoroutinefunction(db_fallback):
                fresh_data = await db_fallback()
            else:
                fresh_data = db_fallback()
            await self.set(key, fresh_data, ttl)
        except Exception as e:
            logger.error(f"Error refreshing cache for key {key}: {e}")
            self.metrics.error_count += 1
        finally:
            self._refresh_tasks.pop(key, None)

    async def get_paginated_stories(self, skip: int = 0, limit: int = 20) -> list:
        """Get paginated stories from cache"""
        cache_key = f"{settings.STORIES_PAGINATED_CACHE_PREFIX}:{skip}:{limit}"
        
        async def fetch_paginated():
            from .stories import StoryService
            with SessionLocal() as db:
                stories = await StoryService.get_stories_paginated(db, skip, limit)
                return [story_to_dict(s) for s in stories]
        
        return await self.get(cache_key, fetch_paginated)

    async def get_paginated_episodes(self, skip: int = 0, limit: int = 20) -> list:
        """Get paginated episodes from cache"""
        cache_key = f"{settings.EPISODES_PAGINATED_CACHE_PREFIX}:{skip}:{limit}"
        
        async def fetch_paginated():
            from .episodes import EpisodeService
            with SessionLocal() as db:
                episodes = await EpisodeService.get_episodes(db, skip, limit)
                return [episode_to_dict(e) for e in episodes]
        
        return await self.get(cache_key, fetch_paginated)

    async def set(self, key: str, data: Any, ttl: int = None):
        """Add set operation to the write queue."""
        await self._write_queue.put(("set", key, data, ttl))

    async def delete(self, key: str):
        """Add delete operation to the write queue."""
        await self._write_queue.put(("delete", key, None, None))

    async def clear_all(self):
        """Clear all caches"""
        if self._redis_client:
            try:
                # A safer way to clear keys with a prefix
                cursor = '0'
                while cursor != 0:
                    cursor, keys = await self._redis_client.scan(
                        cursor,
                        match=f"{settings.CACHE_KEY_PREFIX}:*",
                        count=100
                    )
                    if keys:
                        await self._redis_client.delete(*keys)
                logger.info(f"Cleared Redis keys with prefix '{settings.CACHE_KEY_PREFIX}:*'")
            except RedisError as e:
                logger.error(f"Redis clear error: {e}")

    async def cleanup_stale_keys(self):
        pattern = f"{settings.cache_key_prefix}:expired:*"
        async for key in self._redis_client.scan_iter(match=pattern):
            await self._redis_client.delete(key)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "redis_cache": {
                "connected": self._redis_client is not None,
                "hits": self.metrics.redis_hits,
                "misses": self.metrics.misses, # A miss for redis is a db query
                "hit_rate": self.metrics.redis_hit_rate(),
            },
            "database": {
                "queries": self.metrics.db_queries,
            },
            "background": {
                "active_refresh_tasks": len(self._refresh_tasks),
                "total_refreshes_triggered": self.metrics.background_refreshes,
                "stale_responses_served": self.metrics.stale_serves,
                "errors": self.metrics.error_count,
            },
        }

    async def batch_increment_counters(self, counter_updates: Dict[str, int]):
        """Batch multiple counter increments in a single pipeline"""
        if not self._redis_client or not counter_updates:
            return
        try:
            pipe = self._redis_client.pipeline()
            for key, increment in counter_updates.items():
                pipe.incrby(key, increment)
            await pipe.execute()
        except RedisError as e:
            logger.error(f"Batch increment failed: {e}")

    async def increment_counter(self, key: str):
        if not self._redis_client:
            logger.warning(f"Redis not available, skipping counter increment for {key}")
            return
        try:
            await self._redis_client.incr(key)
        except Exception as e:
            logger.error(f"Redis increment failed for key {key}: {e}")

    async def decrement_counter(self, key: str):
        if not self._redis_client:
            logger.warning(f"Redis not available, skipping counter decrement for {key}")
            return
        try:
            # Ensure counter doesn't go below zero
            current_value = await self._redis_client.get(key)
            if current_value and int(current_value) > 0:
                await self._redis_client.decr(key)
        except Exception as e:
            logger.error(f"Redis decrement failed for key {key}: {e}")

    async def increment_story_likes(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:likes_count")

    async def decrement_story_likes(self, story_id: str):
        await self.decrement_counter(f"story:{story_id}:likes_count")

    async def increment_story_views(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:views_count")

    async def increment_story_shares(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:shares_count")

    async def increment_episode_likes(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:likes_count")

    async def decrement_episode_likes(self, episode_id: str):
        await self.decrement_counter(f"episode:{episode_id}:likes_count")

    async def increment_episode_views(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:views_count")

    async def increment_episode_shares(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:shares_count")

    async def increment_comment_likes(self, comment_id: str):
        await self.increment_counter(f"comment:{comment_id}:comment_like_count")

    async def update_story_rating(self, story_id: str):
        if self._redis_client:
            from .ratings import RatingService # Local import
            with SessionLocal() as db:
                ratings = RatingService.get_ratings_by_story(db, story_id)
                if ratings:
                    avg_rating = sum([r.rating_value for r in ratings]) / len(ratings)
                    avg_rating_count = len(ratings)
                    story_key = f"{settings.story_cache_key_prefix}:{story_id}"
                    story_data = await self.get(story_key, lambda: None)
                    if story_data:
                        story_data["avg_rating"] = avg_rating
                        story_data["avg_rating_count"] = avg_rating_count
                        await self.set(story_key, story_data)

    async def update_episode_rating(self, episode_id: str):
        if self._redis_client:
            from .ratings import RatingService # Local import
            with SessionLocal() as db:
                ratings = RatingService.get_ratings_by_episode(db, episode_id)
                if ratings:
                    avg_rating = sum([r.rating_value for r in ratings]) / len(ratings)
                    avg_rating_count = len(ratings)
                    episode_key = f"{settings.episode_cache_key_prefix}:{episode_id}"
                    episode_data = await self.get(episode_key, lambda: None)
                    if episode_data:
                        episode_data["avg_rating"] = avg_rating
                        episode_data["avg_rating_count"] = avg_rating_count
                        await self.set(episode_key, episode_data)

    async def start_background_tasks(self):
        """Start background refresh tasks"""
        if self._background_tasks_started:
            return
        self._background_tasks_started = True
        asyncio.create_task(self._redis_refresh_loop())
        logger.info("🔄 Background cache refresh tasks started")

    async def _redis_refresh_loop(self):
        """Periodically refresh hot keys in Redis from the database."""
        while True:
            await asyncio.sleep(settings.REDIS_REFRESH_INTERVAL)
            if settings.CACHE_DEBUG_MODE:
                logger.debug("🔄 Redis refresh task running")
            
            for key, refresh_func in self._hot_keys.items():
                try:
                    data = await refresh_func()
                    await self.set(key, data)
                except Exception as e:
                    logger.error(f"Error during background refresh of key '{key}': {e}")
                    self.metrics.error_count += 1


# Global cache service instance
cache_service = MultiTierCacheService()



async def refresh_stories_cache():
    from .stories import StoryService # Local import
    with SessionLocal() as db:
        stories = await StoryService.get_all_stories(db)
        python_data = stories
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}

async def refresh_episodes_cache():
    from .episodes import EpisodeService # Local import
    with SessionLocal() as db:
        episodes = await EpisodeService.get_all_episodes(db)
        python_data = episodes
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}

async def refresh_story_authors_cache():
    with SessionLocal() as db:
        authors = db.query(StoriesAuthors).all()
        python_data = [stories_authors_to_dict(a) for a in authors]
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}

async def refresh_episode_authors_cache():
    with SessionLocal() as db:
        authors = db.query(EpisodeAuthors).all()
        python_data = [episode_authors_to_dict(a) for a in authors]
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}

async def refresh_home_categories_cache():
    from .home_content import HomeContentService # Local import
    with SessionLocal() as db:
        categories = await HomeContentService.get_all_home_content_no_pagination(db)
        python_data = categories
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}

async def refresh_home_series_cache():
    from .home_content_series import HomeContentSeriesService # Local import
    with SessionLocal() as db:
        series = await HomeContentSeriesService.get_all_content_series(db)
        python_data = series
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}

async def refresh_home_slideshow_cache():
    from .home_slideshow import HomeSlideshowService # Local import
    with SessionLocal() as db:
        slideshows = await asyncio.to_thread(HomeSlideshowService.get_active_slideshows, db)
        python_data = slideshows
        json_data = json.dumps(python_data, default=str)
        return {"python": python_data, "json": json_data}