import asyncio
import json
import gzip
import time
import logging
from typing import Any, Optional, Dict, Callable, Coroutine, Union
import redis.asyncio as redis
from redis.exceptions import RedisError, ConnectionError, TimeoutError

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
    Optimized Redis-based caching service with stale-while-revalidate and error handling.
    Flow: Memory -> Redis -> DB
    """

    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self._background_tasks_started = False
        self._refresh_tasks: Dict[str, asyncio.Task] = {}
        self._write_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)  # Prevent memory overflow
        self._write_worker_task: Optional[asyncio.Task] = None
        self.metrics = CacheMetrics()
        self._hot_keys: Dict[str, Callable[[], Coroutine[Any, Any, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._batch_size = 100
        self._is_shutting_down = False
        
        # Memory cache layer
        self._memory_cache: Dict[str, Dict[str, Any]] = {}
        self._memory_cache_max_size = 1000
        self._memory_ttl = 60  # 1 minute memory cache

    def register_hot_key(self, key: str, refresh_function: Callable[[], Coroutine[Any, Any, Any]]):
        """Register a hot key for background refresh"""
        self._hot_keys[key] = refresh_function

    async def _init_redis(self):
        """Initialize Redis connection with comprehensive error handling"""
        if not settings.USE_REDIS:
            logger.info("Redis disabled in configuration")
            return

        max_retries = 5
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                self._redis_client = redis.from_url(
                    settings.REDIS_URL,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    socket_keepalive_options={},
                    retry_on_timeout=True,
                    retry_on_error=[ConnectionError, TimeoutError],
                    health_check_interval=30,
                    decode_responses=False,
                    max_connections=settings.redis_max_connections or 50,
                )

                # Test connection with shorter timeout
                await asyncio.wait_for(self._redis_client.ping(), timeout=3.0)
                logger.info(f"Redis connected successfully: {settings.REDIS_URL}")
                return

            except asyncio.TimeoutError:
                logger.warning(f"Redis connection timeout on attempt {attempt + 1}/{max_retries}")
            except (ConnectionError, RedisError) as e:
                logger.warning(f"Redis connection failed on attempt {attempt + 1}/{max_retries}: {e}")
            except Exception as e:
                logger.error(f"Unexpected Redis connection error on attempt {attempt + 1}/{max_retries}: {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 10)  # Cap at 10 seconds

        logger.error("Redis connection failed after all retries - running in degraded mode")
        self._redis_client = None

    async def warm_up(self):
        """Warm up the cache by pre-loading essential data"""
        logger.info("Warming up cache...")
        
        if not self._hot_keys:
            logger.warning("No hot keys registered for warm-up")
            return

        warm_up_tasks = []
        for key, refresh_func in self._hot_keys.items():
            try:
                logger.info(f"Pre-loading {key} into cache...")
                
                # Create task for concurrent warm-up
                task = asyncio.create_task(self._warm_up_key(key, refresh_func))
                warm_up_tasks.append(task)
                
            except Exception as e:
                logger.error(f"Error creating warm-up task for key {key}: {e}")
                self.metrics.error_count += 1

        # Execute all warm-up tasks concurrently
        if warm_up_tasks:
            results = await asyncio.gather(*warm_up_tasks, return_exceptions=True)
            
            success_count = 0
            for i, result in enumerate(results):
                key = list(self._hot_keys.keys())[i]
                if isinstance(result, Exception):
                    logger.error(f"Warm-up failed for key {key}: {result}")
                else:
                    success_count += 1
                    logger.info(f"Successfully pre-loaded {key}")
            
            logger.info(f"Cache warm-up completed: {success_count}/{len(warm_up_tasks)} keys loaded successfully")

    async def _warm_up_key(self, key: str, refresh_func: Callable):
        """Warm up a single cache key"""
        try:
            if asyncio.iscoroutinefunction(refresh_func):
                data = await refresh_func()
            else:
                data = await asyncio.to_thread(refresh_func)
            
            await self.set(key, data)
            return True
        except Exception as e:
            logger.error(f"Warm-up failed for key {key}: {e}")
            self.metrics.error_count += 1
            return False

    async def startup(self):
        """Initialize cache service on app startup"""
        try:
            await self._init_redis()
            await self.start_background_tasks()
            
            # Start write worker
            self._write_worker_task = asyncio.create_task(self._write_worker())
            
            # Start sync service
            from .sync_service import SyncService
            SyncService.start_sync_counters_job()
            
            logger.info("Cache service started successfully")
            
        except Exception as e:
            logger.error(f"Cache service startup failed: {e}")
            raise

    async def shutdown(self):
        """Gracefully shutdown cache service"""
        logger.info("Shutting down cache service...")
        self._is_shutting_down = True
        
        try:
            # Cancel background tasks
            for task in self._refresh_tasks.values():
                if not task.done():
                    task.cancel()
            
            if self._write_worker_task and not self._write_worker_task.done():
                self._write_worker_task.cancel()
            
            # Process remaining write queue items
            remaining_items = []
            while not self._write_queue.empty():
                try:
                    item = self._write_queue.get_nowait()
                    remaining_items.append(item)
                except asyncio.QueueEmpty:
                    break
            
            if remaining_items:
                logger.info(f"Processing {len(remaining_items)} remaining cache writes...")
                for item in remaining_items[:100]:  # Process only first 100 to avoid blocking
                    await self._process_write_item(*item)
            
            # Close Redis connection
            if self._redis_client:
                await self._redis_client.close()
            
            logger.info("Cache service shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during cache service shutdown: {e}")

    async def _write_worker(self):
        """Background worker to process Redis write operations"""
        logger.info("Starting cache write worker")
        
        while not self._is_shutting_down:
            try:
                # Get item with timeout to allow periodic checks
                try:
                    item = await asyncio.wait_for(self._write_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                await self._process_write_item(*item)
                self._write_queue.task_done()
                
                # Small delay to prevent overwhelming Redis
                await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                logger.info("Write worker cancelled")
                break
            except Exception as e:
                logger.error(f"Write worker error: {e}")
                self.metrics.error_count += 1
                await asyncio.sleep(0.1)  # Brief pause on error

    async def _process_write_item(self, operation: str, key: str, data: Any, ttl: Optional[int]):
        """Process a single write operation"""
        if not self._redis_client:
            return
            
        try:
            if operation == "set":
                final_ttl = ttl if ttl is not None else settings.redis_cache_ttl
                redis_expiry_ttl = final_ttl + settings.STALE_CACHE_EXTENSION
                compressed_data = self._compress_data(data)
                
                await self._redis_client.set(key, compressed_data, ex=redis_expiry_ttl)
                logger.debug(f"Redis: Stored {len(compressed_data)} bytes for key '{key}'")
                
            elif operation == "delete":
                await self._redis_client.delete(key)
                logger.debug(f"Redis: Deleted key '{key}'")
                
        except (RedisError, ConnectionError, TimeoutError) as e:
            logger.error(f"Redis write error for key {key} (operation: {operation}): {e}")
            self.metrics.error_count += 1
        except Exception as e:
            logger.error(f"Unexpected error in write operation for key {key}: {e}")
            self.metrics.error_count += 1

    def _compress_data(self, data: Any) -> bytes:
        """Compress data for Redis storage"""
        try:
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
            
        except Exception as e:
            logger.error(f"Data compression error: {e}")
            # Fallback to uncompressed
            if isinstance(data, str):
                return data.encode("utf-8")
            return json.dumps(data, separators=(",", ":"), default=str).encode("utf-8")

    def _decompress_data(self, data: bytes) -> Any:
        """Decompress data from Redis"""
        try:
            if not settings.ENABLE_COMPRESSION:
                return json.loads(data.decode("utf-8"))

            try:
                decompressed = gzip.decompress(data)
                return json.loads(decompressed.decode("utf-8"))
            except (gzip.BadGzipFile, OSError):
                # Fallback to uncompressed
                return json.loads(data.decode("utf-8"))
                
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Data decompression error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected decompression error: {e}")
            return None

    def _get_current_time(self) -> float:
        return time.time()

    def _memory_cache_get(self, key: str) -> Optional[Any]:
        """Get from memory cache"""
        if key not in self._memory_cache:
            return None
            
        cache_entry = self._memory_cache[key]
        if time.time() - cache_entry['timestamp'] > self._memory_ttl:
            # Expired
            del self._memory_cache[key]
            return None
            
        return cache_entry['data']

    def _memory_cache_set(self, key: str, data: Any):
        """Set in memory cache with LRU eviction"""
        # Simple LRU eviction
        if len(self._memory_cache) >= self._memory_cache_max_size:
            # Remove oldest entry
            oldest_key = min(self._memory_cache.keys(), 
                           key=lambda k: self._memory_cache[k]['timestamp'])
            del self._memory_cache[oldest_key]
        
        self._memory_cache[key] = {
            'data': data,
            'timestamp': time.time()
        }

    async def get(self, key: str, db_fallback: Optional[Callable] = None, ttl: Optional[int] = None) -> Any:
        """Get from multi-tier cache with fallback chain"""
        if settings.CACHE_DEBUG_MODE:
            start_time = time.monotonic()

        # 1. Memory cache check
        memory_data = self._memory_cache_get(key)
        if memory_data is not None:
            self.metrics.hits += 1
            return memory_data

        # 2. Redis cache check
        if self._redis_client:
            try:
                # Get data and TTL in pipeline for efficiency
                pipe = self._redis_client.pipeline()
                pipe.get(key)
                pipe.ttl(key)
                cached_data, item_ttl = await pipe.execute()

                if cached_data is not None:
                    self.metrics.redis_hits += 1
                    data = self._decompress_data(cached_data)
                    
                    if data is not None:
                        # Store in memory cache
                        self._memory_cache_set(key, data)
                        
                        # Check if stale and trigger background refresh
                        if (settings.ENABLE_STALE_WHILE_REVALIDATE and 
                            item_ttl < settings.STALE_CACHE_EXTENSION and 
                            item_ttl > 0):
                            
                            self.metrics.stale_serves += 1
                            
                            # Trigger background refresh
                            if key not in self._refresh_tasks or self._refresh_tasks[key].done():
                                if db_fallback:
                                    self.metrics.background_refreshes += 1
                                    task = asyncio.create_task(self._refresh_cache(key, db_fallback, ttl))
                                    self._refresh_tasks[key] = task
                        
                        return data
                else:
                    self.metrics.redis_misses += 1

            except (RedisError, ConnectionError, TimeoutError) as e:
                logger.warning(f"Redis error for key {key}: {e}")
                self.metrics.error_count += 1
            except Exception as e:
                logger.error(f"Unexpected Redis error for key {key}: {e}")
                self.metrics.error_count += 1

        # 3. Database fallback
        if db_fallback:
            try:
                self.metrics.db_queries += 1
                
                if asyncio.iscoroutinefunction(db_fallback):
                    fresh_data = await db_fallback()
                else:
                    fresh_data = await asyncio.to_thread(db_fallback)
                
                # Cache the fresh data
                if fresh_data is not None:
                    await self.set(key, fresh_data, ttl)
                    self._memory_cache_set(key, fresh_data)
                
                return fresh_data
                
            except Exception as e:
                logger.error(f"Database fallback error for key {key}: {e}")
                self.metrics.error_count += 1
        
        return None

    async def _refresh_cache(self, key: str, db_fallback: Callable, ttl: Optional[int] = None):
        """Background task to refresh cache from DB"""
        try:
            self.metrics.refresh_count += 1
            
            if asyncio.iscoroutinefunction(db_fallback):
                fresh_data = await db_fallback()
            else:
                fresh_data = await asyncio.to_thread(db_fallback)
            
            if fresh_data is not None:
                await self.set(key, fresh_data, ttl)
                self._memory_cache_set(key, fresh_data)
                
        except Exception as e:
            logger.error(f"Error refreshing cache for key {key}: {e}")
            self.metrics.error_count += 1
        finally:
            self._refresh_tasks.pop(key, None)

    async def set(self, key: str, data: Any, ttl: Optional[int] = None):
        """Add set operation to the write queue"""
        if self._is_shutting_down:
            return
            
        try:
            await asyncio.wait_for(
                self._write_queue.put(("set", key, data, ttl)), 
                timeout=1.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Write queue full, dropping cache set for key: {key}")
        except Exception as e:
            logger.error(f"Error queuing cache set for key {key}: {e}")

    async def delete(self, key: str):
        """Add delete operation to the write queue"""
        if self._is_shutting_down:
            return
            
        try:
            # Remove from memory cache immediately
            self._memory_cache.pop(key, None)
            
            await asyncio.wait_for(
                self._write_queue.put(("delete", key, None, None)), 
                timeout=1.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Write queue full, dropping cache delete for key: {key}")
        except Exception as e:
            logger.error(f"Error queuing cache delete for key {key}: {e}")

    async def clear_all(self):
        """Clear all caches"""
        # Clear memory cache
        self._memory_cache.clear()
        
        if self._redis_client:
            try:
                cursor = '0'
                total_deleted = 0
                
                while cursor != 0:
                    cursor, keys = await self._redis_client.scan(
                        cursor,
                        match=f"{settings.CACHE_KEY_PREFIX}:*",
                        count=100
                    )
                    if keys:
                        await self._redis_client.delete(*keys)
                        total_deleted += len(keys)
                
                logger.info(f"Cleared {total_deleted} Redis keys with prefix '{settings.CACHE_KEY_PREFIX}:*'")
                
            except (RedisError, ConnectionError) as e:
                logger.error(f"Redis clear error: {e}")

    async def clear_pattern(self, pattern: str):
        """Clear cache keys matching pattern"""
        # Clear matching keys from memory cache
        keys_to_remove = [k for k in self._memory_cache.keys() if pattern.replace('*', '') in k]
        for key in keys_to_remove:
            del self._memory_cache[key]
        
        if self._redis_client:
            try:
                cursor = '0'
                total_deleted = 0
                
                while cursor != 0:
                    cursor, keys = await self._redis_client.scan(
                        cursor,
                        match=pattern,
                        count=100
                    )
                    if keys:
                        await self._redis_client.delete(*keys)
                        total_deleted += len(keys)
                
                logger.info(f"Cleared {total_deleted} Redis keys matching pattern '{pattern}'")
                
            except (RedisError, ConnectionError) as e:
                logger.error(f"Redis pattern clear error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        memory_cache_size = len(self._memory_cache)
        
        return {
            "memory_cache": {
                "size": memory_cache_size,
                "max_size": self._memory_cache_max_size,
                "hit_rate": "calculated_per_request",
            },
            "redis_cache": {
                "connected": self._redis_client is not None,
                "hits": self.metrics.redis_hits,
                "misses": self.metrics.redis_misses,
                "hit_rate": self.metrics.redis_hit_rate(),
            },
            "database": {
                "queries": self.metrics.db_queries,
            },
            "background": {
                "active_refresh_tasks": len([t for t in self._refresh_tasks.values() if not t.done()]),
                "total_refreshes_triggered": self.metrics.background_refreshes,
                "stale_responses_served": self.metrics.stale_serves,
                "errors": self.metrics.error_count,
            },
            "write_queue": {
                "pending_operations": self._write_queue.qsize(),
                "worker_active": self._write_worker_task is not None and not self._write_worker_task.done(),
            }
        }

    # Counter methods with error handling
    async def increment_counter(self, key: str):
        """Increment counter with error handling"""
        if not self._redis_client:
            logger.debug(f"Redis not available, skipping counter increment for {key}")
            return
        try:
            await self._redis_client.incr(key)
        except Exception as e:
            logger.error(f"Redis increment failed for key {key}: {e}")

    async def decrement_counter(self, key: str):
        """Decrement counter with error handling"""
        if not self._redis_client:
            logger.debug(f"Redis not available, skipping counter decrement for {key}")
            return
        try:
            current_value = await self._redis_client.get(key)
            if current_value and int(current_value) > 0:
                await self._redis_client.decr(key)
        except Exception as e:
            logger.error(f"Redis decrement failed for key {key}: {e}")

    # Specific counter methods
    async def increment_story_likes(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:likes_count")

    async def decrement_story_likes(self, story_id: str):
        await self.decrement_counter(f"story:{story_id}:likes_count")

    async def increment_story_views(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:views_count")

    async def increment_story_shares(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:shares_count")

    async def increment_story_comments(self, story_id: str):
        await self.increment_counter(f"story:{story_id}:comments_count")

    async def increment_episode_likes(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:likes_count")

    async def decrement_episode_likes(self, episode_id: str):
        await self.decrement_counter(f"episode:{episode_id}:likes_count")

    async def increment_episode_views(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:views_count")

    async def increment_episode_shares(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:shares_count")

    async def increment_episode_comments(self, episode_id: str):
        await self.increment_counter(f"episode:{episode_id}:comments_count")

    async def increment_comment_likes(self, comment_id: str):
        await self.increment_counter(f"comment:{comment_id}:comment_like_count")

    async def start_background_tasks(self):
        """Start background refresh tasks"""
        if self._background_tasks_started:
            return
            
        self._background_tasks_started = True
        
        # Start Redis refresh loop
        asyncio.create_task(self._redis_refresh_loop())
        logger.info("Background cache refresh tasks started")

    async def _redis_refresh_loop(self):
        """Periodically refresh hot keys in Redis from the database"""
        while not self._is_shutting_down:
            try:
                await asyncio.sleep(settings.REDIS_REFRESH_INTERVAL)
                
                if settings.CACHE_DEBUG_MODE:
                    logger.debug("Redis refresh task running")
                
                # Refresh hot keys concurrently
                refresh_tasks = []
                for key, refresh_func in self._hot_keys.items():
                    task = asyncio.create_task(self._refresh_hot_key(key, refresh_func))
                    refresh_tasks.append(task)
                
                if refresh_tasks:
                    results = await asyncio.gather(*refresh_tasks, return_exceptions=True)
                    
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            key = list(self._hot_keys.keys())[i]
                            logger.error(f"Error during background refresh of key '{key}': {result}")
                            self.metrics.error_count += 1
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Redis refresh loop: {e}")
                self.metrics.error_count += 1

    async def _refresh_hot_key(self, key: str, refresh_func: Callable):
        """Refresh a single hot key"""
        try:
            if asyncio.iscoroutinefunction(refresh_func):
                data = await refresh_func()
            else:
                data = await asyncio.to_thread(refresh_func)
            
            await self.set(key, data)
            
        except Exception as e:
            logger.error(f"Error refreshing hot key '{key}': {e}")
            raise


# Global cache service instance
cache_service = MultiTierCacheService()


# Cache refresh functions with error handling
async def refresh_stories_cache():
    """Refresh stories cache with error handling"""
    try:
        from .stories import StoryService
        with SessionLocal() as db:
            stories = await StoryService.get_all_stories(db)
            python_data = [story_to_dict(s) for s in stories]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing stories cache: {e}")
        return {"python": [], "json": "[]"}


async def refresh_episodes_cache():
    """Refresh episodes cache with error handling"""
    try:
        from .episodes import EpisodeService
        with SessionLocal() as db:
            episodes = await EpisodeService.get_all_episodes(db)
            python_data = [episode_to_dict(e) for e in episodes]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing episodes cache: {e}")
        return {"python": [], "json": "[]"}


async def refresh_story_authors_cache():
    """Refresh story authors cache with error handling"""
    try:
        with SessionLocal() as db:
            authors = db.query(StoriesAuthors).all()
            python_data = [stories_authors_to_dict(a) for a in authors]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing story authors cache: {e}")
        return {"python": [], "json": "[]"}


async def refresh_episode_authors_cache():
    """Refresh episode authors cache with error handling"""
    try:
        with SessionLocal() as db:
            authors = db.query(EpisodeAuthors).all()
            python_data = [episode_authors_to_dict(a) for a in authors]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing episode authors cache: {e}")
        return {"python": [], "json": "[]"}


async def refresh_home_categories_cache():
    """Refresh home categories cache with error handling"""
    try:
        from .home_content import HomeContentService
        with SessionLocal() as db:
            categories = await HomeContentService.get_all_home_content_no_pagination(db)
            python_data = [home_content_to_dict(c) for c in categories]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing home categories cache: {e}")
        return {"python": [], "json": "[]"}


async def refresh_home_series_cache():
    """Refresh home series cache with error handling"""
    try:
        from .home_content_series import HomeContentSeriesService
        with SessionLocal() as db:
            series = await HomeContentSeriesService.get_all_content_series(db)
            python_data = [home_content_series_to_dict(s) for s in series]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing home series cache: {e}")
        return {"python": [], "json": "[]"}


async def refresh_home_slideshow_cache():
    """Refresh home slideshow cache with error handling"""
    try:
        from .home_slideshow import HomeSlideshowService
        with SessionLocal() as db:
            slideshows = await asyncio.to_thread(HomeSlideshowService.get_active_slideshows, db)
            python_data = [home_slideshow_to_dict(s) for s in slideshows]
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
    except Exception as e:
        logger.error(f"Error refreshing home slideshow cache: {e}")
        return {"python": [], "json": "[]"}