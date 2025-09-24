# app/main.py - Updated with incremental sync

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
import logging
import asyncio
import time

from .routes import api_router
from .database import create_tables, test_connection
from .config import settings
from .dependencies import get_redis
from .services.cache_service import (
    cache_service,
    refresh_stories_cache,
    refresh_episodes_cache,
    refresh_story_authors_cache,
    refresh_episode_authors_cache,
    refresh_home_categories_cache,
    refresh_home_series_cache,
    refresh_home_slideshow_cache,
)
from .custom_json_response import CustomJSONResponse
from .services.sync_service import SyncService # Import SyncService
from .services.search import SearchService

from .health_checks import check_redis_health, check_db_health, check_opensearch_health

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Templates setup (app/templates/)
templates = Jinja2Templates(directory="app/templates")



@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan with proper cache warmup"""
    logger.info("Starting application...")
    
    # Initialize database
    if test_connection():
        create_tables()
        logger.info("Database connected and tables created")
    else:
        logger.error("Database connection failed")
        

    # Initialize OpenSearch
    await SearchService.create_indexes_if_not_exist()
    logger.info("OpenSearch indexes verified")

    # ADD THIS BLOCK:
    # Index existing data to OpenSearch
    from .services.opensearch_service import opensearch_service
    await opensearch_service.setup_complete_opensearch()
    logger.info("OpenSearch data indexed successfully")
    
    # Start cache service
    await cache_service.startup()
    
    # Register hot keys for cache warmup
    cache_service.register_hot_key(settings.stories_cache_key, refresh_stories_cache)
    cache_service.register_hot_key(settings.episodes_cache_key, refresh_episodes_cache)
    cache_service.register_hot_key(settings.story_authors_cache_key, refresh_story_authors_cache)
    cache_service.register_hot_key(settings.episode_authors_cache_key, refresh_episode_authors_cache)
    cache_service.register_hot_key(settings.home_categories_cache_key, refresh_home_categories_cache)
    cache_service.register_hot_key(settings.home_series_cache_key, refresh_home_series_cache)
    cache_service.register_hot_key(settings.home_slideshow_cache_key, refresh_home_slideshow_cache)
    
    # Warm up cache
    await cache_service.warm_up()
    logger.info("Cache warmed up successfully")
    
    # Start background sync services
    redis = await get_redis()
    SyncService.start_sync_comments_job(redis)
    SyncService.start_sync_counters_job()
    
    yield
    
    logger.info("Shutting down application...")
    if 'redis' in locals():
        await redis.close()


# Create FastAPI app
app = FastAPI(
    title="Home Audio API",
    description="Ultra-fast FastAPI backend with Incremental Sync + Multi-Tier Fallback",
    version="3.1.0",
    debug=settings.debug,
    lifespan=lifespan
)

# GZip middleware for large responses
app.add_middleware(GZipMiddleware, minimum_size=1024)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: tighten for production
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    if process_time > 1.0:  # Log slow requests
        logger.warning(f"Slow request: {request.url} took {process_time:.2f}s")
    return response

# Use ORJSON if available for faster JSON responses
try:
    from fastapi.responses import ORJSONResponse
    app.default_response_class = ORJSONResponse
    logger.info("📦 Using ORJSONResponse for faster JSON serialization")
except ImportError:
    app.default_response_class = JSONResponse
    logger.info("📦 Using standard JSONResponse")

# Include routers
app.include_router(api_router, prefix="/api/v1")


# ---------- ROUTES ----------
@app.get("/", response_class=HTMLResponse)
def serve_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health/detailed")
async def detailed_health():
    return {
        "redis": await check_redis_health(),
        "db": check_db_health(),
        "opensearch": await check_opensearch_health()
    }


@app.get("/config")
def get_config():
    return {
        "database_type": settings.database_url.split("://")[0],
        "debug_mode": settings.debug,
        "jwt_algorithm": settings.jwt_algorithm,
        "token_expire_minutes": settings.jwt_access_token_expire_minutes,
        "cache_system": {
            "redis_enabled": settings.USE_REDIS,
            "compression_enabled": settings.ENABLE_COMPRESSION,
            "stale_while_revalidate": settings.ENABLE_STALE_WHILE_REVALIDATE,
            "browser_cache_disabled": settings.DISABLE_BROWSER_CACHE,
            "redis_ttl": settings.REDIS_CACHE_TTL,
            "stale_extension": settings.STALE_CACHE_EXTENSION
        },
        "performance_config": {
            "redis_hit_target_ms": settings.redis_hit_target_ms,
            "db_fallback_target_ms": settings.db_fallback_target_ms,
            "incremental_sync": "enabled"
        },
        "version": "3.1.0"
    }

@app.get("/api/v1/stories/paginated")
async def get_stories_paginated(skip: int = 0, limit: int = 20):
    """Get paginated stories from cache"""
    stories = await cache_service.get_paginated_stories(skip, limit)
    return {
        "stories": stories,
        "skip": skip,
        "limit": limit,
        "total": len(stories)
    }

@app.get("/api/v1/episodes/paginated") 
async def get_episodes_paginated(skip: int = 0, limit: int = 20):
    """Get paginated episodes from cache"""
    episodes = await cache_service.get_paginated_episodes(skip, limit)
    return {
        "episodes": episodes,
        "skip": skip, 
        "limit": limit,
        "total": len(episodes)
    }

@app.get("/api/v1/home/slideshow")
async def get_home_slideshow():
    """Get home slideshow from cache"""
    return await cache_service.get(settings.home_slideshow_cache_key, refresh_home_slideshow_cache)

@app.get("/api/v1/home/categories")
async def get_home_categories():
    """Get home categories from cache"""
    return await cache_service.get(settings.home_categories_cache_key, refresh_home_categories_cache)

@app.get("/api/v1/home/series")
async def get_home_series():
    """Get home series from cache"""
    return await cache_service.get(settings.home_series_cache_key, refresh_home_series_cache)


# ---------- CACHE MANAGEMENT ----------
@app.post("/api/admin/cache/warm")
async def warm_cache_endpoint():
    """Manually warm all cache layers for optimal performance"""
    try:
        start_time = time.time()
        await cache_service.warm_up()
        
        total_time = (time.time() - start_time) * 1000

        return {
            "success": True,
            "total_warmup_time_ms": round(total_time, 2),
            "performance_mode": "incremental_sync_active"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache warming failed: {str(e)}")


@app.delete("/api/admin/cache/clear")
async def clear_cache_endpoint():
    """Clear all cache layers"""
    try:
        await cache_service.clear_all()
        return {
            "success": True,
            "message": "All cache layers cleared successfully",
            "note": "Cache will rebuild automatically on next requests"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache clearing failed: {str(e)}")


@app.get("/api/cache/stats")
def get_comprehensive_cache_stats():
    """Get comprehensive cache statistics and performance metrics"""
    try:
        base_stats = cache_service.get_stats()

        redis_hit_rate = base_stats['redis_cache']['hit_rate']

        performance_analysis = {
            "cache_effectiveness": "excellent" if redis_hit_rate > 90 else "good" if redis_hit_rate > 70 else "needs_improvement",
            "recommendations": []
        }

        if base_stats['redis_cache']['hits'] == 0 and base_stats['redis_cache']['misses'] == 0:
            performance_analysis["recommendations"].append("Warm up cache using /api/admin/cache/warm")
        if redis_hit_rate < 50:
            performance_analysis["recommendations"].append("Consider increasing Redis cache TTL or warming up the cache.")
        if base_stats['background']['errors'] > 0:
            performance_analysis["recommendations"].append("Check background refresh task errors")

        return {
            **base_stats,
            "performance_analysis": performance_analysis,
            "endpoints": {
                "stories_paginated": "/api/stories?skip=0&limit=100",
                "stories_all": "/api/stories/all",
                "individual_story": "/api/stories/{story_id}",
                "search": "/api/search?q={query}&skip=0&limit=20"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get cache stats: {str(e)}")

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": "The requested resource was not found",
            "path": str(request.url.path)
        }
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "cache_status": "degraded_mode_possible"
        }
    )