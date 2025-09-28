# app/routes/stories.py - Enhanced with instant cache + RediSearch integration

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import uuid
import json
import time
import asyncio
from pydantic import BaseModel
from datetime import datetime, timedelta

from ..custom_json_response import CustomJSONResponse
from ..database import get_db
from ..models.stories import Story
from ..services.stories import StoryService
from ..services.search import SearchService
from ..config import settings
from ..services.serializers import episode_to_dict
from ..services.stories import serialize_story

router = APIRouter(tags=["Stories"])

# =====================
# Pydantic Schemas
# =====================
class StoryBase(BaseModel):
    title: str
    meta_title: Optional[str] = None
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    description: Optional[str] = None
    meta_description: Optional[str] = None
    genre: Optional[str] = None
    rating: Optional[str] = None

class StoryCreate(StoryBase):
    pass

class StoryUpdate(BaseModel):
    title: Optional[str] = None
    meta_title: Optional[str] = None
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    description: Optional[str] = None
    meta_description: Optional[str] = None
    hls_url: Optional[str] = None
    duration: Optional[timedelta] = None
    release_date: Optional[datetime] = None

class StoryResponse(StoryBase):
    story_id: uuid.UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# =====================
# Helper Functions
# =====================

def _create_no_cache_headers() -> Dict[str, str]:
    """Create headers that prevent browser caching"""
    return {
        "Cache-Control": "no-cache, no-store, must-revalidate, private",
        "Pragma": "no-cache", 
        "Expires": "0",
        "X-Cache-Policy": "no-browser-cache",
        "Vary": "Accept-Encoding"
    }



def _build_cache_key(prefix: str, **params) -> str:
    """Build a consistent cache key from parameters"""
    sorted_params = sorted(params.items())
    param_str = "&".join(f"{k}={v}" for k, v in sorted_params if v is not None)
    return f"{prefix}:{param_str}" if param_str else prefix

def _safe_rollback(db: Session):
    """Safely rollback database transaction"""
    try:
        db.rollback()
    except Exception as e:
        print(f"Warning: Database rollback failed: {e}")

# =====================
# Database Fallback Functions
# =====================

async def _fetch_stories_from_db(
    skip: int = 0, 
    limit: int = 100, 
    title: Optional[str] = None, 
    genre: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Fetch stories from database - used as fallback function"""
    
    # The db_query needs to be async to await StoryService.get_stories_paginated
    async def _db_query_async():
        from ..database import SessionLocal
        db = SessionLocal()
        try:
            return await StoryService.get_stories_paginated(db, skip, limit, title, genre)
        finally:
            db.close()
    
    return await _db_query_async()

async def _fetch_single_story_from_db(story_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    """Fetch single story from database"""
    
    async def _db_query_async():
        from ..database import SessionLocal
        db = SessionLocal()
        try:
            return await StoryService.get_story_by_id(db, story_id)
        finally:
            db.close()
    
    return await _db_query_async()

async def _fetch_all_stories_from_db() -> Dict[str, Any]:
    """Fetch all stories from database for cache warming"""
    
    async def _db_query_async():
        from ..database import SessionLocal
        db = SessionLocal()
        try:
            stories = await StoryService.get_all_stories(db)
            python_data = stories
            json_data = json.dumps(python_data, default=str)
            return {"python": python_data, "json": json_data}
        finally:
            db.close()
    
    return await _db_query_async()

# =====================
# Ultra-Fast Routes with Instant Cache
# =====================

@router.get("/")
async def get_stories(
    skip: int = 0,
    limit: int = 100,
    title: Optional[str] = None,
    genre: Optional[str] = None,
    request: Request = None
):
    """
    Paginated story listing with Redis cache response.
    
    Cache Flow: Memory -> Redis -> Database
    - NO browser caching
    - Background refresh on stale data
    """
    from ..services.cache_service import cache_service
    start_time = time.time()
    cache_key = _build_cache_key("stories", skip=skip, limit=limit, title=title, genre=genre)

    # Define fallback function for cache service
    async def db_fallback():
        return await _fetch_stories_from_db(skip, limit, title, genre)

    # Get data from cache service (handles memory -> redis -> db fallback)
    stories_data = await cache_service.get(cache_key, db_fallback)

    if stories_data is None:
        raise HTTPException(
            status_code=503, 
            detail="Service is warming up or data is unavailable. Please try again in a moment."
        )

    response_time_ms = (time.time() - start_time) * 1000
    headers = _create_no_cache_headers()
    headers["X-Response-Time"] = f"{response_time_ms:.2f}ms"
    headers["X-Cache-Key"] = cache_key

    return CustomJSONResponse(
        content={
            "success": True,
            "stories": stories_data,
            "pagination": {
                "skip": skip,
                "limit": limit,
                "count": len(stories_data),
                "has_more": len(stories_data) == limit
            },
            "filters": {
                "title": title, 
                "genre": genre
            },
            "meta": {
                "response_time_ms": round(response_time_ms, 2),
                "timestamp": datetime.utcnow().isoformat()
            }
        },
        headers=headers,
    )

@router.get("/all")
async def get_all_stories(
    request: Request = None
):
    """
    Get ALL stories - ultra-fast response from cache.
    
    This endpoint serves the complete dataset from cache with instant fallback.
    """
    from ..services.cache_service import cache_service
    start_time = time.time()
    cache_key = settings.stories_cache_key # Changed

    # Define fallback function
    async def db_fallback():
        return await _fetch_all_stories_from_db()

    # Get from cache service
    cached_data = await cache_service.get(cache_key, db_fallback)

    if not cached_data:
        raise HTTPException(
            status_code=503, 
            detail="Service is warming up or data is unavailable. Please try again in a moment."
        )

    response_time_ms = (time.time() - start_time) * 1000
    headers = _create_no_cache_headers()
    headers["X-Response-Time"] = f"{response_time_ms:.2f}ms"
    headers["X-Story-Count"] = str(len(cached_data.get("python", [])))
    headers["X-Cache-Source"] = "redis-optimized"

    # Return the pre-serialized JSON for maximum performance
    return Response(
        content=cached_data['json'], 
        media_type="application/json", 
        headers=headers
    )

@router.get("/{story_id}")
async def get_story(
    story_id: uuid.UUID,
    db: Session = Depends(get_db)
):
    """Get single story with O(1) lookup"""
    story = await StoryService.get_story_by_id(db, story_id)
    
    if not story:
        raise HTTPException(status_code=404, detail="Story not found")
    
    return {
        "success": True,
        "story": story
    }

# =====================
# Search Endpoints
# =====================

@router.get("/search/")
async def search_stories(
    q: str,
    limit: int = 10,
    offset: int = 0,
    request: Request = None,
    db: Session = Depends(get_db)
):
    """
    Search stories using RediSearch with caching
    """
    from ..services.cache_service import cache_service
    start_time = time.time()
    
    if not q or len(q.strip()) < 2:
        raise HTTPException(
            status_code=400, 
            detail="Search query must be at least 2 characters long"
        )
    
    cache_key = _build_cache_key("search", q=q.strip(), limit=limit, offset=offset)
    
    # Define search fallback function
    async def search_fallback():
        try:
            # Perform search
            results = await SearchService.unified_search(q.strip(), offset, limit)
            
            return {
                "results": results,
                "total": len(results),
                "query": q.strip(),
                "limit": limit,
                "offset": offset
            }
            
        except Exception as e:
            print(f"Search error: {e}")
            # Fallback to database search if RediSearch fails
            stories_data = await _fetch_stories_from_db(
                db, skip=offset, limit=limit, title=q.strip()
            )
            
            return {
                "results": stories_data,
                "total": len(stories_data),
                "query": q.strip(),
                "limit": limit,
                "offset": offset,
                "fallback": "database"
            }
    
    # Get search results from cache
    search_data = await cache_service.get(cache_key, search_fallback)
    
    if search_data is None:
        raise HTTPException(
            status_code=503,
            detail="Search service is unavailable. Please try again in a moment."
        )
    
    response_time_ms = (time.time() - start_time) * 1000
    headers = _create_no_cache_headers()
    headers["X-Response-Time"] = f"{response_time_ms:.2f}ms"
    headers["X-Search-Query"] = q.strip()
    
    return CustomJSONResponse(
        content={
            "success": True,
            "search": search_data,
            "meta": {
                "response_time_ms": round(response_time_ms, 2),
                "timestamp": datetime.utcnow().isoformat()
            }
        },
        headers=headers
    )

# =====================
# Write Operations (Cache Invalidation)
# =====================

@router.post("/", response_model=StoryResponse)
async def create_story(story: StoryCreate, db: Session = Depends(get_db)):
    """Create a new story - invalidates relevant caches and updates search index"""
    from ..services.cache_service import cache_service
    
    try:
        story_data = story.model_dump()
        new_story = await StoryService.create_story(db, story_data)
        
        # Invalidate and refresh caches asynchronously
        async def refresh_caches():
            # Refresh 'all_stories' cache
            await cache_service.set(settings.stories_cache_key, await _fetch_all_stories_from_db(db)) # Changed
            # Refresh specific story cache
            await cache_service.set(f"{settings.story_cache_key_prefix}:{new_story.story_id}", serialize_story(new_story)) # Changed
            # Clear any search-related caches
            await cache_service.clear_pattern("search:*")
            await cache_service.clear_pattern("stories:*")
        
        # Run cache refresh in background
        asyncio.create_task(refresh_caches())

        return new_story
        
    except Exception as e:
        _safe_rollback(db)
        raise HTTPException(
            status_code=400, 
            detail=f"Failed to create story: {str(e)}"
        )

@router.patch("/{story_id}", response_model=StoryResponse)
async def update_story(
    story_id: uuid.UUID, 
    story: StoryUpdate, 
    db: Session = Depends(get_db)
):
    """Update story - invalidates relevant caches and updates search index"""
    from ..services.cache_service import cache_service
    
    try:
        update_data = story.model_dump(exclude_unset=True)
        updated_story = await StoryService.update_story(db, story_id, update_data)
        
        if not updated_story:
            raise HTTPException(status_code=404, detail="Story not found")
        
        # Invalidate and refresh caches asynchronously
        async def refresh_caches():
            # Refresh 'all_stories' cache
            await cache_service.set(settings.stories_cache_key, await _fetch_all_stories_from_db(db)) # Changed
            # Refresh specific story cache
            await cache_service.set(f"{settings.story_cache_key_prefix}:{story_id}", serialize_story(updated_story)) # Changed
            # Clear any search-related caches
            await cache_service.clear_pattern("search:*")
            await cache_service.clear_pattern("stories:*")
        
        asyncio.create_task(refresh_caches())
        
        return updated_story
        
    except HTTPException:
        _safe_rollback(db)
        raise
    except Exception as e:
        _safe_rollback(db)
        raise HTTPException(
            status_code=400, 
            detail=f"Failed to update story: {str(e)}"
        )

@router.delete("/{story_id}")
async def delete_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    """Delete story - invalidates relevant caches and removes from search index"""
    from ..services.cache_service import cache_service
    
    try:
        deleted = await StoryService.delete_story(db, story_id)
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Story not found")
        
        # Invalidate caches asynchronously
        async def invalidate_caches():
            await cache_service.delete(settings.stories_cache_key) # Changed
            await cache_service.delete(settings.stories_cache_key)  # Clear master key only 
            await cache_service.clear_pattern("search:*")
            await cache_service.clear_pattern("stories:*")
        
        asyncio.create_task(invalidate_caches())
        
        return {
            "success": True,
            "message": "Story deleted successfully",
            "story_id": str(story_id),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        _safe_rollback(db)
        raise
    except Exception as e:
        _safe_rollback(db)
        raise HTTPException(
            status_code=400, 
            detail=f"Failed to delete story: {str(e)}"
        )

# =====================
# Cache Management Endpoints
# =====================

@router.get("/admin/cache/stats")
async def get_cache_stats():
    """Get detailed cache statistics for stories"""
    from ..services.cache_service import cache_service
    
    base_stats = cache_service.get_stats()
    
    # Add story-specific information
    story_stats = {
        "service": "stories",
        "endpoint_info": {
            "paginated_endpoint": "/stories?skip=0&limit=100",
            "all_stories_endpoint": "/stories/all",
            "individual_story_endpoint": "/stories/{id}",
            "search_endpoint": "/stories/search?q=query"
        },
        "performance_targets": {
            "memory_hit_target_ms": "<1",
            "redis_fallback_target_ms": "<10", 
            "db_fallback_target_ms": "<100",
            "search_target_ms": "<50"
        },
        "cache_policies": {
            "browser_caching": "disabled",
            "redis_refresh_interval": f"{settings.REDIS_REFRESH_INTERVAL}s",
            "stale_while_revalidate": settings.ENABLE_STALE_WHILE_REVALIDATE,
            "search_cache_ttl": "300s"
        },
        **base_stats
    }
    
    return CustomJSONResponse(content=story_stats, headers=_create_no_cache_headers())

@router.post("/admin/cache/warm")
async def warm_stories_cache(db: Session = Depends(get_db)):
    """Manually warm the stories cache for optimal performance"""
    from ..services.cache_service import cache_service
    
    try:
        start_time = time.time()
        
        # Warm up the cache service
        await cache_service.warm_up()
        
        # Pre-load common cache keys
        tasks = [
            cache_service.get(settings.stories_cache_key, lambda: _fetch_all_stories_from_db(db)), # Changed
            cache_service.get(_build_cache_key("stories", skip=0, limit=10), lambda: _fetch_stories_from_db(db, 0, 10)),
            cache_service.get(_build_cache_key("stories", skip=0, limit=100), lambda: _fetch_stories_from_db(db, 0, 100))
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        total_time = time.time() - start_time
        
        return CustomJSONResponse(
            content={
                "success": True,
                "total_warmup_time_ms": round(total_time * 1000, 2),
                "message": "Stories cache warmed successfully",
                "timestamp": datetime.utcnow().isoformat()
            },
            headers=_create_no_cache_headers()
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Cache warming failed: {str(e)}"
        )

@router.delete("/admin/cache/clear")
async def clear_stories_cache():
    """Clear all story caches"""
    from ..services.cache_service import cache_service
    
    try:
        # Clear all caches
        await cache_service.clear_all()
        
        return CustomJSONResponse(
            content={
                "success": True,
                "message": "All story caches cleared successfully",
                "timestamp": datetime.utcnow().isoformat()
            },
            headers=_create_no_cache_headers()
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Cache clearing failed: {str(e)}"
        )

# =====================
# Search Index Management
# =====================

@router.post("/admin/search/reindex")
async def reindex_stories(db: Session = Depends(get_db)):
    """Rebuild the search index for all stories"""
    
    try:
        start_time = time.time()
        
        # Reindex all stories
        await SearchService.reindex_all_from_db()
        
        total_time = time.time() - start_time
        
        return CustomJSONResponse(
            content={
                "success": True,
                "total_time_ms": round(total_time * 1000, 2),
                "message": "Search index rebuilt successfully",
                "timestamp": datetime.utcnow().isoformat()
            },
            headers=_create_no_cache_headers()
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Search reindexing failed: {str(e)}"
        )

@router.get("/admin/search/stats")
async def get_search_stats():
    """Get search index statistics"""
    
    try:
        search_service = SearchService()
        
        stats = await search_service.get_index_stats()
        
        return CustomJSONResponse(
            content={
                "success": True,
                "search_stats": stats,
                "timestamp": datetime.utcnow().isoformat()
            },
            headers=_create_no_cache_headers()
        )
        
    except Exception as e:
        return CustomJSONResponse(
            content={
                "success": False,
                "error": str(e),
                "message": "Could not retrieve search statistics",
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=500,
            headers=_create_no_cache_headers()
        )

@router.post("/story/{story_id}/share")
async def share_story(
    story_id: uuid.UUID,
    db: Session = Depends(get_db)
):
    """Increment share count for a story"""
    from ..services.cache_service import cache_service
    try:
        await cache_service.increment_story_shares(str(story_id))
        return {"message": "Share count incremented successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to increment share count: {str(e)}")
