from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime, timedelta
import json

from ..database import get_db
from ..services.episodes import EpisodeService
from ..services.serializers import episode_to_dict
from ..services.stories import StoryService as StoriesStoryService
from ..services.cache_service import cache_service
from ..config import settings # Added import

router = APIRouter()

# Pydantic schemas
class EpisodeBase(BaseModel):
    story_id: uuid.UUID
    title: str
    meta_title: Optional[str] = None
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    description: Optional[str] = None
    meta_description: Optional[str] = None
    duration: Optional[timedelta] = None
    release_date: Optional[datetime] = None

class EpisodeCreate(EpisodeBase):
    hls_url: Optional[str] = None

class EpisodeUpdate(BaseModel):
    story_id: Optional[uuid.UUID] = None
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

class EpisodeResponse(EpisodeBase):
    episode_id: uuid.UUID
    hls_url: Optional[str] = None
    genre: Optional[str] = None
    subgenre: Optional[str] = None
    rating: Optional[str] = None
    avg_rating: Optional[float] = None
    author_json: Optional[List[dict]] = None
    likes_count: int = 0
    comments_count: int = 0
    views_count: int = 0
    shares_count: int = 0
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

@router.post("/", response_model=EpisodeResponse)
async def create_episode(episode: EpisodeCreate, db: Session = Depends(get_db)):
    story = StoriesStoryService.get_story_by_id(db, episode.story_id)
    if not story:
        raise HTTPException(status_code=400, detail=f"Story with id {episode.story_id} not found.")
    new_episode = await EpisodeService.create_episode(db, episode.model_dump())
    await cache_service.delete(settings.episodes_cache_key) # Changed
    await cache_service.delete(f"{settings.episode_cache_key_prefix}:{new_episode.episode_id}") # Added
    return new_episode

@router.get("/all")
async def get_all_episodes_explicit(db: Session = Depends(get_db)):
    """
    Get all episodes from the Redis cache, optimized for speed.
    Warning: This endpoint can return a very large dataset, which may cause
    performance issues in browser-based tools like Swagger UI.
    Consider using the paginated endpoint (GET /episodes) for smaller responses.
    """
    async def db_fallback():
        episodes = await EpisodeService.get_all_episodes(db)
        return {"python": episodes, "json": json.dumps(episodes, default=str)}

    cached_data = await cache_service.get(settings.episodes_cache_key, db_fallback=db_fallback) # Changed
    if not cached_data:
        raise HTTPException(status_code=503, detail="Service is warming up or data is unavailable. Please try again in a moment.")

    return Response(content=cached_data['json'], media_type="application/json")

@router.get("/", response_model=List[EpisodeResponse])
async def get_episodes(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    async def db_fallback():
        episodes = await EpisodeService.get_episodes(db, skip, limit)
        return {"python": episodes, "json": json.dumps(episodes, default=str)}

    # The cache key should reflect the pagination parameters
    cache_key = f"{settings.episode_cache_key_prefix}:skip={skip}&limit={limit}"
    cached_data = await cache_service.get(cache_key, db_fallback=db_fallback)
    
    if not cached_data:
        raise HTTPException(status_code=503, detail="Service is warming up or data is unavailable. Please try again in a moment.")

    return cached_data['python']

@router.get("/story/{story_id}", response_model=List[EpisodeResponse])
async def get_episodes_by_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    # This specific query is not cached, but could be if needed.
    return await EpisodeService.get_episodes_by_story(db, story_id)

@router.get("/{episode_id}", response_model=EpisodeResponse)
async def get_episode(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    async def db_fallback():
        episode = await EpisodeService.get_episode(db, episode_id)
        if not episode:
            return None # Indicate not found
        return episode

    cached_episode = await cache_service.get(f"{settings.episode_cache_key_prefix}:{episode_id}", db_fallback=db_fallback)

    if not cached_episode:
        raise HTTPException(status_code=404, detail="Episode not found")
    return cached_episode

@router.patch("/{episode_id}", response_model=EpisodeResponse)
async def update_episode(episode_id: uuid.UUID, episode: EpisodeUpdate, db: Session = Depends(get_db)):
    update_data = episode.model_dump(exclude_unset=True)
    if 'story_id' in update_data:
        story = StoriesStoryService.get_story_by_id(db, update_data['story_id'])
        if not story:
            raise HTTPException(status_code=400, detail=f"Story with id {update_data['story_id']} not found.")

    updated_episode = await EpisodeService.update_episode(db, episode_id, update_data)
    if not updated_episode:
        raise HTTPException(status_code=404, detail="Episode not found")
    await cache_service.delete(settings.episodes_cache_key) # Changed
    await cache_service.delete(f"{settings.episode_cache_key_prefix}:{episode_id}") # Added
    return updated_episode

@router.delete("/{episode_id}")
async def delete_episode(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    deleted = await EpisodeService.delete_episode(db, episode_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Episode not found")
    await cache_service.delete(settings.episodes_cache_key) # Changed
    await cache_service.delete(f"{settings.episode_cache_key_prefix}:{episode_id}") # Added
    return {"message": "Episode deleted successfully"}

@router.get("/clearcache")
async def clear_episodes_cache():
    await cache_service.delete(settings.episodes_cache_key)
    return {"message": "Episodes cache cleared"}

@router.post("/episode/{episode_id}/share")
async def share_episode(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db)
):
    """Increment share count for an episode"""
    try:
        await cache_service.increment_episode_shares(str(episode_id))
        return {"message": "Share count incremented successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to increment share count: {str(e)}")