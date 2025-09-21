from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel, Field
from datetime import datetime
import asyncio # Added import

from ..database import get_db
from ..services.ratings import RatingService
from ..auth.dependencies import get_user_id_from_token
from ..services.cache_service import cache_service

router = APIRouter()

# Pydantic schemas
class RatingBase(BaseModel):
    story_id: Optional[uuid.UUID] = None
    episode_id: Optional[uuid.UUID] = None
    rating_value: int = Field(..., ge=1, le=5)

class RatingCreate(RatingBase):
    pass

class RatingUpdate(BaseModel):
    rating_value: int = Field(..., ge=1, le=5)

class RatingResponse(RatingBase):
    rating_id: uuid.UUID
    user_id: uuid.UUID
    created_at: datetime

    class Config:
        from_attributes = True

class UpsertRatingRequest(BaseModel):
    rating_value: int = Field(..., ge=1, le=5)

# Protected endpoints (require authentication)
@router.post("/", response_model=RatingResponse)
async def create_rating(
    rating: RatingCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create rating for authenticated user"""
    # Validate that either story_id or episode_id is provided, but not both
    if not ((rating.story_id and not rating.episode_id) or (rating.episode_id and not rating.story_id)):
        raise HTTPException(status_code=400, detail="Either story_id or episode_id must be provided, but not both")
    
    rating_data = rating.model_dump()
    rating_data["user_id"] = uuid.UUID(user_id)
    created_rating = RatingService.create_rating(db, rating_data)

    if rating.story_id:
        await cache_service.update_story_rating(str(rating.story_id))
    elif rating.episode_id:
        await cache_service.update_episode_rating(str(rating.episode_id))

    return created_rating

@router.post("/story/{story_id}/upsert", response_model=RatingResponse)
async def upsert_story_rating(
    story_id: uuid.UUID,
    request: UpsertRatingRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create or update rating for story for authenticated user"""
    rating = RatingService.upsert_rating(db, uuid.UUID(user_id), story_id=story_id, rating_value=request.rating_value)
    await cache_service.update_story_rating(str(story_id))
    return rating

@router.post("/episode/{episode_id}/upsert", response_model=RatingResponse)
async def upsert_episode_rating(
    episode_id: uuid.UUID,
    request: UpsertRatingRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create or update rating for episode for authenticated user"""
    rating = RatingService.upsert_rating(db, uuid.UUID(user_id), episode_id=episode_id, rating_value=request.rating_value)
    await cache_service.update_episode_rating(str(episode_id))
    return rating

@router.get("/my-ratings", response_model=List[RatingResponse])
def get_my_ratings(
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Get all ratings by authenticated user"""
    all_ratings = RatingService.get_ratings(db, 0, 1000)  # Get more ratings for filtering
    return [rating for rating in all_ratings if str(rating.user_id) == user_id]

@router.get("/story/{story_id}/my-rating", response_model=Optional[RatingResponse])
def get_my_story_rating(
    story_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Get authenticated user's rating for a specific story"""
    rating = RatingService.get_user_rating_for_story(db, uuid.UUID(user_id), story_id)
    return rating

@router.get("/episode/{episode_id}/my-rating", response_model=Optional[RatingResponse])
def get_my_episode_rating(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Get authenticated user's rating for a specific episode"""
    rating = RatingService.get_user_rating_for_episode(db, uuid.UUID(user_id), episode_id)
    return rating

@router.patch("/{rating_id}", response_model=RatingResponse)
async def update_my_rating(
    rating_id: uuid.UUID,
    rating: RatingUpdate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Update rating for authenticated user (only own ratings)"""
    # Verify ownership
    existing_rating = RatingService.get_rating(db, rating_id)
    if not existing_rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    
    if str(existing_rating.user_id) != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to update this rating")
    
    updated_rating = RatingService.update_rating(db, rating_id, rating.model_dump(exclude_unset=True))
    if not updated_rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    
    if updated_rating.story_id:
        await cache_service.update_story_rating(str(updated_rating.story_id))
    elif updated_rating.episode_id:
        await cache_service.update_episode_rating(str(updated_rating.episode_id))

    return updated_rating

@router.delete("/{rating_id}")
async def delete_my_rating(
    rating_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Delete rating for authenticated user (only own ratings)"""
    # Verify ownership
    existing_rating = RatingService.get_rating(db, rating_id)
    if not existing_rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    
    if str(existing_rating.user_id) != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this rating")
    
    story_id = existing_rating.story_id
    episode_id = existing_rating.episode_id

    if not RatingService.delete_rating(db, rating_id):
        raise HTTPException(status_code=404, detail="Rating not found")

    if story_id:
        await cache_service.update_story_rating(str(story_id))
    elif episode_id:
        await cache_service.update_episode_rating(str(episode_id))

    return {"message": "Rating deleted successfully"}

# Public endpoints (no authentication required)
@router.get("/{rating_id}", response_model=RatingResponse)
def get_rating(rating_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get specific rating"""
    rating = RatingService.get_rating(db, rating_id)
    if not rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    return rating

@router.get("/", response_model=List[RatingResponse])
async def get_ratings(skip: int = 0, limit: int = 100):
    """Get all ratings"""
    cache_key = f"ratings:all:{skip}:{limit}"
    
    async def db_fallback():
        from ..database import SessionLocal
        db = SessionLocal()
        try:
            ratings = await asyncio.to_thread(RatingService.get_ratings, db, skip, limit)
            return [r.to_dict() for r in ratings] # Assuming Rating model has a to_dict method
        finally:
            db.close()

    cached_ratings = await cache_service.get(cache_key, db_fallback)
    return cached_ratings

@router.get("/story/{story_id}", response_model=List[RatingResponse])
def get_ratings_by_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get ratings for a specific story"""
    return RatingService.get_ratings_by_story(db, story_id)

@router.get("/episode/{episode_id}", response_model=List[RatingResponse])
def get_ratings_by_episode(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get ratings for a specific episode"""
    return RatingService.get_ratings_by_episode(db, episode_id)
