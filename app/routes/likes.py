from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime

from ..database import get_db
from ..services.likes import LikeService
from ..auth.dependencies import get_user_id_from_token
from ..services.cache_service import cache_service

router = APIRouter()

# Pydantic schemas
class LikeBase(BaseModel):
    story_id: Optional[uuid.UUID] = None
    episode_id: Optional[uuid.UUID] = None

class LikeCreate(LikeBase):
    pass

class LikeResponse(LikeBase):
    id: uuid.UUID
    user_id: uuid.UUID
    created_at: datetime

    class Config:
        from_attributes = True

class ToggleLikeResponse(BaseModel):
    liked: bool
    message: str

# Protected endpoints (require authentication)
@router.post("/", response_model=LikeResponse)
def create_like(
    like: LikeCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create like for authenticated user"""
    # Validate that either story_id or episode_id is provided, but not both
    if not ((like.story_id and not like.episode_id) or (like.episode_id and not like.story_id)):
        raise HTTPException(status_code=400, detail="Either story_id or episode_id must be provided, but not both")
    
    like_data = like.model_dump()
    like_data["user_id"] = uuid.UUID(user_id)
    return LikeService.create_like(db, like_data)

@router.post("/story/{story_id}/toggle", response_model=ToggleLikeResponse)
async def toggle_like_story(
    story_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Toggle like for story for authenticated user"""
    response = LikeService.toggle_like_story(db, uuid.UUID(user_id), story_id)
    
    # Update Redis counter immediately
    if response["liked"]:
        await cache_service.increment_story_likes(str(story_id))
    else:
        await cache_service.decrement_story_likes(str(story_id))
    
    return response

@router.post("/episode/{episode_id}/toggle", response_model=ToggleLikeResponse)
async def toggle_like_episode(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Toggle like for episode for authenticated user"""
    response = LikeService.toggle_like_episode(db, uuid.UUID(user_id), episode_id)
    
    # Update Redis counter immediately
    if response["liked"]:
        await cache_service.increment_episode_likes(str(episode_id))
    else:
        await cache_service.decrement_episode_likes(str(episode_id))
    
    return response

@router.get("/my-likes", response_model=List[LikeResponse])
def get_my_likes(
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Get all likes by authenticated user"""
    all_likes = LikeService.get_likes(db, 0, 1000)  # Get more likes for filtering
    return [like for like in all_likes if str(like.user_id) == user_id]

@router.get("/story/{story_id}/my-like", response_model=Optional[LikeResponse])
def get_my_story_like(
    story_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Check if authenticated user liked a specific story"""
    like = LikeService.get_user_like_for_story(db, uuid.UUID(user_id), story_id)
    return like

@router.get("/episode/{episode_id}/my-like", response_model=Optional[LikeResponse])
def get_my_episode_like(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Check if authenticated user liked a specific episode"""
    like = LikeService.get_user_like_for_episode(db, uuid.UUID(user_id), episode_id)
    return like

@router.delete("/{like_id}")
def delete_my_like(
    like_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Delete like for authenticated user (only own likes)"""
    # Verify ownership
    like = LikeService.get_like(db, like_id)
    if not like:
        raise HTTPException(status_code=404, detail="Like not found")
    
    if str(like.user_id) != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this like")
    
    if not LikeService.delete_like(db, like_id):
        raise HTTPException(status_code=404, detail="Like not found")
    return {"message": "Like deleted successfully"}

# Public endpoints (no authentication required)
@router.get("/{like_id}", response_model=LikeResponse)
def get_like(like_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get specific like"""
    like = LikeService.get_like(db, like_id)
    if not like:
        raise HTTPException(status_code=404, detail="Like not found")
    return like

@router.get("/", response_model=List[LikeResponse])
def get_likes(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Get all likes"""
    return LikeService.get_likes(db, skip, limit)

@router.get("/story/{story_id}", response_model=List[LikeResponse])
def get_likes_by_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get likes for a specific story"""
    return LikeService.get_likes_by_story(db, story_id)

@router.get("/episode/{episode_id}", response_model=List[LikeResponse])
def get_likes_by_episode(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get likes for a specific episode"""
    return LikeService.get_likes_by_episode(db, episode_id)