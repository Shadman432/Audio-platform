from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional, Dict
import uuid
from pydantic import BaseModel
from datetime import datetime
from redis.asyncio import Redis

from ..database import get_db
from ..services.comments import CommentService
from ..auth.dependencies import get_user_id_from_token
from ..dependencies import get_redis

router = APIRouter()

# Pydantic schemas
class CommentBase(BaseModel):
    story_id: Optional[uuid.UUID] = None
    episode_id: Optional[uuid.UUID] = None
    parent_comment_id: Optional[uuid.UUID] = None
    comment_text: str

class CommentCreate(CommentBase):
    pass

class CommentUpdate(BaseModel):
    comment_text: Optional[str] = None

class CommentResponse(BaseModel):
    comment_id: uuid.UUID
    story_id: Optional[uuid.UUID] = None
    episode_id: Optional[uuid.UUID] = None
    user_id: uuid.UUID
    parent_comment_id: Optional[uuid.UUID] = None
    comment_text: str
    created_at: datetime
    updated_at: datetime
    comment_like_count: int

class RankedCommentResponse(BaseModel):
    comment_id: uuid.UUID
    comment_text: str
    user_id: uuid.UUID
    created_at: datetime
    comment_like_count: int
    replies_count: int
    score: float


# Protected endpoints (require authentication)
@router.post("", response_model=CommentResponse)
async def create_comment(
    comment: CommentCreate,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create comment for authenticated user"""
    if not ((comment.story_id and not comment.episode_id) or (comment.episode_id and not comment.story_id)):
        raise HTTPException(status_code=400, detail="Either story_id or episode_id must be provided, but not both")
    
    comment_data = comment.model_dump()
    comment_data["user_id"] = uuid.UUID(user_id)
    
    created_comment = await CommentService.add_comment(db, redis, comment_data)
    return created_comment

@router.post("/{comment_id}/like", status_code=204)
async def like_comment(
    comment_id: uuid.UUID,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
    user_id: str = Depends(get_user_id_from_token)
):
    """Like a comment"""
    await CommentService.like_comment(redis, db, comment_id, uuid.UUID(user_id))
    return

# Public endpoints

# --- Redis-first endpoints (Live) ---

@router.get("/story/{story_id}/ranked", response_model=List[Dict], summary="Get Ranked Comments for Story")
async def get_ranked_comments_for_story(
    story_id: uuid.UUID,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
    limit: int = 50,
):
    """
    Get smart-ranked comments for a story.
    Tries to fetch from Redis first, falls back to the database if not present.
    """
    return await CommentService.get_ranked_comments(db, redis, story_id=story_id, limit=limit)


@router.get("/episode/{episode_id}/ranked", response_model=List[Dict], summary="Get Ranked Comments for Episode")
async def get_ranked_comments_for_episode(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
    limit: int = 50,
):
    """
    Get smart-ranked comments for an episode.
    Tries to fetch from Redis first, falls back to the database if not present.
    """
    return await CommentService.get_ranked_comments(db, redis, episode_id=episode_id, limit=limit)


# --- DB-only endpoints (For Development/Debugging) ---

@router.get("/story/{story_id}/ranked/db", response_model=List[Dict], summary="Get Ranked Comments for Story (DB)", tags=["Development"])
def get_ranked_comments_for_story_db(
    story_id: uuid.UUID,
    db: Session = Depends(get_db),
    limit: int = 50,
):
    """[DEV] Get ranked comments for a story directly from the database."""
    return CommentService.get_ranked_comments_from_db(db, story_id=story_id, limit=limit)


@router.get("/episode/{episode_id}/ranked/db", response_model=List[Dict], summary="Get Ranked Comments for Episode (DB)", tags=["Development"])
def get_ranked_comments_for_episode_db(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    limit: int = 50,
):
    """[DEV] Get ranked comments for an episode directly from the database."""
    return CommentService.get_ranked_comments_from_db(db, episode_id=episode_id, limit=limit)