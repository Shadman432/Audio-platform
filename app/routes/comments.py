from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
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
@router.post("/", response_model=CommentResponse)
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
    
    created_comment = await CommentService.add_comment(redis, comment_data)
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
@router.get("/story/{story_id}/ranked", response_model=List[RankedCommentResponse])
async def get_ranked_comments(
    story_id: uuid.UUID,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
    limit: int = 10,
):
    """Get ranked comments for a story"""
    return await CommentService.get_comments_with_ranking(redis, db, story_id, limit)
