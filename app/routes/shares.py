from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime

from ..database import get_db
from ..services.shares import ShareService
from ..auth.dependencies import get_user_id_from_token
from ..services.cache_service import cache_service

router = APIRouter()

# Pydantic schemas
class ShareBase(BaseModel):
    story_id: Optional[uuid.UUID] = None
    episode_id: Optional[uuid.UUID] = None

class ShareCreate(ShareBase):
    pass

class ShareResponse(ShareBase):
    share_id: uuid.UUID
    user_id: uuid.UUID
    created_at: datetime

    class Config:
        from_attributes = True

@router.post("/", response_model=ShareResponse)
async def create_share(
    share: ShareCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create share for authenticated user"""
    # Validate that either story_id or episode_id is provided, but not both
    if not ((share.story_id and not share.episode_id) or (share.episode_id and not share.story_id)):
        raise HTTPException(status_code=400, detail="Either story_id or episode_id must be provided, but not both")
    
    share_data = share.model_dump()
    share_data["user_id"] = uuid.UUID(user_id)
    created_share = ShareService.create_share(db, share_data)

    if share.story_id:
        await cache_service.increment_story_shares(str(share.story_id))
    elif share.episode_id:
        await cache_service.increment_episode_shares(str(share.episode_id))

    return created_share

@router.get("/{share_id}", response_model=ShareResponse)
def get_share(share_id: uuid.UUID, db: Session = Depends(get_db)):
    share = ShareService.get_share(db, share_id)
    if not share:
        raise HTTPException(status_code=404, detail="Share not found")
    return share

@router.get("/", response_model=List[ShareResponse])
def get_shares(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return ShareService.get_shares(db, skip, limit)

@router.get("/story/{story_id}", response_model=List[ShareResponse])
def get_shares_by_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    return ShareService.get_shares_by_story(db, story_id)

@router.get("/episode/{episode_id}", response_model=List[ShareResponse])
def get_shares_by_episode(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    return ShareService.get_shares_by_episode(db, episode_id)
