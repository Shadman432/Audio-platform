from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime

from ..database import get_db
from ..services.views import ViewService
from ..services.cache_service import cache_service

router = APIRouter()

# Pydantic schemas
class ViewBase(BaseModel):
    story_id: Optional[uuid.UUID] = None
    episode_id: Optional[uuid.UUID] = None
    user_id: Optional[uuid.UUID] = None
    ip_address: Optional[str] = None

class ViewCreate(ViewBase):
    pass

class ViewResponse(ViewBase):
    view_id: uuid.UUID
    created_at: datetime

    class Config:
        from_attributes = True

class ViewCountResponse(BaseModel):
    count: int

@router.post("/", response_model=ViewResponse)
async def create_view(view: ViewCreate, db: Session = Depends(get_db)):
    if not ((view.story_id and not view.episode_id) or (view.episode_id and not view.story_id)):
        raise HTTPException(status_code=400, detail="Either story_id or episode_id must be provided, but not both")
    
    created_view = await ViewService.create_view_async(db, view.model_dump())  # Use async version
    return created_view

@router.get("/{view_id}", response_model=ViewResponse)
def get_view(view_id: uuid.UUID, db: Session = Depends(get_db)):
    view = ViewService.get_view(db, view_id)
    if not view:
        raise HTTPException(status_code=404, detail="View not found")
    return view

@router.get("/", response_model=List[ViewResponse])
def get_views(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return ViewService.get_views(db, skip, limit)

@router.get("/story/{story_id}", response_model=List[ViewResponse])
def get_views_by_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    return ViewService.get_views_by_story(db, story_id)

@router.get("/episode/{episode_id}", response_model=List[ViewResponse])
def get_views_by_episode(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    return ViewService.get_views_by_episode(db, episode_id)

@router.get("/user/{user_id}", response_model=List[ViewResponse])
def get_views_by_user(user_id: uuid.UUID, db: Session = Depends(get_db)):
    return ViewService.get_views_by_user(db, user_id)

@router.get("/story/{story_id}/count", response_model=ViewCountResponse)
def count_story_views(story_id: uuid.UUID, db: Session = Depends(get_db)):
    count = ViewService.count_story_views(db, story_id)
    return ViewCountResponse(count=count)

@router.get("/episode/{episode_id}/count", response_model=ViewCountResponse)
def count_episode_views(episode_id: uuid.UUID, db: Session = Depends(get_db)):
    count = ViewService.count_episode_views(db, episode_id)
    return ViewCountResponse(count=count)

@router.delete("/{view_id}")
def delete_view(view_id: uuid.UUID, db: Session = Depends(get_db)):
    if not ViewService.delete_view(db, view_id):
        raise HTTPException(status_code=404, detail="View not found")
    return {"message": "View deleted successfully"}