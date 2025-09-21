from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime

from ..database import get_db
from ..services.home_content_series import HomeContentSeriesService

router = APIRouter(prefix="/home-content-series", tags=["Home Content Series"])

# -----------------------
# Schemas
# -----------------------
class HomeContentSeriesCreate(BaseModel):
    categoryid: uuid.UUID
    story_ids: List[uuid.UUID]   # multiple stories in one category

class HomeContentSeriesUpdate(BaseModel):
    title: Optional[str] = None
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    description: Optional[str] = None
    genre: Optional[str] = None
    rating: Optional[str] = None

class HomeContentSeriesResponse(BaseModel):
    content_series_id: uuid.UUID
    categoryid: uuid.UUID
    story_id: uuid.UUID
    title: str
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    description: Optional[str] = None
    genre: Optional[str] = None
    rating: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

# -----------------------
# Routes
# -----------------------

@router.post("/", response_model=List[HomeContentSeriesResponse])
async def create_content_series(payload: HomeContentSeriesCreate, db: Session = Depends(get_db)):
    series_list = await HomeContentSeriesService.create_content_series(
        db, payload.categoryid, payload.story_ids
    )
    if not series_list:
        raise HTTPException(status_code=400, detail="No valid stories found")
    return series_list


@router.get("/{content_series_id}", response_model=HomeContentSeriesResponse)
def get_content_series(content_series_id: uuid.UUID, db: Session = Depends(get_db)):
    series = HomeContentSeriesService.get_content_series(db, content_series_id)
    if not series:
        raise HTTPException(status_code=404, detail="Content series not found")
    return series


@router.get("/", response_model=List[HomeContentSeriesResponse])
def get_all_content_series(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return HomeContentSeriesService.get_all_content_series(db, skip, limit)


@router.get("/category/{categoryid}", response_model=List[HomeContentSeriesResponse])
def get_series_by_category(categoryid: uuid.UUID, db: Session = Depends(get_db)):
    return HomeContentSeriesService.get_series_by_category(db, categoryid)


@router.get("/story/{story_id}", response_model=List[HomeContentSeriesResponse])
def get_series_by_story(story_id: uuid.UUID, db: Session = Depends(get_db)):
    return HomeContentSeriesService.get_series_by_story(db, story_id)


@router.put("/{content_series_id}", response_model=HomeContentSeriesResponse)
def update_content_series(content_series_id: uuid.UUID, payload: HomeContentSeriesUpdate, db: Session = Depends(get_db)):
    series = HomeContentSeriesService.update_content_series(db, content_series_id, payload.dict(exclude_unset=True))
    if not series:
        raise HTTPException(status_code=404, detail="Content series not found")
    return series


@router.delete("/{content_series_id}")
def delete_content_series(content_series_id: uuid.UUID, db: Session = Depends(get_db)):
    if not HomeContentSeriesService.delete_content_series(db, content_series_id):
        raise HTTPException(status_code=404, detail="Content series not found")
    return {"message": "Content series deleted successfully"}
