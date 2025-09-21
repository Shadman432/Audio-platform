from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime

from ..database import get_db
from ..services.home_slideshow import HomeSlideshowService

router = APIRouter()

# ------------------------------
# Pydantic schemas
# ------------------------------
class StoryResponse(BaseModel):
    story_id: uuid.UUID
    title: Optional[str] = None
    description: Optional[str] = None
    genre: Optional[str] = None
    rating: Optional[str] = None
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    backdrop_url: Optional[str] = None
    trailer_url: Optional[str] = None

    class Config:
        from_attributes = True


class HomeSlideshowResponse(BaseModel):
    slideshow_id: uuid.UUID
    story: StoryResponse   # 👈 nested story object
    display_order: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# ------------------------------
# User-only Route
# ------------------------------
@router.get("/active", response_model=List[HomeSlideshowResponse], tags=["home-slideshow"])
def get_active_slideshows(db: Session = Depends(get_db)):
    """Get only active slideshows (user-side API)"""
    return HomeSlideshowService.get_active_slideshows(db)
