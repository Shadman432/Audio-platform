from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from datetime import datetime

from ..database import get_db
from ..services.home_content import HomeContentService
from pydantic import BaseModel

router = APIRouter()

# -----------------------
# Schemas
# -----------------------
class HomeContentSeriesResponse(BaseModel):
    story_id: uuid.UUID
    title: str
    thumbnail_square: Optional[str] = None
    thumbnail_rect: Optional[str] = None
    thumbnail_responsive: Optional[str] = None
    description: Optional[str] = None
    genre: Optional[str] = None
    rating: Optional[str] = None

    class Config:
        from_attributes = True


class HomeContentResponse(BaseModel):
    categoryid: uuid.UUID
    category_name: str
    created_at: datetime
    home_content_series: List[HomeContentSeriesResponse] = []

    class Config:
        from_attributes = True


# -----------------------
# Routes
# -----------------------

@router.get("", response_model=List[HomeContentResponse], tags=["home-content"])
def get_home_content(
    categories: Optional[str] = Query(
        None,
        description=(
            "Filter content using categories:\n\n"
            "- `/content?skip=0&limit=100` → All content with pagination\n"
            "- `/content?categories=uuid` → Single category\n"
            "- `/content?categories=uuid1,uuid2` → Multiple categories\n"
            "- `/content?categories=all` → All categories (without pagination)"
        )
    ),
    skip: int = Query(
        0,
        description="Records to skip (pagination only)",
        ge=0
    ),
    limit: int = Query(
        100,
        description="Number of records to fetch (pagination only)",
        ge=1,
        le=1000
    ),
    db: Session = Depends(get_db)
):
    """
    Get home content with 4 modes:
    1. Pagination only
    2. Single UUID filter
    3. Multiple UUID filter
    4. All categories (no pagination)
    """
    # Case 4 → categories=all → sabhi categories, bina pagination
    if categories == "all":
        return HomeContentService.get_all_home_content_no_pagination(db)

    # Case 2 & 3 → UUIDs diye gaye hain
    if categories:
        try:
            category_ids = [uuid.UUID(cat.strip()) for cat in categories.split(",")]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid UUID format in categories")

        contents = HomeContentService.get_home_content_multiple(db, category_ids, skip, limit)
        if not contents:
            raise HTTPException(status_code=404, detail="No categories found")
        return contents

    # Case 1 → Default sabhi content (pagination ke saath)
    return HomeContentService.get_all_home_content(db, skip, limit)
