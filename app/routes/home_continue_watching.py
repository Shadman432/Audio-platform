from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from pydantic import BaseModel
from datetime import datetime

from ..database import get_db
from ..services.home_continue_watching import HomeContinueWatchingService
from ..auth.dependencies import get_user_id_from_token

router = APIRouter()

# -------------------------------
# Pydantic Schemas
# -------------------------------
class HomeContinueWatchingBase(BaseModel):
    story_id: uuid.UUID
    episode_id: uuid.UUID
    progress_seconds: Optional[int] = 0
    total_duration: Optional[int] = None
    completed: Optional[bool] = False

class HomeContinueWatchingCreate(HomeContinueWatchingBase):
    pass

class HomeContinueWatchingUpdate(BaseModel):
    progress_seconds: Optional[int] = None
    total_duration: Optional[int] = None
    completed: Optional[bool] = None

class HomeContinueWatchingResponse(HomeContinueWatchingBase):
    continue_id: uuid.UUID
    user_id: uuid.UUID
    last_watched_at: datetime

    class Config:
        from_attributes = True

class ProgressUpdateRequest(BaseModel):
    story_id: uuid.UUID
    episode_id: uuid.UUID
    progress_seconds: int
    total_duration: Optional[int] = None


# -------------------------------
# Protected Endpoints (Require Auth)
# -------------------------------
@router.post("/", response_model=HomeContinueWatchingResponse)
def create_continue_watching(
    watching: HomeContinueWatchingCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Create continue watching record for authenticated user"""
    watching_data = watching.model_dump()
    watching_data["user_id"] = uuid.UUID(user_id)
    return HomeContinueWatchingService.create_continue_watching(db, watching_data)


@router.get("/my-list", response_model=List[HomeContinueWatchingResponse])
def get_my_continue_watching(
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Get continue watching list for authenticated user"""
    return HomeContinueWatchingService.get_user_continue_watching(db, uuid.UUID(user_id))


@router.get("/episode/{episode_id}", response_model=HomeContinueWatchingResponse)
def get_my_episode_progress(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Get progress for specific episode for authenticated user"""
    progress = HomeContinueWatchingService.get_user_episode_progress(db, uuid.UUID(user_id), episode_id)
    if not progress:
        raise HTTPException(status_code=404, detail="Progress record not found")
    return progress


@router.post("/update-progress", response_model=HomeContinueWatchingResponse)
def update_my_progress(
    request: ProgressUpdateRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Update progress for authenticated user"""
    return HomeContinueWatchingService.upsert_progress(
        db,
        uuid.UUID(user_id),
        request.story_id,
        request.episode_id,
        request.progress_seconds,
        request.total_duration
    )


@router.post("/mark-completed/{episode_id}", response_model=HomeContinueWatchingResponse)
def mark_my_episode_completed(
    episode_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Mark episode as completed for authenticated user"""
    watching = HomeContinueWatchingService.mark_completed(db, uuid.UUID(user_id), episode_id)
    if not watching:
        raise HTTPException(status_code=404, detail="Progress record not found")
    return watching


@router.delete("/{continue_id}")
def delete_my_continue_watching(
    continue_id: uuid.UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_user_id_from_token)
):
    """Delete continue watching record for authenticated user"""
    watching = HomeContinueWatchingService.get_continue_watching(db, continue_id)
    if not watching:
        raise HTTPException(status_code=404, detail="Continue watching record not found")
    
    if str(watching.user_id) != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this record")
    
    if not HomeContinueWatchingService.delete_continue_watching(db, continue_id):
        raise HTTPException(status_code=404, detail="Continue watching record not found")
    return {"message": "Continue watching record deleted successfully"}


# -------------------------------
# Admin Endpoints (Dev/Testing)
# -------------------------------
@router.get("/{continue_id}", response_model=HomeContinueWatchingResponse)
def get_continue_watching(continue_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get specific continue watching record (admin only)"""
    watching = HomeContinueWatchingService.get_continue_watching(db, continue_id)
    if not watching:
        raise HTTPException(status_code=404, detail="Continue watching record not found")
    return watching


@router.get("/", response_model=List[HomeContinueWatchingResponse])
def get_all_or_user_continue_watching(
    user: Optional[uuid.UUID] = None,   # ✅ now query param instead of /user/{user_id}
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get all continue watching records (admin only).
    If `?user=<uuid>` is passed → filter by that user.
    """
    if user:
        return HomeContinueWatchingService.get_user_continue_watching(db, user)
    return HomeContinueWatchingService.get_all_continue_watching(db, skip, limit)


@router.patch("/{continue_id}", response_model=HomeContinueWatchingResponse)
def update_continue_watching(
    continue_id: uuid.UUID,
    watching: HomeContinueWatchingUpdate,
    db: Session = Depends(get_db)
):
    """Update continue watching record (admin only)"""
    updated_watching = HomeContinueWatchingService.update_continue_watching(db, continue_id, watching.model_dump(exclude_unset=True))
    if not updated_watching:
        raise HTTPException(status_code=404, detail="Continue watching record not found")
    return updated_watching
