from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Optional
import uuid
from ..models.home_continue_watching import HomeContinueWatching

class HomeContinueWatchingService:
    
    @staticmethod
    def create_continue_watching(db: Session, watching_data: dict) -> HomeContinueWatching:
        watching = HomeContinueWatching(**watching_data)
        db.add(watching)
        db.commit()
        db.refresh(watching)
        return watching
    
    @staticmethod
    def get_continue_watching(db: Session, continue_id: uuid.UUID) -> Optional[HomeContinueWatching]:
        return db.query(HomeContinueWatching).filter(HomeContinueWatching.continue_id == continue_id).first()
    
    @staticmethod
    def get_all_continue_watching(db: Session, skip: int = 0, limit: int = 100) -> List[HomeContinueWatching]:
        return db.query(HomeContinueWatching).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_user_continue_watching(db: Session, user_id: uuid.UUID) -> List[HomeContinueWatching]:
        return db.query(HomeContinueWatching).filter(HomeContinueWatching.user_id == user_id).order_by(HomeContinueWatching.last_watched_at.desc()).all()
    
    @staticmethod
    def get_user_episode_progress(db: Session, user_id: uuid.UUID, episode_id: uuid.UUID) -> Optional[HomeContinueWatching]:
        return db.query(HomeContinueWatching).filter(
            and_(HomeContinueWatching.user_id == user_id, HomeContinueWatching.episode_id == episode_id)
        ).first()
    
    @staticmethod
    def update_continue_watching(db: Session, continue_id: uuid.UUID, watching_data: dict) -> Optional[HomeContinueWatching]:
        watching = db.query(HomeContinueWatching).filter(HomeContinueWatching.continue_id == continue_id).first()
        if watching:
            for key, value in watching_data.items():
                setattr(watching, key, value)
            db.commit()
            db.refresh(watching)
        return watching
    
    @staticmethod
    def delete_continue_watching(db: Session, continue_id: uuid.UUID) -> bool:
        watching = db.query(HomeContinueWatching).filter(HomeContinueWatching.continue_id == continue_id).first()
        if watching:
            db.delete(watching)
            db.commit()
            return True
        return False
    
    @staticmethod
    def upsert_progress(db: Session, user_id: uuid.UUID, story_id: uuid.UUID, episode_id: uuid.UUID, progress_seconds: int, total_duration: int = None) -> HomeContinueWatching:
        existing = HomeContinueWatchingService.get_user_episode_progress(db, user_id, episode_id)
        
        if existing:
            existing.progress_seconds = progress_seconds
            existing.total_duration = total_duration or existing.total_duration
            existing.completed = progress_seconds >= (total_duration or existing.total_duration or 0) * 0.9  # 90% considered complete
            db.commit()
            db.refresh(existing)
            return existing
        else:
            new_progress = HomeContinueWatching(
                user_id=user_id,
                story_id=story_id,
                episode_id=episode_id,
                progress_seconds=progress_seconds,
                total_duration=total_duration,
                completed=progress_seconds >= (total_duration or 0) * 0.9
            )
            db.add(new_progress)
            db.commit()
            db.refresh(new_progress)
            return new_progress
    
    @staticmethod
    def mark_completed(db: Session, user_id: uuid.UUID, episode_id: uuid.UUID) -> Optional[HomeContinueWatching]:
        watching = HomeContinueWatchingService.get_user_episode_progress(db, user_id, episode_id)
        if watching:
            watching.completed = True
            db.commit()
            db.refresh(watching)
        return watching