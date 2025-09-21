from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from ..models.views import View

class ViewService:
    
    @staticmethod
    def create_view(db: Session, view_data: dict) -> View:
        view = View(**view_data)
        db.add(view)
        db.commit()
        db.refresh(view)
        return view
    
    @staticmethod
    def get_view(db: Session, view_id: uuid.UUID) -> Optional[View]:
        return db.query(View).filter(View.id == view_id).first()
    
    @staticmethod
    def get_views(db: Session, skip: int = 0, limit: int = 100) -> List[View]:
        return db.query(View).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_views_by_story(db: Session, story_id: uuid.UUID) -> List[View]:
        return db.query(View).filter(View.story_id == story_id).all()
    
    @staticmethod
    def get_views_by_episode(db: Session, episode_id: uuid.UUID) -> List[View]:
        return db.query(View).filter(View.episode_id == episode_id).all()
    
    @staticmethod
    def get_views_by_user(db: Session, user_id: uuid.UUID) -> List[View]:
        return db.query(View).filter(View.user_id == user_id).all()
    
    @staticmethod
    def delete_view(db: Session, view_id: uuid.UUID) -> bool:
        view = db.query(View).filter(View.id == view_id).first()
        if view:
            db.delete(view)
            db.commit()
            return True
        return False
    
    @staticmethod
    def count_story_views(db: Session, story_id: uuid.UUID) -> int:
        return db.query(View).filter(View.story_id == story_id).count()
    
    @staticmethod
    def count_episode_views(db: Session, episode_id: uuid.UUID) -> int:
        return db.query(View).filter(View.episode_id == episode_id).count()