from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Optional
import uuid
from ..models.shares import Share

class ShareService:
    
    @staticmethod
    def create_share(db: Session, share_data: dict) -> Share:
        share = Share(**share_data)
        db.add(share)
        db.commit()
        db.refresh(share)
        return share
    
    @staticmethod
    def get_share(db: Session, share_id: uuid.UUID) -> Optional[Share]:
        return db.query(Share).filter(Share.id == share_id).first()
    
    @staticmethod
    def get_shares(db: Session, skip: int = 0, limit: int = 100) -> List[Share]:
        return db.query(Share).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_user_share_for_story(db: Session, user_id: uuid.UUID, story_id: uuid.UUID) -> Optional[Share]:
        return db.query(Share).filter(
            and_(Share.user_id == user_id, Share.story_id == story_id)
        ).first()
    
    @staticmethod
    def get_user_share_for_episode(db: Session, user_id: uuid.UUID, episode_id: uuid.UUID) -> Optional[Share]:
        return db.query(Share).filter(
            and_(Share.user_id == user_id, Share.episode_id == episode_id)
        ).first()
    
    @staticmethod
    def get_shares_by_story(db: Session, story_id: uuid.UUID) -> List[Share]:
        return db.query(Share).filter(Share.story_id == story_id).all()
    
    @staticmethod
    def get_shares_by_episode(db: Session, episode_id: uuid.UUID) -> List[Share]:
        return db.query(Share).filter(Share.episode_id == episode_id).all()
    
    @staticmethod
    def delete_share(db: Session, share_id: uuid.UUID) -> bool:
        share = db.query(Share).filter(Share.id == share_id).first()
        if share:
            db.delete(share)
            db.commit()
            return True
        return False
