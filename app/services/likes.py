from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Optional
import uuid
from ..models.likes import Like

class LikeService:
    
    @staticmethod
    def create_like(db: Session, like_data: dict) -> Like:
        like = Like(**like_data)
        db.add(like)
        db.commit()
        db.refresh(like)
        return like
    
    @staticmethod
    def get_like(db: Session, like_id: uuid.UUID) -> Optional[Like]:
        return db.query(Like).filter(Like.like_id == like_id).first()
    
    @staticmethod
    def get_likes(db: Session, skip: int = 0, limit: int = 100) -> List[Like]:
        return db.query(Like).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_user_like_for_story(db: Session, user_id: uuid.UUID, story_id: uuid.UUID) -> Optional[Like]:
        return db.query(Like).filter(
            and_(Like.user_id == user_id, Like.story_id == story_id)
        ).first()
    
    @staticmethod
    def get_user_like_for_episode(db: Session, user_id: uuid.UUID, episode_id: uuid.UUID) -> Optional[Like]:
        return db.query(Like).filter(
            and_(Like.user_id == user_id, Like.episode_id == episode_id)
        ).first()
    
    @staticmethod
    def get_likes_by_story(db: Session, story_id: uuid.UUID) -> List[Like]:
        return db.query(Like).filter(Like.story_id == story_id).all()
    
    @staticmethod
    def get_likes_by_episode(db: Session, episode_id: uuid.UUID) -> List[Like]:
        return db.query(Like).filter(Like.episode_id == episode_id).all()
    
    @staticmethod
    def delete_like(db: Session, like_id: uuid.UUID) -> bool:
        like = db.query(Like).filter(Like.id == like_id).first()
        if like:
            db.delete(like)
            db.commit()
            return True
        return False
    
    @staticmethod
    def toggle_like_story(db: Session, user_id: uuid.UUID, story_id: uuid.UUID) -> dict:
        existing_like = LikeService.get_user_like_for_story(db, user_id, story_id)
        if existing_like:
            db.delete(existing_like)
            db.commit()
            return {"liked": False, "message": "Like removed"}
        else:
            new_like = Like(user_id=user_id, story_id=story_id)
            db.add(new_like)
            db.commit()
            return {"liked": True, "message": "Story liked"}
    
    @staticmethod
    def toggle_like_episode(db: Session, user_id: uuid.UUID, episode_id: uuid.UUID) -> dict:
        existing_like = LikeService.get_user_like_for_episode(db, user_id, episode_id)
        if existing_like:
            db.delete(existing_like)
            db.commit()
            return {"liked": False, "message": "Like removed"}
        else:
            new_like = Like(user_id=user_id, episode_id=episode_id)
            db.add(new_like)
            db.commit()
            return {"liked": True, "message": "Episode liked"}