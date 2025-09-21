from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Optional
import uuid
from ..models.ratings import Rating

class RatingService:
    
    @staticmethod
    def create_rating(db: Session, rating_data: dict) -> Rating:
        rating = Rating(**rating_data)
        db.add(rating)
        db.commit()
        db.refresh(rating)
        return rating
    
    @staticmethod
    def get_rating(db: Session, rating_id: uuid.UUID) -> Optional[Rating]:
        return db.query(Rating).filter(Rating.id == rating_id).first()
    
    @staticmethod
    def get_ratings(db: Session, skip: int = 0, limit: int = 100) -> List[Rating]:
        return db.query(Rating).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_user_rating_for_story(db: Session, user_id: uuid.UUID, story_id: uuid.UUID) -> Optional[Rating]:
        return db.query(Rating).filter(
            and_(Rating.user_id == user_id, Rating.story_id == story_id)
        ).first()
    
    @staticmethod
    def get_user_rating_for_episode(db: Session, user_id: uuid.UUID, episode_id: uuid.UUID) -> Optional[Rating]:
        return db.query(Rating).filter(
            and_(Rating.user_id == user_id, Rating.episode_id == episode_id)
        ).first()
    
    @staticmethod
    def get_ratings_by_story(db: Session, story_id: uuid.UUID) -> List[Rating]:
        return db.query(Rating).filter(Rating.story_id == story_id).all()
    
    @staticmethod
    def get_ratings_by_episode(db: Session, episode_id: uuid.UUID) -> List[Rating]:
        return db.query(Rating).filter(Rating.episode_id == episode_id).all()
    
    @staticmethod
    def update_rating(db: Session, rating_id: uuid.UUID, rating_data: dict) -> Optional[Rating]:
        rating = db.query(Rating).filter(Rating.id == rating_id).first()
        if rating:
            for key, value in rating_data.items():
                setattr(rating, key, value)
            db.commit()
            db.refresh(rating)
        return rating
    
    @staticmethod
    def delete_rating(db: Session, rating_id: uuid.UUID) -> bool:
        rating = db.query(Rating).filter(Rating.id == rating_id).first()
        if rating:
            db.delete(rating)
            db.commit()
            return True
        return False
    
    @staticmethod
    def upsert_rating(db: Session, user_id: uuid.UUID, story_id: uuid.UUID = None, episode_id: uuid.UUID = None, rating_value: int = None) -> Rating:
        if story_id:
            existing_rating = RatingService.get_user_rating_for_story(db, user_id, story_id)
        else:
            existing_rating = RatingService.get_user_rating_for_episode(db, user_id, episode_id)
        
        if existing_rating:
            existing_rating.rating_value = rating_value
            db.commit()
            db.refresh(existing_rating)
            return existing_rating
        else:
            new_rating = Rating(
                user_id=user_id,
                story_id=story_id,
                episode_id=episode_id,
                rating_value=rating_value
            )
            db.add(new_rating)
            db.commit()
            db.refresh(new_rating)
            return new_rating