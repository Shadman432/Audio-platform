from __future__ import annotations
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select
from ..models.stories import Story
import uuid
import json
from ..services.cache_service import cache_service

def serialize_story(story: Story) -> Dict[str, Any]:
    """Converts a Story SQLAlchemy object to a dictionary for caching."""
    return {
        "story_id": str(story.story_id),
        "title": story.title,
        "meta_title": story.meta_title,
        "thumbnail_square": story.thumbnail_square,
        "thumbnail_rect": story.thumbnail_rect,
        "thumbnail_responsive": story.thumbnail_responsive,
        "description": story.description,
        "meta_description": story.meta_description,
        "genre": story.genre,
        "subgenre": story.subgenre,
        "rating": story.rating,
        "avg_rating": float(story.avg_rating) if story.avg_rating is not None else None,
        "avg_rating_count": story.avg_rating_count,
        "likes_count": story.likes_count,
        "comments_count": story.comments_count,
        "shares_count": story.shares_count,
        "views_count": story.views_count,
        "author_json": story.author_json,
        "created_at": story.created_at.isoformat() if story.created_at else None,
        "updated_at": story.updated_at.isoformat() if story.updated_at else None,
    }





class StoryService:
    @staticmethod
    async def get_stories_paginated(
        db: Session, 
        skip: int = 0, 
        limit: int = 100, 
        title: Optional[str] = None, 
        genre: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        cache_key = f"stories:paginated:{skip}:{limit}:{title or ''}:{genre or ''}"
        
        async def db_fallback():
            stmt = select(Story)
            if title:
                stmt = stmt.where(Story.title.ilike(f"%{title}%"))
            if genre:
                stmt = stmt.where(Story.genre.ilike(f"%{genre}%"))
            
            stmt = stmt.order_by(Story.updated_at.desc()).offset(skip).limit(limit)
            stories = db.execute(stmt).scalars().all()
            
            return [serialize_story(story) for story in stories]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data

    @staticmethod
    async def get_all_stories(db: Session) -> List[Dict[str, Any]]:
        cache_key = "stories:all"
        
        async def db_fallback():
            stmt = select(Story).order_by(Story.updated_at.desc())
            stories = db.execute(stmt).scalars().all()
            
            return [serialize_story(story) for story in stories]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data

    @staticmethod
    async def get_story_by_id(db: Session, story_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        cache_key = f"stories:id:{story_id}"
        
        async def db_fallback():
            stmt = select(Story).where(Story.story_id == story_id)
            story = db.execute(stmt).scalar_one_or_none()
            
            if story:
                return serialize_story(story)
            return None

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data

    @staticmethod
    async def create_story(db: Session, payload: Dict[str, Any]) -> Story:
        obj = Story(**payload)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        # Index story in Redisearch - re-indexing will be handled externally
        
        return obj

    @staticmethod
    async def update_story(db: Session, story_id: uuid.UUID, changes: Dict[str, Any]) -> Optional[Story]:
        obj = db.query(Story).filter(Story.story_id == story_id).first()
        if not obj:
            return None
        
        for k, v in changes.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        
        db.add(obj)
        db.commit()
        db.refresh(obj)
        # Update story in Redisearch - re-indexing will be handled externally
        
        return obj

    @staticmethod
    async def delete_story(db: Session, story_id: uuid.UUID) -> bool:
        obj = db.query(Story).filter(Story.story_id == story_id).first()
        if not obj:
            return False
        
        db.delete(obj)
        db.commit()
        # Delete story from Redisearch - re-indexing will be handled externally
        
        return True