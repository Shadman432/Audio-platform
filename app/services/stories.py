from __future__ import annotations
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select
from ..models.stories import Story
import uuid
import json
from ..config import settings
from ..services.cache_service import cache_service
from ..services.serializers import story_to_dict as serialize_story





class StoryService:
    @staticmethod
    async def get_stories_paginated(
        db: Session, 
        skip: int = 0, 
        limit: int = 100, 
        title: Optional[str] = None, 
        genre: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        # If filters are present, fallback to DB (filters are not cached)
        if title or genre:
            stmt = select(Story)
            if title:
                stmt = stmt.where(Story.title.ilike(f"%{title}%"))
            if genre:
                stmt = stmt.where(Story.genre.ilike(f"%{genre}%"))
            
            stmt = stmt.order_by(Story.updated_at.desc()).offset(skip).limit(limit)
            stories = db.execute(stmt).scalars().all()
            return [serialize_story(story) for story in stories]
        
        # No filters: use master key pagination
        return await cache_service.get_paginated_stories(skip, limit)
    
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
        # First try fast hash lookup
        cached_story = await cache_service.get_story_by_id_fast(str(story_id))
        if cached_story:
            return cached_story
        
        # Fallback to DB only if not found
        story = db.query(Story).filter(Story.story_id == story_id).first()
        return serialize_story(story) if story else None

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