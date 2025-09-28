from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import uuid
import json
from ..config import settings
from ..models.episodes import Episode
from ..services.cache_service import cache_service
from ..services.serializers import episode_to_dict as serialize_episode




class EpisodeService:
    
    @staticmethod
    async def create_episode(db: Session, episode_data: dict) -> Episode:
        episode = Episode(**episode_data)
        db.add(episode)
        db.commit()
        db.refresh(episode)
        # Index episode in Redisearch - re-indexing will be handled externally
        
        return episode
    
    @staticmethod
    async def get_episode(db: Session, episode_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        # First try fast hash lookup
        cached_episode = await cache_service.get_episode_by_id(str(episode_id))
        if cached_episode:
            return cached_episode
        
        # Fallback to DB only if not found
        episode = db.query(Episode).filter(Episode.episode_id == episode_id).first()
        return serialize_episode(episode) if episode else None
        
    @staticmethod
    async def get_episodes(db: Session, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        # Get from master key via cache_service
        return await cache_service.get_paginated_episodes(skip, limit)


    @staticmethod
    async def get_all_episodes(db: Session) -> List[Dict[str, Any]]:
        cache_key = "episodes:all"
        
        async def db_fallback():
            episodes = db.query(Episode).all()
            
            return [serialize_episode(episode) for episode in episodes]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_episodes_by_story(db: Session, story_id: uuid.UUID) -> List[Dict[str, Any]]:
        # Try fast Redis hash lookup first
        episodes_data = await cache_service.get_episodes_by_story_fast(str(story_id))
        if episodes_data:
            return episodes_data
        
        # Fallback to original method
        cache_key = f"episodes:by_story:{story_id}"
        
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return cached_data
        
        # Fallback to database
        async def db_fallback():
            episodes = db.query(Episode).filter(Episode.story_id == story_id).all()
            return [serialize_episode(episode) for episode in episodes]

        result = await cache_service.get(cache_key, db_fallback, ttl=3600)
        return result if result else []
    
    @staticmethod
    async def update_episode(db: Session, episode_id: uuid.UUID, episode_data: dict) -> Optional[Episode]:
        episode = db.query(Episode).filter(Episode.episode_id == episode_id).first()
        if episode:
            for key, value in episode_data.items():
                setattr(episode, key, value)
            db.commit()
            db.refresh(episode)
            # Update episode in Redisearch - re-indexing will be handled externally
            
        return episode
    
    @staticmethod
    async def delete_episode(db: Session, episode_id: uuid.UUID) -> bool:
        episode = db.query(Episode).filter(Episode.episode_id == episode_id).first()
        if episode:
            db.delete(episode)
            db.commit()
            # Delete episode from Redisearch - re-indexing will be handled externally
            
            return True
        return False