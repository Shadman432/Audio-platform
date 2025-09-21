from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import uuid
import json
from ..models.episodes import Episode
from ..services.cache_service import cache_service

def serialize_episode(episode: Episode) -> Dict[str, Any]:
    """Converts an Episode SQLAlchemy object to a dictionary for caching."""
    return {
        "episode_id": str(episode.episode_id),
        "story_id": str(episode.story_id),
        "title": episode.title,
        "meta_title": episode.meta_title,
        "thumbnail_square": episode.thumbnail_square,
        "thumbnail_rect": episode.thumbnail_rect,
        "thumbnail_responsive": episode.thumbnail_responsive,
        "description": episode.description,
        "meta_description": episode.meta_description,
        "hls_url": episode.hls_url,
        "duration": str(episode.duration) if episode.duration else None, # Convert timedelta to string
        "release_date": episode.release_date.isoformat() if episode.release_date else None,
        "genre": episode.genre,
        "subgenre": episode.subgenre,
        "rating": episode.rating,
        "avg_rating": float(episode.avg_rating) if episode.avg_rating is not None else None,
        "avg_rating_count": episode.avg_rating_count,
        "likes_count": episode.likes_count,
        "comments_count": episode.comments_count,
        "shares_count": episode.shares_count,
        "views_count": episode.views_count,
        "author_json": episode.author_json,
        "created_at": episode.created_at.isoformat() if episode.created_at else None,
        "updated_at": episode.updated_at.isoformat() if episode.updated_at else None,
    }




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
        cache_key = f"episodes:id:{episode_id}"
        
        async def db_fallback():
            episode = db.query(Episode).filter(Episode.episode_id == episode_id).first()
            
            if episode:
                return serialize_episode(episode)
            return None

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_episodes(db: Session, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        cache_key = f"episodes:paginated:{skip}:{limit}"
        
        async def db_fallback():
            episodes = db.query(Episode).offset(skip).limit(limit).all()
            
            return [serialize_episode(episode) for episode in episodes]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data

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
        cache_key = f"episodes:by_story:{story_id}"
        
        async def db_fallback():
            episodes = db.query(Episode).filter(Episode.story_id == story_id).all()
            
            return [serialize_episode(episode) for episode in episodes]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
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