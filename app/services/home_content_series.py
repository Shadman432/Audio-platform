from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import uuid
import json
from ..models.home_content_series import HomeContentSeries
from ..models.stories import Story   # ✅ import Story model
from ..services.cache_service import cache_service

def serialize_home_content_series(series: HomeContentSeries) -> Dict[str, Any]:
    """Converts a HomeContentSeries SQLAlchemy object to a dictionary for caching."""
    return {
        "content_series_id": str(series.content_series_id),
        "category_id": str(series.category_id),
        "story_id": str(series.story_id),
        "title": series.title,
        "thumbnail_square": series.thumbnail_square,
        "thumbnail_rect": series.thumbnail_rect,
        "thumbnail_responsive": series.thumbnail_responsive,
        "description": series.description,
        "genre": series.genre,
        "rating": series.rating,
        "avg_rating": float(series.avg_rating) if series.avg_rating is not None else None,
        "created_at": series.created_at.isoformat() if series.created_at else None,
    }


class HomeContentSeriesService:
    
    @staticmethod
    def create_content_series(db: Session, categoryid: uuid.UUID, story_ids: List[uuid.UUID]) -> List[HomeContentSeries]:
        created_series = []
        for story_id in story_ids:
            story = db.query(Story).filter(Story.story_id == story_id).first()
            if not story:
                continue  # skip invalid story_id

            series = HomeContentSeries(
                categoryid=categoryid,
                story_id=story.story_id,
                title=story.title,
                thumbnail_square=story.thumbnail_square,
                thumbnail_rect=story.thumbnail_rect,
                thumbnail_responsive=story.thumbnail_responsive,
                description=story.description,
                genre=story.genre,
                rating=story.rating,
            )
            db.add(series)
            created_series.append(series)

        db.commit()
        for s in created_series:
            db.refresh(s)
        return created_series
    
    @staticmethod
    async def get_content_series(db: Session, content_series_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        cache_key = f"home_content_series:id:{content_series_id}"
        
        async def db_fallback():
            series = db.query(HomeContentSeries).filter(HomeContentSeries.content_series_id == content_series_id).first()
            
            if series:
                return serialize_home_content_series(series)
            return None

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_all_content_series(db: Session, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        cache_key = f"home_content_series:all:{skip}:{limit}"
        
        async def db_fallback():
            all_series = db.query(HomeContentSeries).offset(skip).limit(limit).all()
            
            return [serialize_home_content_series(series) for series in all_series]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_series_by_category(db: Session, categoryid: uuid.UUID) -> List[Dict[str, Any]]:
        cache_key = f"home_content_series:by_category:{categoryid}"
        
        async def db_fallback():
            series_by_category = db.query(HomeContentSeries).filter(HomeContentSeries.categoryid == categoryid).all()
            
            return [serialize_home_content_series(series) for series in series_by_category]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_series_by_story(db: Session, story_id: uuid.UUID) -> List[Dict[str, Any]]:
        cache_key = f"home_content_series:by_story:{story_id}"
        
        async def db_fallback():
            series_by_story = db.query(HomeContentSeries).filter(HomeContentSeries.story_id == story_id).all()
            
            return [serialize_home_content_series(series) for series in series_by_story]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    def update_content_series(db: Session, content_series_id: uuid.UUID, series_data: dict) -> Optional[HomeContentSeries]:
        series = db.query(HomeContentSeries).filter(HomeContentSeries.content_series_id == content_series_id).first()
        if series:
            for key, value in series_data.items():
                setattr(series, key, value)
            db.commit()
            db.refresh(series)
        return series
    
    @staticmethod
    def delete_content_series(db: Session, content_series_id: uuid.UUID) -> bool:
        series = db.query(HomeContentSeries).filter(HomeContentSeries.content_series_id == content_series_id).first()
        if series:
            db.delete(series)
            db.commit()
            return True
        return False