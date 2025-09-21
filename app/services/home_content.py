from sqlalchemy.orm import Session, joinedload
from typing import List, Optional, Union, Dict, Any
import uuid
import json
from ..models.home_content import HomeContent
from ..models.home_content_series import HomeContentSeries
from ..services.cache_service import cache_service
from .home_content_series import serialize_home_content_series
from .stories import serialize_story

def serialize_home_content(home_content: HomeContent) -> Dict[str, Any]:
    """Converts a HomeContent SQLAlchemy object to a dictionary for caching."""
    serialized_series = []
    if home_content.series:
        for series_item in home_content.series:
            serialized_series_item = serialize_home_content_series(series_item)
            if hasattr(series_item, 'story') and series_item.story:
                serialized_series_item['story'] = serialize_story(series_item.story)
            serialized_series.append(serialized_series_item)

    return {
        "category_id": str(home_content.category_id),
        "category_name": home_content.category_name,
        "created_at": home_content.created_at.isoformat() if home_content.created_at else None,
        "series": serialized_series,
    }



class HomeContentService:
    
    @staticmethod
    def create_home_content(db: Session, content_data: dict) -> HomeContent:
        content = HomeContent(**content_data)
        db.add(content)
        db.commit()
        db.refresh(content)
        return content
    
    @staticmethod
    async def get_home_content(db: Session, categoryid: uuid.UUID) -> Optional[Dict[str, Any]]:
        cache_key = f"home_content:id:{categoryid}"
        
        async def db_fallback():
            content = (
                db.query(HomeContent)
                .options(joinedload(HomeContent.series).joinedload(HomeContentSeries.story))
                .filter(HomeContent.categoryid == categoryid)
                .first()
            )
            if content:
                return serialize_home_content(content)
            return None

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_all_home_content(db: Session, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """ Paginated all categories """
        cache_key = f"home_content:all:paginated:{skip}:{limit}"
        
        async def db_fallback():
            all_content = (
                db.query(HomeContent)
                .options(joinedload(HomeContent.series).joinedload(HomeContentSeries.story))
                .offset(skip)
                .limit(limit)
                .all()
            )
            return [serialize_home_content(content) for content in all_content]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    async def get_all_home_content_no_pagination(db: Session) -> List[Dict[str, Any]]:
        """ All categories without pagination (used for ?categories=all) """
        cache_key = "home_content:all"
        
        async def db_fallback():
            all_content = (
                db.query(HomeContent)
                .options(joinedload(HomeContent.series).joinedload(HomeContentSeries.story))
                .all()
            )
            return [serialize_home_content(content) for content in all_content]

        cached_data = await cache_service.get(cache_key, db_fallback, ttl=300)
        return cached_data
    
    @staticmethod
    def update_home_content(db: Session, categoryid: uuid.UUID, content_data: dict) -> Optional[HomeContent]:
        content = db.query(HomeContent).filter(HomeContent.categoryid == categoryid).first()
        if content:
            for key, value in content_data.items():
                setattr(content, key, value)
            db.commit()
            db.refresh(content)
        return content
    
    @staticmethod
    def delete_home_content(db: Session, categoryid: uuid.UUID) -> bool:
        content = db.query(HomeContent).filter(HomeContent.categoryid == categoryid).first()
        if content:
            db.delete(content)
            db.commit()
            return True
        return False
    
    @staticmethod
    def get_content_by_name(db: Session, category_name: str) -> Optional[HomeContent]:
        return (
            db.query(HomeContent)
            .options(joinedload(HomeContent.series).joinedload(HomeContentSeries.story))
            .filter(HomeContent.category_name.ilike(f"%{category_name}%"))
            .first()
        )
    
    @staticmethod
    def get_home_content_multiple(
        db: Session,
        category_ids: Union[List[uuid.UUID], str],
        skip: int = 0,
        limit: int = 100
    ) -> List[HomeContent]:
        """
        Fetch multiple categories.
        If category_ids == "all" -> return all without pagination
        Else -> filter by given UUID list with pagination
        """
        if isinstance(category_ids, str) and category_ids.lower() == "all":
            return (
                db.query(HomeContent)
                .options(joinedload(HomeContent.series).joinedload(HomeContentSeries.story))
                .all()
            )
        
        return (
            db.query(HomeContent)
            .options(joinedload(HomeContent.series).joinedload(HomeContentSeries.story))
            .filter(HomeContent.categoryid.in_(category_ids))
            .offset(skip)
            .limit(limit)
            .all()
        )
