# app/services/home_slideshow.py
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional
import uuid
from ..models.home_slideshow import HomeSlideshow
from ..models.stories import Story

class HomeSlideshowService:

    @staticmethod
    def create_slideshow(db: Session, slideshow_data: dict) -> HomeSlideshow:
        # story fetch करके उसके fields duplicate करो
        story = db.query(Story).filter(Story.story_id == slideshow_data["story_id"]).first()
        if not story:
            raise ValueError("Story not found")

        slideshow = HomeSlideshow(
            story_id=story.story_id,
            title=story.title,
            description=story.description,
            genre=story.genre,
            rating=story.rating,
            thumbnail_square=story.thumbnail_square,
            thumbnail_rect=story.thumbnail_rect,
            thumbnail_responsive=story.thumbnail_responsive,
            backdrop_url=getattr(story, "backdrop_url", None),
            trailer_url=getattr(story, "trailer_url", None),
            display_order=slideshow_data.get("display_order", 0),
            is_active=slideshow_data.get("is_active", True),
        )
        db.add(slideshow)
        db.commit()
        db.refresh(slideshow)
        return slideshow

    @staticmethod
    def get_slideshow(db: Session, slideshow_id: uuid.UUID) -> Optional[HomeSlideshow]:
        return (
            db.query(HomeSlideshow)
            .options(joinedload(HomeSlideshow.story))
            .filter(HomeSlideshow.slideshow_id == slideshow_id)
            .first()
        )

    @staticmethod
    def get_all_slideshows(db: Session, skip: int = 0, limit: int = 100) -> List[HomeSlideshow]:
        return (
            db.query(HomeSlideshow)
            .options(joinedload(HomeSlideshow.story))
            .offset(skip).limit(limit)
            .all()
        )

    @staticmethod
    def get_active_slideshows(db: Session) -> List[HomeSlideshow]:
        return (
            db.query(HomeSlideshow)
            .options(joinedload(HomeSlideshow.story))
            .filter(HomeSlideshow.is_active)
            .order_by(HomeSlideshow.display_order)
            .all()
        )

    @staticmethod
    def get_slideshows_by_story(db: Session, story_id: uuid.UUID) -> List[HomeSlideshow]:
        return (
            db.query(HomeSlideshow)
            .options(joinedload(HomeSlideshow.story))
            .filter(HomeSlideshow.story_id == story_id)
            .all()
        )

    @staticmethod
    def update_slideshow(db: Session, slideshow_id: uuid.UUID, slideshow_data: dict) -> Optional[HomeSlideshow]:
        slideshow = db.query(HomeSlideshow).filter(HomeSlideshow.slideshow_id == slideshow_id).first()
        if slideshow:
            # अगर story_id बदली है तो story से नए fields copy करो
            if "story_id" in slideshow_data:
                story = db.query(Story).filter(Story.story_id == slideshow_data["story_id"]).first()
                if story:
                    slideshow.story_id = story.story_id
                    slideshow.title = story.title
                    slideshow.description = story.description
                    slideshow.genre = story.genre
                    slideshow.rating = story.rating
                    slideshow.thumbnail_square = story.thumbnail_square
                    slideshow.thumbnail_rect = story.thumbnail_rect
                    slideshow.thumbnail_responsive = story.thumbnail_responsive
                    slideshow.backdrop_url = getattr(story, "backdrop_url", None)
                    slideshow.trailer_url = getattr(story, "trailer_url", None)

            # बाकी slideshow fields update करो
            for key, value in slideshow_data.items():
                if hasattr(slideshow, key) and key != "story_id":
                    setattr(slideshow, key, value)

            db.commit()
            db.refresh(slideshow)
        return slideshow

    @staticmethod
    def delete_slideshow(db: Session, slideshow_id: uuid.UUID) -> bool:
        slideshow = db.query(HomeSlideshow).filter(HomeSlideshow.slideshow_id == slideshow_id).first()
        if slideshow:
            db.delete(slideshow)
            db.commit()
            return True
        return False

    @staticmethod
    def toggle_slideshow_status(db: Session, slideshow_id: uuid.UUID) -> Optional[HomeSlideshow]:
        slideshow = db.query(HomeSlideshow).filter(HomeSlideshow.slideshow_id == slideshow_id).first()
        if slideshow:
            slideshow.is_active = not slideshow.is_active
            db.commit()
            db.refresh(slideshow)
        return slideshow
