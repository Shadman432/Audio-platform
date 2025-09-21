from sqlalchemy import Column, Text, DateTime, Numeric, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy import String as SQLString
import uuid

from ..database import Base, engine

# UUID handling: PostgreSQL gets real UUID type, others fallback to String(36)
if engine.dialect.name == "postgresql":
    UUIDType = UUID(as_uuid=True)
else:
    UUIDType = SQLString(36)


class Story(Base):
    __tablename__ = "stories"

    story_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    title = Column(Text, nullable=False)
    meta_title = Column(Text)
    thumbnail_square = Column(Text)
    thumbnail_rect = Column(Text)
    thumbnail_responsive = Column(Text)
    description = Column(Text)
    meta_description = Column(Text)
    genre = Column(Text)
    subgenre = Column(Text)
    rating = Column(Text)
    avg_rating = Column(Numeric(2, 1))
    avg_rating_count = Column(Integer, default=0)
    likes_count = Column(Integer, default=0)
    comments_count = Column(Integer, default=0)
    shares_count = Column(Integer, default=0)
    views_count = Column(Integer, default=0)
    author_json = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        index=True
    )

    # Relationships
    episodes = relationship("Episode", back_populates="story", cascade="all, delete-orphan")
    comments = relationship("Comment", back_populates="story", cascade="all, delete-orphan")
    likes = relationship("Like", back_populates="story", cascade="all, delete-orphan")
    ratings = relationship("Rating", back_populates="story", cascade="all, delete-orphan")
    views = relationship("View", back_populates="story", cascade="all, delete-orphan")
    shares = relationship("Share", back_populates="story", cascade="all, delete-orphan")
    home_continue_watching = relationship("HomeContinueWatching", back_populates="story", cascade="all, delete-orphan")
    home_content_series = relationship("HomeContentSeries", back_populates="story", cascade="all, delete-orphan")
    home_slideshow = relationship("HomeSlideshow", back_populates="story", cascade="all, delete-orphan")
    authors = relationship("StoriesAuthors", back_populates="story", cascade="all, delete-orphan")
