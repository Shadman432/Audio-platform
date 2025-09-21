from sqlalchemy import Column, Text, DateTime, ForeignKey, Interval, Numeric, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy import String as SQLString
import uuid

from ..database import Base, engine

# UUID handling
if engine.dialect.name == 'postgresql':
    UUIDType = UUID(as_uuid=True)
else:
    UUIDType = SQLString(36)


class Episode(Base):
    __tablename__ = "episodes"

    episode_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=False)

    title = Column(Text, nullable=False)
    meta_title = Column(Text)
    thumbnail_square = Column(Text)
    thumbnail_rect = Column(Text)
    thumbnail_responsive = Column(Text)
    description = Column(Text)
    meta_description = Column(Text)
    hls_url = Column(Text, nullable=False)
    duration = Column(Interval)
    release_date = Column(DateTime(timezone=True))
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
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    story = relationship("Story", back_populates="episodes")
    comments = relationship("Comment", back_populates="episode", cascade="all, delete-orphan")
    likes = relationship("Like", back_populates="episode", cascade="all, delete-orphan")
    ratings = relationship("Rating", back_populates="episode", cascade="all, delete-orphan")
    views = relationship("View", back_populates="episode", cascade="all, delete-orphan")
    shares = relationship("Share", back_populates="episode", cascade="all, delete-orphan")
    home_continue_watching = relationship("HomeContinueWatching", back_populates="episode", cascade="all, delete-orphan")
    authors = relationship("EpisodeAuthors", back_populates="episode", cascade="all, delete-orphan")