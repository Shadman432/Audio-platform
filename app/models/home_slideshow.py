from sqlalchemy import Column, Text, DateTime, ForeignKey, Boolean, Integer, Numeric
from sqlalchemy.dialects.postgresql import UUID
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


class HomeSlideshow(Base):
    __tablename__ = "home_slideshow"

    slideshow_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=False)

    title = Column(Text)
    thumbnail_square = Column(Text)
    thumbnail_rect = Column(Text)
    thumbnail_responsive = Column(Text)
    backdrop_url = Column(Text)
    description = Column(Text)
    genre = Column(Text)
    subgenre = Column(Text)
    rating = Column(Text)
    avg_rating = Column(Numeric(2, 1))
    trailer_url = Column(Text)
    button_text = Column(Text)
    button_link = Column(Text)
    display_order = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    story = relationship("Story", back_populates="home_slideshow")