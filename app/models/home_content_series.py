from sqlalchemy import Column, Text, DateTime, ForeignKey, Numeric
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


class HomeContentSeries(Base):
    __tablename__ = "home_content_series"

    content_series_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    category_id = Column(UUIDType, ForeignKey("home_content.category_id", ondelete="CASCADE"), nullable=False)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=False)

    title = Column(Text, nullable=False)
    thumbnail_square = Column(Text)
    thumbnail_rect = Column(Text)
    thumbnail_responsive = Column(Text)
    description = Column(Text)
    genre = Column(Text)
    subgenre = Column(Text)
    rating = Column(Text)
    avg_rating = Column(Numeric(2, 1))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    category = relationship("HomeContent", back_populates="series")
    story = relationship("Story", back_populates="home_content_series")