from sqlalchemy import Column, Integer, DateTime, ForeignKey, CheckConstraint, UniqueConstraint
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


class Rating(Base):
    __tablename__ = "ratings"

    rating_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=True)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"), nullable=True)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)

    rating_value = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        CheckConstraint("rating_value BETWEEN 1 AND 5", name="rating_range"),
        CheckConstraint(
            "(story_id IS NOT NULL AND episode_id IS NULL) OR (story_id IS NULL AND episode_id IS NOT NULL)",
            name="one_rating_parent"
        ),
        UniqueConstraint("user_id", "story_id", "episode_id", name="unique_rating"),
    )

    # Relationships
    story = relationship("Story", back_populates="ratings")
    episode = relationship("Episode", back_populates="ratings")
    user = relationship("User", back_populates="ratings")