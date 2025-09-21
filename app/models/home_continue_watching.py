from sqlalchemy import Column, Integer, DateTime, ForeignKey, Boolean, UniqueConstraint
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


class HomeContinueWatching(Base):
    __tablename__ = "home_continue_watching"

    continue_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=False)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"), nullable=False)
    progress_seconds = Column(Integer, default=0)
    total_duration = Column(Integer)
    last_watched_at = Column(DateTime(timezone=True), server_default=func.now())
    completed = Column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint("user_id", "episode_id", name="unique_continue"),
    )

    # Relationships
    story = relationship("Story", back_populates="home_continue_watching")
    episode = relationship("Episode", back_populates="home_continue_watching")
    user = relationship("User", back_populates="home_continue_watching")
