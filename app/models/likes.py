from sqlalchemy import Column, DateTime, ForeignKey, CheckConstraint, UniqueConstraint, Index
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


class Like(Base):
    __tablename__ = "likes"

    like_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=True)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"), nullable=True)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        CheckConstraint(
            "(story_id IS NOT NULL AND episode_id IS NULL) OR (story_id IS NULL AND episode_id IS NOT NULL)",
            name="one_like_parent"
        ),
        UniqueConstraint("user_id", "story_id", "episode_id", name="unique_like"),
        Index('idx_likes_story_user', story_id, user_id),
        Index('idx_likes_episode_user', episode_id, user_id),
        Index('idx_likes_created_at', created_at),
    )

    # Relationships
    story = relationship("Story", back_populates="likes")
    episode = relationship("Episode", back_populates="likes")
    user = relationship("User", back_populates="likes")