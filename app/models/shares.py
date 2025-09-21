from sqlalchemy import Column, DateTime, ForeignKey, CheckConstraint
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


class Share(Base):
    __tablename__ = "shares"

    share_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=True)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"), nullable=True)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        CheckConstraint(
            "(story_id IS NOT NULL AND episode_id IS NULL) OR (story_id IS NULL AND episode_id IS NOT NULL)",
            name="one_share_parent"
        ),
    )

    # Relationships
    story = relationship("Story", back_populates="shares")
    episode = relationship("Episode", back_populates="shares")
    user = relationship("User", back_populates="shares")