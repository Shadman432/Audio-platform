from sqlalchemy import Column, Text, DateTime, ForeignKey, BigInteger
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy import String as SQLString

from ..database import Base, engine

# UUID handling
if engine.dialect.name == 'postgresql':
    UUIDType = UUID(as_uuid=True)
else:
    UUIDType = SQLString(36)

class Engagement(Base):
    __tablename__ = "engagements"

    engagement_id = Column(BigInteger, primary_key=True)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"))
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"))
    type = Column(Text)
    ref_id = Column(UUIDType)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")
    episode = relationship("Episode")
    story = relationship("Story")
