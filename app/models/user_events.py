from sqlalchemy import Column, Text, DateTime, ForeignKey, BigInteger, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB, INET
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy import String as SQLString

from ..database import Base, engine

# UUID handling
if engine.dialect.name == 'postgresql':
    UUIDType = UUID(as_uuid=True)
else:
    UUIDType = SQLString(36)

class UserEvent(Base):
    __tablename__ = "user_events"

    event_id = Column(BigInteger, primary_key=True)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"))
    session_id = Column(UUIDType)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="SET NULL"))
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="SET NULL"))
    group_id = Column(UUIDType)
    genre_id = Column(UUIDType)
    subgenre_id = Column(UUIDType)
    category_id = Column(UUIDType)
    level = Column(Integer, nullable=False)
    event_type = Column(Text, nullable=False)
    event_metadata = Column(JSONB) # Renamed from 'metadata'
    device = Column(Text)
    platform = Column(Text)
    ip_address = Column(INET)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")
    story = relationship("Story")
    episode = relationship("Episode")
