from sqlalchemy import Column, Text, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy import String as SQLString
import uuid

from ..database import Base, engine

# UUID handling
if engine.dialect.name == 'postgresql':
    UUIDType = UUID(as_uuid=True)
else:
    UUIDType = SQLString(36)

class EpisodeAuthors(Base):
    __tablename__ = "episode_authors"

    episode_author_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"))
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE")) # added for easy lookup
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"))
    role = Column(Text)
    contribution_percentage = Column(Integer)

    episode = relationship("Episode", back_populates="authors")
    author = relationship("User")
