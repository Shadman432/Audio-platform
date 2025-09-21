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

class StoriesAuthors(Base):
    __tablename__ = "stories_authors"

    stories_author_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"))
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"))
    role = Column(Text)
    contribution_percentage = Column(Integer)

    story = relationship("Story", back_populates="authors")
    author = relationship("User")
