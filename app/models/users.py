from sqlalchemy import Column, Text, DateTime
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


class User(Base):
    __tablename__ = "users"

    user_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    email = Column(Text, unique=True, nullable=False)
    full_name = Column(Text)
    role = Column(Text, default='user')
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    comments = relationship("Comment", back_populates="user", cascade="all, delete-orphan")
    likes = relationship("Like", back_populates="user", cascade="all, delete-orphan")
    ratings = relationship("Rating", back_populates="user", cascade="all, delete-orphan")
    views = relationship("View", back_populates="user", cascade="all, delete-orphan")
    shares = relationship("Share", back_populates="user", cascade="all, delete-orphan")
    home_continue_watching = relationship("HomeContinueWatching", back_populates="user", cascade="all, delete-orphan")
    comment_likes = relationship("CommentLike", back_populates="user", cascade="all, delete-orphan")
