from sqlalchemy import Column, Text, DateTime, ForeignKey, CheckConstraint, Integer, Index, Boolean, Computed
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


class Comment(Base):
    __tablename__ = "comments"

    comment_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    story_id = Column(UUIDType, ForeignKey("stories.story_id", ondelete="CASCADE"), nullable=True)
    episode_id = Column(UUIDType, ForeignKey("episodes.episode_id", ondelete="CASCADE"), nullable=True)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    parent_comment_id = Column(UUIDType, ForeignKey("comments.comment_id", ondelete="CASCADE"))

    comment_text = Column(Text, nullable=False)
    is_reply = Column(Boolean, Computed("parent_comment_id IS NOT NULL"))
    comment_like_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0, nullable=False)
    is_edited = Column(Boolean, default=False, nullable=False)
    is_pinned = Column(Boolean, default=False)
    is_visible = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint(
            "(story_id IS NOT NULL AND episode_id IS NULL) OR (story_id IS NULL AND episode_id IS NOT NULL)",
            name="one_parent"
        ),
        CheckConstraint("comment_like_count >= 0", name="comment_like_count_non_negative"),
        Index('idx_comments_story_id_created_at', 'story_id', func.desc('created_at')),
        Index('idx_comments_comment_like_count_desc', func.desc('comment_like_count')),
        Index('idx_comments_episode_id', 'episode_id'),
        Index('idx_comments_parent_comment_id', 'parent_comment_id'),
    )

    # Relationships
    story = relationship("Story", back_populates="comments")
    episode = relationship("Episode", back_populates="comments")
    user = relationship("User", back_populates="comments")
    parent = relationship("Comment", remote_side=[comment_id], backref="replies")
    comment_likes = relationship("CommentLike", back_populates="comment", cascade="all, delete-orphan")