from sqlalchemy import Column, DateTime, ForeignKey, UniqueConstraint
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


class CommentLike(Base):
    __tablename__ = "comment_likes"

    comment_like_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    comment_id = Column(UUIDType, ForeignKey("comments.comment_id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("comment_id", "user_id", name="unique_comment_like"),
    )

    # Relationships
    comment = relationship("Comment", back_populates="comment_likes")
    user = relationship("User", back_populates="comment_likes")
