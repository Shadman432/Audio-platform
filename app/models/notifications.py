from sqlalchemy import Column, Text, DateTime, ForeignKey, Boolean
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

class Notification(Base):
    __tablename__ = "notifications"

    notification_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    type = Column(Text, nullable=False)
    ref_id = Column(UUIDType)
    message = Column(Text)
    is_read = Column(Boolean, default=False)
    is_clicked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    read_at = Column(DateTime(timezone=True))
    clicked_at = Column(DateTime(timezone=True))

    user = relationship("User")
