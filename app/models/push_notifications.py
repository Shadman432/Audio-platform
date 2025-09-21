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

class PushNotification(Base):
    __tablename__ = "push_notifications"

    push_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"))
    title = Column(Text, nullable=False)
    body = Column(Text, nullable=False)
    deep_link = Column(Text)
    sent_at = Column(DateTime(timezone=True), server_default=func.now())
    opened_at = Column(DateTime(timezone=True))
    is_opened = Column(Boolean, default=False)

    user = relationship("User")
