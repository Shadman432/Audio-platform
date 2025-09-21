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

class UserDevice(Base):
    __tablename__ = "user_devices"

    device_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    user_id = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    device_token = Column(Text, nullable=False)
    device_type = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    last_active_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")
