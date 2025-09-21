from sqlalchemy import Column, Text, DateTime, ForeignKey
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

class Report(Base):
    __tablename__ = "reports"

    report_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    type = Column(Text, nullable=False)
    ref_id = Column(UUIDType, nullable=False)
    reported_by = Column(UUIDType, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    reason = Column(Text, nullable=False)
    status = Column(Text, default="pending")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    reporter = relationship("User")
