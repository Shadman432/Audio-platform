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


class HomeContent(Base):
    __tablename__ = "home_content"

    category_id = Column(UUIDType, primary_key=True, default=uuid.uuid4)
    category_name = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    series = relationship("HomeContentSeries", back_populates="category", cascade="all, delete-orphan")