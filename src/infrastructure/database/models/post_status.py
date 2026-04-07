"""SQLAlchemy PostStatus model под реальную схему tad-backend."""
import uuid
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID

from src.core.database import Base


class PostStatusModel(Base):
    """
    Модель статуса поста — соответствует таблице post_status в tad-backend.

    Колонки:
    - id (UUID PK)
    - name (str) — название статуса (pending, posting, published, etc.)
    """
    __tablename__ = "post_status"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(50), nullable=False, unique=True)
