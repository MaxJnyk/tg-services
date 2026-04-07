"""SQLAlchemy TgBot model под реальную схему tad-backend."""
import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID

from src.core.database import Base


class TgBotModel(Base):
    """
    Модель Telegram бота — соответствует таблице tgbots в tad-backend.

    Колонки:
    - id (UUID PK)
    - token (str) — Bot API token
    - name (str) — имя бота
    - is_active (bool) — активен ли бот
    - created_at, updated_at (DateTime)
    """
    __tablename__ = "tgbots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token = Column(String(255), nullable=False)
    name = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
