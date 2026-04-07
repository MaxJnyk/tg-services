"""SQLAlchemy Platform model под реальную схему tad-backend."""
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from src.core.database import Base


class PlatformModel(Base):
    """
    Модель платформы — соответствует таблице platforms в tad-backend.

    Колонки:
    - id (UUID PK)
    - name (str) — название канала/платформы
    - url (str) — ссылка на канал
    - remote_id (str) — Telegram ID канала (например, -1001234567890)
    - status (str) — статус платформы (active, inactive, etc.)
    - tg_bot_id (UUID FK на tgbots) — бот для публикации
    - owner_id (UUID) — владелец платформы
    - is_active (bool) — активна ли платформа
    - created_at, updated_at (DateTime)
    """
    __tablename__ = "platforms"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    url = Column(String(500), nullable=False)
    remote_id = Column(String(100), nullable=True)  # Telegram chat_id
    status = Column(String(50), nullable=False, default="active")
    tg_bot_id = Column(UUID(as_uuid=True), ForeignKey("tgbots.id"), nullable=True)
    owner_id = Column(UUID(as_uuid=True), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
