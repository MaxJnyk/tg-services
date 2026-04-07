"""SQLAlchemy Post model под реальную схему tad-backend."""
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Numeric, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from src.core.database import Base


class PostModel(Base):
    """
    Модель поста — соответствует таблице posts в tad-backend.

    Колонки:
    - id (UUID PK)
    - platform_id (UUID FK на platforms)
    - post_id_on_platform (int) — ID сообщения в Telegram после публикации
    - current_ad_revision_id (UUID FK на revision) — текущая ревизия
    - status_id (UUID FK на post_status) — статус поста
    - estimated_cost (Decimal) — оценочная стоимость
    - placement_type_id (UUID FK) — тип размещения
    - created_at, published_at (DateTime)
    """
    __tablename__ = "posts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    platform_id = Column(UUID(as_uuid=True), nullable=False)
    post_id_on_platform = Column(Integer, nullable=True, unique=True)
    current_ad_revision_id = Column(UUID(as_uuid=True), nullable=True)
    status_id = Column(UUID(as_uuid=True), nullable=False)
    estimated_cost = Column(Numeric(20, 8), nullable=True)
    placement_type_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    published_at = Column(DateTime, nullable=True)
