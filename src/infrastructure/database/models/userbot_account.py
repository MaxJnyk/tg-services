"""SQLAlchemy UserbotAccount model с device fingerprint."""
import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, JSON, Boolean
from sqlalchemy.dialects.postgresql import UUID

from src.core.database import Base


class UserbotAccount(Base):
    """Модель userbot аккаунта с device fingerprint для anti-detect."""
    
    __tablename__ = "userbot_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id = Column(String(100), nullable=False, unique=True)
    phone = Column(String(20), nullable=True)
    session_string = Column(String, nullable=False)
    
    # Device fingerprint для anti-detect (Telegram anti-ban)
    device_json = Column(JSON, nullable=False)
    # Proxy config: {"type": "socks5", "host": "...", "port": 1080, ...}
    proxy_json = Column(JSON, nullable=True)
    
    # Статус аккаунта
    status = Column(String(20), default="cold")  # cold, warm, active, banned, archived
    is_active = Column(Boolean, default=True)
    
    # Метаданные
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_used_at = Column(DateTime, nullable=True)
    
    def __repr__(self) -> str:
        return f"<UserbotAccount {self.account_id} status={self.status}>"
