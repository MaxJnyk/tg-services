"""SQLAlchemy models for tad-worker.

Read-only models для работы с БД tad-backend.
Миграции ведутся только в tad-backend.
"""
from src.infrastructure.database.models.post import PostModel
from src.infrastructure.database.models.platform import PlatformModel
from src.infrastructure.database.models.tgbot import TgBotModel
from src.infrastructure.database.models.post_status import PostStatusModel
from src.infrastructure.database.models.userbot_account import UserbotAccountModel

__all__ = [
    "PostModel",
    "PlatformModel", 
    "TgBotModel",
    "PostStatusModel",
    "UserbotAccountModel",
]
