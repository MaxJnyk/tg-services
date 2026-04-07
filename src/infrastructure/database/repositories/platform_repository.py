"""SQLAlchemy реализация PlatformRepository."""
from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.repositories.platform_repository import PlatformRepository
from src.infrastructure.database.models.platform import PlatformModel
from src.infrastructure.database.models.tgbot import TgBotModel


class SQLAlchemyPlatformRepository(PlatformRepository):
    """Репозиторий платформ с реальными SQLAlchemy запросами."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, platform_id: str) -> Optional[dict]:
        """Get platform data by ID with bot token."""
        # Запрашиваем платформу с join на tgbots для получения токена
        query = (
            select(
                PlatformModel.id,
                PlatformModel.name,
                PlatformModel.url,
                PlatformModel.remote_id,
                PlatformModel.status,
                TgBotModel.token.label("bot_token"),
            )
            .outerjoin(TgBotModel, PlatformModel.tg_bot_id == TgBotModel.id)
            .where(PlatformModel.id == platform_id)
        )
        
        result = await self.session.execute(query)
        row = result.mappings().one_or_none()
        
        if not row:
            return None
        
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "url": row["url"],
            "telegram_id": row["remote_id"],  # Telegram chat_id
            "status": row["status"],
            "bot_token": row["bot_token"],
        }

    async def update_stats(self, platform_id: str, stats: dict) -> None:
        """Update platform statistics."""
        # TODO: Реализовать обновление статистики в таблице platforms
        # или создать отдельную таблицу platform_stats
        pass
