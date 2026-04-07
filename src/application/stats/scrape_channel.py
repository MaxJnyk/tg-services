"""Use case для сбора статистики канала.

Использует StatsClient (Telethon userbot) для сбора данных.
Отправляет результаты в tad-backend через HTTP callback.
"""
from loguru import logger

from src.domain.interfaces.infrastructure import RetryableError, NonRetryableError
from src.domain.interfaces.stats import StatsClient, ChannelStats
from src.domain.repositories.platform_repository import PlatformRepository


class ScrapeChannelUseCase:
    """
    Use case для сбора статистики Telegram канала.
    
    Flow:
    1. Получает platform из БД (telegram_id канала).
    2. Использует StatsClient (Telethon userbot) для сбора статистики.
    3. Отправляет результаты в tad-backend через HTTP callback.
    
    Зависимости:
    - platform_repo: репозиторий платформ
    - stats_client: клиент для сбора статистики (Telethon userbot)
    """
    
    def __init__(
        self,
        platform_repo: PlatformRepository,
        stats_client: StatsClient,
    ) -> None:
        """
        Args:
            platform_repo: Репозиторий платформ (для получения telegram_id).
            stats_client: Клиент для сбора статистики (UserbotStatsClient).
        """
        self._platform_repo = platform_repo
        self._stats_client = stats_client
    
    async def execute(self, platform_id: str) -> dict:
        """
        Собрать статистику канала и отправить в backend.
        
        Args:
            platform_id: UUID платформы в БД.
            
        Returns:
            dict с результатом:
            - {"status": "collected", "subscriber_count": ..., "avg_views": ...}
            
        Raises:
            RetryableError: Временная ошибка (FloodWait, network).
            NonRetryableError: Фатальная ошибка (канал не найден).
        """
        logger.info(f"ScrapeChannel: начинаем для platform_id={platform_id}")
        
        # 1. Получаем платформу из БД
        platform = await self._platform_repo.get(platform_id)
        if not platform:
            raise NonRetryableError(f"Platform {platform_id} not found")
        
        channel_id = platform.get("telegram_id") or platform.get("url")
        if not channel_id:
            raise NonRetryableError(f"Platform {platform_id}: no telegram_id or url")
        
        # 2. Проверяем наличие активных userbot аккаунтов
        active_accounts = await self._stats_client.get_active_accounts_count()
        if active_accounts == 0:
            logger.warning(f"ScrapeChannel: нет активных userbot аккаунтов")
            raise RetryableError("No active userbot accounts available")
        
        logger.info(
            f"ScrapeChannel: собираем статистику для {channel_id} "
            f"({active_accounts} активных аккаунтов)"
        )
        
        # 3. Собираем статистику через Telethon
        try:
            stats: ChannelStats = await self._stats_client.scrape_channel(channel_id)
        except RetryableError:
            # Пробрасываем для retry
            raise
        except Exception as exc:
            logger.exception(f"ScrapeChannel: ошибка сбора статистики: {exc}")
            raise RetryableError(f"Stats collection failed: {exc}")
        
        logger.info(
            f"ScrapeChannel: собрано для {channel_id}: "
            f"{stats.subscriber_count} подписчиков, "
            f"{stats.avg_views:.1f} avg views"
        )
        
        # 4. Отправляем результаты в tad-backend через HTTP callback
        # NOTE: Реальная отправка в backend делается в task,
        # здесь только возвращаем данные для task.
        return {
            "status": "collected",
            "platform_id": platform_id,
            "channel_id": channel_id,
            "subscriber_count": stats.subscriber_count,
            "avg_views": stats.avg_views,
            "avg_reactions": stats.avg_reactions,
            "messages_per_day": stats.messages_per_day,
            "last_post_date": stats.last_post_date,
            "collected_at": stats.collected_at,
        }
