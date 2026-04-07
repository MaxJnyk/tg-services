"""Абстракции для сбора статистики (domain слой).

Реализации:
- UserbotStatsClient (infrastructure/telegram/userbot_pool.py)
  Использует Telethon для сбора статистики через userbot аккаунты.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ChannelStats:
    """Статистика канала."""
    platform_id: str
    subscriber_count: int
    avg_views: float
    avg_reactions: float
    messages_per_day: float
    last_post_date: Optional[str]
    collected_at: float


@dataclass
class MessageStats:
    """Статистика одного сообщения."""
    message_id: int
    views: int
    reactions: int
    replies: int
    collected_at: float


class StatsClient(ABC):
    """Абстракция для сбора статистики (Telegram userbot).
    
    Реализации: UserbotStatsClient (infrastructure/telegram/)
    """
    
    @abstractmethod
    async def scrape_channel(self, channel_id: str) -> ChannelStats:
        """Собрать статистику канала.
        
        Args:
            channel_id: Telegram ID канала (например, '@channelname' или '-1001234567890').
            
        Returns:
            ChannelStats с метриками канала.
            
        Raises:
            RetryableError: Временная ошибка (FloodWait, network).
            NonRetryableError: Фатальная ошибка (канал не найден, бан).
        """
        pass
    
    @abstractmethod
    async def get_message_stats(self, channel_id: str, message_id: int) -> MessageStats:
        """Получить статистику одного сообщения.
        
        Args:
            channel_id: Telegram ID канала.
            message_id: ID сообщения в Telegram.
            
        Returns:
            MessageStats с метриками сообщения.
        """
        pass
    
    @abstractmethod
    async def get_active_accounts_count(self) -> int:
        """Количество активных userbot аккаунтов в пуле.
        
        Returns:
            Количество аккаунтов в статусе 'active'.
        """
        pass
