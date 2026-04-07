"""Абстракции инфраструктуры для domain слоя.

Эти интерфейсы определены в domain/ чтобы application/
не зависел от конкретной инфраструктуры (Redis, Telegram, etc).

Реализации в infrastructure/ имплементируют эти ABC.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Protocol


class LockManager(ABC):
    """Абстракция для distributed locking.
    
    Реализации: RedisLock (infrastructure/locking/)
    """
    
    @abstractmethod
    async def acquire(self, key: str, ttl_seconds: int) -> bool:
        """Попытка захватить лок.
        
        Returns:
            True если лок получен, False если уже занят.
        """
        pass
    
    @abstractmethod
    async def release(self, key: str) -> None:
        """Освобождение лока."""
        pass
    
    @abstractmethod
    async def extend(self, key: str, ttl_seconds: int) -> bool:
        """Продление TTL лока (если задача долгая)."""
        pass


class IdempotencyStore(ABC):
    """Абстракция для хранения результатов идемпотентных операций.
    
    Реализации: RedisIdempotency (infrastructure/idempotency/)
    """
    
    @abstractmethod
    async def get(self, key: str) -> Optional[dict]:
        """Получение сохранённого результата.
        
        Returns:
            Результат операции или None если ключ не найден.
        """
        pass
    
    @abstractmethod
    async def save(self, key: str, result: dict, ttl_seconds: int) -> None:
        """Сохранение результата операции."""
        pass


@dataclass
class MessageResult:
    """Результат отправки сообщения."""
    message_id: int
    chat_id: str
    text: str
    timestamp: float


class MessengerClient(ABC):
    """Абстракция для отправки сообщений (Telegram, etc).
    
    Реализации: BotPool (infrastructure/telegram/)
    """
    
    @abstractmethod
    async def send_message(
        self,
        token: str,
        chat_id: str,
        text: str,
        **kwargs
    ) -> MessageResult:
        """Отправка сообщения.
        
        Raises:
            RetryableError: Временная ошибка (retry возможен).
            NonRetryableError: Фатальная ошибка (retry бессмысленен).
            RateLimitError: Превышен лимит (нужно подождать).
        """
        pass
    
    @abstractmethod
    async def check_health(self, token: str) -> bool:
        """Проверка работоспособности токена."""
        pass


class RetryableError(Exception):
    """Временная ошибка — можно retry."""
    pass


class NonRetryableError(Exception):
    """Фатальная ошибка — retry бессмысленен."""
    pass


class RateLimitError(Exception):
    """Превышен rate limit — нужно подождать."""
    def __init__(self, retry_after_seconds: int):
        self.retry_after = retry_after_seconds
        super().__init__(f"Rate limit exceeded, retry after {retry_after_seconds}s")
