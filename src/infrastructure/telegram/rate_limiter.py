"""
Двухуровневый rate limiter для Telegram Bot API.

Уровни:
- Global: N msg/sec per bot token (настройка TELEGRAM_BOT_RATE_LIMIT).
- Per-chat: 1 msg/sec per (token, chat_id) — ограничение Telegram.
- RetryAfter: Telegram ответил 429 → мы ждём указанное время.
  RetryAfter ПРИОРИТЕТНЕЕ наших лимитов.

Используется aiolimiter (token bucket) — точнее чем asyncio.Semaphore
для sustained rate limiting.
"""
import time

from aiolimiter import AsyncLimiter
from loguru import logger

from src.core.config import settings


class TelegramRateLimiter:
    """
    Rate limiter для отправки сообщений через Telegram Bot API.

    Гарантирует:
    - Не более TELEGRAM_BOT_RATE_LIMIT msg/sec на один bot token.
    - Не более 1 msg/sec на пару (token, chat_id).
    - Полный стоп при RetryAfter от Telegram (429).
    """

    def __init__(self) -> None:
        # Global limiter per token: N msg/sec
        self._global: dict[str, AsyncLimiter] = {}
        # Per-chat limiter: 1 msg/sec per (token, chat_id)
        self._per_chat: dict[str, AsyncLimiter] = {}
        # RetryAfter: token → timestamp когда можно продолжить
        self._retry_after: dict[str, float] = {}

    def _get_global_limiter(self, token: str) -> AsyncLimiter:
        """Получить или создать global limiter для токена."""
        if token not in self._global:
            self._global[token] = AsyncLimiter(
                max_rate=settings.TELEGRAM_BOT_RATE_LIMIT,
                time_period=1.0,
            )
        return self._global[token]

    def _get_chat_limiter(self, token: str, chat_id: str) -> AsyncLimiter:
        """Получить или создать per-chat limiter для пары (token, chat_id)."""
        key = f"{token[:10]}:{chat_id}"
        if key not in self._per_chat:
            self._per_chat[key] = AsyncLimiter(
                max_rate=1,
                time_period=1.0,
            )
        return self._per_chat[key]

    async def acquire(self, token: str, chat_id: str) -> None:
        """
        Дождаться разрешения на отправку сообщения.

        Порядок проверок:
        1. RetryAfter от Telegram (абсолютный приоритет).
        2. Global rate limit (per token).
        3. Per-chat rate limit (per token+chat).

        Args:
            token: Bot token.
            chat_id: Telegram chat_id (строка).

        Raises:
            TelegramRetryAfterError: если Telegram сказал ждать
                и время ожидания ещё не прошло.
        """
        # 1. RetryAfter — Telegram сказал ждать
        resume_at = self._retry_after.get(token)
        if resume_at is not None:
            remaining = resume_at - time.monotonic()
            if remaining > 0:
                raise TelegramRetryAfterError(
                    token=token,
                    retry_after=remaining,
                )
            else:
                # Время вышло — убираем ограничение
                del self._retry_after[token]

        # 2. Global limit
        global_limiter = self._get_global_limiter(token)
        await global_limiter.acquire()

        # 3. Per-chat limit
        chat_limiter = self._get_chat_limiter(token, chat_id)
        await chat_limiter.acquire()

    def handle_retry_after(self, token: str, seconds: int) -> None:
        """
        Обработать RetryAfter (429) от Telegram.

        Блокирует ВСЕ запросы для данного токена на указанное время.
        Вызывается из BotPool при получении FloodWait/RetryAfter.

        Args:
            token: Bot token.
            seconds: сколько секунд ждать (из ответа Telegram).
        """
        resume_at = time.monotonic() + seconds
        self._retry_after[token] = resume_at
        logger.warning(
            f"RetryAfter received: token={token[:10]}..., "
            f"wait={seconds}s, resume_at={resume_at:.0f}"
        )

    def is_token_blocked(self, token: str) -> bool:
        """Проверить заблокирован ли токен из-за RetryAfter."""
        resume_at = self._retry_after.get(token)
        if resume_at is None:
            return False
        if time.monotonic() >= resume_at:
            del self._retry_after[token]
            return False
        return True

    def cleanup_expired_chat_limiters(self, max_idle: float = 300.0) -> int:
        """
        Удалить per-chat limiters, которые давно не использовались.

        Вызывается периодически для предотвращения memory leak
        при большом количестве уникальных chat_id.

        Args:
            max_idle: максимальное время простоя в секундах (default: 5 мин).

        Returns:
            Количество удалённых limiters.
        """
        # aiolimiter не хранит last_used, поэтому просто чистим все
        # per-chat limiters периодически. Они пересоздаются при следующем acquire.
        count = len(self._per_chat)
        self._per_chat.clear()
        return count


class TelegramRetryAfterError(Exception):
    """
    Telegram вернул 429 RetryAfter.

    Не нужно класть в DLQ — нужно подождать и повторить.
    """

    def __init__(self, token: str, retry_after: float) -> None:
        self.token = token
        self.retry_after = retry_after
        super().__init__(
            f"Telegram RetryAfter: token={token[:10]}..., "
            f"wait={retry_after:.1f}s"
        )
