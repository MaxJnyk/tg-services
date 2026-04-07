"""
Bot Pool V2 — управление aiogram Bot instances.

Исправлено: теперь использует внешние CircuitBreaker и RateLimiter (Redis-backed).
Это обеспечивает кластерную консистентность при нескольких worker'ах.

Интеграция:
- RateLimiter: двухуровневый rate limit (global + per-chat). Может быть Redis-backed.
- CircuitBreaker: per token, CLOSED → OPEN → HALF_OPEN. Может быть Redis-backed.
- Error taxonomy: classify_error() определяет retry / dlq / rate_limit.
- Eviction: неиспользуемые бот-сессии закрываются через 5 мин.
- Health check: при первом реальном запросе, НЕ get_me() cron.
"""
import time
from typing import Any, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramUnauthorizedError
from loguru import logger

from src.domain.interfaces.infrastructure import (
    MessengerClient,
    MessageResult,
    RetryableError,
    NonRetryableError,
    RateLimitError,
)
from src.infrastructure.telegram.errors import (
    CircuitOpenError,
    ErrorAction,
    classify_error,
    extract_retry_after,
)


# Время простоя бота до закрытия сессии (секунды)
_EVICTION_IDLE_TIMEOUT: float = 300.0


class BotPool(MessengerClient):
    """
    Пул aiogram Bot объектов с rate limiting, circuit breaker и eviction.
    Реализует интерфейс MessengerClient из domain/.

    Lifecycle:
    1. get_bot(token) → lazy create + health check при первом вызове.
    2. send_message(token, chat_id, ...) → rate limit → circuit check → send.
    3. Eviction: бот не использовался 5 мин → close session, удалить из пула.
    4. close() → graceful shutdown всех сессий.
    """

    def __init__(
        self,
        circuit_breaker=None,  # RedisCircuitBreaker или None для in-memory
        rate_limiter=None,     # RedisRateLimiter или None для in-memory
    ) -> None:
        """
        Args:
            circuit_breaker: Внешний circuit breaker (Redis-backed для кластера)
            rate_limiter: Внешний rate limiter (Redis-backed для кластера)
        """
        self._bots: dict[str, Bot] = {}
        self._circuits: dict[str, any] = {}  # Circuit breaker instances
        self._last_used: dict[str, float] = {}
        # Токены, прошедшие health check (первый реальный запрос)
        self._verified: set[str] = set()
        
        # Rate limiter - external (Redis-backed) or will fail
        self._rate_limiter = rate_limiter
        if self._rate_limiter is None:
            raise ValueError("BotPool requires rate_limiter (Redis-backed). In-memory fallback removed.")
        
        # Circuit breaker - external (Redis-backed) or will fail  
        self._circuit_breaker = circuit_breaker
        if self._circuit_breaker is None:
            raise ValueError("BotPool requires circuit_breaker (Redis-backed). In-memory fallback removed.")

    def _get_circuit(self, token: str):
        """Get circuit breaker for token (always external Redis-backed)."""
        return self._circuit_breaker

    async def get_bot(self, token: str) -> Optional[Bot]:
        """
        Получить Bot instance для токена.

        Lazy create: Bot создаётся при первом вызове.
        Circuit check: если circuit OPEN — возвращает None.

        Args:
            token: Telegram Bot API token.

        Returns:
            Bot instance или None если circuit OPEN.

        Raises:
            CircuitOpenError: если circuit breaker в состоянии OPEN.
        """
        circuit = self._get_circuit(token)
        if not circuit.allow_request():
            raise CircuitOpenError(
                token=token,
                recovery_timeout=circuit._recovery_timeout,
            )

        if token not in self._bots:
            self._bots[token] = Bot(token=token)

        self._last_used[token] = time.monotonic()
        return self._bots[token]

    async def send_message(
        self,
        token: str,
        chat_id: str,
        text: str,
        **kwargs,
    ) -> dict:
        """
        Отправить сообщение через бот с полным flow.

        Flow:
        1. Rate limit acquire (global + per-chat).
        2. Circuit breaker check.
        3. Get/create Bot instance.
        4. Send message.
        5. Record success/failure.

        Args:
            token: Bot token.
            chat_id: Telegram chat_id.
            text: Текст сообщения.
            **kwargs: Дополнительные параметры aiogram send_message.

        Returns:
            dict с message_id при успехе.

        Raises:
            CircuitOpenError: circuit breaker OPEN.
            TelegramRetryAfterError: нужно ждать RetryAfter.
            Exception: другие ошибки (retry или DLQ решает вызывающий).
        """
        circuit = self._get_circuit(token)

        # 1. Rate limit
        await self._rate_limiter.acquire(token, chat_id)

        # 2. Circuit check + get bot
        bot = await self.get_bot(token)
        if bot is None:
            raise CircuitOpenError(
                token=token,
                recovery_timeout=circuit._recovery_timeout,
            )

        try:
            # 3. Send
            message = await bot.send_message(
                chat_id=int(chat_id),
                text=text,
                **kwargs,
            )

            # 4. Success
            circuit.record_success()
            self._verified.add(token)
            self._last_used[token] = time.monotonic()

            return {"message_id": message.message_id}

        except TelegramRetryAfter as exc:
            # 429 — записать RetryAfter, поднять наверх
            retry_seconds = extract_retry_after(exc)
            self._rate_limiter.handle_retry_after(token, retry_seconds)
            circuit.record_failure()
            raise

        except TelegramUnauthorizedError:
            # 401 — токен невалиден, circuit OPEN навсегда
            circuit.record_failure()
            # Закрываем бот — токен мёртв
            await self._close_bot(token)
            raise

        except Exception as exc:
            # Классифицируем ошибку
            action = classify_error(exc)
            if action in (ErrorAction.RETRY, ErrorAction.RATE_LIMIT):
                circuit.record_failure()
            else:
                # DLQ — не считаем как circuit failure
                # (BadRequest, NotFound — это проблема данных, не бота)
                pass
            raise

    async def _close_bot(self, token: str) -> None:
        """Закрыть сессию одного бота и удалить из пула."""
        bot = self._bots.pop(token, None)
        if bot:
            try:
                await bot.session.close()
            except Exception as exc:
                logger.warning(
                    f"Error closing bot session: token={token[:10]}..., {exc}"
                )
        self._last_used.pop(token, None)
        self._circuits.pop(token, None)
        self._verified.discard(token)

    async def evict_idle(self) -> int:
        """
        Закрыть бот-сессии, неиспользуемые дольше EVICTION_IDLE_TIMEOUT.

        Вызывается периодически (например, каждые 60 сек из background task).

        Returns:
            Количество закрытых сессий.
        """
        now = time.monotonic()
        to_evict = [
            token
            for token, last in self._last_used.items()
            if (now - last) > _EVICTION_IDLE_TIMEOUT
        ]

        for token in to_evict:
            logger.info(
                f"Evicting idle bot: token={token[:10]}..., "
                f"idle={now - self._last_used[token]:.0f}s"
            )
            await self._close_bot(token)

        return len(to_evict)

    def get_healthy_count(self) -> int:
        """Количество ботов с circuit breaker в состоянии CLOSED."""
        return sum(
            1 for token in self._bots
            if self._get_circuit(token).allow_request()
        )

    def get_total_count(self) -> int:
        """Общее количество ботов в пуле."""
        return len(self._bots)

    async def close(self) -> None:
        """Graceful shutdown: закрыть все bot sessions."""
        tokens = list(self._bots.keys())
        for token in tokens:
            await self._close_bot(token)
        logger.info(f"BotPool closed: {len(tokens)} sessions released")
