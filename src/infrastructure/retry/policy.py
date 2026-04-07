"""
Retry Policy для задач с exponential backoff + jitter.

Используется в middleware и задачах для повторных попыток.
"""
import asyncio
import random
from dataclasses import dataclass
from typing import Sequence, Callable, Optional
import enum


class RetryDecision(enum.Enum):
    """Решение retry handler."""
    RETRY = "retry"
    DLQ = "dlq"
    RATE_LIMIT = "rate_limit"


@dataclass(frozen=True)
class RetryPolicy:
    """
    Политика повторных попыток.

    Attributes:
        max_retries: Максимальное количество попыток.
        backoff: Список задержек между попытками в секундах.
        jitter: Доля jitter (0.0-1.0), ±jitter% к backoff.
    """
    max_retries: int = 3
    backoff: Sequence[float] = (1.0, 2.0, 4.0)
    jitter: float = 0.2

    def get_delay(self, attempt: int) -> float:
        """
        Вычислить задержку для попытки с jitter.

        Args:
            attempt: Номер попытки (0-based).

        Returns:
            Задержка в секундах.
        """
        if attempt >= len(self.backoff):
            base = self.backoff[-1] if self.backoff else 1.0
        else:
            base = self.backoff[attempt]

        # Jitter ±jitter%
        jitter_amount = base * self.jitter
        return base + random.uniform(-jitter_amount, jitter_amount)

    async def execute(
        self,
        func: Callable,
        *args,
        should_retry: Optional[Callable[[Exception], RetryDecision]] = None,
        **kwargs,
    ):
        """
        Выполнить функцию с retry policy.

        Args:
            func: Функция для выполнения.
            should_retry: Функция для определения retry/abort/DLQ.
            *args, **kwargs: Аргументы функции.

        Raises:
            Exception: После исчерпания попыток.
        """
        last_exc = None

        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc

                # Определить что делать
                if should_retry:
                    decision = should_retry(exc)
                    if decision == RetryDecision.DLQ:
                        raise  # Сразу в DLQ
                    if decision == RetryDecision.RATE_LIMIT:
                        # Ждём RetryAfter (handled separately)
                        raise

                # Последняя попытка — не ждём
                if attempt == self.max_retries - 1:
                    break

                delay = self.get_delay(attempt)
                await asyncio.sleep(delay)

        raise last_exc
