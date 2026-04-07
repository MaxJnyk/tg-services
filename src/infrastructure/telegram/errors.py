"""
Классификация ошибок Telegram Bot API.

Три категории:
- retry: временная ошибка, повторить через backoff.
- rate_limit: RetryAfter от Telegram, ждать указанное время.
- dlq: невосстановимая ошибка, класть в Dead Letter Queue.

Используется в BotPool и tasks для принятия решения
о retry/DLQ/rate_limit при ошибке.
"""
import enum

from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)


class ErrorAction(enum.Enum):
    """Действие при ошибке."""

    RETRY = "retry"
    RATE_LIMIT = "rate_limit"
    DLQ = "dlq"


class CircuitOpenError(Exception):
    """Circuit Breaker открыт — запросы к этому токену заблокированы.
    
    Args:
        token: Telegram bot token (masked).
        recovery_timeout: Секунд до автоматического восстановления.
    """
    
    def __init__(self, token: str, recovery_timeout: int) -> None:
        self.token = token
        self.recovery_timeout = recovery_timeout
        super().__init__(
            f"Circuit OPEN for token {token[:10]}... "
            f"Try again in {recovery_timeout}s"
        )


# Ошибки, при которых стоит повторить запрос
RETRYABLE_ERRORS = (
    TelegramServerError,       # 5xx от Telegram
    TelegramNetworkError,      # Сетевые проблемы
    TimeoutError,              # asyncio timeout
    ConnectionError,           # Разрыв соединения
    OSError,                   # Low-level сетевые ошибки
)

# Ошибки, при которых повторять бесполезно — сразу в DLQ
NON_RETRYABLE_ERRORS = (
    TelegramUnauthorizedError,  # 401 — токен невалиден
    TelegramForbiddenError,     # 403 — бот удалён из канала / заблокирован
    TelegramNotFound,           # 404 — чат не найден
    TelegramBadRequest,         # 400 — невалидные данные (message too long, etc.)
)


def classify_error(exc: Exception) -> ErrorAction:
    """
    Классифицировать ошибку Telegram API.

    Args:
        exc: пойманное исключение.

    Returns:
        ErrorAction — что делать: retry, rate_limit или dlq.
    """
    # RetryAfter (429) — специальный случай: не retry, а ЖДАТЬ
    if isinstance(exc, TelegramRetryAfter):
        return ErrorAction.RATE_LIMIT

    # Retryable — повторить через backoff
    if isinstance(exc, RETRYABLE_ERRORS):
        return ErrorAction.RETRY

    # Non-retryable — сразу в DLQ
    if isinstance(exc, NON_RETRYABLE_ERRORS):
        return ErrorAction.DLQ

    # Неизвестная ошибка — по умолчанию retry
    # (лучше повторить и разобраться, чем сразу потерять задачу)
    return ErrorAction.RETRY


def extract_retry_after(exc: TelegramRetryAfter) -> int:
    """
    Извлечь количество секунд ожидания из TelegramRetryAfter.

    Args:
        exc: TelegramRetryAfter исключение.

    Returns:
        Количество секунд (минимум 1).
    """
    return max(1, int(exc.retry_after))
