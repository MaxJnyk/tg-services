"""
Circuit Breaker для Telegram Bot API.

States:
  CLOSED    → нормальная работа, считаем ошибки
  OPEN      → N ошибок за M секунд → блокируем все запросы
  HALF_OPEN → через recovery_timeout пропускаем 1 тестовый запрос

Один CircuitBreaker per bot token.
Не зависит от Redis — in-memory, per worker process.
"""
import enum
import time
from collections import deque

from loguru import logger


class CircuitState(enum.Enum):
    """Состояние circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit breaker для одного bot token.

    Параметры:
    - failure_threshold: кол-во ошибок для перехода CLOSED → OPEN.
    - failure_window: временное окно для подсчёта ошибок (сек).
    - recovery_timeout: время в OPEN до перехода в HALF_OPEN (сек).
    """

    def __init__(
        self,
        token: str,
        failure_threshold: int = 5,
        failure_window: float = 60.0,
        recovery_timeout: float = 60.0,
    ) -> None:
        self._token_short = token[:10]
        self._failure_threshold = failure_threshold
        self._failure_window = failure_window
        self._recovery_timeout = recovery_timeout

        self._state = CircuitState.CLOSED
        # Timestamps ошибок в текущем окне
        self._failures: deque[float] = deque()
        # Когда circuit перешёл в OPEN
        self._opened_at: float = 0.0

    @property
    def state(self) -> CircuitState:
        """Текущее состояние с автоматическим переходом OPEN → HALF_OPEN."""
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self._recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    f"CircuitBreaker HALF_OPEN: token={self._token_short}..., "
                    f"testing with next request"
                )
        return self._state

    def allow_request(self) -> bool:
        """
        Можно ли отправлять запрос?

        Returns:
            True если запрос разрешён, False если circuit OPEN.
        """
        current = self.state
        if current == CircuitState.CLOSED:
            return True
        if current == CircuitState.HALF_OPEN:
            # Пропускаем один тестовый запрос
            return True
        # OPEN — блокируем
        return False

    def record_success(self) -> None:
        """
        Записать успешный запрос.

        HALF_OPEN + success → CLOSED (circuit восстановлен).
        """
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            self._failures.clear()
            logger.info(
                f"CircuitBreaker CLOSED: token={self._token_short}..., "
                f"recovered after successful test request"
            )

    def record_failure(self) -> None:
        """
        Записать ошибку.

        HALF_OPEN + failure → OPEN (не восстановился).
        CLOSED + N failures in window → OPEN.
        """
        now = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            # Тестовый запрос провалился — обратно в OPEN
            self._state = CircuitState.OPEN
            self._opened_at = now
            logger.warning(
                f"CircuitBreaker OPEN (test failed): token={self._token_short}..., "
                f"will retry in {self._recovery_timeout}s"
            )
            return

        # CLOSED — считаем ошибки в окне
        self._failures.append(now)

        # Удаляем старые ошибки за пределами окна
        cutoff = now - self._failure_window
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()

        if len(self._failures) >= self._failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = now
            logger.warning(
                f"CircuitBreaker OPEN: token={self._token_short}..., "
                f"{len(self._failures)} failures in {self._failure_window}s window, "
                f"blocking requests for {self._recovery_timeout}s"
            )

    def reset(self) -> None:
        """Ручной сброс circuit breaker (для admin CLI)."""
        self._state = CircuitState.CLOSED
        self._failures.clear()
        self._opened_at = 0.0
        logger.info(f"CircuitBreaker RESET: token={self._token_short}...")


class CircuitOpenError(Exception):
    """
    Circuit breaker в состоянии OPEN.

    Запрос блокирован — нужно переключиться на другой token
    или подождать recovery_timeout.
    """

    def __init__(self, token: str, recovery_timeout: float) -> None:
        self.token = token
        self.recovery_timeout = recovery_timeout
        super().__init__(
            f"Circuit breaker OPEN for token={token[:10]}..., "
            f"recovery in {recovery_timeout:.0f}s"
        )
