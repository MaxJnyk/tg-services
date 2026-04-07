"""
Централизованная конфигурация tad-worker.

Все настройки читаются из переменных окружения (.env файл).
Ни одного hardcoded значения — всё через Settings.
"""
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Конфигурация tad-worker.

    Источник: переменные окружения или .env файл.
    Приоритет: env vars > .env > defaults.
    """

    # --- Database (read-only к БД tad-backend) ---
    DATABASE_URL: str = Field(
        description="PostgreSQL async connection string (asyncpg)",
    )
    DB_POOL_SIZE: int = Field(
        default=5,
        description="SQLAlchemy connection pool size",
    )
    DB_MAX_OVERFLOW: int = Field(
        default=10,
        description="SQLAlchemy max overflow connections",
    )
    DB_STATEMENT_TIMEOUT: int = Field(
        default=30,
        description="SQL statement timeout in seconds (SRE requirement)",
    )

    # --- Redis ---
    REDIS_URL: str = Field(
        default="redis://redis:6379",
        description="Redis base URL (without db number)",
    )
    REDIS_DB_BROKER: int = Field(
        default=0,
        description="Redis DB for Taskiq broker streams (shared with tad-backend)",
    )
    REDIS_DB_DLQ: int = Field(
        default=1,
        description="Redis DB for DLQ (HSET) and distributed locks",
    )
    REDIS_DB_CACHE: int = Field(
        default=2,
        description="Redis DB for rate limiters and cache (volatile, TTL-based)",
    )

    # --- Worker ---
    WORKER_CONCURRENCY: int = Field(
        default=50,
        description="Max concurrent tasks per worker process",
    )
    STRICT_SCHEMA_CHECK: bool = Field(
        default=False,
        description="If True, worker exits on schema mismatch (enable in production)",
    )
    WORKER_ID: str = Field(
        default="worker-1",
        description="Unique worker identifier (for DLQ attempt_history and leader election)",
    )

    # --- Telegram ---
    TELEGRAM_BOT_RATE_LIMIT: int = Field(
        default=30,
        description="Max messages per second per bot token",
    )
    TELEGRAM_USERBOT_RATE_LIMIT: int = Field(
        default=20,
        description="Max requests per minute per userbot account",
    )

    # --- Backend API (HTTP callbacks) ---
    BACKEND_URL: str = Field(
        default="http://tad-backend:8000",
        description="URL tad-backend для HTTP callbacks (billing, stats)",
    )
    WORKER_API_TOKEN: str = Field(
        default="",
        description="API token для аутентификации в tad-backend (пустой = выключено)",
    )

    # --- Alerting (Telegram) ---
    ALERT_TELEGRAM_BOT_TOKEN: str = Field(
        default="",
        description="Telegram bot token для отправки алертов (пустой = выключено)",
    )
    ALERT_TELEGRAM_CHAT_ID: str = Field(
        default="",
        description="Telegram chat ID для алертов (пустой = выключено)",
    )

    # --- Sentry ---
    SENTRY_DSN: str = Field(
        default="",
        description="Sentry DSN for error tracking (empty = disabled)",
    )

    # --- Logging ---
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Log level: DEBUG, INFO, WARNING, ERROR",
    )
    LOG_FORMAT: str = Field(
        default="json",
        description="Log format: json or text (text for local dev)",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


# Singleton — создаётся один раз при импорте.
# Все модули используют этот объект: from src.core.config import settings
settings = Settings()
