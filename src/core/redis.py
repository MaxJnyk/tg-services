"""
Redis клиенты для tad-worker.

Три отдельные базы данных — изоляция по назначению:
- db0 (broker): Taskiq streams, shared с tad-backend. Persistent (AOF).
- db1 (dlq): Dead Letter Queue (HSET) + distributed locks (SET NX). Persistent (AOF).
- db2 (cache): Rate limiters + временный кэш. Volatile (TTL-based, можно потерять).

Каждая база — свой connection pool. Это предотвращает contention
между Taskiq streams и DLQ/lock операциями.
"""
from redis.asyncio import Redis
from loguru import logger

from src.core.config import settings


# Singletons per database. Ленивая инициализация — создаются при первом вызове.
_broker_redis: Redis | None = None
_dlq_redis: Redis | None = None
_cache_redis: Redis | None = None


def _build_url(db: int) -> str:
    """Собирает Redis URL с номером базы данных."""
    return f"{settings.REDIS_URL}/{db}"


async def get_broker_redis() -> Redis:
    """
    Redis db0 — Taskiq broker streams.

    Shared с tad-backend. tad-backend пишет задачи, tad-worker читает.
    Persistent (AOF) — потеря данных = потеря задач в очереди.
    """
    global _broker_redis
    if _broker_redis is None:
        _broker_redis = Redis.from_url(
            _build_url(settings.REDIS_DB_BROKER),
            decode_responses=True,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
        )
        logger.debug(f"Redis broker connected: db={settings.REDIS_DB_BROKER}")
    return _broker_redis


async def get_dlq_redis() -> Redis:
    """
    Redis db1 — DLQ (HSET + Sorted Set) и distributed locks (SET NX EX).

    Persistent (AOF) — потеря DLQ = потеря информации о failed задачах.
    Locks тоже здесь — при потере Redis locks автоматически expire по TTL.
    """
    global _dlq_redis
    if _dlq_redis is None:
        _dlq_redis = Redis.from_url(
            _build_url(settings.REDIS_DB_DLQ),
            decode_responses=True,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
        )
        logger.debug(f"Redis DLQ connected: db={settings.REDIS_DB_DLQ}")
    return _dlq_redis


async def get_cache_redis() -> Redis:
    """
    Redis db2 — rate limiters и кэш.

    Volatile — все ключи с TTL. При перезагрузке Redis:
    - Rate limiters сбросятся (допустимо — просто обнулятся счётчики)
    - Кэш потеряется (допустимо — перезагрузится при следующем запросе)
    """
    global _cache_redis
    if _cache_redis is None:
        _cache_redis = Redis.from_url(
            _build_url(settings.REDIS_DB_CACHE),
            decode_responses=True,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
        )
        logger.debug(f"Redis cache connected: db={settings.REDIS_DB_CACHE}")
    return _cache_redis


async def close_all() -> None:
    """Graceful shutdown: закрыть все Redis connections."""
    global _broker_redis, _dlq_redis, _cache_redis

    for name, client in [("broker", _broker_redis), ("dlq", _dlq_redis), ("cache", _cache_redis)]:
        if client is not None:
            await client.close()
            logger.debug(f"Redis {name} connection closed")

    _broker_redis = None
    _dlq_redis = None
    _cache_redis = None
