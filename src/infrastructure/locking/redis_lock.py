"""
Distributed lock на Redis SET NX EX.

НЕ RedLock — у нас один Redis, RedLock бессмысленен.
Используется для:
- Предотвращения двойного выполнения одной задачи.
- Idempotency check при rotate_post.

Release через Lua script — атомарная проверка ownership + DEL.
"""
from typing import Optional

from loguru import logger
from redis.asyncio import Redis

from src.core.config import settings

# Lua script для безопасного release:
# Проверяем что lock принадлежит нам (по worker_id), потом DEL.
# Без этого можно удалить чужой lock если наш TTL истёк.
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


class RedisLock:
    """
    Distributed lock через Redis SET NX EX.

    Usage:
        lock = RedisLock(redis, "post:uuid-123")
        if await lock.acquire():
            try:
                # critical section
                ...
            finally:
                await lock.release()

    Или как context manager:
        async with RedisLock(redis, "post:uuid-123") as acquired:
            if acquired:
                ...
    """

    def __init__(
        self,
        redis: Redis,
        key: str,
        ttl: int = 60,
        owner: Optional[str] = None,
    ) -> None:
        """
        Args:
            redis: Redis client (db1 — DLQ + Locks).
            key: Ключ блокировки (без префикса, добавляется автоматически).
            ttl: Время жизни lock в секундах.
            owner: Идентификатор владельца (default: settings.WORKER_ID).
        """
        self._redis = redis
        self._key = f"lock:{key}"
        self._ttl = ttl
        self._owner = owner or settings.WORKER_ID
        self._acquired = False

    async def acquire(self) -> bool:
        """
        Попытаться захватить lock.

        Returns:
            True если lock захвачен, False если уже занят.
        """
        result = await self._redis.set(
            self._key,
            self._owner,
            nx=True,
            ex=self._ttl,
        )
        self._acquired = result is not None
        if self._acquired:
            logger.debug(f"Lock acquired: {self._key} (owner={self._owner}, ttl={self._ttl}s)")
        else:
            logger.debug(f"Lock busy: {self._key}")
        return self._acquired

    async def release(self) -> bool:
        """
        Освободить lock (только если мы владелец).

        Returns:
            True если lock был освобождён, False если мы не владелец.
        """
        if not self._acquired:
            return False

        result = await self._redis.eval(
            _RELEASE_SCRIPT,
            1,
            self._key,
            self._owner,
        )
        released = result == 1
        if released:
            logger.debug(f"Lock released: {self._key}")
        else:
            logger.warning(
                f"Lock release failed (not owner?): {self._key}, owner={self._owner}"
            )
        self._acquired = False
        return released

    async def extend(self, extra_ttl: int = 30) -> bool:
        """
        Продлить TTL lock (только если мы владелец).

        Используется для long-running задач (media download, etc.)

        Args:
            extra_ttl: дополнительное время в секундах.

        Returns:
            True если TTL продлён.
        """
        current_owner = await self._redis.get(self._key)
        if current_owner and current_owner.decode() == self._owner:
            await self._redis.expire(self._key, extra_ttl)
            logger.debug(f"Lock extended: {self._key} (+{extra_ttl}s)")
            return True
        return False

    async def __aenter__(self) -> bool:
        return await self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.release()
