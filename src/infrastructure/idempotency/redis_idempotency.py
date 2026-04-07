"""
Idempotency через Redis — SET NX EX.

Используется для предотвращения двойного выполнения задач.
Ключ живёт 24 часа — достаточно для retry policy.

Flow:
1. Проверить idempotency_key — если есть, вернуть cached result.
2. Выполнить задачу.
3. Сохранить результат в idempotency_key с TTL.
"""
import json
from typing import Optional, Any

from redis.asyncio import Redis


class RedisIdempotency:
    """
    Idempotency store на Redis.

    Key format: idempotency:{key}
    Value: JSON с результатом и метаданными.
    TTL: 24 часа (86400 сек).
    """

    DEFAULT_TTL: int = 86400  # 24 часа

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def _make_key(self, key: str) -> str:
        """Префикс ключа idempotency."""
        return f"idempotency:{key}"

    async def check(self, key: str) -> Optional[dict]:
        """
        Проверить существование idempotency ключа.

        Args:
            key: Уникальный ключ операции (например, "post:{post_id}:{revision_id}").

        Returns:
            dict с cached результатом или None если ключ не найден.
        """
        data = await self._redis.get(self._make_key(key))
        if data is None:
            return None
        return json.loads(data)

    async def save(
        self,
        key: str,
        result: Any,
        ttl: int = DEFAULT_TTL,
    ) -> bool:
        """
        Сохранить результат операции для idempotency.

        Args:
            key: Уникальный ключ операции.
            result: Результат для кеширования (должен быть JSON serializable).
            ttl: Время жизни в секундах (default: 24 часа).

        Returns:
            True если сохранено успешно.
        """
        payload = {
            "result": result,
            "key": key,
        }
        await self._redis.set(
            self._make_key(key),
            json.dumps(payload),
            ex=ttl,
        )
        return True

    async def delete(self, key: str) -> bool:
        """Удалить idempotency ключ (ручной сброс)."""
        result = await self._redis.delete(self._make_key(key))
        return result > 0

    async def extend_ttl(self, key: str, extra_ttl: int) -> bool:
        """Продлить TTL существующего ключа."""
        return await self._redis.expire(self._make_key(key), extra_ttl)
