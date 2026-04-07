"""Leader Election для Scheduler — простой lease через Redis SET NX EX.

НЕ RedLock (у нас один Redis), простой SET NX EX с heartbeat.

Flow:
1. try_become_leader() — пытаемся стать лидером.
2. heartbeat() — продлеваем lease каждые 30 сек.
3. Если lease истёк — другой worker становится лидером.

Ключи Redis:
- scheduler:leader — текущий лидер (worker_id)
- scheduler:leader:heartbeat — timestamp последнего heartbeat
"""
import asyncio
import time

from loguru import logger
from redis.asyncio import Redis

from src.core.config import settings


class LeaderElection:
    """
    Leader election через Redis SET NX EX.
    
    Lease TTL: 60 секунд.
    Heartbeat: каждые 30 секунд.
    """

    LEASE_TTL: int = 60  # секунд
    HEARTBEAT_INTERVAL: int = 30  # секунд
    
    # Redis keys
    LEADER_KEY: str = "scheduler:leader"
    HEARTBEAT_KEY: str = "scheduler:leader:heartbeat"

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._worker_id: str = settings.WORKER_ID
        self._is_leader: bool = False
        self._heartbeat_task: asyncio.Task | None = None

    async def try_become_leader(self) -> bool:
        """
        Попытаться стать лидером.
        
        Returns:
            True если мы стали лидером, False если кто-то другой.
        """
        # Пытаемся установить ключ с NX (только если не существует)
        acquired = await self._redis.set(
            self.LEADER_KEY,
            self._worker_id,
            nx=True,  # Only if not exists
            ex=self.LEASE_TTL,
        )
        
        if acquired:
            self._is_leader = True
            await self._redis.set(self.HEARTBEAT_KEY, str(time.time()))
            logger.info(f"Worker {self._worker_id}: became scheduler leader")
            # Запускаем heartbeat
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            return True
        
        # Ключ уже есть — проверим кто лидер
        current_leader = await self._redis.get(self.LEADER_KEY)
        if current_leader and current_leader.decode() == self._worker_id:
            # Мы уже лидер (что-то пошло не так)
            self._is_leader = True
            return True
        
        logger.debug(f"Worker {self._worker_id}: leader is {current_leader}")
        return False

    async def step_down(self) -> None:
        """Отказаться от лидерства (graceful shutdown)."""
        if not self._is_leader:
            return
        
        # Отменяем heartbeat
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Удаляем ключ только если мы владелец
        current = await self._redis.get(self.LEADER_KEY)
        if current and current.decode() == self._worker_id:
            await self._redis.delete(self.LEADER_KEY)
            await self._redis.delete(self.HEARTBEAT_KEY)
            logger.info(f"Worker {self._worker_id}: stepped down as leader")
        
        self._is_leader = False

    def is_leader(self) -> bool:
        """Проверить, являемся ли мы сейчас лидером."""
        return self._is_leader

    async def _heartbeat_loop(self) -> None:
        """Фоновая задача — продлеваем lease каждые HEARTBEAT_INTERVAL секунд."""
        while self._is_leader:
            try:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                
                if not self._is_leader:
                    break
                
                # Проверяем что мы всё ещё лидер
                current = await self._redis.get(self.LEADER_KEY)
                if not current or current.decode() != self._worker_id:
                    logger.warning(f"Worker {self._worker_id}: lost leadership")
                    self._is_leader = False
                    break
                
                # Обновляем lease
                await self._redis.set(self.LEADER_KEY, self._worker_id, ex=self.LEASE_TTL)
                await self._redis.set(self.HEARTBEAT_KEY, str(time.time()))
                logger.debug(f"Worker {self._worker_id}: heartbeat OK")
                
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Worker {self._worker_id}: heartbeat error: {exc}")
                # Не прерываем loop — попробуем ещё раз
                await asyncio.sleep(5)

    async def get_leader_info(self) -> dict:
        """Получить информацию о текущем лидере."""
        leader = await self._redis.get(self.LEADER_KEY)
        heartbeat = await self._redis.get(self.HEARTBEAT_KEY)
        
        ttl = await self._redis.ttl(self.LEADER_KEY)
        
        return {
            "leader": leader.decode() if leader else None,
            "last_heartbeat": float(heartbeat.decode()) if heartbeat else None,
            "lease_ttl_remaining": ttl,
            "we_are_leader": self._is_leader,
        }
