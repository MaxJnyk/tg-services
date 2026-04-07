"""Stats Scheduler — cron job для сбора статистики.

Flow:
1. Каждый час (CRON_INTERVAL) — собираем статистику всех активных платформ.
2. Round-robin по userbot аккаунтам.
3. Jitter 0-5 минут между задачами (избежать burst).

Tasks:
- scrape_channel — полный сбор статистики канала.
- collect_message_stat — обновление статистики сообщений.
"""
import asyncio
import random
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_readonly_session
from src.entrypoints.broker import broker
from src.infrastructure.database.models.platform import PlatformModel


class StatsScheduler:
    """
    Scheduler для stats задач.
    
    Запускается только на leader worker'е.
    """

    # Интервал сбора статистики (секунды) — 1 час
    CRON_INTERVAL: int = 3600
    # Jitter между задачами (секунды) — до 5 минут
    JITTER_MAX: int = 300
    # Batch size
    BATCH_SIZE: int = 100

    def __init__(self) -> None:
        self._running: bool = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Запустить scheduler."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("StatsScheduler started")

    async def stop(self) -> None:
        """Остановить scheduler."""
        if not self._running:
            return
        
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info("StatsScheduler stopped")

    async def _run_loop(self) -> None:
        """Основной цикл scheduler'а."""
        # Первый запуск — сразу
        await self._schedule_stats_collection()
        
        while self._running:
            try:
                await asyncio.sleep(self.CRON_INTERVAL)
                
                if not self._running:
                    break
                
                await self._schedule_stats_collection()
                
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception(f"StatsScheduler error: {exc}")
                await asyncio.sleep(60)  # Retry через минуту

    async def _schedule_stats_collection(self) -> int:
        """
        Запланировать сбор статистики для всех активных платформ.
        
        Returns:
            Количество запланированных задач.
        """
        async with get_readonly_session() as session:
            # Находим все активные платформы
            # TODO: Добавить фильтр status='active'
            query = (
                select(PlatformModel)
                .limit(self.BATCH_SIZE)
            )
            
            result = await session.execute(query)
            platforms = result.scalars().all()
            
            if not platforms:
                logger.debug("No platforms found for stats collection")
                return 0
            
            scheduled = 0
            for platform in platforms:
                try:
                    # Jitter — случайная задержка до 5 минут
                    jitter = random.uniform(0, self.JITTER_MAX)
                    
                    # Создаём задачу
                    task = broker.task_by_name("worker.scrape_channel")
                    if task:
                        await task.kiq(
                            platform_id=str(platform.id),
                            priority=False,
                        )
                        scheduled += 1
                        logger.info(
                            f"Scheduled scrape_channel: platform_id={platform.id}, "
                            f"jitter={jitter:.1f}s"
                        )
                        
                        # Ждём jitter
                        await asyncio.sleep(jitter)
                    else:
                        logger.error("worker.scrape_channel task not found!")
                        
                except Exception as exc:
                    logger.error(f"Failed to schedule stats for {platform.id}: {exc}")
            
            logger.info(f"StatsScheduler: scheduled {scheduled} tasks for {len(platforms)} platforms")
            return scheduled

    async def schedule_priority_scrape(self, platform_id: str) -> None:
        """
        Запланировать приоритетный сбор статистики (вне очереди).
        
        Args:
            platform_id: UUID платформы.
        """
        try:
            task = broker.task_by_name("worker.scrape_channel")
            if task:
                await task.kiq(platform_id=platform_id, priority=True)
                logger.info(f"Scheduled priority scrape: platform_id={platform_id}")
            else:
                logger.error("worker.scrape_channel task not found!")
        except Exception as exc:
            logger.error(f"Failed to schedule priority scrape for {platform_id}: {exc}")
