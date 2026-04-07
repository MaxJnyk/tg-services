"""Posting Scheduler — cron job для поиска и публикации постов.

Flow:
1. Каждые SCHEDULE_INTERVAL (default: 30 сек) проверяем ready посты.
2. FOR UPDATE SKIP LOCKED — берём посты атомарно.
3. Создаём Taskiq задачи rotate_post.kiq() для каждого поста.
4. Jitter: случайная задержка 0-10 сек между задачами.

Recovery:
- При старте: ищем posts.status='posting' старше 10 минут → сбрасываем в 'pending'.
"""
import asyncio
import random
from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_session
from src.entrypoints.broker import broker
from src.infrastructure.database.models.post import PostModel
from src.infrastructure.database.models.post_status import PostStatusModel


class PostingScheduler:
    """
    Scheduler для posting задач.
    
    Запускается только на leader worker'е.
    """

    # Интервал проверки (секунды)
    SCHEDULE_INTERVAL: int = 30
    # Jitter для старта задач (секунды)
    JITTER_MAX: int = 10
    # Считаем пост "зависшим" после этого времени (минуты)
    STUCK_POST_TIMEOUT: int = 10
    # Batch size для обработки
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
        
        # Recovery при старте
        await self._recover_stuck_posts()
        
        logger.info("PostingScheduler started")

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
        
        logger.info("PostingScheduler stopped")

    async def _run_loop(self) -> None:
        """Основной цикл scheduler'а."""
        while self._running:
            try:
                await self._schedule_ready_posts()
            except Exception as exc:
                logger.exception(f"PostingScheduler error: {exc}")
            
            try:
                await asyncio.sleep(self.SCHEDULE_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _schedule_ready_posts(self) -> int:
        """
        Найти и запланировать ready посты.
        
        Returns:
            Количество запланированных постов.
        """
        async with get_session() as session:
            # Получаем ID статуса 'pending'
            pending_status_id = await self._get_status_id(session, "pending")
            
            # Находим посты ready для публикации
            # TODO: Добавить логику next_post_at <= NOW()
            query = (
                select(PostModel)
                .where(
                    PostModel.status_id == pending_status_id,
                    # TODO: PostModel.next_post_at <= datetime.now(timezone.utc)
                )
                .limit(self.BATCH_SIZE)
                .order_by(PostModel.created_at)
            )
            
            result = await session.execute(query)
            posts = result.scalars().all()
            
            if not posts:
                return 0
            
            scheduled = 0
            for post in posts:
                try:
                    # Jitter — случайная задержка
                    jitter = random.uniform(0, self.JITTER_MAX)
                    
                    # Создаём задачу через Taskiq
                    task = broker.task_by_name("worker.rotate_post")
                    if task:
                        await task.kiq(post_id=str(post.id))
                        scheduled += 1
                        logger.info(
                            f"Scheduled rotate_post: post_id={post.id}, "
                            f"jitter={jitter:.1f}s"
                        )
                        
                        # Ждём jitter перед следующей задачей
                        await asyncio.sleep(jitter)
                    else:
                        logger.error("worker.rotate_post task not found!")
                        
                except Exception as exc:
                    logger.error(f"Failed to schedule post {post.id}: {exc}")
            
            return scheduled

    async def _recover_stuck_posts(self) -> int:
        """
        Recovery: сбросить "зависшие" посты из 'posting' в 'pending'.
        
        Returns:
            Количество восстановленных постов.
        """
        async with get_session() as session:
            try:
                # Получаем ID статусов
                posting_status_id = await self._get_status_id(session, "posting")
                pending_status_id = await self._get_status_id(session, "pending")
                
                # Ищем посты в статусе 'posting' старше STUCK_POST_TIMEOUT
                cutoff = datetime.now(timezone.utc) - timedelta(
                    minutes=self.STUCK_POST_TIMEOUT
                )
                
                query = (
                    select(PostModel)
                    .where(
                        PostModel.status_id == posting_status_id,
                        PostModel.updated_at < cutoff,
                    )
                )
                
                result = await session.execute(query)
                stuck_posts = result.scalars().all()
                
                if not stuck_posts:
                    return 0
                
                # Сбрасываем в 'pending'
                for post in stuck_posts:
                    post.status_id = pending_status_id
                    logger.warning(
                        f"Recovered stuck post: {post.id} "
                        f"(was 'posting' for >{self.STUCK_POST_TIMEOUT}min)"
                    )
                
                await session.commit()
                
                logger.info(f"Recovered {len(stuck_posts)} stuck posts")
                return len(stuck_posts)
                
            except Exception as exc:
                logger.error(f"Failed to recover stuck posts: {exc}")
                await session.rollback()
                return 0

    async def _get_status_id(self, session: AsyncSession, name: str) -> str:
        """Получить UUID статуса по имени."""
        result = await session.execute(
            select(PostStatusModel.id).where(PostStatusModel.name == name)
        )
        row = result.scalar_one_or_none()
        if not row:
            raise ValueError(f"Post status '{name}' not found")
        return str(row)
