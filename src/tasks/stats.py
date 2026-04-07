"""Задачи статистики — label: stats.

Исправлено: теперь используем ScrapeChannelUseCase и отправляем результаты в tad-backend.

Задачи:
- scrape_channel — сбор статистики канала через Telethon userbot
- collect_message_stat — статистика одного сообщения (views, reactions)
"""
import aiohttp
from loguru import logger
from taskiq import Context

from src.core.config import settings
from src.core.database import get_readonly_session
from src.entrypoints.broker import broker
from src.shared.schemas.tasks import CollectMessageStatPayload, ScrapeChannelPayload
from src.application.stats.scrape_channel import ScrapeChannelUseCase
from src.infrastructure.database.repositories.platform_repository import SQLAlchemyPlatformRepository
from src.infrastructure.telegram.userbot_pool import UserbotPool


class StatsBackendError(Exception):
    """Ошибка backend'а при отправке статистики — можно retry."""
    pass


@broker.task(
    task_name="worker.scrape_channel",
    retry_on_error=True,
    max_retries=2,
    labels={"queue": "stats"},
)
async def scrape_channel(platform_id: str, priority: bool = False) -> dict:
    """
    Сбор статистики канала через Telethon userbot.
    
    Flow:
    1. Собирает статистику через ScrapeChannelUseCase.
    2. Отправляет результаты в tad-backend через HTTP POST.
    
    Args:
        platform_id: UUID платформы.
        priority: если True — обрабатывается вне очереди (не используется).

    Returns:
        dict с результатом.
        
    Raises:
        StatsBackendError: Ошибка backend'а — retry через Taskiq.
    """
    payload = ScrapeChannelPayload(platform_id=platform_id, priority=priority)

    logger.info(
        f"scrape_channel: начинаем для platform_id={payload.platform_id}"
    )

    # Получаем context и state
    context = Context.get_current()
    state = context.state
    
    async with get_readonly_session() as session:
        try:
            # Создаём репозиторий
            platform_repo = SQLAlchemyPlatformRepository(session)
            
            # Получаем UserbotPool из state
            userbot_pool = getattr(state, "userbot_pool", None)
            if userbot_pool is None:
                # Инициализируем если ещё не создан
                userbot_pool = UserbotPool()
                await userbot_pool.initialize_from_db(session)
                state.userbot_pool = userbot_pool
            
            # Создаём use case
            use_case = ScrapeChannelUseCase(
                platform_repo=platform_repo,
                stats_client=userbot_pool,
            )
            
            # Собираем статистику
            result = await use_case.execute(payload.platform_id)
            
            # Отправляем результаты в tad-backend
            await _send_stats_to_backend(
                endpoint="/api/v1/stats/channel",
                data=result,
            )
            
            logger.info(
                f"scrape_channel: завершено для platform_id={payload.platform_id}, "
                f"{result.get('subscriber_count', 0)} подписчиков"
            )
            return result

        except Exception as exc:
            logger.error(
                f"scrape_channel: ошибка для platform_id={payload.platform_id}: {exc}"
            )
            raise


@broker.task(
    task_name="worker.collect_message_stat",
    retry_on_error=True,
    max_retries=2,
    labels={"queue": "stats"},
)
async def collect_message_stat(
    platform_id: str,
    message_id: int,
    post_id: str,
) -> dict:
    """
    Сбор статистики одного сообщения (views, reactions, comments).

    Вызывается периодически после публикации для трекинга эффективности.

    Args:
        platform_id: UUID платформы.
        message_id: Telegram message_id.
        post_id: UUID поста (для привязки).

    Returns:
        dict со статистикой сообщения.
    """
    payload = CollectMessageStatPayload(
        platform_id=platform_id,
        message_id=message_id,
        post_id=post_id,
    )

    logger.info(
        f"collect_message_stat started: post_id={payload.post_id}, "
        f"message_id={payload.message_id}"
    )

    try:
        # TODO TAD-PHASE3: Подключить CollectMessageStatUseCase
        logger.info(f"collect_message_stat completed: post_id={payload.post_id}")
        return {"status": "stub", "post_id": payload.post_id}

    except Exception as exc:
        logger.error(
            f"collect_message_stat failed: post_id={payload.post_id}, error={exc}"
        )
        raise


async def _send_stats_to_backend(endpoint: str, data: dict) -> None:
    """
    Отправить статистику в tad-backend через HTTP POST.
    
    Args:
        endpoint: Endpoint API (например, "/api/v1/stats/channel").
        data: Данные статистики.
        
    Raises:
        StatsBackendError: Ошибка backend'а — можно retry.
    """
    backend_url = f"{settings.BACKEND_URL}{endpoint}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                backend_url,
                json=data,
                headers={
                    "Authorization": f"Bearer {settings.WORKER_API_TOKEN}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 201, 409):
                    # 200/201 — success, 409 — already exists (idempotency)
                    logger.debug(f"Stats sent to backend: {endpoint}")
                    return
                elif resp.status >= 500:
                    logger.warning(f"Backend error {resp.status}, will retry")
                    raise StatsBackendError(f"Backend error: {resp.status}")
                else:
                    logger.error(f"Unexpected status {resp.status}")
                    raise StatsBackendError(f"Unexpected status: {resp.status}")
                    
    except aiohttp.ClientError as exc:
        logger.warning(f"Network error sending stats: {exc}")
        raise StatsBackendError(f"Network error: {exc}")
