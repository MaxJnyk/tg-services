"""
Задачи постинга — label: posting.

Запуск worker'ов только для постинга:
    taskiq worker src.entrypoints.broker:broker --labels posting

Задачи:
- rotate_post — ротация аукционного поста (основной flow)

Бизнес-логика делегируется use case'ам из application/.
Этот файл — тонкая обёртка: валидация payload → вызов use case → error handling.
"""
from loguru import logger
from taskiq import Context

from src.entrypoints.broker import broker
from src.core.database import get_session
from src.core.redis import get_dlq_redis, get_cache_redis
from src.shared.schemas.tasks import RotatePostPayload
from src.application.posting.rotate_post import RotatePostUseCase
from src.infrastructure.database.repositories.post_repository import SQLAlchemyPostRepository
from src.infrastructure.database.repositories.platform_repository import SQLAlchemyPlatformRepository
from src.infrastructure.locking.redis_lock import RedisLock
from src.infrastructure.idempotency.redis_idempotency import RedisIdempotency


@broker.task(
    task_name="worker.rotate_post",
    retry_on_error=True,
    max_retries=3,
    labels={"queue": "posting"},
)
async def rotate_post(post_id: str) -> dict:
    """
    Ротация аукционного поста.

    1. Валидирует payload через Pydantic.
    2. Делегирует бизнес-логику в RotatePostUseCase.
    3. При ошибке — Taskiq retry + DLQ после max_retries.

    Args:
        post_id: UUID поста из таблицы posts (tad-backend БД).

    Returns:
        dict с результатом: {"status": "published", "message_id": ...}
    """
    # Валидация payload
    payload = RotatePostPayload(post_id=post_id)

    logger.info(f"rotate_post started: post_id={payload.post_id}")

    # Получаем context и state worker'а
    context = Context.get_current()
    state = context.state
    
    async with get_session() as session:
        try:
            # Создаём репозитории
            post_repo = SQLAlchemyPostRepository(session)
            platform_repo = SQLAlchemyPlatformRepository(session)
            
            # Получаем зависимости из state worker'а
            bot_pool = getattr(state, "bot_pool", None)
            if bot_pool is None:
                # Fallback: создаём с дефолтными параметрами
                from src.infrastructure.telegram.bot_pool import BotPool
                bot_pool = BotPool()
            
            # Redis-backed инфраструктура
            dlq_redis = await get_dlq_redis()
            cache_redis = await get_cache_redis()
            
            lock_manager = RedisLock(dlq_redis)
            idempotency_store = RedisIdempotency(cache_redis)
            
            # MessengerClient — BotPool
            messenger_client = bot_pool
            
            # Создаём use case со всеми зависимостями (Dependency Injection)
            use_case = RotatePostUseCase(
                post_repo=post_repo,
                platform_repo=platform_repo,
                lock_manager=lock_manager,
                idempotency_store=idempotency_store,
                messenger_client=messenger_client,
            )
            
            result = await use_case.execute(payload.post_id)
            
            # Проверяем результат
            if result.get("status") == "published":
                logger.info(f"rotate_post published: post_id={payload.post_id}, message_id={result.get('message_id')}")
            elif result.get("status") == "already_processed":
                logger.info(f"rotate_post already processed: post_id={payload.post_id}")
            elif result.get("status") == "already_processing":
                logger.info(f"rotate_post already processing: post_id={payload.post_id}")
            else:
                logger.warning(f"rotate_post unexpected status: {result.get('status')}, post_id={payload.post_id}")
            
            return result

        except Exception as exc:
            logger.exception(f"rotate_post failed: post_id={payload.post_id}, error={exc}")
            # Пробрасываем ошибку для Taskiq retry / DLQ
            raise
