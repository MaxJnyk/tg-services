"""
Taskiq worker entrypoint с graceful shutdown, DLQ, schema validation и scheduler.

Запуск:
    # Dev (все задачи, 2 worker'а):
    taskiq worker src.entrypoints.broker:broker --workers 2

    # Production (по labels):
    taskiq worker src.entrypoints.broker:broker --labels posting --workers 8
    taskiq worker src.entrypoints.broker:broker --labels billing --workers 2
    taskiq worker src.entrypoints.broker:broker --labels stats --workers 3

Graceful shutdown (SIGTERM):
    1. Перестаём забирать новые задачи (Taskiq делает это автоматически).
    2. Ждём текущие задачи (Taskiq shutdown timeout).
    3. Останавливаем schedulers (если были запущены).
    4. Закрываем BotPool (все bot sessions).
    5. Закрываем Redis connections.
    6. Dispose SQLAlchemy engine.
"""
import asyncio
import signal
import sys

from loguru import logger
from taskiq import TaskiqEvents, TaskiqState

from src.core.config import settings
from src.core.database import engine
from src.core.redis import close_all as close_all_redis, get_dlq_redis, get_cache_redis
from src.core.schema_validator import validate_and_report
from src.entrypoints.broker import broker
from src.entrypoints.health import start_health_server
from src.infrastructure.queue.dlq import DeadLetterQueue
from src.infrastructure.telegram.bot_pool import BotPool
from src.infrastructure.telegram.userbot_pool import UserbotPool
from src.infrastructure.telegram.redis_circuit_breaker import RedisCircuitBreaker
from src.infrastructure.telegram.redis_rate_limiter import RedisRateLimiter
from src.application.scheduler import LeaderElection, PostingScheduler, StatsScheduler

# Explicit import задач — гарантирует регистрацию на broker.
# Без этого --fs-discover может не найти задачи при запуске из Docker.
import src.tasks.posting  # noqa: F401
import src.tasks.stats  # noqa: F401
import src.tasks.billing  # noqa: F401

# Shared state — доступен через state в задачах.
# shutdown_event используется для graceful shutdown background tasks.
shutdown_event = asyncio.Event()


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def on_worker_startup(state: TaskiqState) -> None:
    """
    Инициализация при старте worker'а.

    1. Schema validation — проверяем что БД tad-backend имеет нужные колонки.
    2. DLQ check — предупреждаем если есть failed задачи.
    3. Инициализируем BotPool.
    4. Пытаемся стать лидером scheduler'а.
    5. Запускаем background eviction task.
    6. Регистрируем SIGTERM handler.
    """
    # 1. Schema validation
    schema_ok = await validate_and_report(engine, strict=settings.STRICT_SCHEMA_CHECK)
    if not schema_ok:
        logger.error("Schema validation failed in STRICT mode. Exiting.")
        sys.exit(1)

    # 2. DLQ size check
    dlq_redis = await get_dlq_redis()
    dlq = DeadLetterQueue(dlq_redis)
    dlq_size = await dlq.get_size()
    if dlq_size > 0:
        logger.warning(f"DLQ contains {dlq_size} failed jobs on startup")

    # 3. Redis-backed инфраструктура (кластерная консистентность)
    dlq_redis = await get_dlq_redis()
    cache_redis = await get_cache_redis()
    
    # Circuit Breaker и Rate Limiter — кластер-wide через Redis
    circuit_breaker = RedisCircuitBreaker(cache_redis)
    rate_limiter = RedisRateLimiter(cache_redis)
    
    # Сохраняем в state для использования в задачах
    state.circuit_breaker = circuit_breaker
    state.rate_limiter = rate_limiter
    state.dlq_redis = dlq_redis
    state.cache_redis = cache_redis

    # 4. BotPool с Redis-backed защитой
    bot_pool = BotPool(
        circuit_breaker=circuit_breaker,
        rate_limiter=rate_limiter,
    )
    state.bot_pool = bot_pool

    # 5. UserbotPool — загрузка аккаунтов + warm-up
    # NOTE: UserbotPool требует DB session, создаётся когда задачи stats запускаются.
    # Здесь только сохраняем placeholder. Реальная инициализация — в задачах stats.
    state.userbot_pool = None  # lazy init в tasks/stats.py

    # 6. Leader Election и Schedulers
    state.leader_election = None
    state.posting_scheduler = None
    state.stats_scheduler = None
    
    try:
        # Пытаемся стать лидером
        leader_election = LeaderElection(await get_dlq_redis())
        is_leader = await leader_election.try_become_leader()
        
        if is_leader:
            logger.info(f"Worker {settings.WORKER_ID}: became scheduler leader, starting schedulers")
            state.leader_election = leader_election
            
            # Запускаем posting scheduler
            posting_scheduler = PostingScheduler()
            await posting_scheduler.start()
            state.posting_scheduler = posting_scheduler
            
            # Запускаем stats scheduler
            stats_scheduler = StatsScheduler()
            await stats_scheduler.start()
            state.stats_scheduler = stats_scheduler
        else:
            logger.info(f"Worker {settings.WORKER_ID}: not leader, schedulers not started")
    except Exception as exc:
        logger.error(f"Failed to start schedulers: {exc}")
        # Не падаем — worker всё ещё может обрабатывать задачи

    # 7. Background task: eviction idle bots каждые 60 сек
    eviction_task = asyncio.create_task(_eviction_loop(bot_pool))
    state.eviction_task = eviction_task

    # 8. Запускаем healthcheck сервер на порту 8080
    try:
        health_runner = await start_health_server(
            db_engine=engine,
            redis=await get_cache_redis(),
            bot_pool=bot_pool,
            userbot_pool=None,  # Lazy init
            dlq=dlq,
            port=8080,
        )
        state.health_runner = health_runner
    except Exception as exc:
        logger.error(f"Failed to start health server: {exc}")
        # Не критично — worker может работать без healthcheck

    # 9. SIGTERM → set shutdown_event
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)

    logger.info(
        f"Worker started (id={settings.WORKER_ID}, "
        f"concurrency={settings.WORKER_CONCURRENCY}, "
        f"leader={state.leader_election is not None})"
    )


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def on_worker_shutdown(state: TaskiqState) -> None:
    """
    Graceful shutdown.

    1. Signal background tasks to stop.
    2. Stop schedulers (if running).
    3. Step down from leader election.
    4. Close BotPool (all bot sessions).
    5. Close UserbotPool.
    6. Close Redis connections.
    7. Dispose SQLAlchemy engine.
    """
    logger.info("Worker shutting down...")

    # 1. Stop background tasks
    shutdown_event.set()
    eviction_task = getattr(state, "eviction_task", None)
    if eviction_task and not eviction_task.done():
        eviction_task.cancel()
        try:
            await eviction_task
        except asyncio.CancelledError:
            pass

    # 2. Stop schedulers
    posting_scheduler = getattr(state, "posting_scheduler", None)
    if posting_scheduler:
        await posting_scheduler.stop()
    
    stats_scheduler = getattr(state, "stats_scheduler", None)
    if stats_scheduler:
        await stats_scheduler.stop()

    # 3. Step down from leader election
    leader_election = getattr(state, "leader_election", None)
    if leader_election:
        await leader_election.step_down()

    # 4. Close BotPool
    bot_pool: BotPool | None = getattr(state, "bot_pool", None)
    if bot_pool:
        await bot_pool.close()

    # 5. Close UserbotPool
    userbot_pool: UserbotPool | None = getattr(state, "userbot_pool", None)
    if userbot_pool:
        await userbot_pool.close_all()

    # 6. Close Redis
    await close_all_redis()

    # 7. Stop healthcheck server
    health_runner = getattr(state, "health_runner", None)
    if health_runner:
        try:
            await health_runner.cleanup()
            logger.info("Health server stopped")
        except Exception as exc:
            logger.warning(f"Error stopping health server: {exc}")

    # 8. Dispose DB engine
    await engine.dispose()

    logger.info("Worker shutdown complete")


async def _eviction_loop(bot_pool: BotPool) -> None:
    """Background task: evict idle bot sessions каждые 60 секунд."""
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(60)
            evicted = await bot_pool.evict_idle()
            if evicted > 0:
                logger.info(f"Evicted {evicted} idle bot sessions")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Eviction loop error: {exc}")


def _handle_sigterm() -> None:
    """SIGTERM handler — сигнализирует graceful shutdown."""
    logger.info("SIGTERM received, initiating graceful shutdown...")
    shutdown_event.set()

