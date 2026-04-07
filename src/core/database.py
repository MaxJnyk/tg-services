"""
Database engine и session фабрики для tad-worker.

tad-worker подключается к БД tad-backend.
Миграции (Alembic) ведутся ТОЛЬКО в tad-backend.
tad-worker только читает существующую схему.

Две фабрики сессий:
- get_readonly_session() — для SELECT запросов (CQRS light)
- get_session() — для UPDATE статусов постов, DLQ записей
"""
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

from src.core.config import settings

Base = declarative_base()


def create_engine() -> AsyncEngine:
    """
    Создаёт async engine с настройками из config.

    statement_timeout — SRE requirement: запросы дольше N секунд убиваются.
    Предотвращает long-running queries от блокировки connection pool.
    """
    timeout_ms = str(settings.DB_STATEMENT_TIMEOUT * 1000)
    return create_async_engine(
        settings.DATABASE_URL,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        connect_args={
            "command_timeout": settings.DB_STATEMENT_TIMEOUT,
            "server_settings": {"statement_timeout": timeout_ms},
        },
    )


engine = create_engine()
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def get_readonly_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Read-only сессия для SELECT запросов (CQRS light).

    SET TRANSACTION READ ONLY гарантирует что через эту сессию
    нельзя случайно сделать INSERT/UPDATE/DELETE.
    Используется для: загрузка постов, платформ, revision'ов.
    """
    async with async_session() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        try:
            yield session
        finally:
            await session.rollback()


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Read-write сессия для операций записи.

    Используется для: update post status, set message_id.
    Коммит — ответственность вызывающего кода.
    """
    async with async_session() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
