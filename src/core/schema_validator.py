"""
Runtime schema validation для tad-worker.

Проверяет что таблицы, которые worker читает из БД tad-backend,
имеют ожидаемые колонки. Запускается при старте worker'а.

Поведение:
- Лишние колонки в БД — НЕ ошибка (tad-backend добавил новую, worker не использует)
- Отсутствующие колонки — WARNING или FATAL (зависит от STRICT_SCHEMA_CHECK)

Это НЕ замена Alembic миграциям. Миграции ведутся ТОЛЬКО в tad-backend.
"""
from loguru import logger
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncEngine


# Минимальный набор колонок, которые tad-worker реально использует.
# Если tad-backend добавит новую колонку — worker не сломается.
# Если tad-backend удалит колонку из этого списка — worker заметит.
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "posts": [
        "id",
        "platform_id",
        "post_id_on_platform",
        "current_ad_revision_id",
        "status_id",
    ],
    "platforms": [
        "id",
        "url",
        "name",
        "status",
        "tg_bot_id",
        "remote_id",
    ],
    "tgbots": [
        "id",
        "token",
        "is_active",
    ],
    "revisions": [  # FIXED: было "revision", должно быть "revisions"
        "id",
        "advertisement_id",
        "content",
        "content_type",
        "erid",
    ],
    "post_status": [
        "id",
        "name",
    ],
}


async def validate_schema(engine: AsyncEngine) -> list[str]:
    """
    Проверяет наличие ожидаемых колонок в таблицах БД.

    Использует sqlalchemy.inspect() для получения реальной структуры.

    Returns:
        Список проблем (пустой = всё ок).
        Формат: ["posts.post_id_on_platform: column missing", ...]
    """
    problems: list[str] = []

    async with engine.connect() as conn:
        # inspect() требует sync connection — используем run_sync
        def _inspect(sync_conn) -> list[str]:  # noqa: ANN001
            insp = inspect(sync_conn)
            _problems: list[str] = []

            for table_name, required_cols in REQUIRED_COLUMNS.items():
                # Проверяем что таблица существует
                if not insp.has_table(table_name):
                    _problems.append(f"{table_name}: table missing")
                    continue

                # Получаем реальные колонки
                actual_columns = {col["name"] for col in insp.get_columns(table_name)}

                # Проверяем только ОТСУТСТВУЮЩИЕ колонки
                # Лишние колонки — не проблема (tad-backend может добавлять новые)
                for col in required_cols:
                    if col not in actual_columns:
                        _problems.append(f"{table_name}.{col}: column missing")

            return _problems

        problems = await conn.run_sync(_inspect)

    return problems


async def validate_and_report(engine: AsyncEngine, strict: bool = False) -> bool:
    """
    Запускает валидацию и логирует результаты.

    Args:
        engine: SQLAlchemy async engine.
        strict: если True и есть проблемы — возвращает False (worker должен упасть).

    Returns:
        True если схема валидна (или strict=False).
        False если strict=True и есть проблемы.
    """
    problems = await validate_schema(engine)

    if not problems:
        logger.info("Schema validation passed: all required columns present")
        return True

    for problem in problems:
        logger.warning(f"Schema drift detected: {problem}")

    if strict:
        logger.error(
            f"STRICT_SCHEMA_CHECK=true: {len(problems)} schema problems found. "
            "Worker will not start. Fix schema or set STRICT_SCHEMA_CHECK=false."
        )
        return False

    logger.warning(
        f"Schema validation: {len(problems)} problems found. "
        "Worker continues (STRICT_SCHEMA_CHECK=false). "
        "Set STRICT_SCHEMA_CHECK=true in production."
    )
    return True
