"""Taskiq middleware для автоматической отправки failed задач в DLQ.

Интеграция:
    broker.add_middlewares(DLQMiddleware())

Как работает:
1. Taskiq вызывает post_execute() после каждой задачи.
2. Если задача упала (result.is_err) и retry исчерпаны — кладём в DLQ.
3. attempt_history записывает каждую попытку с worker_id и error.

Важно: middleware НЕ содержит бизнес-логику. Только маршрутизация ошибок.
"""
from loguru import logger
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from src.core.config import settings
from src.core.redis import get_dlq_redis
from src.infrastructure.queue.dlq import DeadLetterQueue


class DLQMiddleware(TaskiqMiddleware):
    """
    Middleware для маршрутизации failed задач в Dead Letter Queue.

    Перехватывает ошибки после исчерпания retry'ев Taskiq
    и сохраняет задачу в DLQ с полной историей попыток.
    """

    # Ключ для хранения retry count в labels
    _RETRY_COUNT_KEY = "retry_count"

    async def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        """Инкрементируем retry count перед выполнением."""
        # Получаем текущий retry count из labels
        current_retry = message.labels.get(self._RETRY_COUNT_KEY, 0)
        message.labels[self._RETRY_COUNT_KEY] = int(current_retry) + 1
        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
    ) -> None:
        """
        Вызывается после выполнения задачи (успех или ошибка).

        Если задача упала и retry исчерпаны — записываем в DLQ.
        Если успех — ничего не делаем.
        """
        if not result.is_err:
            return

        # Проверяем retry count
        retry_count = message.labels.get(self._RETRY_COUNT_KEY, 1)
        max_retries = getattr(message, "max_retries", 3)  # Default Taskiq behavior

        # Если ещё есть retries — Taskiq сделает retry, не кладём в DLQ
        if retry_count <= max_retries:
            logger.debug(
                f"Task {message.task_name} failed (attempt {retry_count}/{max_retries}), "
                "will be retried by Taskiq"
            )
            return

        # Retry исчерпаны — кладём в DLQ
        error_text = str(result.error) if result.error else "Unknown error"

        try:
            redis = await get_dlq_redis()
            dlq = DeadLetterQueue(redis)

            await dlq.enqueue(
                job_name=message.task_name,
                args=message.args,
                kwargs=message.kwargs,
                error=error_text,
                worker_id=settings.WORKER_ID,
            )
            logger.warning(
                f"Task {message.task_name} exhausted retries ({retry_count}) "
                f"and moved to DLQ: {error_text[:100]}"
            )
        except Exception as dlq_exc:
            # DLQ сама не должна ронять worker.
            # Логируем и идём дальше — задача уже потеряна,
            # но worker продолжает работать.
            logger.error(
                f"DLQ enqueue failed for task {message.task_name}: {dlq_exc}"
            )


class ConcurrencyMiddleware(TaskiqMiddleware):
    """
    Middleware для ограничения concurrency через asyncio.Semaphore.

    Предотвращает перегрузку worker'а при burst нагрузки.
    Лимит берётся из settings.WORKER_CONCURRENCY.
    Timeout 30 сек — если семафор не освободился, задача падает с ошибкой.
    """

    ACQUIRE_TIMEOUT: float = 30.0

    def __init__(self) -> None:
        import asyncio
        self._semaphore: asyncio.Semaphore | None = None

    def _get_semaphore(self) -> "asyncio.Semaphore":
        """Lazy init — semaphore создаётся при первом вызове."""
        if self._semaphore is None:
            import asyncio
            self._semaphore = asyncio.Semaphore(settings.WORKER_CONCURRENCY)
        return self._semaphore

    async def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        """Acquire semaphore перед выполнением задачи с timeout."""
        import asyncio

        try:
            await asyncio.wait_for(
                self._get_semaphore().acquire(),
                timeout=self.ACQUIRE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Worker overloaded: semaphore acquire timeout "
                f"({self.ACQUIRE_TIMEOUT}s) for task {message.task_name}"
            )
        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
    ) -> None:
        """Release semaphore после выполнения (успех или ошибка)."""
        sem = self._get_semaphore()
        sem.release()
