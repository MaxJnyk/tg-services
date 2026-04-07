"""
Taskiq broker для tad-worker.

Подключается к ТОМУ ЖЕ RedisStreamBroker, что используется в tad-backend.
Один Redis, одни streams — tad-backend кладёт задачи, tad-worker забирает.

Это единственное место, где создаётся broker.
Все задачи (@broker.task) импортируют broker отсюда.

Middleware chain:
1. ConcurrencyMiddleware — ограничение параллельных задач (semaphore)
2. DLQMiddleware — маршрутизация failed задач в Dead Letter Queue
"""
from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

from src.core.config import settings
from src.infrastructure.queue.dlq_middleware import ConcurrencyMiddleware, DLQMiddleware

# URL для broker = base Redis URL + номер БД для broker streams
_broker_url = f"{settings.REDIS_URL}/{settings.REDIS_DB_BROKER}"

result_backend = RedisAsyncResultBackend(_broker_url)

broker = RedisStreamBroker(_broker_url).with_result_backend(result_backend)

# Middleware: порядок важен — concurrency ДО dlq,
# чтобы семафор освобождался даже при ошибке.
broker.add_middlewares(ConcurrencyMiddleware())
broker.add_middlewares(DLQMiddleware())
