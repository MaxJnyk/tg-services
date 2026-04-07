# TAD-Worker: План реализации (финальный)

> Согласован: 2026-04-07
> Автор: Lead Python Engineer
> Принцип: Не переписываем то, что работает. Чиним сломанное. Добавляем недостающее.

---

## Контекст

**tad-worker** — сервис для выполнения задач (posting, stats), заменяющий tg-messenger-service.

**Что НЕ входит в tad-worker (вынесено в tad-backend):**
- Billing/finance — управление балансами и транзакциями
- Аутентификация и авторизация
- Управление пользователями и ролями

**Текущее состояние:**
- ARQ worker (нужен Taskiq — tad-backend уже на нём)
- Hardcoded DB/Redis URLs (нужен Pydantic Settings)
- DLQ на Redis LIST — O(n) lookup (нужен HSET)
- PostModel/PlatformRepository — заглушки (нужны реальные read-only модели)
- UserbotPool на Telethon — работает, не трогаем

**tad-backend уже имеет:**
- Taskiq + RedisStreamBroker (`core/task_broker.py`)
- Kafka → tg-messenger-service (убираем, заменяем на Taskiq задачи в tad-worker)
- Полную схему: Platform, Post, Campaign, Revision, TgBot, PostingLog

---

## ФАЗА 0: ПОДГОТОВКА ИНФРАСТРУКТУРЫ (1 день)

### 0.1. Pydantic Settings — `src/core/config.py`

**Что делаем:** Единый конфиг из переменных окружения.

**Файл:** `src/core/config.py` (новый)

```python
class Settings(BaseSettings):
    # Database (read-only к БД tad-backend)
    DATABASE_URL: str
    DB_POOL_SIZE: int = 5
    DB_STATEMENT_TIMEOUT: int = 30  # секунд
    
    # Redis
    REDIS_URL: str = "redis://redis:6379"
    REDIS_DB_BROKER: int = 0    # Taskiq streams
    REDIS_DB_DLQ: int = 1       # DLQ + Locks
    REDIS_DB_CACHE: int = 2     # Rate limiters + Cache
    
    # Worker
    WORKER_CONCURRENCY: int = 50
    STRICT_SCHEMA_CHECK: bool = False  # True в production
    
    # Telegram
    TELEGRAM_BOT_RATE_LIMIT: int = 30   # msg/sec per token
    TELEGRAM_USERBOT_RATE_LIMIT: int = 20  # req/min per account
    
    # Sentry
    SENTRY_DSN: str = ""
    
    model_config = SettingsConfigDict(env_file=".env")
```

**Конкретные шаги:**
1. Создать `src/core/config.py` с классом `Settings`
2. Создать `.env.example` со всеми переменными
3. Обновить все файлы, которые используют hardcoded URLs:
   - `src/core/database.py` — заменить hardcoded `DATABASE_URL`
   - `src/core/redis.py` — заменить `redis://localhost:6379/0`
   - `src/entrypoints/worker.py` — убрать `RedisSettings(host="redis")`

**Результат:** Ни одного hardcoded URL в коде.

---

### 0.2. Database — readonly session + schema validation

**Что делаем:** Переписать `src/core/database.py`. Добавить read-only session и проверку схемы.

**Файл:** `src/core/database.py` (переписать)

```python
# Engine с настройками из config
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=settings.DB_POOL_SIZE,
    connect_args={
        "command_timeout": settings.DB_STATEMENT_TIMEOUT,
        "server_settings": {"statement_timeout": str(settings.DB_STATEMENT_TIMEOUT * 1000)},
    },
)

# Read-only session (для CQRS light)
async def get_readonly_session() -> AsyncSession:
    async with async_session() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        yield session

# Write session (для DLQ, locks, post status updates)
async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
```

**Файл:** `src/core/schema_validator.py` (новый)

```python
async def validate_schema(engine) -> list[str]:
    """
    Проверяет что таблицы, которые worker читает, имеют нужные колонки.
    Возвращает список проблем. Пустой список = всё ок.
    """
    REQUIRED_COLUMNS = {
        "posts": ["id", "platform_id", "post_id_on_platform", "current_ad_revision_id", "status_id"],
        "platforms": ["id", "url", "name", "status", "tg_bot_id", "remote_id"],
        "tgbots": ["id", "token", "is_active"],
        "revision": ["id", "advertisement_id", "content", "content_type", "erid"],
    }
    # sqlalchemy.inspect() → сравниваем с REQUIRED_COLUMNS
    # Лишние колонки — НЕ ошибка (tad-backend добавил новую, worker её не использует)
    # Отсутствующие — WARNING или FATAL (зависит от STRICT_SCHEMA_CHECK)
```

**Конкретные шаги:**
1. Переписать `src/core/database.py` — engine из config, две фабрики сессий
2. Создать `src/core/schema_validator.py` — проверка при старте
3. Вызвать `validate_schema()` в `on_startup` worker'а
4. Если `STRICT_SCHEMA_CHECK=true` и есть проблемы → `sys.exit(1)`
5. Если `false` → `logger.warning()`, продолжаем работу

**Результат:** Worker знает про расхождения схемы ДО того, как задача упадёт.

---

### 0.3. Redis — 3 базы, отдельные клиенты

**Что делаем:** Переписать `src/core/redis.py`. Три отдельных клиента.

**Файл:** `src/core/redis.py` (переписать)

```python
# db0 — Taskiq broker (shared с tad-backend, persistent AOF)
# db1 — DLQ + Locks (persistent AOF)  
# db2 — Rate limiters + Cache (volatile, TTL-based)

async def get_broker_redis() -> Redis:   # db0
async def get_dlq_redis() -> Redis:      # db1
async def get_cache_redis() -> Redis:    # db2
```

**Конкретные шаги:**
1. Переписать `src/core/redis.py` — три функции, URL из config
2. Обновить все потребители:
   - `infrastructure/queue/dlq.py` → `get_dlq_redis()`
   - `application/posting/rotate_post.py` → `get_dlq_redis()` для locks
   - `infrastructure/telegram/bot_pool.py` → `get_cache_redis()` для rate limiters
3. Убрать старый `get_redis()` (deprecated)

**Результат:** Taskiq streams не конкурируют с DLQ операциями.

---

### 0.4. DLQ — HSET + Sorted Set

**Что делаем:** Переписать `src/infrastructure/queue/dlq.py`. LIST → HSET + ZADD.

**Текущие проблемы:**
- `_get_job()` — O(n), перебирает весь LIST
- `_update_job()` — O(n), LREM + LPUSH
- `list_all()` — загружает весь LIST в память
- Нет TTL для completed jobs
- Нет attempt_history

**Новая структура Redis:**
```
HSET dlq:jobs {job_id} {json_payload}     # O(1) get/set
ZADD dlq:timeline {timestamp} {job_id}     # O(log n) range queries
```

**JSON payload:**
```json
{
  "id": "rotate_post:2026-04-07T12:00:00",
  "job_name": "rotate_post",
  "args": {"post_id": "uuid"},
  "status": "failed",
  "replay_count": 0,
  "created_at": "2026-04-07T12:00:00",
  "attempt_history": [
    {
      "timestamp": "2026-04-07T12:00:00",
      "error": "FloodWait: 30 seconds",
      "worker_id": "worker-posting-3"
    }
  ]
}
```

**Конкретные шаги:**
1. Переписать `DeadLetterQueue` класс:
   - `enqueue()` → HSET + ZADD
   - `get_job()` → HGET — O(1)
   - `replay()` → HGET + обновить replay_count + HSET
   - `list_all()` → ZRANGEBYSCORE (по времени) + HMGET
   - `get_size()` → HLEN — O(1)
   - `clear()` → DEL dlq:jobs + DEL dlq:timeline
   - `cleanup_completed()` → scan HGETALL, удалить completed старше 7 дней
2. Обновить `src/cli/dlq.py` под новый API
3. Добавить `attempt_history` в enqueue — список попыток с timestamp, error, worker_id

**Результат:** O(1) доступ по job_id. Мониторинг по возрасту через ZRANGEBYSCORE.

---

### 0.5. pyproject.toml — зависимости

**Что делаем:** Обновить зависимости под новую архитектуру.

**Добавить:**
```toml
taskiq = ">=0.11"
taskiq-redis = ">=1.0"
aiolimiter = ">=1.1"
pydantic-settings = ">=2.0"
```

**Убрать:**
```toml
arq = ">=0.26"  # заменён на taskiq
```

**Оставить:**
```toml
telethon = ">=1.34"  # работает, не трогаем
aiogram = ">=3.0"    # боты
sqlalchemy = ">=2.0"  # ORM
```

**Конкретные шаги:**
1. Обновить `pyproject.toml`
2. Убрать `arq` из dependencies
3. Добавить `taskiq`, `taskiq-redis`, `aiolimiter`, `pydantic-settings`

---

### 0.6. .env.example

```env
# Database (read-only к БД tad-backend)
DATABASE_URL=postgresql+asyncpg://tad:tad@postgres:5432/tad

# Redis
REDIS_URL=redis://redis:6379
REDIS_DB_BROKER=0
REDIS_DB_DLQ=1
REDIS_DB_CACHE=2

# Worker
WORKER_CONCURRENCY=50
STRICT_SCHEMA_CHECK=false

# Telegram
TELEGRAM_BOT_RATE_LIMIT=30
TELEGRAM_USERBOT_RATE_LIMIT=20

# Sentry
SENTRY_DSN=
```

---

### 0.7. Dockerfile + docker-compose.yml + Makefile

**Dockerfile:** Заменить `CMD arq ...` на `CMD taskiq worker ...`

**docker-compose.yml:** Обновить command и healthcheck. НЕ менять структуру сервисов (postgres, redis, worker).

**Makefile:** Обновить команды dev/test.

**Критерий готовности Фазы 0:**
```bash
docker-compose up -d
# worker стартует без ошибок импорта
# schema validation проходит (warning или ok)
# redis подключается к 3 базам
# healthcheck = 200
```

---

## ФАЗА 1: БРОКЕР ЗАДАЧ И СЕГМЕНТАЦИЯ (2 дня)

### 1.1. Taskiq Broker — `src/entrypoints/broker.py`

**Что делаем:** Создать единый broker, подключённый к тому же Redis что и tad-backend.

**Файл:** `src/entrypoints/broker.py` (новый)

```python
from taskiq_redis import RedisStreamBroker, RedisAsyncResultBackend
from src.core.config import settings

redis_url = f"{settings.REDIS_URL}/{settings.REDIS_DB_BROKER}"
result_backend = RedisAsyncResultBackend(redis_url)
broker = RedisStreamBroker(redis_url).with_result_backend(result_backend)
```

**Ключевой момент:** Это ТОТ ЖЕ брокер, что в `tad-backend/app/src/core/task_broker.py`. Один Redis, одни streams. tad-backend кладёт задачу через `.kiq()` → tad-worker забирает.

**Конкретные шаги:**
1. Создать `src/entrypoints/broker.py`
2. Создать `src/entrypoints/worker.py` — startup/shutdown hooks
3. Удалить старый ARQ-based `src/entrypoints/worker.py`
4. Зарегистрировать задачи через `@broker.task`

---

### 1.2. Task Definitions — с labels

**Файлы:**
- `src/tasks/posting.py` — задачи постинга (label: `posting`)
- `src/tasks/stats.py` — задачи статистики (label: `stats`)
- `src/tasks/billing.py` — задачи биллинга (label: `billing`)

```python
@broker.task(task_name="rotate_post", labels={"queue": "posting"})
async def rotate_post(post_id: str, revision_id: str) -> dict:
    ...

@broker.task(task_name="scrape_channel_stats", labels={"queue": "stats"})
async def scrape_channel_stats(platform_id: str) -> dict:
    ...

@broker.task(task_name="hold_balance", labels={"queue": "billing"})
async def hold_balance(post_id: str, user_id: str, amount: str, idempotency_key: str) -> dict:
    ...
```

**Конкретные шаги:**
1. Создать `src/tasks/posting.py` с задачами: `rotate_post`, `edit_post`, `delete_post`
2. Создать `src/tasks/stats.py` с задачами: `scrape_channel_stats`, `collect_message_stat`
3. Создать `src/tasks/billing.py` с задачами: `hold_balance`, `charge_balance`, `release_balance`
4. Каждая задача валидирует input через Pydantic schema ДО вызова use case
5. Каждая задача оборачивает use case в try/except → DLQ при exhausted retries

---

### 1.3. Task Schemas — `src/shared/schemas/tasks.py`

**Файл:** `src/shared/schemas/tasks.py` (новый)

```python
class RotatePostTask(BaseModel):
    post_id: str  # UUID
    revision_id: str  # UUID

class ScrapeStatsTask(BaseModel):
    platform_id: str
    account_id: str | None = None  # если не указан — round-robin
    depth_messages: int = 50

class HoldBalanceTask(BaseModel):
    post_id: str
    user_id: str
    amount: Decimal
    idempotency_key: str
```

---

### 1.4. Worker Segmentation — docker-compose

**Обновить `docker-compose.yml`:**

```yaml
# Dev: один worker, все задачи
worker:
  command: taskiq worker src.entrypoints.broker:broker --workers 2

# Production (docker-compose.prod.yml):
worker-posting:
  command: taskiq worker src.entrypoints.broker:broker --labels posting --workers 8
worker-billing:
  command: taskiq worker src.entrypoints.broker:broker --labels billing --workers 2
worker-stats:
  command: taskiq worker src.entrypoints.broker:broker --labels stats --workers 3
```

**Конкретные шаги:**
1. Обновить `docker-compose.yml` — worker command на taskiq
2. Создать `docker-compose.prod.yml` — 3 отдельных worker сервиса с labels
3. Обновить `Makefile` — команды для dev и prod режимов

**Backpressure:** Semaphore в middleware:
```python
class ConcurrencyMiddleware(TaskiqMiddleware):
    def __init__(self, max_concurrent: int = 50):
        self.semaphore = asyncio.Semaphore(max_concurrent)
```

---

### 1.5. DLQ Middleware — `src/infrastructure/queue/dlq_middleware.py`

**Файл:** `src/infrastructure/queue/dlq_middleware.py` (новый)

```python
class DLQMiddleware(TaskiqMiddleware):
    """Перехватывает failed задачи после exhausted retries → DLQ."""
    
    async def on_error(self, message, result, exception):
        if message.labels.get("retry_count", 0) >= 3:
            await self.dlq.enqueue(
                job_name=message.task_name,
                args=message.args,
                kwargs=message.kwargs,
                error=str(exception),
                worker_id=self.worker_id,
            )
```

---

**Критерий готовности Фазы 1:**
```
tad-backend: await rotate_post.kiq(post_id="xxx", revision_id="yyy")
  → Redis Stream: задача с label "posting"
  → tad-worker (--labels posting) забирает
  → выполняет RotatePostUseCase
  → результат в result backend
```

---

## ФАЗА 2: BOT POOL V2 (2 дня)

### 2.1. Rate Limiting — `src/infrastructure/telegram/rate_limiter.py`

**Файл:** `src/infrastructure/telegram/rate_limiter.py` (новый)

```python
class TelegramRateLimiter:
    """
    Двухуровневый rate limiter:
    - Global: 30 msg/sec per bot token
    - Per-chat: 1 msg/sec per (token, chat_id)
    - Telegram RetryAfter ПРИОРИТЕТНЕЕ наших лимитов
    """
    def __init__(self):
        self._global: dict[str, AsyncLimiter] = {}      # token → limiter
        self._per_chat: dict[str, AsyncLimiter] = {}     # "token:chat_id" → limiter
        self._retry_after: dict[str, float] = {}          # token → resume_at timestamp
    
    async def acquire(self, token: str, chat_id: str):
        # 1. Проверить RetryAfter от Telegram
        # 2. Global limit
        # 3. Per-chat limit
    
    def handle_retry_after(self, token: str, seconds: int):
        # Telegram сказал ждать → записать resume_at
```

**Конкретные шаги:**
1. Создать `rate_limiter.py` с `TelegramRateLimiter`
2. Интегрировать в `BotPool.send_message()` — acquire перед отправкой
3. При `RetryAfter` от Telegram — обновить `_retry_after`, НЕ retry задачу сразу

---

### 2.2. Circuit Breaker — `src/infrastructure/telegram/circuit_breaker.py`

**Файл:** `src/infrastructure/telegram/circuit_breaker.py` (новый)

```python
class CircuitBreaker:
    """
    States: CLOSED → OPEN → HALF_OPEN
    - CLOSED: нормальная работа, считаем ошибки
    - OPEN: 5 ошибок за 60 сек → блокируем запросы
    - HALF_OPEN: через 60 сек пропускаем 1 тестовый запрос
    """
    FAILURE_THRESHOLD = 5
    FAILURE_WINDOW = 60       # секунд
    RECOVERY_TIMEOUT = 60     # секунд
```

**Конкретные шаги:**
1. Создать `circuit_breaker.py` с классом `CircuitBreaker`
2. Один CircuitBreaker per bot token
3. Интегрировать в `BotPool` — перед каждым запросом check state
4. Добавить Prometheus gauge: `circuit_breaker_state{token}`

---

### 2.3. Bot Pool V2 — переписать `src/infrastructure/telegram/bot_pool.py`

**Изменения к текущему коду:**
- Добавить `last_used: dict[str, float]` — timestamp последнего использования
- Eviction: закрывать Bot sessions неиспользуемые >5 минут
- Интегрировать RateLimiter и CircuitBreaker
- Health check: НЕ get_me() по крону. Проверка при первом реальном запросе. 401 → invalid.

**Конкретные шаги:**
1. Переписать `bot_pool.py` — добавить RateLimiter, CircuitBreaker, eviction
2. Метод `send_message()` — полный flow: acquire rate limit → check circuit → send → record result
3. `close()` — graceful shutdown всех sessions

---

### 2.4. Error Taxonomy — `src/infrastructure/telegram/errors.py`

**Файл:** `src/infrastructure/telegram/errors.py` (новый)

```python
RETRYABLE_ERRORS = (FloodWait, TimeoutError, NetworkError, RetryAfter)
NON_RETRYABLE_ERRORS = (Unauthorized, ChatNotFound, BadRequest)

def classify_error(exc: Exception) -> str:
    """Возвращает 'retry', 'dlq', или 'rate_limit'"""
```

**Конкретные шаги:**
1. Создать `errors.py` с классификацией ошибок
2. В задачах posting: retry при retryable, DLQ при non-retryable
3. При `RetryAfter` — вызвать `rate_limiter.handle_retry_after()`

---

### 2.5. Graceful Shutdown

**В `src/entrypoints/worker.py`:**

```python
shutdown_event = asyncio.Event()

def handle_sigterm():
    shutdown_event.set()

async def on_worker_shutdown(state):
    # 1. Перестаём забирать новые задачи (shutdown_event)
    # 2. Ждём текущие задачи (timeout 30 сек)
    # 3. bot_pool.close()
    # 4. userbot_pool.close_all()
    # 5. Отправляем метрику "worker_shutdown"
```

---

**Критерий готовности Фазы 2:**
- При FloodWait → circuit breaker OPEN, другие токены работают
- При SIGTERM → текущие задачи завершаются, новые не берутся
- Rate limiter не пропускает >30 msg/sec per token

---

## ФАЗА 3: USERBOT POOL V2 + ANTI-DETECT (2 дня)

### 3.1. Anti-detect — доработка текущего `userbot_pool.py`

**НЕ переписываем** — текущий код на Telethon работает. Добавляем:

- `DeviceFingerprint` — уже есть, фиксирован PER ACCOUNT при создании
- **Убрать** ротацию User-Agent между запросами (подозрительнее стабильного fingerprint)
- Behavioral jitter: `await asyncio.sleep(random.uniform(0.5, 3.0))` перед запросом
- Session rotation: разные DC ids — уже есть в Telethon (auto-detect)

**Конкретные шаги:**
1. Добавить jitter в `get_client()` — random delay перед возвратом клиента
2. Добавить proxy support в `add_account()` — `PySocks` proxy через config
3. Проверить что fingerprint НЕ меняется между запросами одного аккаунта

---

### 3.2. Proxy Management — `src/infrastructure/telegram/proxy_manager.py`

**Файл:** `src/infrastructure/telegram/proxy_manager.py` (новый)

```python
class ProxyManager:
    """Один аккаунт = один прокси. Статический mapping из БД."""
    
    async def get_proxy_for_account(self, account_id: str) -> ProxyConfig | None:
        # SELECT proxy_* FROM userbot_accounts WHERE account_id = :id
    
    async def health_check(self, proxy: ProxyConfig) -> bool:
        # TCP connect timeout 5 сек
    
    async def mark_proxy_compromised(self, proxy_id: str, reason: str):
        # UPDATE proxies SET status = 'compromised' WHERE id = :id
```

---

### 3.3. Account Lifecycle

**Состояния:** `active` → `cooldown` (5 мин) → `banned` → `archived`

**Доработать `UserbotPool`:**
- При `FloodWait` → cooldown (5 мин), переключиться на другой аккаунт
- При 3 подряд `UserBannedInChannelError` → banned
- `get_client()` — round-robin только по `active` аккаунтам
- Cooldown recovery: asyncio timer через 5 мин переводит обратно в active

---

### 3.4. Connection Pooling — доработка

**Текущее поведение:** `initialize_from_db()` подключает ВСЕ аккаунты при старте.

**Новое:**
- При старте worker'а: подключить первые 3 аккаунта (warm-up для немедленной готовности)
- Остальные: lazy connect при первом `get_client(account_id)`
- Max concurrent: Semaphore(10) per account
- Session eviction: disconnect через 5 мин неактивности

**Конкретные шаги:**
1. Добавить `warm_up(count=3)` — подключить N аккаунтов при старте
2. Переписать `get_client()` — lazy connect если не подключен
3. Добавить `Semaphore(10)` per account для ограничения concurrent requests
4. Background task: каждые 60 сек проверять `last_used`, отключать idle >5 мин

---

**Критерий готовности Фазы 3:**
- Бан одного аккаунта → автоматическое переключение на другой
- FloodWait → cooldown, другие аккаунты продолжают
- Прокси проверяются перед использованием

---

## ФАЗА 4: POSTING USE CASE — IDEMPOTENCY (2 дня)

### 4.1. Distributed Locking

**Простой SET NX EX** (НЕ RedLock — у нас один Redis):

```python
async def acquire_lock(redis, key: str, ttl: int = 60) -> bool:
    return await redis.set(f"lock:{key}", worker_id, nx=True, ex=ttl)

async def release_lock(redis, key: str):
    # Lua script: проверить что lock принадлежит нам, потом DEL
```

**PostgreSQL advisory lock как fallback:**
```python
await session.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": hash(post_id)})
```

**Конкретные шаги:**
1. Создать `src/infrastructure/locking/redis_lock.py` — SET NX EX + Lua release
2. Создать `src/infrastructure/locking/pg_advisory_lock.py` — fallback
3. Переписать `RotatePostUseCase.execute()` — использовать новый lock manager

---

### 4.2. Idempotency

```python
idempotency_key = f"idempotency:{post_id}:{revision_id}"
# Redis SET NX EX 86400 (24 часа)
# Если ключ существует → return cached result
```

**Конкретные шаги:**
1. Создать `src/infrastructure/idempotency/redis_idempotency.py`
2. Интегрировать в `rotate_post` задачу — проверка ДО вызова use case

---

### 4.3. Post State Machine

**Состояния в tad-backend:** Post имеет `status_id` → FK на `post_status` таблицу.

**tad-worker использует optimistic locking:**
```sql
UPDATE posts SET status_id = :posting_status 
WHERE id = :post_id AND status_id = :scheduled_status
-- rowcount = 0 → кто-то другой уже взял этот пост
```

**Конкретные шаги:**
1. Создать `src/infrastructure/database/repositories/post_repository.py` — реальный, НЕ заглушка
2. Методы: `get_with_revision()`, `update_status()`, `set_message_id()`
3. `update_status()` — optimistic locking через WHERE status_id = :expected

---

### 4.4. Media Handling

```python
class MediaDownloader:
    MAX_SIZE = 50 * 1024 * 1024  # 50MB Telegram limit
    TEMP_DIR = "/tmp/tad-worker-media/"
    
    async def download(self, url: str) -> Path:
        # aiohttp + chunk streaming
        # MIME detection через magic bytes
        # cleanup в finally
```

**Конкретные шаги:**
1. Создать `src/infrastructure/media/downloader.py`
2. Cleanup temp dir при startup worker'а
3. Интегрировать в `rotate_post` — скачать media из revision.images

---

### 4.5. Retry Logic

```python
RETRY_POLICY = {
    "max_retries": 3,
    "backoff": [1, 2, 4],       # секунды
    "jitter": 0.2,               # ±20%
    "retryable": RETRYABLE_ERRORS,
    "non_retryable": NON_RETRYABLE_ERRORS,
}
```

**Конкретные шаги:**
1. Создать `src/infrastructure/retry/policy.py` — RetryPolicy класс
2. Интегрировать в Taskiq middleware или в каждую задачу
3. При exhausted retries → DLQ с полным контекстом

---

**Критерий готовности Фазы 4:**
- Двойной запуск одной задачи → НЕ создаёт дубликат в Telegram
- Optimistic locking → второй worker получает "already processing"
- Media >50MB → отклоняется до отправки

---

## ФАЗА 5: BILLING — OUT OF SCOPE (в tad-backend)

**Billing/finance не входит в tad-worker.**

**Где billing:**
- **tad-backend** — управляет балансами, hold/charge/release, транзакциями
- **accounting-service** (существующий) — если используется отдельно

**Flow posting с billing:**
```
tad-backend: hold() → create_task(posting) → wait result
                              ↓
tad-worker: выполняет posting → сообщает результат
                              ↓
tad-backend: charge() или release() в зависимости от результата
```

**tad-worker НЕ управляет деньгами** — только исполняет задачи и возвращает статус.

---

## ФАЗА 5: SCHEDULER (2 дня)

### 6.1. Leader Election

**Простой leader lease** (НЕ RedLock):

```python
async def try_become_leader(redis, worker_id: str) -> bool:
    return await redis.set("scheduler:leader", worker_id, nx=True, ex=60)

async def heartbeat(redis, worker_id: str):
    # Каждые 30 сек: SET scheduler:leader {worker_id} EX 60
    # Если мы не лидер (другой worker_id) → skip
```

---

### 6.2. Posting Schedule

```python
async def find_ready_platforms(session):
    """
    SELECT p.id, p.name FROM platforms p
    JOIN campaign_posting_settings cps ON ...
    WHERE next_post_at <= NOW() AND p.status = 'active'
    ORDER BY next_post_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
    """
```

Батчами по 100. Для каждой platform → `rotate_post.kiq()`.

---

### 6.3. Stats Scheduler

- Cron: каждый час
- Round-robin по userbot accounts
- `scrape_channel_stats.kiq()` для каждой active platform

---

**Критерий готовности Фазы 5:**
- 3 worker реплики → только один делает scheduling
- Kill лидера → другой подхватывает через <60 сек
- Задачи создаются по расписанию без дубликатов

---

## ФАЗА 7: MONITORING + ALERTING (2 дня)

### 7.1. Prometheus Metrics

**Business:**
- `tad_posts_total{status, placement_type}` — counter
- `tad_rotation_duration_seconds` — histogram (0.1, 0.5, 1, 2, 5, 10)
- `tad_scheduler_lag_seconds` — gauge

**Infrastructure:**
- `tad_userbot_pool_active` — gauge
- `tad_userbot_pool_banned` — gauge
- `tad_bot_pool_healthy_tokens` — gauge
- `tad_dlq_size` — gauge
- `tad_circuit_breaker_state{token}` — gauge (0/1/2)

---

### 7.2. Structured Logging

```python
# loguru с JSON serialization
logger.bind(
    event="posting.completed",
    post_id=post_id,
    duration_ms=elapsed,
    correlation_id=ctx.get("correlation_id"),
).info("Post published")
```

- Mask bot tokens: показывать первые 10 символов
- Session strings: скрывать полностью
- Correlation ID: из Taskiq message → через все слои

---

### 7.3. Healthcheck — `/health`

```python
# aiohttp server на отдельном порту (8080)
async def health_handler(request):
    checks = {
        "postgresql": await check_pg(),    # SELECT 1, <100ms
        "redis": await check_redis(),       # PING, <50ms  
        "bot_pool": bot_pool.get_healthy_ratio(),  # >50%
        "dlq": await dlq.get_size() < 100,
    }
    status = "healthy" if all(checks.values()) else "degraded"
    return web.json_response({"status": status, "checks": checks})
```

---

### 7.4. Alerting

| Приоритет | Условие | Действие |
|-----------|---------|----------|
| **P0** | `dlq_size > 10` | Telegram alert немедленно |
| **P1** | `dlq_size > 0` | Telegram alert через 5 мин |
| **P1** | `userbot_active < 3` | Telegram alert через 5 мин |
| **P1** | `scheduler_lag > 600s` | Telegram alert через 5 мин |
| **P2** | `bot_healthy_tokens < 5` | Log warning |

---

**Критерий готовности Фазы 7:**
- Kill PostgreSQL → healthcheck 503 → alert в Telegram <1 мин
- Grafana dashboard с бизнес и инфра метриками
- Structured JSON logs в stdout

---

## ФАЗА 8: ТЕСТИРОВАНИЕ + HARDENING (1.5 дня)

### 8.1. Unit Tests

- Domain: `DeviceFingerprint`, `PostId`, `Post.publish()` state transitions
- Use cases: `RotatePostUseCase` с InMemory repos
- Edge cases: пустая revision_queue, locked post

### 8.2. Integration Tests

- Testcontainers: PostgreSQL + Redis в Docker
- Contract: tad-backend task payload → tad-worker parses correctly
- Schema compatibility: новый код со старой схемой БД

### 8.3. Chaos Engineering

| Тест | Ожидание |
|------|----------|
| SIGTERM mid-posting | Задача в DLQ, нет дубликатов |
| Redis down 30 сек | Retry, graceful degradation |
| PostgreSQL +1000ms latency | Timeout handling, no deadlocks |
| Telegram 50% FloodWait | Circuit breaker + retry |

### 8.4. Load Testing

**Первый тест: 100 постов/мин × 10 минут** (не 1000 — это peak load).

Контрольные точки:
- p95 latency < 5 сек
- 0% дубликатов
- Circuit breaker НЕ открывался
- Memory stable (нет leak'ов)
- DLQ = 0

**Peak test (когда базовый пройдён): 1000 постов/мин × 5 минут.**

### 8.5. Documentation

- `RUNBOOK.md` — действия при инцидентах
- `ARCHITECTURE.md` — C4 диаграммы
- `DEPLOYMENT.md` — пошаговый деплой с чеклистом

---

**Критерий готовности Фазы 8:**
- Load test проходит без алертов
- Chaos test = graceful degradation
- On-call engineer разбирается за 5 минут по RUNBOOK

---

## ВРЕМЕННЫЕ ОЦЕНКИ

| Фаза | Дни | Результат | Основной риск |
|------|-----|-----------|---------------|
| **0** | 1 | Инфраструктура: config, DB, Redis, DLQ | — |
| **1** | 2 | Taskiq broker + worker labels + tasks | Backward compat Taskiq |
| **2** | 2 | Bot Pool V2: rate limit + circuit breaker | Telegram rate limits |
| **3** | 2 | Userbot Pool V2: anti-detect + proxy | Нужны реальные аккаунты |
| **4** | 2 | Posting: idempotency + state machine | Distributed locks |
| **5** | 2 | Scheduler: leader + cron | Leader failover |
| **6** | 2 | Monitoring: metrics + alerts + health | Grafana setup |
| **7** | 1.5 | Testing: unit + integration + chaos + load | Найдём новые баги |
| **ИТОГО** | **14.5** | **Production-ready** | **Buffer +20% = 17 дней** |

**Примечание:** Billing/finance не входит в tad-worker (реализуется в tad-backend).

---

## ПРИНЯТЫЕ АРХИТЕКТУРНЫЕ РЕШЕНИЯ

| Решение | Обоснование |
|---------|-------------|
| Inline read-only модели (не git submodule) | Нет внешних зависимостей, schema validation при старте |
| Schema validation: warning + strict mode | Dev не блокируется, prod — блокируется |
| Single Redis node, 3 базы | 5 баз = лишние connection pools |
| SET NX EX (не RedLock) | Один Redis = RedLock бессмысленен |
| READ COMMITTED + FOR UPDATE (не SERIALIZABLE) | Предсказуемый, нет retry storm |
| Taskiq labels (не разные Redis streams) | Один broker, масштабирование через docker |
| Telethon (не Pyrogram) | Работает, нет бизнес-причины менять |
| Bot health: при первом use (не cron) | Экономия rate limit quota |
| DLQ: HSET + 1 Sorted Set | O(1) lookup, мониторинг по возрасту. Второй SS — при необходимости |
| Hold timeout: 5 мин auto-release | Защита от зависших hold'ов |
| Load test: 100/мин (не 1000) | 1000 = peak load, не baseline |
| DLQ alert: P1 при >0, P0 при >10 | 1 задача ≠ катастрофа |
