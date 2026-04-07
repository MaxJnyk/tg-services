# TAD-Worker: Идеальный план реализации (исправленный)

> Статус: 5 фаз выполнены, нужны критические доработки
> Дата: 2026-04-07
> Архитектура: Чистая (только tad-backend + tad-worker, никаких сервисов Паши)

---

## ✅ ЧТО УЖЕ ГОТОВО (не трогаем)

### P0: Инфраструктура — ✅ ГОТОВО
- [x] `src/core/config.py` — Pydantic Settings, нет hardcoded URLs
- [x] `src/core/database.py` — read-only + read-write сессии
- [x] `src/core/redis.py` — 3 базы (broker, DLQ, cache)
- [x] `src/core/schema_validator.py` — проверка при старте
- [x] `pyproject.toml` — Taskiq, нет ARQ

### P1: Брокер задач — ✅ ГОТОВО  
- [x] `src/entrypoints/broker.py` — Taskiq RedisStreamBroker
- [x] `src/entrypoints/worker.py` — startup/shutdown hooks
- [x] `src/tasks/posting.py` — label: posting
- [x] DLQ Middleware с retry count tracking

### P2: Bot Pool — ✅ ГОТОВО (но in-memory)
- [x] `src/infrastructure/telegram/bot_pool.py` — lazy create, eviction
- [x] `src/infrastructure/telegram/rate_limiter.py` — global + per-chat
- [x] `src/infrastructure/telegram/circuit_breaker.py` — states + recovery
- [x] `src/infrastructure/telegram/errors.py` — error classification

### P3: Userbot Pool — ✅ ГОТОВО
- [x] `src/infrastructure/telegram/userbot_pool.py` — Telethon, anti-detect
- [x] Device fingerprint per account
- [x] Proxy support
- [x] Account lifecycle (active/cooldown/banned)

### P4: Scheduler — ✅ ГОТОВО
- [x] `src/application/scheduler/leader.py` — leader election
- [x] `src/application/scheduler/posting.py` — posting schedule
- [x] `src/application/scheduler/stats.py` — stats schedule
- [x] Graceful shutdown с schedulers

---

## 🔴 КРИТИЧЕСКИЕ ПРОБЛЕМЫ (фиксим за 3 дня)

### ❌ Проблема 1: Circuit Breaker — IN-MEMORY (критично для кластера)

**Где:** `src/infrastructure/telegram/circuit_breaker.py:49-51`

**Проблема:**
```python
self._state = CircuitState.CLOSED  # in-memory per worker
self._failures: deque[float] = deque()  # только этот worker видит ошибки
```

**При 3 workers:**
- Worker-1: 3 ошибки на token-A → ещё CLOSED
- Worker-2: 2 ошибки на token-A → ещё CLOSED  
- Worker-3: отправляет запрос → Telegram банит ВСЕХ

**Фикс:** Redis-backed circuit breaker

```python
# src/infrastructure/telegram/redis_circuit_breaker.py (новый файл)
class RedisCircuitBreaker:
    """Cluster-aware circuit breaker using Redis."""
    
    def __init__(self, redis: Redis):
        self._redis = redis
        self._redis_down_logged = False
    
    async def record_failure(self, token: str):
        try:
            # LPUSH circuit:failures:{token} {timestamp}
            await self._redis.lpush(f"circuit:failures:{token}", str(time.time()))
            # LTRIM circuit:failures:{token} 0 4  # храним 5 последних
            await self._redis.ltrim(f"circuit:failures:{token}", 0, 4)
            
            # Проверяем количество ошибок за 60 сек
            failures = await self._redis.lrange(f"circuit:failures:{token}", 0, -1)
            now = time.time()
            recent_failures = [f for f in failures if now - float(f) < 60]
            
            if len(recent_failures) >= 5:
                # SET circuit:state:{token} OPEN EX 60
                await self._redis.set(f"circuit:state:{token}", "OPEN", ex=60)
        except RedisError:
            # Redis недоступен — логируем один раз
            if not self._redis_down_logged:
                logger.critical("Redis down, circuit breaker recording disabled")
                self._redis_down_logged = True
    
    async def allow_request(self, token: str) -> bool:
        try:
            state = await self._redis.get(f"circuit:state:{token}")
            if state is None or state.decode() == "HALF_OPEN":
                return True
            if state.decode() == "OPEN":
                return False
            return True
        except RedisError:
            # Критичный выбор: Redis down = блокируем запросы
            # Лучше простой чем бан от Telegram
            if not self._redis_down_logged:
                logger.critical("Redis down, circuit breaker blocking all requests (safe mode)")
                self._redis_down_logged = True
            return False  # Safe fallback: блокируем
```

**Время:** 4 часа

---

### ❌ Проблема 2: Rate Limiter — IN-MEMORY (race conditions)

**Где:** `src/infrastructure/telegram/bot_pool.py:54`

**Проблема:**
```python
self._rate_limiter = TelegramRateLimiter()  # in-memory
```

**При concurrency:** два worker'а одновременно:
- Worker-1: проверяет лимит (29/30) → OK → отправляет
- Worker-2: проверяет лимит (29/30) → OK → отправляет  
- Результат: 31 msg/sec → Telegram 429 FloodWait

**Фикс:** Redis-backed rate limiter

```python
# src/infrastructure/telegram/redis_rate_limiter.py (новый файл)
class RedisRateLimiter:
    """Token bucket на Redis INCR + EXPIRE с fallback."""
    
    def __init__(self, redis: Redis):
        self._redis = redis
        self._redis_down_logged = False
        self._global_limit = 30  # msg/sec per token
        self._chat_limit = 1     # msg/sec per chat
    
    async def acquire(self, token: str, chat_id: str):
        try:
            # Глобальный лимит: INCR rate:global:{token}
            global_key = f"rate:global:{token}"
            current_global = await self._redis.incr(global_key)
            if current_global == 1:
                await self._redis.expire(global_key, 1)  # 1 секундное окно
            
            if current_global > self._global_limit:
                raise RateLimitExceeded(f"Global limit: {current_global}/{self._global_limit}")
            
            # Per-chat лимит: INCR rate:chat:{token}:{chat_id}
            chat_key = f"rate:chat:{token}:{chat_id}"
            current_chat = await self._redis.incr(chat_key)
            if current_chat == 1:
                await self._redis.expire(chat_key, 1)
            
            if current_chat > self._chat_limit:
                raise RateLimitExceeded(f"Chat limit: {current_chat}/{self._chat_limit}")
                
        except RedisError:
            # Redis недоступен — логируем и пропускаем (опасно, но лучше чем полный простой)
            if not self._redis_down_logged:
                logger.critical("Redis down, rate limiting disabled — risk of FloodWait!")
                self._redis_down_logged = True
            # Fallback: пропускаем запрос без rate limiting
            # Риск: можем получить FloodWait от Telegram, но лучше чем полный простой
    
    async def handle_retry_after(self, token: str, seconds: int):
        """Telegram сказал ждать — блокируем токен на указанное время."""
        try:
            await self._redis.set(f"rate:retry_after:{token}", str(time.time() + seconds), ex=seconds)
        except RedisError:
            pass  # Не критично
```

**Время:** 4 часа

---

### ❌ Проблема 3: BotPool создаётся в каждой задаче (утечка connections)

**Где:** `src/tasks/posting.py:58`

**Проблема:**
```python
async def rotate_post(post_id: str):
    async with get_session() as session:
        bot_pool = BotPool()  # ← НОВЫЙ пул на КАЖДУЮ задачу!
        # ... 1000 задач = 1000 пулов = утечка
```

**Фикс:** Использовать BotPool из worker state

```python
# src/tasks/posting.py (исправить)
async def rotate_post(post_id: str, context: TaskiqContext):
    bot_pool = context.state.bot_pool  # ← Из state, один на worker
    # ...
```

**Время:** 30 минут

---

### ❌ Проблема 4: Stats task — ЗАГЛУШКА

**Где:** `src/tasks/stats.py:45-51`

**Текущий код:**
```python
# TODO TAD-PHASE3: Подключить ScrapeChannelStatsUseCase
return {"status": "stub", "platform_id": payload.platform_id}
```

**Нужно:** Реальный use case

```python
# src/application/stats/scrape_channel.py (создать)
class ScrapeChannelUseCase:
    async def execute(self, platform_id: str) -> dict:
        # 1. Получить platform из БД
        # 2. Получить userbot аккаунт из UserbotPool
        # 3. Telethon: получить статистику канала
        # 4. Сохранить в БД (или отправить обратно в tad-backend)
        pass
```

**Время:** 4 часа

---

### ❌ Проблема 5: Billing task — ЗАГЛУШКА + АРХИТЕКТУРНАЯ ОШИБКА

**Где:** `src/tasks/billing.py:53-59` 

**Текущий код:**
```python
# TODO TAD-PHASE5: Подключить BillingChargeUseCase
return {"status": "stub", "hold_id": payload.hold_id}
```

**Проблема:** Планировали делать charge в worker — это ОШИБКА.

**Почему ошибка:**
- Worker может упасть mid-charge → деньги в лимбо
- Нет единого аудита транзакций
- Worker имеет доступ к списанию (security risk)

**Правильное решение:** HTTP callback в tad-backend (SAGA pattern)

```python
# src/tasks/billing.py (исправленный)
async def billing_charge(hold_id: str, post_id: str, amount_kopecks: int):
    # Worker НЕ трогает деньги — только вызывает backend API
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{settings.BACKEND_URL}/api/v1/billing/execute-hold",
            json={"hold_id": hold_id, "post_id": post_id},
            headers={"Authorization": f"Bearer {settings.WORKER_API_TOKEN}"}
        ) as resp:
            if resp.status == 200:
                return {"status": "charged", "hold_id": hold_id}
            elif resp.status == 409:
                return {"status": "already_charged"}  # Idempotency
            else:
                raise RetryableError(f"Backend billing failed: {resp.status}")
```

**Backend делает:**
1. Проверяет hold существует и активен
2. SELECT FOR UPDATE NOWAIT на баланс пользователя
3. Charge атомарно
4. Возвращает result
5. Если worker не дозвонился — backend сам разбирается по таймауту

**Время:** 4 часа (включая endpoint в tad-backend)

---

### ❌ Проблема 6: Нет Healthcheck endpoint

**Нужно:** `src/entrypoints/health.py`

```python
# aiohttp сервер на порту 8080
async def health_handler(request):
    checks = {
        "postgresql": await check_pg(),
        "redis": await check_redis(),
        "bot_pool": bot_pool.get_healthy_ratio(),
        "userbot_pool": userbot_pool.get_active_count() > 3,  # минимум 3 активных
        "dlq": await dlq.get_size() < 100,  # DLQ не переполнен
    }
    status = "healthy" if all(checks.values()) else "degraded"
    return web.json_response({"status": status, "checks": checks})
```

**Время:** 2 часа

---

### ❌ Проблема 7: Нет Metrics (Prometheus)

**Нужно:** `src/core/metrics.py`

```python
from prometheus_client import Counter, Histogram, Gauge

posts_total = Counter('tad_posts_total', 'Total posts', ['status'])
rotation_duration = Histogram('tad_rotation_duration_seconds', 'Duration')
dlq_size = Gauge('tad_dlq_size', 'DLQ size')
```

**Время:** 2 часа

---

## 📋 ИСПРАВЛЕННЫЙ ПЛАН (3 дня)

### День 1: Кластерная консистентность (критично!)

| Время | Задача | Файл |
|-------|--------|------|
| 4ч | Redis Circuit Breaker | `redis_circuit_breaker.py` |
| 4ч | Redis Rate Limiter | `redis_rate_limiter.py` |
| 30м | Исправить posting task | `tasks/posting.py:58` |
| 30м | Интегрировать в BotPool | `bot_pool.py` |

**Критерий:** Запустить 3 worker'а, убить один — circuit breaker остаётся консистентным.

---

### День 2: Реальные use cases (функционал)

| Время | Задача | Файл |
|-------|--------|------|
| 4ч | ScrapeChannelUseCase | `application/stats/` |
| 4ч | BillingChargeUseCase | `application/billing/` |
| 2ч | Подключить tasks | `tasks/stats.py`, `tasks/billing.py` |

**Критерий:** `scrape_channel` собирает реальную статистику, `billing_charge` списывает деньги.

---

### День 3: Observability (не падаем вслепую)

| Время | Задача | Файл |
|-------|--------|------|
| 2ч | Healthcheck endpoint | `entrypoints/health.py` |
| 2ч | Prometheus metrics | `core/metrics.py` |
| 2ч | Alerting (Telegram) | `infrastructure/alerts/` |
| 2ч | Load test script | `tests/load/` |

**Критерий:** DLQ > 0 → алерт в Telegram за <1 мин.

---

## 🎯 ИТОГОВАЯ АРХИТЕКТУРА (чистая)

```
┌─────────────────────────────────────────────────────────────┐
│                        tad-backend                           │
│  (FastAPI, Taskiq producer, PostgreSQL, Scheduler API)       │
└──────────────┬────────────────────────────────────────────────┘
               │ Taskiq Redis (db0)
               ▼
┌─────────────────────────────────────────────────────────────┐
│                        tad-worker                            │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Tasks Layer                                          │ │
│  │  • rotate_post (posting) ✅                            │ │
│  │  • scrape_channel (stats) 🔧 (день 2)                 │ │
│  │  • billing_charge (billing) 🔧 (день 2)               │ │
│  └────────────────────────────────────────────────────────┘ │
│                         │                                   │
│  ┌──────────────────────▼────────────────────────┐         │
│  │  Use Cases Layer                               │         │
│  │  • RotatePostUseCase ✅                        │         │
│  │  • ScrapeChannelUseCase 🔧 (день 2)           │         │
│  │  • BillingChargeUseCase 🔧 (день 2)           │         │
│  └────────────────────────────────────────────────┘         │
│                         │                                   │
│  ┌──────────────────────▼────────────────────────┐         │
│  │  Infrastructure Layer                          │         │
│  │  • BotPool (Redis CB + RL) 🔧 (день 1)        │         │
│  │  • UserbotPool ✅                              │         │
│  │  • RedisLock ✅                                 │         │
│  │  • Idempotency ✅                              │         │
│  │  • DLQ (HSET+ZADD) ✅                         │         │
│  └────────────────────────────────────────────────┘         │
│                         │                                   │
│  ┌──────────────────────▼────────────────────────┐         │
│  │  Data Layer                                    │         │
│  │  • PostgreSQL (read-only) ✅                  │         │
│  │  • Redis (3 базы) ✅                          │         │
│  └────────────────────────────────────────────────┘         │
│                                                              │
│  🔧 Health + Metrics (день 3)                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Deployment (без Kubernetes)

### docker-compose.yml (один сервер)
```yaml
version: '3.8'
services:
  worker:
    build: .
    command: taskiq worker src.entrypoints.broker:broker --workers 8
    environment:
      - DATABASE_URL=postgresql+asyncpg://tad:tad@postgres:5432/tad
      - REDIS_URL=redis://redis:6379
    deploy:
      replicas: 2  # 2 контейнера × 8 workers = 16 concurrent tasks
```

### docker-compose.prod.yml (разделение по labels)
```yaml
worker-posting:
  command: taskiq worker src.entrypoints.broker:broker --labels posting --workers 8
  deploy:
    replicas: 2

worker-stats:
  command: taskiq worker src.entrypoints.broker:broker --labels stats --workers 4
  deploy:
    replicas: 1

worker-billing:
  command: taskiq worker src.entrypoints.broker:broker --labels billing --workers 2
  deploy:
    replicas: 1
```

---

## ✅ Чеклист перед боем (обязательно!)

- [ ] Redis Circuit Breaker работает с 3 worker'ами
- [ ] Redis Rate Limiter не даёт превысить 30 msg/sec
- [ ] `scrape_channel` собирает реальную статистику
- [ ] `billing_charge` списывает деньги (SELECT FOR UPDATE)
- [ ] Healthcheck на порту 8080 возвращает 200
- [ ] DLQ > 0 → алерт в Telegram
- [ ] Load test: 1000 posts/min sustained 10 минут
- [ ] Chaos test: kill worker mid-task → нет дубликатов
- [ ] Chaos test: Redis down 30 сек → graceful recovery

---

## 📝 Примечания

### Почему billing НЕ в tad-worker?

**Ошибка в первоначальном плане:** Планировали делать charge в worker.

**Почему это ошибка:**
- Worker может упасть mid-charge → деньги в лимбо
- Нет единого аудита транзакций (логи в разных worker'ах)
- Worker имеет прямой доступ к списанию (security risk)
- Откат сложный (worker уже dead, не знаем статус)

**Правильная архитектура (SAGA pattern):**
```
tad-backend: создаёт hold (с таймаутом 5 мин)
    ↓
tad-worker: выполняет posting → отправляет webhook
    ↓
tad-backend: получает результат → делает charge/release
    ↓
(если worker не ответил за 5 мин → backend сам release'ит)
```

**Worker делает только:**
```python
async def billing_charge(hold_id: str):
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{BACKEND_URL}/api/v1/billing/execute-hold",
            json={"hold_id": hold_id}
        )
    # Backend сам управляет транзакцией
```

**Преимущества:**
- Backend — единый источник правды для денег
- Worker не знает про балансы (security)
- Автоматический откат по таймауту
- Полный аудит в одном месте

### Почему не Kafka?
Заменили на Taskiq Redis Streams:
- Проще (один Redis)
- Нет дополнительной инфраструктуры
- tad-backend уже использует Taskiq

### Почему не HTTP API к accounting-service?
Сервисы Паши удалены. Читаем userbot аккаунты напрямую из PostgreSQL:
- Проще
- Нет сетевых ошибок
- Одна транзакция

---

**Готов начать Day 1 (Redis Circuit Breaker)?**
