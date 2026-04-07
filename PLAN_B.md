# TAD-Worker: План Б — Anti-Fragile Architecture

> Для нагрузки "1000 → пиздец вообще"
> Принцип: Не идеально, но не падает при перегрузке

---

## 🎯 Новые приоритеты (пересмотренные)

### Tier 1: НЕ ПАДАЕТ (обязательно, 3 дня)
- Redis-backed Circuit Breaker (per token, кластер)
- Redis-backed Rate Limiter (per token+chat, кластер)
- Backpressure на входе (Taskiq semaphore + Redis counter)
- Graceful degradation: при перегрузке — отклоняем новые задачи, не падаем

### Tier 2: ВИДИМ ЧТО ПРОИСХОДИТ (2 дня)
- Prometheus metrics (бизнес + тех)
- Healthcheck endpoint с детальной диагностикой
- Alerting: P0 при dlq>10, P1 при circuit breaker open
- Structured logging с correlation_id

### Tier 3: ВОССТАНАВЛИВАЕТСЯ (2 дня)
- DLQ replay API (не just CLI)
- Dead letter monitoring dashboard
- Automatic stuck post recovery
- Leader election failover <30 сек

### Tier 4: МАСШТАБИРУЕТСЯ (2 дня)
- Worker segmentation by label (posting/stats/billing)
- Horizontal scaling: 1 → N без переконфигурации
- Connection pooling limits (DB, Redis, Telegram)

**Итого: 9 дней вместо 14.5, но с фокусом на нагрузку**

---

## 🔴 Что ОТБРАСЫВАЕМ из оригинала

| Было | Стало | Почему |
|------|-------|--------|
| Идеальный Use Case с media | Text-only MVP + media v2 | 80% постов — текст, media можно докрутить |
| Полный test suite | Smoke + chaos tests only | Unit-тесты не спасут от нагрузки |
| Идеальная документация | README + RUNBOOK minimal | Время лучше потратить на мониторинг |
| Billing в worker'е | Billing в tad-backend (API call) | Finance не должен быть в задачах |

---

## 🏗️ Ключевые архитектурные решения

### 1. Redis-backed Circuit Breaker

```python
# Каждый worker читает/пишет в Redis, а не in-memory
class RedisCircuitBreaker:
    def record_failure(self, token: str):
        # LPUSH failures:{token} {timestamp}
        # LTRIM failures:{token} 0 4  # храним 5 последних
        # Если 5 ошибок за 60 сек → SET circuit:{token} OPEN EX 60
```

**Зачем:** Worker-1 видит ошибки Worker-2, circuit открывается глобально.

### 2. Backpressure на входе

```python
@broker.task()
async def rotate_post(post_id: str):
    # Проверяем глобальную нагрузку
    current_tasks = await redis.incr("load:active_tasks")
    if current_tasks > MAX_CONCURRENT:
        raise BackpressureError("System overloaded, task rejected")
    
    try:
        await do_work()
    finally:
        await redis.decr("load:active_tasks")
```

**Зачем:** При нагрузке "пиздец" — новые задачи отклоняются сразу, не ждут в очереди.

### 3. Graceful degradation levels

```python
LEVEL_0: Normal (< 100 tasks/min)
LEVEL_1: Elevated (100-500) — увеличиваем jitter, batch size ↓
LEVEL_2: High (500-1000) — пропускаем stats collection
LEVEL_3: Critical (>1000) — только posting, остальное REJECT
```

**Зачем:** Главное flow (posting) работает даже при перегрузке.

### 4. DLQ как первоклассный citizen

```python
# DLQ не просто хранилище, а управляемая очередь
class DLQManager:
    async def should_alert(self) -> bool:
        # P0: >10 failed jobs за 5 минут
        # P1: >0 failed jobs старше 1 часа
        pass
    
    async def auto_replay(self, max_per_hour: int = 10):
        # Автоматический replay с backoff
        pass
```

**Зачем:** Оператор не должен копаться в Redis руками.

---

## 📊 Мониторинг: что алертить

### P0 (Wake me up at 3am)
- `dlq_size > 10` — что-то системно ломается
- `circuit_breaker_open > 5 tokens` — массовые баны/проблемы
- `worker_lag_seconds > 300` — задачи висят 5+ минут
- `db_connection_pool_exhausted` — кончились коннекшены

### P1 (Fix tomorrow)
- `dlq_size > 0` — есть failed jobs
- `retry_rate > 20%` — много ретраев
- `telegram_flood_wait_count` — Telegram throttle'ит

### P2 (Trending)
- `posts_per_minute` — бизнес метрика
- `avg_post_latency_seconds` — производительность

---

## 🚀 Дорожная карта (9 дней)

### День 1: Redis-backed State
- [ ] Circuit breaker на Redis
- [ ] Rate limiter на Redis
- [ ] Backpressure counter
- [ ] Degradation levels

### День 2: Observability Core
- [ ] Prometheus metrics endpoint
- [ ] Structured logging (JSON)
- [ ] Correlation ID propagation
- [ ] Healthcheck /health + /ready

### День 3: Alerting + DLQ API
- [ ] DLQ HTTP API (не just CLI)
- [ ] Alert rules (P0/P1/P2)
- [ ] Telegram alerts for P0
- [ ] DLQ auto-replay

### День 4: Resilience Testing
- [ ] Chaos test: kill worker mid-task
- [ ] Chaos test: Redis down 30 сек
- [ ] Chaos test: DB connection leak
- [ ] Load test: 1000 posts/min

### День 5: Worker Segmentation
- [ ] Labels: posting/stats/billing
- [ ] Resource limits per worker type
- [ ] Priority queues

### День 6: Horizontal Scaling
- [ ] Docker Compose для 3+ workers
- [ ] Load balancer healthchecks
- [ ] Rolling deployment без downtime

### День 7: Optimization
- [ ] Connection pool tuning
- [ ] Redis pipeline optimization
- [ ] DB query optimization (EXPLAIN ANALYZE)

### День 8: Runbook + Incident Response
- [ ] RUNBOOK.md (действия при алертах)
- [ ] Playbook: "DLQ растёт"
- [ ] Playbook: "Telegram банит"
- [ ] Playbook: "DB перегружен"

### День 9: Final Testing
- [ ] End-to-end: post → publish → stats
- [ ] Failover test: kill leader
- [ ] Load test: sustained 1000/min
- [ ] Recovery test: DLQ replay

---

## 🎲 Trade-offs (честно)

### Что жертвуем ради сроков:

1. **Media handling** — пока text-only, media в v2
   - *Риск:* 20% постов с фото не будут работать
   - *Mitigation:* фallback на text-only для таких

2. **Unit test coverage** — только критичные пути
   - *Риск:* regression при изменениях
   - *Mitigation:* strong integration tests

3. **Multi-region** — один datacenter
   - *Риск:* если DC упадёт — downtime
   - *Mitigation:* daily backups, RTO 4 hours

4. **Advanced billing** — simple hold/charge only
   - *Риск:* не все edge cases покрыты
   - *Mitigation:* manual reconcile раз в день

### Что НЕ жертвуем ни при каких обстоятельствах:

1. **Data consistency** — ACID, optimistic locking
2. **Idempotency** — никаких double-post
3. **Observability** — всё измеряется
4. **Graceful degradation** — не падаем при нагрузке

---

## ✅ Критерий готовности к "пиздец" нагрузке

```python
# Load test: 1000 posts/min sustained
def test_sustained_load():
    for _ in range(60):  # 1 hour
        send_1000_posts()
        assert p95_latency < 5_000  # ms
        assert error_rate < 0.1%     # 1 error per 1000
        assert dlq_size == 0         # no surprises
        assert memory_stable()       # no leaks
```

**Если этот тест проходит — готовы к production.**

---

## 🤔 Вопросы к команде

1. **Согласны с trade-offs?** (media v2, simple billing)
2. **Какой RTO/RPO приемлемы?** (4 hours / 1 hour?)
3. **Есть ли devops для monitoring stack?** (Prometheus, Grafana)
4. **Когда первый реальный пост?** (через 3 дня? 9 дней?)

**Жду вашего решения.**
