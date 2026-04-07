# TAD-Worker: План Б — Модульный монолит

> Для техлидов без dedicated DevOps
> Принцип: Один codebase, множественные deployment profiles
> Дата: 2026-04-07

---

## 🏗️ Архитектура: Модульный монолит

```
┌────────────────────────────────────────────────────────────────┐
│                    tad-worker (один образ)                     │
│                                                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Posting    │  │    Stats     │  │   Billing    │       │
│  │   Module     │  │   Module     │  │   Module     │       │
│  │              │  │              │  │              │       │
│  │ • rotate_post│  │ • scrape     │  │ • hold       │       │
│  │ • edit_post  │  │ • collect    │  │ • charge     │       │
│  │ • delete_post│  │ • message    │  │ • release    │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │                │
│         └─────────────────┼─────────────────┘                │
│                           │                                  │
│                    ┌──────▼──────┐                          │
│                    │ Taskiq      │                          │
│                    │ Broker      │                          │
│                    │ (Redis)     │                          │
│                    └──────┬──────┘                          │
│                           │                                  │
└───────────────────────────┼──────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
   ┌─────────┐       ┌─────────┐       ┌─────────┐
   │ Worker  │       │ Worker  │       │ Worker  │
   │ #1      │       │ #2      │       │ #3      │
   │         │       │         │       │         │
   │--labels │       │--labels │       │--labels │
   │posting  │       │stats    │       │billing  │
   └─────────┘       └─────────┘       └─────────┘
```

### Профили деплоя:

| Profile | Команда | Workers | Для чего |
|---------|---------|---------|----------|
| **dev** | `taskiq worker --workers 2` | 2 | Локальная разработка |
| **posting** | `taskiq worker --labels posting --workers 8` | 8 | Публикация постов |
| **stats** | `taskiq worker --labels stats --workers 4` | 4 | Сбор статистики |
| **billing** | `taskiq worker --labels billing --workers 2` | 2 | Финансовые операции |
| **all** | `taskiq worker --workers 4` | 4 | Минимальный prod (всё вместе) |

---

## 📁 Структура модулей

```
src/
├── modules/
│   ├── posting/           # Всё для постинга
│   │   ├── tasks.py       # @broker.task с label="posting"
│   │   ├── use_cases.py   # RotatePostUseCase, etc.
│   │   └── __init__.py
│   │
│   ├── stats/             # Всё для статистики
│   │   ├── tasks.py       # @broker.task с label="stats"
│   │   ├── use_cases.py   # ScrapeChannelUseCase, etc.
│   │   └── __init__.py
│   │
│   └── billing/           # Всё для биллинга
│       ├── tasks.py       # @broker.task с label="billing"
│       ├── use_cases.py   # HoldBalanceUseCase, etc.
│       └── __init__.py
│
├── shared/                # Общее (models, repos, infra)
│   ├── models/
│   ├── repositories/
│   └── infrastructure/
│
└── entrypoints/
    ├── broker.py          # Один broker для всех
    ├── worker.py          # Один worker entrypoint
    └── scheduler.py       # Один scheduler (leader election)
```

---

## 🚀 Deployment Profiles (Docker Compose)

### Profile: dev (локальная разработка)
```yaml
# docker-compose.yml
services:
  worker:
    build: .
    command: taskiq worker src.entrypoints.broker:broker --workers 2
    environment:
      - LABELS=posting,stats,billing  # Все задачи
```

### Profile: production (разделённые workers)
```yaml
# docker-compose.prod.yml
services:
  worker-posting:
    build: .
    command: taskiq worker src.entrypoints.broker:broker --labels posting --workers 8
    deploy:
      replicas: 2  # 2 контейнера × 8 workers = 16 posting workers

  worker-stats:
    build: .
    command: taskiq worker src.entrypoints.broker:broker --labels stats --workers 4
    deploy:
      replicas: 1

  worker-billing:
    build: .
    command: taskiq worker src.entrypoints.broker:broker --labels billing --workers 2
    deploy:
      replicas: 1
```

### Profile: minimal (дешёвый старт)
```yaml
# docker-compose.minimal.yml
services:
  worker:
    build: .
    command: taskiq worker src.entrypoints.broker:broker --workers 4
    # Все задачи в одном контейнере (дешево, но рискованно)
```

---

## 📊 Масштабирование без Kubernetes

### Шаг 1: Один сервер (startup)
```
Сервер #1:
  - tad-backend (API + scheduler)
  - tad-worker --labels posting,stats,billing --workers 4
  - PostgreSQL
  - Redis
```

### Шаг 2: Два сервера (рост)
```
Сервер #1 (API + Scheduler + Posting):
  - tad-backend
  - tad-worker --labels posting --workers 8
  - PostgreSQL (master)
  - Redis

Сервер #2 (Stats + Billing):
  - tad-worker --labels stats,billing --workers 4
  - PostgreSQL (read replica, если нужно)
```

### Шаг 3: Три сервера (scale)
```
Сервер #1 (API + Posting):
  - tad-backend
  - tad-worker --labels posting --workers 8 (×2 replicas)

Сервер #2 (Stats):
  - tad-worker --labels stats --workers 4

Сервер #3 (Billing + DB + Cache):
  - tad-worker --labels billing --workers 2
  - PostgreSQL
  - Redis
```

### Шаг 4: N серверов (без Kubernetes)
```
- Docker Swarm Mode (встроен в Docker)
- Или просто nginx upstream для балансировки
- Или Consul + Fabio (service discovery)
```

---

## 🔧 Практика: Деплой новой версии

### Rolling deployment без downtime:
```bash
# 1. Постинг workers (критично, делаем осторожно)
docker-compose up -d worker-posting-2  # Новая версия
sleep 30  # Ждём стабилизации
docker-compose stop worker-posting-1   # Старая версия

# 2. Stats workers (менее критично)
docker-compose up -d worker-stats-2
sleep 10
docker-compose stop worker-stats-1

# 3. Billing workers (самые осторожные — деньги)
docker-compose up -d worker-billing-2
sleep 30
# Проверяем, что транзакции не потерялись
docker-compose stop worker-billing-1
```

### Если что-то сломалось:
```bash
# Rollback за 10 секунд
docker-compose stop worker-posting-2
docker-compose start worker-posting-1
```

---

## 💰 Cost optimization

### Вариант "Дёшево" (начало):
- Один сервер $20/month
- Один контейнер: `taskiq worker --workers 4`
- Все модули вместе

### Вариант "Надёжно" (рост):
- Два сервера $40/month
- Posting отдельно (8 workers × 2 replicas)
- Stats + Billing вместе

### Вариант "Масштаб" (пиздец нагрузка):
- Три сервера $60/month
- Каждый модуль на своём сервере
- PostgreSQL на dedicated сервере ($20)

---

## 🎯 План Б — Обновлённые сроки

| Фаза | Дни | Что делаем | Результат |
|------|-----|-----------|-----------|
| **0** | 1 | Config, DB, Redis, DLQ | Инфра готова |
| **1** | 1.5 | Taskiq broker, модули posting/stats/billing | Модульный монолит |
| **2** | 1.5 | BotPool + UserbotPool | Telegram интеграция |
| **3** | 1.5 | Posting use case (RedisLock, Idempotency) | Публикация работает |
| **4** | 1 | Scheduler + Leader election | Cron задачи |
| **5** | 1.5 | Monitoring + Alerting | Видим что происходит |
| **6** | 1 | Docker Compose профили | 3 варианта деплоя |
| **7** | 1 | Load testing + Chaos engineering | 1000/min устойчиво |
| **8** | 1 | Runbook + Documentation | On-call ready |
| **ИТОГО** | **10 дней** | | **Production-ready** |

**Было:** 14.5 дней (сложная интеграция)
**Стало:** 10 дней (модульный монолит, чистая архитектура)

---

## ✅ Чеклист "Готовы к пиздец-нагрузке"

- [ ] Профиль `posting` работает на 2+ серверах
- [ ] Профиль `stats` не мешает `posting` (разные контейнеры)
- [ ] DLQ alerts в Telegram (P0 при >10 failed)
- [ ] Rollback работает за <30 секунд
- [ ] Load test: 1000 posts/min sustained 1 hour
- [ ] Chaos test: kill worker mid-task → no data loss
- [ ] Chaos test: Redis down 30s → graceful recovery
- [ ] Восстановление после сбоя за <5 минут по RUNBOOK

---

## 🚀 Старт завтра

**День 1:**
```bash
# Утро: структура модулей
mkdir -p src/modules/{posting,stats,billing}
touch src/modules/{posting,stats,billing}/__init__.py

# День: перенос posting логики
cp src/application/posting/* src/modules/posting/

# Вечер: первый тест
python -m src.cli.dlq stats  # DLQ работает?
```

Согласен с планом? Или хочешь что-то подкорректировать?
