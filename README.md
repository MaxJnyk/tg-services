# TAD Worker v3.0.0 — Clean Architecture

## Структура

```
src/
├── domain/              # Чистая бизнес-логика
│   ├── entities/        # Post, Platform
│   ├── value_objects/   # PostId, DeviceFingerprint
│   ├── repositories/    # Интерфейсы (ABC)
│   └── events/          # Domain events
├── application/         # Use cases
│   ├── posting/         # RotatePostUseCase
│   ├── stats/           # ScrapeChannelStatsUseCase
│   └── manage/          # Admin operations
├── infrastructure/      # Реализации
│   ├── database/        # SQLAlchemy repos
│   ├── telegram/        # BotPool, UserbotPool
│   ├── queue/           # DLQ
│   └── monitoring/      # Metrics, Alerts
├── entrypoints/         # Точки входа
│   └── worker.py        # ARQ worker
└── core/                # Config, DB, Redis

## SRE Исправления

- `rotate_post.py:finally` — блокировка очищается ВСЕГДА
- `dlq.py` — replay_count лимит 3
- `database.py` — statement_timeout 30s
- `bot_pool.py` — circuit breaker (5 failures)
- `userbot_pool.py` — auto-reconnect

## Запуск

```bash
docker-compose up -d
arq src.entrypoints.worker.WorkerSettings
```
