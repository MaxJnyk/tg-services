"""
Dead Letter Queue на Redis HSET + Sorted Set.

Предыдущая реализация на LIST имела O(n) lookup по job_id.
Новая: HSET для O(1) доступа + ZADD для range queries по времени.

Структура Redis:
    HSET  dlq:jobs     {job_id} → {json_payload}
    ZADD  dlq:timeline {timestamp} {job_id}

attempt_history внутри JSON payload — полная история попыток
с timestamp, error, worker_id для траблшутинга.
"""
import json
import time
from datetime import datetime, timezone

from loguru import logger
from redis.asyncio import Redis


class DeadLetterQueue:
    """
    Dead Letter Queue для failed задач.

    Когда задача падает max_retries раз — она попадает в DLQ
    и может быть перезапущена вручную через CLI или API.

    Хранилище: Redis HSET (O(1) по job_id) + Sorted Set (range по времени).
    """

    MAX_REPLAY: int = 3
    MAX_ENTRY_SIZE: int = 10_000
    ALERT_THRESHOLD: int = 10
    COMPLETED_TTL_DAYS: int = 7

    # Redis key names
    JOBS_KEY: str = "dlq:jobs"
    TIMELINE_KEY: str = "dlq:timeline"

    def __init__(self, redis_client: Redis) -> None:
        self.redis = redis_client

    async def enqueue(
        self,
        job_name: str,
        args: tuple | list,
        kwargs: dict,
        error: str,
        worker_id: str = "unknown",
    ) -> str:
        """
        Добавить failed задачу в DLQ.

        O(1) HSET + O(log n) ZADD.

        Args:
            job_name: имя задачи (e.g. "rotate_post").
            args: позиционные аргументы задачи.
            kwargs: именованные аргументы задачи.
            error: текст ошибки (обрезается до 500 символов).
            worker_id: ID worker'а, где задача упала.

        Returns:
            job_id для последующего replay/get_job.
        """
        now = datetime.now(tz=timezone.utc)
        job_id = f"{job_name}:{now.isoformat()}"
        timestamp = now.timestamp()

        entry = {
            "id": job_id,
            "job_name": job_name,
            "args": self._truncate(args),
            "kwargs": self._truncate(kwargs),
            "status": "failed",
            "replay_count": 0,
            "created_at": now.isoformat(),
            "attempt_history": [
                {
                    "timestamp": now.isoformat(),
                    "error": error[:500],
                    "worker_id": worker_id,
                },
            ],
        }

        # HSET + ZADD — атомарно через pipeline
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.JOBS_KEY, job_id, json.dumps(entry, default=str))
            pipe.zadd(self.TIMELINE_KEY, {job_id: timestamp})
            await pipe.execute()

        # Проверка алерта
        size = await self.get_size()
        if size >= self.ALERT_THRESHOLD:
            logger.critical(
                f"DLQ ALERT: {size} failed jobs in queue (threshold: {self.ALERT_THRESHOLD})"
            )

        logger.warning(f"Job {job_name} moved to DLQ (id={job_id}, error={error[:100]})")
        return job_id

    async def get_job(self, job_id: str) -> dict | None:
        """
        Получить задачу по ID. O(1).

        Returns:
            dict с данными задачи или None если не найдена.
        """
        raw = await self.redis.hget(self.JOBS_KEY, job_id)
        if raw is None:
            return None
        return json.loads(raw)

    async def replay(self, job_id: str) -> dict:
        """
        Пометить задачу для replay. Увеличивает replay_count.

        Raises:
            KeyError: если job_id не найден в DLQ.
            ValueError: если replay_count >= MAX_REPLAY.

        Returns:
            Обновлённый dict задачи (для повторного запуска).
        """
        entry = await self.get_job(job_id)
        if entry is None:
            raise KeyError(f"Job {job_id} not found in DLQ")

        if entry.get("replay_count", 0) >= self.MAX_REPLAY:
            raise ValueError(
                f"Job {job_id}: max replays ({self.MAX_REPLAY}) exceeded"
            )

        entry["replay_count"] += 1
        entry["status"] = "replaying"

        await self.redis.hset(self.JOBS_KEY, job_id, json.dumps(entry, default=str))
        logger.info(f"Job {job_id} marked for replay (attempt {entry['replay_count']})")
        return entry

    async def mark_completed(self, job_id: str) -> None:
        """Пометить задачу как успешно выполненную после replay."""
        entry = await self.get_job(job_id)
        if entry is None:
            return

        entry["status"] = "completed"
        entry["completed_at"] = datetime.now(tz=timezone.utc).isoformat()
        await self.redis.hset(self.JOBS_KEY, job_id, json.dumps(entry, default=str))
        logger.info(f"Job {job_id} marked as completed")

    async def record_attempt(
        self,
        job_id: str,
        error: str,
        worker_id: str = "unknown",
    ) -> None:
        """
        Записать неудачную попытку в attempt_history.

        Вызывается при повторном падении задачи после replay.
        """
        entry = await self.get_job(job_id)
        if entry is None:
            return

        entry["attempt_history"].append({
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "error": error[:500],
            "worker_id": worker_id,
        })
        entry["status"] = "failed"

        await self.redis.hset(self.JOBS_KEY, job_id, json.dumps(entry, default=str))

    async def list_all(
        self,
        limit: int = 100,
        status: str | None = None,
    ) -> list[dict]:
        """
        Получить задачи из DLQ, отсортированные по времени (новые первые).

        Использует ZREVRANGEBYSCORE для пагинации — НЕ загружает весь DLQ в память.

        Args:
            limit: максимальное количество задач.
            status: фильтр по статусу (failed, replaying, completed). None = все.

        Returns:
            Список задач.
        """
        # Получаем job_ids из Sorted Set (новые первые)
        job_ids: list[str] = await self.redis.zrevrange(self.TIMELINE_KEY, 0, limit - 1)

        if not job_ids:
            return []

        # Batch HGET через pipeline — один round-trip к Redis
        async with self.redis.pipeline(transaction=False) as pipe:
            for jid in job_ids:
                pipe.hget(self.JOBS_KEY, jid)
            raw_values = await pipe.execute()

        jobs: list[dict] = []
        for raw in raw_values:
            if raw is None:
                continue
            entry = json.loads(raw)
            if status is None or entry.get("status") == status:
                jobs.append(entry)

        return jobs

    async def get_stale_jobs(self, older_than_seconds: int = 3600) -> list[dict]:
        """
        Найти задачи старше N секунд. Для мониторинга и алертов.

        Использует ZRANGEBYSCORE — O(log n + k), НЕ загружает весь DLQ.

        Args:
            older_than_seconds: порог в секундах (default: 1 час).

        Returns:
            Список старых задач.
        """
        cutoff = time.time() - older_than_seconds
        job_ids: list[str] = await self.redis.zrangebyscore(
            self.TIMELINE_KEY, "-inf", cutoff,
        )

        if not job_ids:
            return []

        async with self.redis.pipeline(transaction=False) as pipe:
            for jid in job_ids:
                pipe.hget(self.JOBS_KEY, jid)
            raw_values = await pipe.execute()

        return [json.loads(raw) for raw in raw_values if raw is not None]

    async def clear(self, only_completed: bool = False) -> int:
        """
        Очистить DLQ.

        Args:
            only_completed: если True — удалить только completed задачи.

        Returns:
            Количество удалённых задач.
        """
        if not only_completed:
            # Удаляем оба ключа атомарно
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.delete(self.JOBS_KEY)
                pipe.delete(self.TIMELINE_KEY)
                results = await pipe.execute()
            removed = results[0]  # HSET delete returns number of keys
            logger.info(f"DLQ cleared ({removed} jobs removed)")
            return removed

        # Удаляем только completed — scan HSET, фильтруем, удаляем batch
        removed = 0
        cursor = "0"
        while True:
            cursor, items = await self.redis.hscan(
                self.JOBS_KEY, cursor=cursor, count=100,
            )
            for job_id, raw in items.items():
                entry = json.loads(raw)
                if entry.get("status") == "completed":
                    async with self.redis.pipeline(transaction=True) as pipe:
                        pipe.hdel(self.JOBS_KEY, job_id)
                        pipe.zrem(self.TIMELINE_KEY, job_id)
                        await pipe.execute()
                    removed += 1

            if cursor == "0":
                break

        logger.info(f"DLQ cleared {removed} completed jobs")
        return removed

    async def cleanup_old_completed(self, max_age_days: int | None = None) -> int:
        """
        Удалить completed задачи старше N дней. Для cron job.

        Args:
            max_age_days: максимальный возраст в днях. Default: COMPLETED_TTL_DAYS (7).

        Returns:
            Количество удалённых задач.
        """
        days = max_age_days or self.COMPLETED_TTL_DAYS
        cutoff = time.time() - (days * 86400)

        # Находим старые job_ids
        old_job_ids: list[str] = await self.redis.zrangebyscore(
            self.TIMELINE_KEY, "-inf", cutoff,
        )

        if not old_job_ids:
            return 0

        removed = 0
        for job_id in old_job_ids:
            raw = await self.redis.hget(self.JOBS_KEY, job_id)
            if raw is None:
                # Orphan в timeline — удаляем
                await self.redis.zrem(self.TIMELINE_KEY, job_id)
                removed += 1
                continue

            entry = json.loads(raw)
            if entry.get("status") == "completed":
                async with self.redis.pipeline(transaction=True) as pipe:
                    pipe.hdel(self.JOBS_KEY, job_id)
                    pipe.zrem(self.TIMELINE_KEY, job_id)
                    await pipe.execute()
                removed += 1

        if removed > 0:
            logger.info(f"DLQ cleanup: removed {removed} old completed jobs (>{days} days)")
        return removed

    async def get_size(self) -> int:
        """Текущий размер DLQ. O(1) через HLEN."""
        return await self.redis.hlen(self.JOBS_KEY)

    def _truncate(self, data: tuple | list | dict) -> tuple | list | dict | str:
        """Ограничить размер данных для предотвращения memory bloat в Redis."""
        as_json = json.dumps(data, default=str)
        if len(as_json) > self.MAX_ENTRY_SIZE:
            return as_json[:self.MAX_ENTRY_SIZE] + "... [truncated]"
        return data
