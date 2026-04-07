#!/usr/bin/env python3
"""CLI для управления Dead Letter Queue."""
import asyncio
import sys
from datetime import datetime

from loguru import logger
from src.core.redis import get_dlq_redis
from src.infrastructure.queue.dlq import DeadLetterQueue


async def list_jobs(status: str = None, limit: int = 100):
    """Показать задачи в DLQ."""
    redis = await get_dlq_redis()
    dlq = DeadLetterQueue(redis)

    jobs = await dlq.list_all(limit=limit, status=status)

    if not jobs:
        print("DLQ пуста")
        return

    print(f"\n{'ID':<50} {'Status':<12} {'Replay':<8} {'Last Error':<30}")
    print("=" * 100)

    for job in jobs:
        job_id = job.get('id', 'N/A')[:48]
        job_status = job.get('status', 'unknown')
        replay = job.get('replay_count', 0)
        # Последняя ошибка из attempt_history
        history = job.get('attempt_history', [])
        last_error = history[-1].get('error', '')[:28] if history else 'N/A'
        print(f"{job_id:<50} {job_status:<12} {replay:<8} {last_error:<30}")

    print(f"\nВсего: {len(jobs)} задач")


async def replay_job(job_id: str):
    """Перезапустить задачу."""
    redis = await get_dlq_redis()
    dlq = DeadLetterQueue(redis)

    try:
        await dlq.replay(job_id)
        print(f"✅ Задача {job_id} помечена для replay")
    except KeyError:
        print(f"❌ Задача {job_id} не найдена")
        sys.exit(1)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)


async def clear_dlq(only_completed: bool = False):
    """Очистить DLQ."""
    redis = await get_dlq_redis()
    dlq = DeadLetterQueue(redis)

    removed = await dlq.clear(only_completed=only_completed)

    if only_completed:
        print(f"✅ Удалено {removed} completed задач")
    else:
        print(f"✅ DLQ полностью очищена ({removed} задач удалено)")


async def stats():
    """Статистика DLQ."""
    redis = await get_dlq_redis()
    dlq = DeadLetterQueue(redis)

    total = await dlq.get_size()
    failed = len(await dlq.list_all(status="failed"))
    replaying = len(await dlq.list_all(status="replaying"))
    completed = len(await dlq.list_all(status="completed"))

    print(f"\nDLQ Statistics")
    print("=" * 30)
    print(f"Total:     {total}")
    print(f"Failed:    {failed}")
    print(f"Replaying: {replaying}")
    print(f"Completed: {completed}")

    if total >= dlq.ALERT_THRESHOLD:
        print(f"\nALERT: DLQ size ({total}) >= threshold ({dlq.ALERT_THRESHOLD})")


def main():
    """Entry point."""
    if len(sys.argv) < 2:
        print("""
Usage: python -m src.cli.dlq <command> [args]

Commands:
    list [status] [limit]  - Показать задачи (status: failed/replaying/completed)
    replay <job_id>        - Перезапустить задачу
    clear [--completed]     - Очистить DLQ (только completed если флаг)
    stats                   - Статистика DLQ

Examples:
    python -m src.cli.dlq list failed 50
    python -m src.cli.dlq replay rotate_post:2024-01-01T12:00:00
    python -m src.cli.dlq clear --completed
        """)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "list":
        status = sys.argv[2] if len(sys.argv) > 2 else None
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 100
        asyncio.run(list_jobs(status, limit))
    
    elif command == "replay":
        if len(sys.argv) < 3:
            print("Usage: python -m src.cli.dlq replay <job_id>")
            sys.exit(1)
        asyncio.run(replay_job(sys.argv[2]))
    
    elif command == "clear":
        only_completed = "--completed" in sys.argv
        asyncio.run(clear_dlq(only_completed))
    
    elif command == "stats":
        asyncio.run(stats())
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
