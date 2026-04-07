"""
Pydantic schemas для задач tad-worker.

Эти схемы определяют контракт между tad-backend (producer)
и tad-worker (consumer). Валидация на входе в задачу
предотвращает неявные ошибки в бизнес-логике.

Каждая схема — Pydantic BaseModel с strict валидацией.
"""
from uuid import UUID

from pydantic import BaseModel, Field


class RotatePostPayload(BaseModel):
    """
    Payload для задачи ротации аукционного поста.

    Источник: tad-backend → PostingTaskService.create_auction_post()
    Consumer: tad-worker → rotate_post task
    """

    post_id: str = Field(
        description="UUID поста в БД tad-backend (таблица posts).",
    )


class ScrapeChannelPayload(BaseModel):
    """
    Payload для задачи сбора статистики канала.

    Источник: tad-backend scheduler или ручной запуск.
    Consumer: tad-worker → scrape_channel task.
    """

    platform_id: str = Field(
        description="UUID платформы в БД tad-backend (таблица platforms).",
    )
    priority: bool = Field(
        default=False,
        description="Приоритетный сбор (обрабатывается раньше обычных).",
    )


class CollectMessageStatPayload(BaseModel):
    """
    Payload для задачи сбора статистики одного сообщения.

    Используется для трекинга views/reactions после публикации.
    """

    platform_id: str = Field(
        description="UUID платформы.",
    )
    message_id: int = Field(
        description="Telegram message_id для отслеживания.",
    )
    post_id: str = Field(
        description="UUID поста (для привязки статистики).",
    )


class BillingChargePayload(BaseModel):
    """
    Payload для задачи списания средств.

    Flow: hold → post published → charge.
    Идемпотентность: billing_idempotency_key в PostgreSQL.
    """

    hold_id: str = Field(
        description="UUID hold записи в billing.",
    )
    post_id: str = Field(
        description="UUID поста (для аудита).",
    )
    amount_kopecks: int = Field(
        description="Сумма списания в копейках (целое число, без float arithmetic).",
        gt=0,
    )


class BillingReleasePayload(BaseModel):
    """
    Payload для задачи освобождения холда.

    Вызывается если пост не был опубликован (timeout, ошибка).
    """

    hold_id: str = Field(
        description="UUID hold записи.",
    )
    reason: str = Field(
        default="timeout",
        description="Причина освобождения (timeout, error, manual).",
    )
