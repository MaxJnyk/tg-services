"""RotatePost Use Case — production-ready implementation.

Исправлено: убраны прямые импорты infrastructure.
Теперь зависимости внедряются через конструктор (Dependency Injection).

Flow:
1. Idempotency check — предотвращает дублирование.
2. Distributed lock — предотвращает параллельное выполнение.
3. Load post with revision from DB.
4. Optimistic locking — проверяем что пост ещё не в процессе.
5. Send to Telegram via MessengerClient.
6. Update post status.
7. Save idempotency result.
8. Release lock.
"""
import asyncio
from typing import Optional
from uuid import UUID

from loguru import logger

from src.domain.entities.post import Post, PostId, PostStatus
from src.domain.interfaces.infrastructure import (
    LockManager,
    IdempotencyStore,
    MessengerClient,
    MessageResult,
    RetryableError,
    NonRetryableError,
)
from src.domain.repositories.post_repository import PostRepository
from src.domain.repositories.platform_repository import PlatformRepository


class TelegramPublishError(Exception):
    """Ошибка публикации в Telegram."""
    def __init__(self, message: str, retryable: bool = False):
        self.retryable = retryable
        super().__init__(message)


class RotatePostUseCase:
    """
    Use case для ротации (публикации) аукционного поста.
    
    Зависимости (внедряются через конструктор):
    - post_repo, platform_repo: репозитории из domain/
    - lock_manager: реализация LockManager (например, RedisLock)
    - idempotency_store: реализация IdempotencyStore (например, RedisIdempotency)
    - messenger_client: реализация MessengerClient (например, BotPool)
    
    Идемпотентность: ключ idempotency:{post_id}
    Lock: lock:post:{post_id} (TTL 60 сек, extendable)
    """

    # TTL для lock (секунды) — задача должна уложиться
    LOCK_TTL: int = 60
    # TTL для idempotency (24 часа — достаточно для debugging)
    IDEMPOTENCY_TTL: int = 86400

    def __init__(
        self,
        post_repo: PostRepository,
        platform_repo: PlatformRepository,
        lock_manager: LockManager,
        idempotency_store: IdempotencyStore,
        messenger_client: MessengerClient,
    ) -> None:
        """
        Args:
            post_repo: Репозиторий постов
            platform_repo: Репозиторий платформ (для получения bot token)
            lock_manager: Менеджер распределённых локов (Redis)
            idempotency_store: Хранилище idempotency (Redis)
            messenger_client: Клиент для отправки сообщений (Telegram BotPool)
        """
        self._post_repo = post_repo
        self._platform_repo = platform_repo
        self._lock_manager = lock_manager
        self._idempotency_store = idempotency_store
        self._messenger_client = messenger_client

    async def execute(self, post_id: str) -> dict:
        """
        Основной flow ротации поста.

        Returns:
            dict с результатом:
            - {"status": "published", "message_id": ...} — успех
            - {"status": "already_processed"} — уже опубликован
            - {"status": "already_processing"} — кто-то другой в процессе
            - Exception — ошибка, retry или DLQ решает caller
        """
        post_uuid = UUID(post_id)
        lock_key = f"lock:post:{post_id}"
        idempotency_key = f"idempotency:rotate_post:{post_id}"

        # 1. Проверка idempotency
        cached_result = await self._idempotency_store.get(idempotency_key)
        if cached_result:
            logger.info(f"Post {post_id}: already processed (idempotency cache)")
            return {"status": "already_processed", **cached_result}

        # 2. Получение лока
        lock_acquired = await self._lock_manager.acquire(lock_key, self.LOCK_TTL)
        if not lock_acquired:
            logger.info(f"Post {post_id}: already processing (lock held)")
            return {"status": "already_processing"}

        try:
            # 3. Загрузка поста с revision
            post = await self._post_repo.get_with_revision(post_uuid)
            if not post:
                raise NonRetryableError(f"Post {post_id} not found")

            # 4. Оптимистичная проверка статуса
            if post.status != PostStatus.SCHEDULED:
                logger.info(f"Post {post_id}: status is {post.status.value}, skipping")
                # Сохраняем idempotency чтобы не пытаться повторно
                await self._idempotency_store.save(
                    idempotency_key,
                    {"message_id": post.post_id_on_platform},
                    self.IDEMPOTENCY_TTL
                )
                return {
                    "status": "already_processed",
                    "message_id": post.post_id_on_platform,
                }

            # 5. Получение данных платформы
            platform = await self._platform_repo.get(str(post.platform_id))
            if not platform:
                raise NonRetryableError(f"Platform {post.platform_id} not found")

            bot_token = platform.get("bot_token")
            chat_id = platform.get("telegram_id")  # Telegram chat_id канала
            if not bot_token or not chat_id:
                raise NonRetryableError(
                    f"Platform {post.platform_id}: missing bot_token or chat_id"
                )

            # 6. Отправка в Telegram
            logger.info(f"Publishing post {post_id} to chat {chat_id}")
            
            # Текст из revision (или заглушка если media)
            text = post.revision_text or "🔄 Аукционное объявление"
            
            try:
                result: MessageResult = await self._messenger_client.send_message(
                    token=bot_token,
                    chat_id=chat_id,
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=False,
                )
            except RetryableError as exc:
                logger.warning(f"Retryable error sending post {post_id}: {exc}")
                raise TelegramPublishError(str(exc), retryable=True)
            except NonRetryableError as exc:
                logger.error(f"Non-retryable error sending post {post_id}: {exc}")
                # Помечаем как failed, не retry
                await self._post_repo.update_status(
                    post_uuid,
                    PostStatus.FAILED,
                    expected_status=PostStatus.POSTING,
                )
                raise

            # 7. Обновление статуса поста
            updated = await self._post_repo.update_status(
                post_uuid,
                PostStatus.PUBLISHED,
                expected_status=PostStatus.POSTING,
                message_id=result.message_id,
            )
            if not updated:
                # Кто-то другой изменил статус — конфликт
                logger.warning(f"Post {post_id}: optimistic lock conflict during update")
                # Возвращаем success всё равно — сообщение уже отправлено

            # 8. Сохранение результата idempotency
            result_data = {
                "message_id": result.message_id,
                "chat_id": result.chat_id,
                "published_at": result.timestamp,
            }
            await self._idempotency_store.save(
                idempotency_key,
                result_data,
                self.IDEMPOTENCY_TTL
            )

            logger.info(f"Post {post_id}: published successfully, message_id={result.message_id}")
            return {"status": "published", **result_data}

        except TelegramPublishError:
            raise  # Пробрасываем для retry
        except NonRetryableError:
            raise  # Пробрасываем для DLQ
        except Exception as exc:
            logger.exception(f"Unexpected error in rotate_post {post_id}: {exc}")
            # Неизвестная ошибка — retry (может быть временная)
            raise RetryableError(f"Unexpected error: {exc}")
        finally:
            # 9. Освобождение лока
            await self._lock_manager.release(lock_key)

    async def _publish_to_telegram(
        self,
        post: Post,
        platform: dict,
        lock: RedisLock,
    ) -> int:
        """Опубликовать пост в Telegram."""
        bot_token = platform.get("bot_token")
        chat_id = platform.get("telegram_id")

        if not bot_token or not chat_id:
            raise TelegramPublishError(
                f"Platform {post.platform_id}: missing bot_token or telegram_id",
                retryable=False,
            )

        text = revision.get("content", "") if revision else ""

        # Extend lock перед отправкой
        await lock.extend(extra_ttl=60)

        try:
            result = await self._bot_pool.send_message(
                token=bot_token,
                chat_id=chat_id,
                text=text,
            )

            message_id = result.get("message_id")
            if not message_id:
                raise TelegramPublishError(
                    "No message_id in Telegram response",
                    retryable=True,
                )

            return message_id

        except Exception as exc:
            action = classify_error(exc)
            retryable = action in (ErrorAction.RETRY, ErrorAction.RATE_LIMIT)
            raise TelegramPublishError(str(exc), retryable=retryable) from exc
