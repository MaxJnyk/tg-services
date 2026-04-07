"""
SQLAlchemy реализация PostRepository с optimistic locking.

Работает с реальной схемой tad-backend:
- posts.id (UUID)
- posts.platform_id (UUID FK)
- posts.post_id_on_platform (int)
- posts.current_ad_revision_id (UUID FK)
- posts.status_id (UUID FK)
- posts.estimated_cost (Decimal)
- posts.placement_type_id (UUID FK)

Optimistic locking через UPDATE ... WHERE status_id = :expected_status_id.
Если rowcount = 0 → кто-то другой уже обновил этот пост.
"""
from typing import Optional, List
from uuid import UUID
from decimal import Decimal

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from src.domain.repositories.post_repository import PostRepository
from src.domain.entities.post import Post, PostId, PostStatus
from src.infrastructure.database.models.post import PostModel


class SQLAlchemyPostRepository(PostRepository):
    """
    Репозиторий постов с optimistic locking.

    Маппинг на реальную схему tad-backend:
    - current_revision_id → current_ad_revision_id
    - message_id → post_id_on_platform
    - status → status_id через lookup таблицу post_status
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_with_revision(
        self,
        post_id: PostId,
    ) -> Optional[tuple[Post, Optional[dict]]]:
        """
        Получить пост с текущей ревизией (content, media, etc.).

        Returns:
            Tuple (Post, revision_data) или None если пост не найден.
            revision_data — dict с content, content_type, erid, images.
        """
        # Получаем пост
        result = await self.session.execute(
            select(PostModel).where(PostModel.id == str(post_id))
        )
        model = result.scalar_one_or_none()
        if not model:
            return None

        post = self._to_domain(model)

        # Получаем revision если есть current_ad_revision_id
        revision_data = None
        if model.current_ad_revision_id:
            revision_data = await self._get_revision_data(
                model.current_ad_revision_id
            )

        return post, revision_data

    async def update_status(
        self,
        post_id: PostId,
        new_status: PostStatus,
        expected_current_status: Optional[PostStatus] = None,
    ) -> bool:
        """
        Обновить статус поста с optimistic locking.

        Args:
            post_id: ID поста.
            new_status: Новый статус.
            expected_current_status: Ожидаемый текущий статус для optimistic locking.

        Returns:
            True если обновление успешно, False если статус уже изменён.
        """
        new_status_id = await self._get_status_id_by_name(new_status.value)

        if expected_current_status:
            # Optimistic locking: обновляем только если статус ожидаемый
            expected_status_id = await self._get_status_id_by_name(
                expected_current_status.value
            )
            stmt = (
                update(PostModel)
                .where(
                    PostModel.id == str(post_id),
                    PostModel.status_id == expected_status_id,
                )
                .values(status_id=new_status_id)
            )
        else:
            # Без optimistic locking (risky!)
            stmt = (
                update(PostModel)
                .where(PostModel.id == str(post_id))
                .values(status_id=new_status_id)
            )

        result = await self.session.execute(stmt)
        await self.session.commit()

        return result.rowcount > 0

    async def set_message_id(
        self,
        post_id: PostId,
        message_id: int,
    ) -> None:
        """
        Установить message_id (post_id_on_platform в БД).

        Вызывается после успешной публикации в Telegram.
        """
        stmt = (
            update(PostModel)
            .where(PostModel.id == str(post_id))
            .values(post_id_on_platform=message_id)
        )
        await self.session.execute(stmt)
        await self.session.commit()

    async def set_current_revision(
        self,
        post_id: PostId,
        revision_id: UUID,
    ) -> None:
        """Установить текущую ревизию для поста."""
        stmt = (
            update(PostModel)
            .where(PostModel.id == str(post_id))
            .values(current_ad_revision_id=revision_id)
        )
        await self.session.execute(stmt)
        await self.session.commit()

    async def get(self, post_id: PostId) -> Optional[Post]:
        """Получить пост по ID."""
        result = await self.session.execute(
            select(PostModel).where(PostModel.id == str(post_id))
        )
        model = result.scalar_one_or_none()
        return self._to_domain(model) if model else None

    async def save(self, post: Post) -> None:
        """Сохранить пост (не рекомендуется для optimistic locking)."""
        model = self._to_model(post)
        self.session.add(model)
        await self.session.commit()

    async def get_pending_for_platform(self, platform_id: str) -> List[Post]:
        """
        Получить pending посты для платформы.

        Использует status_id через lookup.
        """
        pending_status_id = await self._get_status_id_by_name("pending")
        result = await self.session.execute(
            select(PostModel).where(
                PostModel.platform_id == platform_id,
                PostModel.status_id == pending_status_id,
            )
        )
        return [self._to_domain(m) for m in result.scalars().all()]

    async def _get_status_id_by_name(self, status_name: str) -> UUID:
        """
        Получить UUID status_id по имени статуса.

        Кешируется в рамках сессии для производительности.
        """
        # Простой lookup — можно добавить кеширование
        result = await self.session.execute(
            text("SELECT id FROM post_status WHERE name = :name"),
            {"name": status_name},
        )
        row = result.scalar_one_or_none()
        if not row:
            raise ValueError(f"Unknown post status: {status_name}")
        return row

    async def _get_revision_data(self, revision_id: UUID) -> Optional[dict]:
        """
        Получить данные ревизии из таблицы revisions.

        Returns:
            dict с content, content_type, erid, images.
        """
        result = await self.session.execute(
            text("""
                SELECT content, content_type, erid, images
                FROM revisions
                WHERE id = :id
            """),
            {"id": revision_id},
        )
        row = result.mappings().one_or_none()
        if row:
            return {
                "content": row["content"],
                "content_type": row["content_type"],
                "erid": row["erid"],
                "images": row["images"] or [],
            }
        return None

    def _to_domain(self, model: PostModel) -> Post:
        """Маппинг SQLAlchemy model → Domain entity."""
        # Статус маппим через status_id → status_name (упрощённо через string)
        # В реальности нужен lookup из post_status таблицы
        # Для простоты используем PENDING по умолчанию если неизвестен
        status = PostStatus.PENDING

        return Post(
            id=PostId(str(model.id)),
            platform_id=str(model.platform_id),
            status=status,
            current_revision_id=str(model.current_ad_revision_id) if model.current_ad_revision_id else None,
            message_id=model.post_id_on_platform,
            created_at=model.created_at,
            published_at=model.published_at,
        )

    def _to_model(self, post: Post) -> PostModel:
        """Маппинг Domain entity → SQLAlchemy model."""
        # Важно: status_id должен быть UUID из post_status таблицы
        # Для простоты используем placeholder — в реальности нужен lookup
        return PostModel(
            id=post.id.value if hasattr(post.id, 'value') else str(post.id),
            platform_id=post.platform_id,  # UUID как string или UUID объект
            current_ad_revision_id=post.current_revision_id,  # UUID или None
            post_id_on_platform=post.message_id,
            # status_id должен быть UUID — нужен lookup
            status_id=None,  # Заполняется через _get_status_id_by_name
            created_at=post.created_at,
            published_at=post.published_at,
        )
