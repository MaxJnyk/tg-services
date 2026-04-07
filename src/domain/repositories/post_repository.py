"""Post Repository ABC interface."""
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple
from uuid import UUID

from src.domain.entities.post import Post, PostId, PostStatus


class PostRepository(ABC):
    @abstractmethod
    async def get(self, post_id: PostId) -> Optional[Post]:
        """Get post by ID."""
        pass

    @abstractmethod
    async def get_with_revision(
        self,
        post_id: PostId,
    ) -> Optional[Tuple[Post, Optional[dict]]]:
        """
        Get post with current revision data (content, media, etc.).

        Returns:
            Tuple of (Post, revision_data) or None if post not found.
            revision_data contains content, content_type, erid, images.
        """
        pass

    @abstractmethod
    async def save(self, post: Post) -> None:
        """Save post."""
        pass

    @abstractmethod
    async def update_status(
        self,
        post_id: PostId,
        new_status: PostStatus,
        expected_current_status: Optional[PostStatus] = None,
    ) -> bool:
        """
        Update post status with optimistic locking.

        Args:
            post_id: Post ID.
            new_status: New status to set.
            expected_current_status: Expected current status for optimistic locking.

        Returns:
            True if update succeeded, False if status was already changed.
        """
        pass

    @abstractmethod
    async def set_message_id(
        self,
        post_id: PostId,
        message_id: int,
    ) -> None:
        """
        Set message_id (post_id_on_platform in DB).

        Called after successful Telegram publication.
        """
        pass

    @abstractmethod
    async def set_current_revision(
        self,
        post_id: PostId,
        revision_id: UUID,
    ) -> None:
        """Set current revision for post."""
        pass

    @abstractmethod
    async def get_pending_for_platform(self, platform_id: str) -> List[Post]:
        """Get pending posts for platform."""
        pass
