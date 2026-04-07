from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from src.domain.value_objects.post_id import PostId


class PostStatus(Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Post:
    id: PostId
    platform_id: str
    status: PostStatus
    current_revision_id: Optional[str] = None
    message_id: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    published_at: Optional[datetime] = None

    def publish(self, message_id: int):
        if self.status != PostStatus.PENDING:
            raise ValueError(f"Cannot publish post in status {self.status}")
        self.message_id = message_id
        self.published_at = datetime.utcnow()
        self.status = PostStatus.PUBLISHED

    def complete(self):
        if self.status != PostStatus.PUBLISHED:
            raise ValueError(f"Cannot complete post in status {self.status}")
        self.status = PostStatus.COMPLETED

    def fail(self, reason: str):
        self.status = PostStatus.FAILED
