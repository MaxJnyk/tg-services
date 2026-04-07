from dataclasses import dataclass
from datetime import datetime


@dataclass
class PostPublishedEvent:
    post_id: str
    platform_id: str
    message_id: int
    published_at: datetime
