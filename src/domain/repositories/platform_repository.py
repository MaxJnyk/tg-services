"""Platform Repository ABC interface."""
from abc import ABC, abstractmethod
from typing import Optional


class PlatformRepository(ABC):
    @abstractmethod
    async def get(self, platform_id: str) -> Optional[dict]:
        """Get platform by ID."""
        pass

    @abstractmethod
    async def update_stats(self, platform_id: str, stats: dict) -> None:
        """Update platform statistics."""
        pass
