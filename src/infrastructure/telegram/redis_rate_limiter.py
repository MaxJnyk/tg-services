"""Redis-backed Rate Limiter for cluster-wide rate limiting.

Problem: In-memory rate limiter causes race conditions with multiple workers.
Two workers check (29/30) simultaneously → both send → 31 msg/sec → FloodWait.

Solution: Redis INCR + EXPIRE for atomic counter operations.
- Global limit: 30 msg/sec per bot token
- Per-chat limit: 1 msg/sec per (token, chat_id)
- Fallback: When Redis is down, allow requests (risk of FloodWait, but better than downtime)
"""
import time
from dataclasses import dataclass

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import RedisError


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    global_per_second: int = 30  # Telegram Bot API limit
    per_chat_per_second: int = 1  # Per-chat limit
    window_seconds: int = 1  # Sliding window size


class RedisRateLimiter:
    """Cluster-aware rate limiter using Redis.
    
    Uses INCR + EXPIRE pattern for atomic counter operations.
    Safe fallback: When Redis is unavailable, allow requests.
    Risk of FloodWait, but better than complete downtime.
    """

    def __init__(
        self,
        redis: Redis,
        config: RateLimitConfig | None = None
    ) -> None:
        self._redis = redis
        self._config = config or RateLimitConfig()
        self._redis_down_logged = False

    async def acquire(self, token: str, chat_id: str) -> None:
        """Acquire rate limit permission.
        
        Raises:
            RateLimitExceeded: If rate limit exceeded for token or chat.
            RetryableError: If Redis is temporarily unavailable (caller decides).
        """
        try:
            # Check global token limit
            await self._check_global_limit(token)
            
            # Check per-chat limit
            await self._check_chat_limit(token, chat_id)
            
        except RedisError as exc:
            # Fallback: Redis unavailable - allow request
            # Risk of FloodWait, but better than complete downtime
            if not self._redis_down_logged:
                logger.critical(
                    f"Redis down, rate limiting disabled - risk of FloodWait: {exc}"
                )
                self._redis_down_logged = True
            # Allow request without rate limiting
            # Circuit breaker will catch problematic tokens

    async def _check_global_limit(self, token: str) -> None:
        """Check global rate limit for token."""
        key = f"rate:global:{token[:20]}"  # Shortened token in key
        
        # Atomically increment and set expiry if first increment
        current = await self._redis.incr(key)
        if current == 1:
            # First request in this window, set expiry
            await self._redis.expire(key, self._config.window_seconds)
        
        if current > self._config.global_per_second:
            raise RateLimitExceeded(
                f"Global rate limit exceeded: {current}/{self._config.global_per_second} "
                f"for token {token[:10]}..."
            )

    async def _check_chat_limit(self, token: str, chat_id: str) -> None:
        """Check per-chat rate limit."""
        key = f"rate:chat:{token[:20]}:{chat_id}"
        
        current = await self._redis.incr(key)
        if current == 1:
            await self._redis.expire(key, self._config.window_seconds)
        
        if current > self._config.per_chat_per_second:
            raise RateLimitExceeded(
                f"Chat rate limit exceeded: {current}/{self._config.per_chat_per_second} "
                f"for chat {chat_id}"
            )

    async def handle_retry_after(self, token: str, seconds: int) -> None:
        """Handle Telegram RetryAfter - block token for specified duration.
        
        This is called when Telegram returns 429 with RetryAfter.
        """
        try:
            key = f"rate:retry_after:{token[:20]}"
            resume_at = time.time() + seconds
            await self._redis.set(key, str(resume_at), ex=seconds)
            logger.warning(
                f"Token blocked by RetryAfter: {token[:10]}... for {seconds}s"
            )
        except RedisError:
            pass  # Not critical

    async def check_retry_after(self, token: str) -> float:
        """Check if token is blocked by RetryAfter.
        
        Returns:
            Seconds to wait, or 0 if not blocked.
        """
        try:
            key = f"rate:retry_after:{token[:20]}"
            value = await self._redis.get(key)
            if value is None:
                return 0.0
            
            resume_at = float(value)
            now = time.time()
            if resume_at > now:
                return resume_at - now
            return 0.0
        except (RedisError, ValueError):
            return 0.0  # Allow on error

    def _reset_logging_flag(self) -> None:
        """Reset the Redis down logging flag (for testing)."""
        self._redis_down_logged = False
