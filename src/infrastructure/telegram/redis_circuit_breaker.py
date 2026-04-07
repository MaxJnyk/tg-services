"""Redis-backed Circuit Breaker for cluster-wide state.

Problem: In-memory circuit breaker only tracks failures per worker.
With 3 workers, each sees only its own failures → Telegram bans all tokens.

Solution: Redis-backed state with cluster-wide visibility.
- LPUSH errors per token, LTRIM to keep last 5
- SET state (CLOSED/OPEN/HALF_OPEN) with TTL
- Safe fallback: return False when Redis is down (stop rather than ban)
"""
import time
from typing import Protocol

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import RedisError


class RedisCircuitBreaker:
    """Cluster-aware circuit breaker using Redis.
    
    States:
        CLOSED: Normal operation, track failures
        OPEN: 5 errors in 60 sec → block all requests for 60 sec
        HALF_OPEN: After recovery timeout, allow 1 test request
    
    Safe fallback: When Redis is unavailable, return False (block requests).
    Better to pause than get banned by Telegram forever.
    """

    FAILURE_THRESHOLD = 5
    FAILURE_WINDOW = 60.0  # seconds
    RECOVERY_TIMEOUT = 60.0  # seconds

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._redis_down_logged = False

    async def record_failure(self, token: str) -> None:
        """Record a failure for the given token.
        
        If 5 failures within 60 seconds, transition to OPEN state.
        """
        try:
            token_short = token[:10]
            failures_key = f"circuit:failures:{token}"
            state_key = f"circuit:state:{token}"

            # Add timestamp to failures list
            now = str(time.time())
            await self._redis.lpush(failures_key, now)
            # Keep only last 5 failures
            await self._redis.ltrim(failures_key, 0, 4)
            # Set expiry on failures list
            await self._redis.expire(failures_key, int(self.FAILURE_WINDOW))

            # Check if we should transition to OPEN
            failures = await self._redis.lrange(failures_key, 0, -1)
            recent_count = 0
            cutoff = time.time() - self.FAILURE_WINDOW
            
            for f in failures:
                try:
                    if float(f) > cutoff:
                        recent_count += 1
                except (ValueError, TypeError):
                    continue

            if recent_count >= self.FAILURE_THRESHOLD:
                # Transition to OPEN
                await self._redis.set(
                    state_key, 
                    "OPEN", 
                    ex=int(self.RECOVERY_TIMEOUT)
                )
                logger.warning(
                    f"Circuit OPEN: token={token_short}..., "
                    f"{recent_count} failures in {self.FAILURE_WINDOW}s, "
                    f"blocking for {self.RECOVERY_TIMEOUT}s"
                )

        except RedisError as exc:
            if not self._redis_down_logged:
                logger.critical(
                    f"Redis unavailable, circuit breaker recording disabled: {exc}"
                )
                self._redis_down_logged = True

    async def record_success(self, token: str) -> None:
        """Record a successful request.
        
        If state was HALF_OPEN, transition back to CLOSED.
        """
        try:
            state_key = f"circuit:state:{token}"
            current_state = await self._redis.get(state_key)
            
            if current_state and current_state.decode() == "HALF_OPEN":
                # Transition back to CLOSED
                await self._redis.delete(state_key)
                logger.info(
                    f"Circuit CLOSED: token={token[:10]}..., recovered after test request"
                )
        except RedisError:
            pass  # Not critical

    async def allow_request(self, token: str) -> bool:
        """Check if request should be allowed.
        
        Returns:
            True if request allowed (CLOSED or HALF_OPEN)
            False if circuit OPEN or Redis unavailable
        """
        try:
            state_key = f"circuit:state:{token}"
            current_state = await self._redis.get(state_key)

            if current_state is None:
                return True  # CLOSED (default)
            
            state = current_state.decode()
            
            if state == "OPEN":
                # Check if recovery timeout passed (TTL expired)
                ttl = await self._redis.ttl(state_key)
                if ttl <= 0:
                    # Transition to HALF_OPEN
                    await self._redis.set(
                        state_key, 
                        "HALF_OPEN", 
                        ex=int(self.RECOVERY_TIMEOUT)
                    )
                    logger.info(
                        f"Circuit HALF_OPEN: token={token[:10]}..., testing with next request"
                    )
                    return True
                return False
            
            if state == "HALF_OPEN":
                return True  # Allow test request
            
            return True  # Unknown state, assume CLOSED

        except RedisError as exc:
            # CRITICAL DECISION: Redis down = block all requests
            # Better to pause than get banned by Telegram forever
            if not self._redis_down_logged:
                logger.critical(
                    f"Redis down, circuit breaker in SAFE MODE (blocking all): {exc}"
                )
                self._redis_down_logged = True
            return False

    async def reset(self, token: str) -> None:
        """Manual reset for admin operations."""
        try:
            await self._redis.delete(f"circuit:state:{token}")
            await self._redis.delete(f"circuit:failures:{token}")
            logger.info(f"Circuit RESET: token={token[:10]}...")
        except RedisError:
            pass

    def _reset_logging_flag(self) -> None:
        """Reset the Redis down logging flag (for testing)."""
        self._redis_down_logged = False
