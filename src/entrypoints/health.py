"""Healthcheck endpoint для tad-worker.

Запускается на отдельном порту (8080) для проверки состояния worker'а.
Используется load balancer'ом и мониторингом.

Endpoints:
- GET /health — полная проверка всех компонентов
- GET /health/live — Kubernetes liveness probe (только что процесс жив)
- GET /health/ready — Kubernetes readiness probe (готовность к трафику)
- GET /metrics — Prometheus метрики

Проверки:
- postgresql: SELECT 1 (должно быть <100ms)
- redis: PING (должно быть <50ms)
- bot_pool: healthy_ratio > 0.5 (минимум 50% токенов работают)
- userbot_pool: active_count >= 3 (минимум 3 активных аккаунта)
- dlq: size < 100 (DLQ не переполнен)

Статусы:
- 200 OK: all checks passed
- 503 Service Unavailable: один или более checks failed
- 500 Internal Error: исключение при проверке
"""
import asyncio
from typing import Any

from aiohttp import web
from loguru import logger
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from src.core.config import settings
from src.core.metrics import get_metrics


class HealthChecker:
    """Класс для проверки состояния компонентов."""
    
    def __init__(
        self,
        db_engine: AsyncEngine,
        redis: Redis,
        bot_pool=None,
        userbot_pool=None,
        dlq=None,
    ):
        self._db_engine = db_engine
        self._redis = redis
        self._bot_pool = bot_pool
        self._userbot_pool = userbot_pool
        self._dlq = dlq
    
    async def check_postgresql(self) -> dict:
        """Проверка PostgreSQL: SELECT 1 с таймаутом 2 сек."""
        try:
            import time
            start = time.monotonic()
            
            # Таймаут 2 секунды — защита от зависания
            async with asyncio.timeout(2.0):
                async with self._db_engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
            
            elapsed_ms = (time.monotonic() - start) * 1000
            
            return {
                "status": "ok",
                "response_time_ms": round(elapsed_ms, 2),
                "threshold_ms": 100,
            }
        except asyncio.TimeoutError:
            return {
                "status": "error",
                "error": "PostgreSQL timeout after 2s",
            }
        except Exception as exc:
            return {
                "status": "error",
                "error": str(exc),
            }
    
    async def check_redis(self) -> dict:
        """Проверка Redis: PING с таймаутом 1 сек."""
        try:
            import time
            start = time.monotonic()
            
            # Таймаут 1 секунда
            async with asyncio.timeout(1.0):
                await self._redis.ping()
            
            elapsed_ms = (time.monotonic() - start) * 1000
            
            return {
                "status": "ok",
                "response_time_ms": round(elapsed_ms, 2),
                "threshold_ms": 50,
            }
        except asyncio.TimeoutError:
            return {
                "status": "error",
                "error": "Redis timeout after 1s",
            }
        except Exception as exc:
            return {
                "status": "error",
                "error": str(exc),
            }
    
    async def check_bot_pool(self) -> dict:
        """Проверка BotPool: healthy_ratio > 0.5."""
        if self._bot_pool is None:
            return {
                "status": "unknown",
                "message": "BotPool not initialized",
            }
        
        try:
            total = self._bot_pool.get_total_count()
            healthy = self._bot_pool.get_healthy_count()
            
            ratio = healthy / total if total > 0 else 1.0
            
            if ratio >= 0.5:
                return {
                    "status": "ok",
                    "healthy_tokens": healthy,
                    "total_tokens": total,
                    "ratio": round(ratio, 2),
                    "threshold": 0.5,
                }
            else:
                return {
                    "status": "warning",
                    "healthy_tokens": healthy,
                    "total_tokens": total,
                    "ratio": round(ratio, 2),
                    "threshold": 0.5,
                    "message": f"Only {healthy}/{total} tokens healthy",
                }
        except Exception as exc:
            return {
                "status": "error",
                "error": str(exc),
            }
    
    async def check_userbot_pool(self) -> dict:
        """Проверка UserbotPool: active_count >= 3."""
        if self._userbot_pool is None:
            return {
                "status": "unknown",
                "message": "UserbotPool not initialized",
            }
        
        try:
            active = await self._userbot_pool.get_active_count()
            
            if active >= 3:
                return {
                    "status": "ok",
                    "active_accounts": active,
                    "threshold": 3,
                }
            else:
                return {
                    "status": "warning",
                    "active_accounts": active,
                    "threshold": 3,
                    "message": f"Only {active} active accounts (need 3+)",
                }
        except Exception as exc:
            return {
                "status": "error",
                "error": str(exc),
            }
    
    async def check_dlq(self) -> dict:
        """Проверка DLQ: size < 100."""
        if self._dlq is None:
            return {
                "status": "unknown",
                "message": "DLQ not initialized",
            }
        
        try:
            size = await self._dlq.get_size()
            
            if size < 100:
                return {
                    "status": "ok",
                    "size": size,
                    "threshold": 100,
                }
            else:
                return {
                    "status": "warning",
                    "size": size,
                    "threshold": 100,
                    "message": f"DLQ has {size} jobs (threshold 100)",
                }
        except Exception as exc:
            return {
                "status": "error",
                "error": str(exc),
            }
    
    async def check_all(self) -> dict:
        """Запустить все проверки."""
        checks = {
            "postgresql": await self.check_postgresql(),
            "redis": await self.check_redis(),
            "bot_pool": await self.check_bot_pool(),
            "userbot_pool": await self.check_userbot_pool(),
            "dlq": await self.check_dlq(),
        }
        
        # Определяем общий статус
        has_error = any(c.get("status") == "error" for c in checks.values())
        has_warning = any(c.get("status") == "warning" for c in checks.values())
        
        if has_error:
            overall_status = "unhealthy"
            http_status = 503
        elif has_warning:
            overall_status = "degraded"
            http_status = 200  # Degraded — не критично
        else:
            overall_status = "healthy"
            http_status = 200
        
        return {
            "status": overall_status,
            "http_status": http_status,
            "checks": checks,
            "worker_id": settings.WORKER_ID,
        }


async def health_handler(request: web.Request) -> web.Response:
    """HTTP handler для /health."""
    health_checker = request.app["health_checker"]
    
    result = await health_checker.check_all()
    http_status = result.pop("http_status", 200)
    
    return web.json_response(result, status=http_status)


async def liveness_handler(request: web.Request) -> web.Response:
    """HTTP handler для /health/live (Kubernetes liveness probe).
    
    Простая проверка — только что процесс жив.
    """
    return web.json_response({"status": "alive"})


async def metrics_handler(request: web.Request) -> web.Response:
    """HTTP handler для /metrics (Prometheus metrics)."""
    metrics_data = get_metrics()
    return web.Response(
        body=metrics_data,
        content_type="text/plain; version=0.0.4; charset=utf-8",
    )


async def readiness_handler(request: web.Request) -> web.Response:
    """HTTP handler для /health/ready (Kubernetes readiness probe).
    
    Проверка готовности принимать трафик.
    """
    health_checker = request.app["health_checker"]
    result = await health_checker.check_all()
    http_status = result.pop("http_status", 200)
    
    # Для readiness: degraded = не готов
    if result["status"] == "unhealthy":
        http_status = 503
    
    return web.json_response(result, status=http_status)


def create_health_app(
    db_engine: AsyncEngine,
    redis: Redis,
    bot_pool=None,
    userbot_pool=None,
    dlq=None,
) -> web.Application:
    """Создать aiohttp приложение для healthcheck."""
    app = web.Application()
    
    # Сохраняем зависимости
    app["health_checker"] = HealthChecker(
        db_engine=db_engine,
        redis=redis,
        bot_pool=bot_pool,
        userbot_pool=userbot_pool,
        dlq=dlq,
    )
    
    # Регистрируем роуты
    app.router.add_get("/health", health_handler)
    app.router.add_get("/health/live", liveness_handler)
    app.router.add_get("/health/ready", readiness_handler)
    app.router.add_get("/metrics", metrics_handler)
    
    return app


async def start_health_server(
    db_engine: AsyncEngine,
    redis: Redis,
    bot_pool=None,
    userbot_pool=None,
    dlq=None,
    port: int = 8080,
) -> web.AppRunner:
    """Запустить healthcheck сервер.
    
    Returns:
        AppRunner для graceful shutdown.
    """
    app = create_health_app(db_engine, redis, bot_pool, userbot_pool, dlq)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logger.info(f"Healthcheck server started on port {port}")
    logger.info(f"  /health   — health check")
    logger.info(f"  /metrics  — Prometheus metrics")
    
    return runner
