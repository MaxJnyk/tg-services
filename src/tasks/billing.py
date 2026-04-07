"""
Задачи биллинга — label: billing.

Исправлено: worker НЕ управляет деньгами напрямую.
Вместо этого — HTTP callback в tad-backend (SAGA pattern).

Flow (SAGA):
1. tad-backend создаёт hold (с таймаутом 5 мин)
2. tad-worker выполняет posting
3. tad-worker вызывает backend API: /billing/execute-hold
4. tad-backend делает charge/release атомарно
5. Если worker не ответил за 5 мин — backend сам release'ит по таймауту

Преимущества:
- Backend — единый источник правды для денег
- Worker не знает про балансы (security)
- Автоматический откат по таймауту
- Полный аудит в одном месте
"""
import aiohttp
from loguru import logger

from src.core.config import settings
from src.entrypoints.broker import broker
from src.shared.schemas.tasks import BillingChargePayload, BillingReleasePayload


class BillingBackendError(Exception):
    """Ошибка backend'а биллинга — можно retry."""
    pass


@broker.task(
    task_name="worker.billing_charge",
    retry_on_error=True,
    max_retries=3,
    labels={"queue": "billing"},
)
async def billing_charge(hold_id: str, post_id: str, amount_kopecks: int) -> dict:
    """
    Уведомление backend'а о необходимости списания средств.
    
    Worker НЕ трогает деньги напрямую — только вызывает backend API.
    Backend сам управляет транзакцией (SELECT FOR UPDATE NOWAIT).

    Args:
        hold_id: UUID hold записи.
        post_id: UUID поста (для аудита).
        amount_kopecks: сумма в копейках (для валидации).

    Returns:
        dict с результатом:
        - {"status": "charged"} — успешно списано
        - {"status": "already_charged"} — уже списано (idempotency)
        
    Raises:
        BillingBackendError: Ошибка backend'а — retry через Taskiq.
    """
    payload = BillingChargePayload(
        hold_id=hold_id,
        post_id=post_id,
        amount_kopecks=amount_kopecks,
    )

    logger.info(
        f"billing_charge: вызываем backend API для hold_id={payload.hold_id}"
    )

    # HTTP callback в tad-backend (SAGA pattern)
    backend_url = f"{settings.BACKEND_URL}/api/v1/billing/execute-hold"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                backend_url,
                json={
                    "hold_id": str(payload.hold_id),
                    "post_id": str(payload.post_id),
                    "amount_kopecks": payload.amount_kopecks,
                },
                headers={
                    "Authorization": f"Bearer {settings.WORKER_API_TOKEN}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=10),  # 10 секунд таймаут
            ) as resp:
                if resp.status == 200:
                    # Успешно списано
                    result = await resp.json()
                    logger.info(
                        f"billing_charge: списано hold_id={payload.hold_id}, "
                        f"transaction_id={result.get('transaction_id')}"
                    )
                    return {"status": "charged", "hold_id": payload.hold_id}
                    
                elif resp.status == 409:
                    # Уже списано (idempotency)
                    logger.info(
                        f"billing_charge: already charged hold_id={payload.hold_id}"
                    )
                    return {"status": "already_charged", "hold_id": payload.hold_id}
                    
                elif resp.status == 404:
                    # Hold не найден — не retry (баг в данных)
                    logger.error(
                        f"billing_charge: hold не найден hold_id={payload.hold_id}"
                    )
                    raise BillingBackendError(f"Hold {payload.hold_id} not found")
                    
                elif resp.status >= 500:
                    # Ошибка backend'а — можно retry
                    logger.warning(
                        f"billing_charge: backend error {resp.status}, will retry"
                    )
                    raise BillingBackendError(f"Backend error: {resp.status}")
                    
                else:
                    # Другая ошибка — retry
                    logger.error(
                        f"billing_charge: unexpected status {resp.status}"
                    )
                    raise BillingBackendError(f"Unexpected status: {resp.status}")
                    
    except aiohttp.ClientError as exc:
        # Сетевая ошибка — retry
        logger.warning(f"billing_charge: network error, will retry: {exc}")
        raise BillingBackendError(f"Network error: {exc}")
    except Exception as exc:
        logger.exception(f"billing_charge: unexpected error: {exc}")
        raise


@broker.task(
    task_name="worker.billing_release",
    retry_on_error=True,
    max_retries=3,
    labels={"queue": "billing"},
)
async def billing_release(hold_id: str, reason: str = "timeout") -> dict:
    """
    Уведомление backend'а об освобождении холда.
    
    Worker НЕ трогает деньги напрямую — только вызывает backend API.

    Args:
        hold_id: UUID hold записи.
        reason: причина освобождения (timeout/error/manual).

    Returns:
        dict с результатом.
        
    Raises:
        BillingBackendError: Ошибка backend'а — retry через Taskiq.
    """
    payload = BillingReleasePayload(hold_id=hold_id, reason=reason)

    logger.info(
        f"billing_release: вызываем backend API для hold_id={payload.hold_id}, "
        f"reason={payload.reason}"
    )

    # HTTP callback в tad-backend
    backend_url = f"{settings.BACKEND_URL}/api/v1/billing/release-hold"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                backend_url,
                json={
                    "hold_id": str(payload.hold_id),
                    "reason": payload.reason,
                },
                headers={
                    "Authorization": f"Bearer {settings.WORKER_API_TOKEN}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 409):
                    # 200 — успешно, 409 — уже released
                    logger.info(
                        f"billing_release: освобождён hold_id={payload.hold_id}"
                    )
                    return {"status": "released", "hold_id": payload.hold_id}
                    
                elif resp.status >= 500:
                    # Ошибка backend'а — retry
                    logger.warning(
                        f"billing_release: backend error {resp.status}, will retry"
                    )
                    raise BillingBackendError(f"Backend error: {resp.status}")
                    
                else:
                    logger.error(
                        f"billing_release: unexpected status {resp.status}"
                    )
                    raise BillingBackendError(f"Unexpected status: {resp.status}")
                    
    except aiohttp.ClientError as exc:
        logger.warning(f"billing_release: network error, will retry: {exc}")
        raise BillingBackendError(f"Network error: {exc}")
    except Exception as exc:
        logger.exception(f"billing_release: unexpected error: {exc}")
        raise
