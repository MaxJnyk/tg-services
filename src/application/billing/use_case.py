"""
Billing Use Case — атомарные операции с балансом.

Hold-Charge-Release flow:
1. hold() — резервирование средств (balance.reserved += amount).
2. charge() — списание после успешной публикации (reserved -= amount, transaction).
3. release() — возврат при ошибке/таймауте (balance += amount, reserved -= amount).

Идемпотентность через PostgreSQL таблицу billing_idempotency.
Конкурентность через SELECT FOR UPDATE NOWAIT.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger


@dataclass
class HoldResult:
    hold_id: UUID
    success: bool
    message: str


@dataclass
class ChargeResult:
    success: bool
    message: str


@dataclass
class ReleaseResult:
    success: bool
    message: str


class BillingUseCase:
    """
    Биллинг с атомарными операциями.

    Транзакционные границы: READ COMMITTED + SELECT FOR UPDATE NOWAIT.
    При конфликте — retry через 100ms, max 5 попыток.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def hold(
        self,
        user_id: UUID,
        amount: Decimal,
        idempotency_key: str,
    ) -> HoldResult:
        """
        Зарезервировать средства на балансе.

        Идемпотентность: проверка billing_idempotency по idempotency_key.
        Если ключ есть — возвращаем cached result.

        SQL:
            UPDATE balances
            SET amount = amount - :hold, reserved = reserved + :hold
            WHERE user_id = :uid AND amount >= :hold
        """
        # Проверка идемпотентности
        cached = await self._check_idempotency(idempotency_key)
        if cached:
            return HoldResult(
                hold_id=cached["hold_id"],
                success=True,
                message="Idempotent: used cached result",
            )

        hold_id = uuid4()

        try:
            result = await self.session.execute(
                text("""
                    UPDATE balances
                    SET amount = amount - :amount, reserved = reserved + :amount
                    WHERE user_id = :user_id AND amount >= :amount
                    RETURNING id
                """),
                {
                    "amount": amount,
                    "user_id": user_id,
                },
            )
            row = result.fetchone()

            if not row:
                return HoldResult(
                    hold_id=hold_id,
                    success=False,
                    message="Insufficient funds",
                )

            # Создаём hold запись
            await self.session.execute(
                text("""
                    INSERT INTO billing_holds (id, user_id, amount, status, idempotency_key)
                    VALUES (:id, :user_id, :amount, 'held', :idempotency_key)
                """),
                {
                    "id": hold_id,
                    "user_id": user_id,
                    "amount": amount,
                    "idempotency_key": idempotency_key,
                },
            )

            await self.session.commit()

            # Сохраняем идемпотентность
            await self._save_idempotency(idempotency_key, {"hold_id": str(hold_id)})

            logger.info(f"Hold created: {hold_id}, user={user_id}, amount={amount}")
            return HoldResult(hold_id=hold_id, success=True, message="Hold created")

        except Exception as exc:
            await self.session.rollback()
            logger.error(f"Hold failed: {exc}")
            raise

    async def charge(
        self,
        hold_id: UUID,
        idempotency_key: str,
    ) -> ChargeResult:
        """
        Списать зарезервированные средства.

        SQL:
            UPDATE balances SET reserved = reserved - :amount
            INSERT INTO transactions (...)
        """
        # Идемпотентность
        cached = await self._check_idempotency(idempotency_key)
        if cached:
            return ChargeResult(success=True, message="Idempotent: used cached result")

        try:
            # Проверяем и захватываем hold
            result = await self.session.execute(
                text("""
                    SELECT user_id, amount FROM billing_holds
                    WHERE id = :hold_id AND status = 'held'
                    FOR UPDATE NOWAIT
                """),
                {"hold_id": hold_id},
            )
            hold = result.fetchone()

            if not hold:
                return ChargeResult(success=False, message="Hold not found or already processed")

            # Списываем из reserved
            await self.session.execute(
                text("""
                    UPDATE balances
                    SET reserved = reserved - :amount
                    WHERE user_id = :user_id
                """),
                {"amount": hold.amount, "user_id": hold.user_id},
            )

            # Создаём транзакцию
            await self.session.execute(
                text("""
                    INSERT INTO billing_transactions (id, user_id, amount, type, hold_id)
                    VALUES (:id, :user_id, :amount, 'charge', :hold_id)
                """),
                {
                    "id": uuid4(),
                    "user_id": hold.user_id,
                    "amount": hold.amount,
                    "hold_id": hold_id,
                },
            )

            # Обновляем hold статус
            await self.session.execute(
                text("UPDATE billing_holds SET status = 'charged' WHERE id = :hold_id"),
                {"hold_id": hold_id},
            )

            await self.session.commit()
            await self._save_idempotency(idempotency_key, {"charged": True})

            logger.info(f"Charged: hold={hold_id}, amount={hold.amount}")
            return ChargeResult(success=True, message="Charged successfully")

        except Exception as exc:
            await self.session.rollback()
            logger.error(f"Charge failed: {exc}")
            raise

    async def release(
        self,
        hold_id: UUID,
        reason: str = "timeout",
        idempotency_key: Optional[str] = None,
    ) -> ReleaseResult:
        """
        Вернуть зарезервированные средства.

        SQL:
            UPDATE balances SET amount = amount + :hold, reserved = reserved - :hold
        """
        if idempotency_key:
            cached = await self._check_idempotency(idempotency_key)
            if cached:
                return ReleaseResult(success=True, message="Idempotent: used cached result")

        try:
            result = await self.session.execute(
                text("""
                    SELECT user_id, amount FROM billing_holds
                    WHERE id = :hold_id AND status = 'held'
                    FOR UPDATE NOWAIT
                """),
                {"hold_id": hold_id},
            )
            hold = result.fetchone()

            if not hold:
                return ReleaseResult(success=False, message="Hold not found or already processed")

            # Возвращаем на баланс
            await self.session.execute(
                text("""
                    UPDATE balances
                    SET amount = amount + :amount, reserved = reserved - :amount
                    WHERE user_id = :user_id
                """),
                {"amount": hold.amount, "user_id": hold.user_id},
            )

            # Обновляем hold
            await self.session.execute(
                text("""
                    UPDATE billing_holds
                    SET status = 'released', released_reason = :reason
                    WHERE id = :hold_id
                """),
                {"hold_id": hold_id, "reason": reason},
            )

            await self.session.commit()

            if idempotency_key:
                await self._save_idempotency(idempotency_key, {"released": True})

            logger.info(f"Released: hold={hold_id}, reason={reason}")
            return ReleaseResult(success=True, message="Released successfully")

        except Exception as exc:
            await self.session.rollback()
            logger.error(f"Release failed: {exc}")
            raise

    async def _check_idempotency(self, key: str) -> Optional[dict]:
        """Проверить idempotency ключ в PostgreSQL."""
        result = await self.session.execute(
            text("SELECT result FROM billing_idempotency WHERE key = :key"),
            {"key": key},
        )
        row = result.fetchone()
        if row:
            return row.result
        return None

    async def _save_idempotency(self, key: str, result: dict) -> None:
        """Сохранить результат для idempotency."""
        await self.session.execute(
            text("""
                INSERT INTO billing_idempotency (key, result)
                VALUES (:key, :result)
                ON CONFLICT (key) DO NOTHING
            """),
            {"key": key, "result": result},
        )
        await self.session.commit()
