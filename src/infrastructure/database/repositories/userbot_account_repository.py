"""Userbot Account Repository с валидацией fingerprint."""
from datetime import datetime
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from src.domain.value_objects.device_fingerprint import DeviceFingerprint
from src.infrastructure.database.models.userbot_account import UserbotAccount


class UserbotAccountRepository:
    """Репозиторий для управления userbot аккаунтами с anti-detect."""

    # Обязательные поля device fingerprint
    REQUIRED_DEVICE_FIELDS = ["device", "sdk", "app_version", "app_id", "app_hash"]

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_active_accounts(self) -> list[UserbotAccount]:
        """Получить все активные аккаунты."""
        result = await self.session.execute(
            select(UserbotAccount)
            .where(UserbotAccount.is_active == True)
            .where(UserbotAccount.status != "banned")
        )
        return list(result.scalars().all())

    async def get_by_account_id(self, account_id: str) -> Optional[UserbotAccount]:
        """Получить аккаунт по ID."""
        result = await self.session.execute(
            select(UserbotAccount).where(UserbotAccount.account_id == account_id)
        )
        return result.scalar_one_or_none()

    async def validate_and_get_fingerprint(
        self, account: UserbotAccount
    ) -> Optional[DeviceFingerprint]:
        """
        Валидировать и создать DeviceFingerprint из device_json.
        
        Returns:
            DeviceFingerprint если валидация успешна
            None если device_json отсутствует или невалиден
        """
        if not account.device_json:
            logger.error(
                f"Account {account.account_id}: device_json отсутствует. "
                "Аккаунт будет отклонён."
            )
            return None

        # Проверка обязательных полей
        missing_fields = [
            field for field in self.REQUIRED_DEVICE_FIELDS
            if field not in account.device_json
        ]
        
        if missing_fields:
            logger.error(
                f"Account {account.account_id}: отсутствуют обязательные поля: "
                f"{missing_fields}. Аккаунт будет отклонён."
            )
            return None

        try:
            fingerprint = DeviceFingerprint.from_json(account.device_json)
            logger.info(
                f"Account {account.account_id}: fingerprint валиден "
                f"(device={fingerprint.device_model}, "
                f"sdk={fingerprint.system_version}, "
                f"app={fingerprint.app_version})"
            )
            return fingerprint
        except ValueError as e:
            logger.error(
                f"Account {account.account_id}: невалидный fingerprint: {e}"
            )
            return None

    async def mark_as_banned(self, account_id: str, reason: str = "") -> None:
        """Пометить аккаунт как забаненный."""
        account = await self.get_by_account_id(account_id)
        if account:
            account.status = "banned"
            account.is_active = False
            await self.session.commit()
            logger.warning(f"Account {account_id} помечен как banned. Reason: {reason}")

    async def update_last_used(self, account_id: str) -> None:
        """Обновить время последнего использования."""
        account = await self.get_by_account_id(account_id)
        if account:
            account.last_used_at = datetime.utcnow()
            await self.session.commit()
