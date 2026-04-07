"""
Userbot Pool V2 — управление Telethon userbot аккаунтами.

Anti-detect:
- DeviceFingerprint фиксирован PER ACCOUNT (не ротация между запросами).
- Behavioral jitter: random delay 0.5–3.0 сек перед каждым запросом.
- Proxy support: один прокси per account (статический mapping из БД).

Account lifecycle:
- active   → нормальная работа
- cooldown → FloodWait получен, ждём 5 мин
- banned   → 3+ ошибки UserBannedInChannelError подряд
- archived → ручной вывод из эксплуатации

Connection management:
- warm_up(N) при старте — подключить N аккаунтов для готовности.
- Lazy connect — остальные подключаются при первом get_client().
- Semaphore(10) per account — ограничение concurrent requests.
- Eviction: disconnect аккаунтов idle >5 мин.
"""
import asyncio
import enum
import random
import time
from dataclasses import dataclass, field
from typing import Optional

from loguru import logger
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import (
    AuthKeyUnregisteredError,
    FloodWaitError,
    UserBannedInChannelError,
    UserDeactivatedError,
)
from telethon.sessions import StringSession

from src.domain.value_objects.device_fingerprint import DeviceFingerprint
from src.infrastructure.database.repositories.userbot_account_repository import (
    UserbotAccountRepository,
)
from src.infrastructure.telegram.proxy_manager import ProxyConfig, ProxyManager


# Константы
_COOLDOWN_DURATION: float = 300.0  # 5 минут
_EVICTION_IDLE_TIMEOUT: float = 300.0  # 5 минут
_JITTER_MIN: float = 0.5
_JITTER_MAX: float = 3.0
_MAX_CONCURRENT_PER_ACCOUNT: int = 10
_BAN_THRESHOLD: int = 3  # подряд UserBannedInChannelError → banned
_WARM_UP_COUNT: int = 3


class AccountState(enum.Enum):
    """Состояние аккаунта в пуле."""

    ACTIVE = "active"
    COOLDOWN = "cooldown"
    BANNED = "banned"
    ARCHIVED = "archived"


@dataclass
class AccountEntry:
    """Запись об аккаунте в пуле."""

    account_id: str
    session_string: str
    fingerprint: DeviceFingerprint
    proxy: Optional[ProxyConfig] = None
    client: Optional[TelegramClient] = None
    state: AccountState = AccountState.ACTIVE
    semaphore: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(_MAX_CONCURRENT_PER_ACCOUNT)
    )
    last_used: float = 0.0
    cooldown_until: float = 0.0
    # Счётчик подряд UserBannedInChannelError
    consecutive_bans: int = 0


class UserbotPool:
    """
    Пул userbot аккаунтов V2.

    Lifecycle:
    1. warm_up(N) при старте — подключить N аккаунтов.
    2. get_client() → round-robin по active, lazy connect если не подключен.
    3. Behavioral jitter перед каждым возвратом клиента.
    4. record_success() / record_error() → lifecycle transitions.
    5. evict_idle() — disconnect idle accounts.
    6. close_all() — graceful shutdown.
    """

    def __init__(
        self,
        account_repo: UserbotAccountRepository,
        proxy_manager: Optional[ProxyManager] = None,
    ) -> None:
        self._account_repo = account_repo
        self._proxy_manager = proxy_manager or ProxyManager()
        # account_id → AccountEntry
        self._accounts: dict[str, AccountEntry] = {}
        # Round-robin index
        self._rr_index: int = 0

    async def load_accounts(self) -> int:
        """
        Загрузить все активные аккаунты из БД (без подключения).

        Подключение — lazy (при get_client) или через warm_up().

        Returns:
            Количество загруженных аккаунтов.
        """
        db_accounts = await self._account_repo.get_active_accounts()
        loaded = 0

        for account in db_accounts:
            fingerprint = await self._account_repo.validate_and_get_fingerprint(account)
            if not fingerprint:
                logger.warning(
                    f"Account {account.account_id}: skipped (invalid fingerprint)"
                )
                continue

            # Proxy из БД (если есть)
            proxy = None
            proxy_json = getattr(account, "proxy_json", None)
            if proxy_json:
                try:
                    proxy = ProxyConfig.from_json(proxy_json)
                    self._proxy_manager.register(account.account_id, proxy)
                except ValueError as exc:
                    logger.warning(
                        f"Account {account.account_id}: invalid proxy config: {exc}"
                    )

            self._accounts[account.account_id] = AccountEntry(
                account_id=account.account_id,
                session_string=account.session_string,
                fingerprint=fingerprint,
                proxy=proxy,
            )
            loaded += 1

        logger.info(f"UserbotPool: {loaded}/{len(db_accounts)} accounts loaded")
        return loaded

    async def warm_up(self, count: int = _WARM_UP_COUNT) -> int:
        """
        Подключить первые N аккаунтов при старте для немедленной готовности.

        Args:
            count: сколько аккаунтов подключить.

        Returns:
            Количество успешно подключённых.
        """
        connected = 0
        for account_id, entry in self._accounts.items():
            if connected >= count:
                break
            if entry.state != AccountState.ACTIVE:
                continue
            try:
                await self._connect(entry)
                connected += 1
            except Exception as exc:
                logger.error(
                    f"Account {account_id}: warm-up connect failed: {exc}"
                )
        logger.info(f"UserbotPool warm-up: {connected}/{count} accounts connected")
        return connected

    async def get_client(
        self,
        account_id: Optional[str] = None,
    ) -> TelegramClient:
        """
        Получить Telethon клиент.

        Если account_id не указан — round-robin по active аккаунтам.
        Lazy connect если аккаунт загружен, но не подключён.
        Behavioral jitter перед возвратом.

        Args:
            account_id: конкретный аккаунт (или None для round-robin).

        Returns:
            Подключённый TelegramClient.

        Raises:
            RuntimeError: нет доступных аккаунтов.
        """
        if account_id:
            entry = self._accounts.get(account_id)
            if not entry:
                raise RuntimeError(f"Account {account_id} not found in pool")
            if entry.state != AccountState.ACTIVE:
                raise RuntimeError(
                    f"Account {account_id} is {entry.state.value}"
                )
        else:
            entry = self._select_next_active()

        # Lazy connect
        if entry.client is None or not entry.client.is_connected():
            await self._connect(entry)

        # Acquire semaphore (ограничение concurrent requests per account)
        await entry.semaphore.acquire()

        # Behavioral jitter — anti-detect
        jitter = random.uniform(_JITTER_MIN, _JITTER_MAX)
        await asyncio.sleep(jitter)

        entry.last_used = time.monotonic()
        return entry.client

    def release_client(self, account_id: str) -> None:
        """
        Освободить semaphore после использования клиента.

        Вызывать в finally блоке после get_client().

        Args:
            account_id: ID аккаунта.
        """
        entry = self._accounts.get(account_id)
        if entry:
            entry.semaphore.release()

    def record_success(self, account_id: str) -> None:
        """Записать успешный запрос — сбросить счётчик банов."""
        entry = self._accounts.get(account_id)
        if entry:
            entry.consecutive_bans = 0

    async def record_error(
        self,
        account_id: str,
        exc: Exception,
    ) -> None:
        """
        Обработать ошибку для аккаунта.

        - FloodWaitError → cooldown на N секунд (мин 5 мин).
        - UserBannedInChannelError → increment counter, 3+ → banned.
        - UserDeactivatedError/AuthKeyUnregisteredError → banned сразу.
        """
        entry = self._accounts.get(account_id)
        if not entry:
            return

        if isinstance(exc, FloodWaitError):
            # Cooldown: max(FloodWait seconds, 5 min)
            wait_seconds = max(exc.seconds, _COOLDOWN_DURATION)
            entry.state = AccountState.COOLDOWN
            entry.cooldown_until = time.monotonic() + wait_seconds
            logger.warning(
                f"Account {account_id}: COOLDOWN for {wait_seconds:.0f}s "
                f"(FloodWait={exc.seconds}s)"
            )

        elif isinstance(exc, UserBannedInChannelError):
            entry.consecutive_bans += 1
            if entry.consecutive_bans >= _BAN_THRESHOLD:
                entry.state = AccountState.BANNED
                await self._account_repo.mark_as_banned(account_id, str(exc))
                logger.error(
                    f"Account {account_id}: BANNED "
                    f"({entry.consecutive_bans} consecutive bans)"
                )
            else:
                logger.warning(
                    f"Account {account_id}: ban count={entry.consecutive_bans}/{_BAN_THRESHOLD}"
                )

        elif isinstance(exc, (UserDeactivatedError, AuthKeyUnregisteredError)):
            entry.state = AccountState.BANNED
            await self._account_repo.mark_as_banned(account_id, str(exc))
            logger.error(f"Account {account_id}: BANNED (deactivated/auth invalid)")

        else:
            logger.warning(f"Account {account_id}: error: {exc}")

    def _select_next_active(self) -> AccountEntry:
        """
        Round-robin выбор следующего active аккаунта.

        Cooldown → проверяем, не истёк ли таймер.

        Raises:
            RuntimeError: нет доступных аккаунтов.
        """
        now = time.monotonic()
        account_ids = list(self._accounts.keys())
        total = len(account_ids)

        if total == 0:
            raise RuntimeError("No userbot accounts loaded")

        # Попробовать total раз (полный круг round-robin)
        for _ in range(total):
            idx = self._rr_index % total
            self._rr_index = (self._rr_index + 1) % total
            entry = self._accounts[account_ids[idx]]

            # Проверить cooldown recovery
            if entry.state == AccountState.COOLDOWN and now >= entry.cooldown_until:
                entry.state = AccountState.ACTIVE
                logger.info(f"Account {entry.account_id}: recovered from COOLDOWN")

            if entry.state == AccountState.ACTIVE:
                return entry

        raise RuntimeError("No active userbot accounts available")

    async def _connect(self, entry: AccountEntry) -> None:
        """
        Подключить Telethon клиент для аккаунта.

        Использует fingerprint и proxy из entry.
        """
        fp = entry.fingerprint

        proxy = None
        if entry.proxy:
            try:
                proxy = entry.proxy.to_telethon_proxy()
            except Exception as exc:
                logger.warning(
                    f"Account {entry.account_id}: proxy convert failed: {exc}, "
                    f"connecting without proxy"
                )

        client = TelegramClient(
            StringSession(entry.session_string),
            api_id=fp.api_id,
            api_hash=fp.api_hash,
            device_model=fp.device_model,
            system_version=fp.system_version,
            app_version=fp.app_version,
            lang_code=fp.lang_code,
            proxy=proxy,
            connection_retries=3,
            retry_delay=1,
            auto_reconnect=True,
        )

        try:
            await client.connect()

            if not await client.is_user_authorized():
                raise AuthKeyUnregisteredError(
                    request=None,
                    message=f"Account {entry.account_id}: session invalid",
                )

            entry.client = client
            entry.last_used = time.monotonic()
            logger.info(
                f"Account {entry.account_id}: connected "
                f"(device={fp.device_model}, proxy={'yes' if proxy else 'no'})"
            )

        except (UserDeactivatedError, AuthKeyUnregisteredError) as exc:
            entry.state = AccountState.BANNED
            await self._account_repo.mark_as_banned(entry.account_id, str(exc))
            raise ConnectionError(f"Account {entry.account_id} banned: {exc}")

        except Exception as exc:
            raise ConnectionError(
                f"Account {entry.account_id} connect failed: {exc}"
            )

    async def evict_idle(self) -> int:
        """
        Disconnect аккаунтов, не использовавшихся дольше EVICTION_IDLE_TIMEOUT.

        Returns:
            Количество отключённых аккаунтов.
        """
        now = time.monotonic()
        evicted = 0

        for entry in self._accounts.values():
            if entry.client is None:
                continue
            if not entry.client.is_connected():
                continue
            if entry.last_used == 0:
                continue
            if (now - entry.last_used) <= _EVICTION_IDLE_TIMEOUT:
                continue

            try:
                await entry.client.disconnect()
                logger.info(
                    f"Account {entry.account_id}: evicted "
                    f"(idle={now - entry.last_used:.0f}s)"
                )
            except Exception as exc:
                logger.warning(
                    f"Account {entry.account_id}: eviction disconnect error: {exc}"
                )
            entry.client = None
            evicted += 1

        return evicted

    def get_healthy_count(self) -> int:
        """Количество аккаунтов в состоянии ACTIVE."""
        now = time.monotonic()
        count = 0
        for entry in self._accounts.values():
            if entry.state == AccountState.ACTIVE:
                count += 1
            elif (
                entry.state == AccountState.COOLDOWN
                and now >= entry.cooldown_until
            ):
                count += 1
        return count

    def get_total_count(self) -> int:
        """Общее количество аккаунтов (все состояния)."""
        return len(self._accounts)

    async def close_all(self) -> None:
        """Graceful shutdown: disconnect всех подключённых клиентов."""
        disconnected = 0
        for entry in self._accounts.values():
            if entry.client and entry.client.is_connected():
                try:
                    await entry.client.disconnect()
                    disconnected += 1
                except Exception as exc:
                    logger.warning(
                        f"Account {entry.account_id}: disconnect error: {exc}"
                    )
                entry.client = None
        logger.info(f"UserbotPool closed: {disconnected} clients disconnected")
