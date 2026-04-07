"""
Proxy Manager для userbot аккаунтов.

Один аккаунт = один прокси. Статический mapping.
Прокси данные хранятся в поле proxy_json таблицы userbot_accounts.

Формат proxy_json:
{
    "type": "socks5",        # socks5 | socks4 | http
    "host": "1.2.3.4",
    "port": 1080,
    "username": "user",      # optional
    "password": "pass"       # optional
}

Telethon proxy format:
    (socks.SOCKS5, 'host', port, True, 'user', 'pass')
"""
import asyncio
import socket
from dataclasses import dataclass
from typing import Optional

from loguru import logger


@dataclass(frozen=True)
class ProxyConfig:
    """Конфигурация прокси для одного userbot аккаунта."""

    proxy_type: str  # socks5, socks4, http
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None

    def to_telethon_proxy(self) -> tuple:
        """
        Конвертировать в формат Telethon proxy.

        Returns:
            Tuple для передачи в TelegramClient(proxy=...).
        """
        import socks

        type_map = {
            "socks5": socks.SOCKS5,
            "socks4": socks.SOCKS4,
            "http": socks.HTTP,
        }
        proxy_type = type_map.get(self.proxy_type.lower())
        if proxy_type is None:
            raise ValueError(f"Unknown proxy type: {self.proxy_type}")

        return (
            proxy_type,
            self.host,
            self.port,
            True,  # rdns
            self.username,
            self.password,
        )

    @classmethod
    def from_json(cls, data: dict) -> "ProxyConfig":
        """
        Создать ProxyConfig из JSON (proxy_json из БД).

        Args:
            data: dict с ключами type, host, port, username?, password?.

        Raises:
            ValueError: если обязательные поля отсутствуют.
        """
        required = ["type", "host", "port"]
        missing = [f for f in required if f not in data]
        if missing:
            raise ValueError(f"Missing proxy fields: {missing}")

        return cls(
            proxy_type=data["type"],
            host=data["host"],
            port=int(data["port"]),
            username=data.get("username"),
            password=data.get("password"),
        )


class ProxyManager:
    """
    Управление прокси для userbot аккаунтов.

    Один аккаунт = один прокси (статический mapping из БД).
    Health check через TCP connect с timeout.
    """

    HEALTH_CHECK_TIMEOUT: float = 5.0

    def __init__(self) -> None:
        # account_id → ProxyConfig
        self._proxies: dict[str, ProxyConfig] = {}
        # account_id с проблемными прокси
        self._compromised: set[str] = set()

    def register(self, account_id: str, proxy: ProxyConfig) -> None:
        """Зарегистрировать прокси для аккаунта."""
        self._proxies[account_id] = proxy

    def get_proxy(self, account_id: str) -> Optional[ProxyConfig]:
        """Получить прокси для аккаунта (None если без прокси)."""
        if account_id in self._compromised:
            return None
        return self._proxies.get(account_id)

    async def health_check(self, proxy: ProxyConfig) -> bool:
        """
        Проверить доступность прокси через TCP connect.

        Args:
            proxy: конфигурация прокси.

        Returns:
            True если прокси отвечает в пределах timeout.
        """
        try:
            loop = asyncio.get_running_loop()
            # TCP connect с timeout
            conn = loop.create_connection(
                asyncio.Protocol,
                host=proxy.host,
                port=proxy.port,
            )
            transport, _ = await asyncio.wait_for(
                conn,
                timeout=self.HEALTH_CHECK_TIMEOUT,
            )
            transport.close()
            return True
        except (asyncio.TimeoutError, OSError, socket.error) as exc:
            logger.warning(
                f"Proxy health check failed: {proxy.host}:{proxy.port} — {exc}"
            )
            return False

    async def mark_compromised(self, account_id: str, reason: str) -> None:
        """
        Пометить прокси аккаунта как скомпрометированный.

        Аккаунт больше не будет использовать прокси до ручного сброса.

        Args:
            account_id: ID аккаунта.
            reason: причина (leak detected, banned after use, etc).
        """
        self._compromised.add(account_id)
        logger.warning(
            f"Proxy compromised: account={account_id}, reason={reason}"
        )

    async def check_all(self) -> dict[str, bool]:
        """
        Проверить все зарегистрированные прокси.

        Returns:
            dict: account_id → is_healthy.
        """
        results = {}
        for account_id, proxy in self._proxies.items():
            if account_id in self._compromised:
                results[account_id] = False
                continue
            results[account_id] = await self.health_check(proxy)
        return results

    def get_healthy_count(self) -> int:
        """Количество аккаунтов с незаблокированными прокси."""
        return len(self._proxies) - len(self._compromised)
