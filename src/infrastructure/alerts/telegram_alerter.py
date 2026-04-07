"""Telegram alerting для критических событий.

Отправляет уведомления в Telegram при:
- DLQ > 0 (failed задачи)
- Circuit breaker открыт (возможен бан)
- Worker упал (неожиданно завершился)

Конфигурация через env vars:
- ALERT_TELEGRAM_BOT_TOKEN — токен бота для отправки
- ALERT_TELEGRAM_CHAT_ID — ID чата для уведомлений
"""
import asyncio
from typing import Optional

from aiogram import Bot
from loguru import logger

from src.core.config import settings


class TelegramAlerter:
    """Отправка алертов в Telegram.
    
    Singleton — один экземпляр на worker.
    Использует отдельный bot token (может быть дежурным ботом).
    """
    
    _instance: Optional["TelegramAlerter"] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
            
        self._bot: Optional[Bot] = None
        self._chat_id: Optional[str] = None
        self._last_alert_time: dict[str, float] = {}
        self._alert_cooldown = 300  # 5 минут между повторными алертами
        
        # Инициализация если есть настройки
        token = getattr(settings, "ALERT_TELEGRAM_BOT_TOKEN", None)
        chat_id = getattr(settings, "ALERT_TELEGRAM_CHAT_ID", None)
        
        if token and chat_id:
            self._bot = Bot(token=token)
            self._chat_id = chat_id
            logger.info("TelegramAlerter: инициализирован")
        else:
            logger.warning(
                "TelegramAlerter: не инициализирован — "
                "отсутствуют ALERT_TELEGRAM_BOT_TOKEN или ALERT_TELEGRAM_CHAT_ID"
            )
        
        self._initialized = True
    
    async def alert_dlq(self, size: int) -> None:
        """Отправить алерт о непустой DLQ.
        
        Args:
            size: Количество сообщений в DLQ.
        """
        if not self._can_alert("dlq"):
            return
            
        message = (
            f"⚠️ <b>DLQ Alert</b>\n\n"
            f"Worker: <code>{settings.WORKER_ID}</code>\n"
            f"DLQ size: <b>{size}</b> failed jobs\n\n"
            f"Проверь: <code>taskiq replay-broker --acknowledge src.entrypoints.broker:broker</code>"
        )
        
        await self._send(message)
        self._record_alert("dlq")
    
    async def alert_circuit_open(self, token: str) -> None:
        """Отправить алерт об открытом circuit breaker.
        
        Args:
            token: Токен бота (сокращённый для логов).
        """
        if not self._can_alert(f"circuit_{token}"):
            return
            
        message = (
            f"🚨 <b>Circuit Breaker OPEN</b>\n\n"
            f"Worker: <code>{settings.WORKER_ID}</code>\n"
            f"Token: <code>{token[:10]}...</code>\n\n"
            f"Возможно FloodWait или бан от Telegram."
        )
        
        await self._send(message)
        self._record_alert(f"circuit_{token}")
    
    async def alert_worker_shutdown(self, error: Optional[str] = None) -> None:
        """Отправить алерт о неожиданном падении worker'а.
        
        Args:
            error: Описание ошибки (optional).
        """
        message = (
            f"💥 <b>Worker Shutdown</b>\n\n"
            f"Worker: <code>{settings.WORKER_ID}</code>\n"
        )
        
        if error:
            message += f"Error: <pre>{error[:200]}</pre>\n"
        
        message += "\nWorker завершил работу."
        
        await self._send(message)
    
    async def _send(self, message: str) -> None:
        """Отправить сообщение в Telegram."""
        if self._bot is None or self._chat_id is None:
            logger.debug(f"Alerter disabled, message: {message[:50]}...")
            return
        
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=message,
                parse_mode="HTML",
            )
            logger.info("Alert sent to Telegram")
        except Exception as exc:
            logger.error(f"Failed to send Telegram alert: {exc}")
    
    def _can_alert(self, alert_type: str) -> bool:
        """Проверка cooldown между алертами одного типа."""
        import time
        last_time = self._last_alert_time.get(alert_type, 0)
        return (time.time() - last_time) > self._alert_cooldown
    
    def _record_alert(self, alert_type: str) -> None:
        """Записать время алерта."""
        import time
        self._last_alert_time[alert_type] = time.time()
    
    async def close(self) -> None:
        """Закрыть сессию бота."""
        if self._bot:
            await self._bot.session.close()
            logger.info("TelegramAlerter: сессия закрыта")


# Singleton instance
_alerter: Optional[TelegramAlerter] = None


def get_alerter() -> TelegramAlerter:
    """Получить singleton экземпляр TelegramAlerter."""
    global _alerter
    if _alerter is None:
        _alerter = TelegramAlerter()
    return _alerter
