"""Invio di messaggi Telegram senza esporre il token nei log."""
from __future__ import annotations

import html
import logging
from typing import Any, Optional

import httpx

from supabase_manager import SupabaseManager

logger = logging.getLogger("telegram_notifier")

TELEGRAM_MAX_LEN = 4096


def esc(value: Any) -> str:
    return html.escape(str(value))


class TelegramNotifier:
    def __init__(self, token: Optional[str], chat_id: Optional[str]) -> None:
        self.token = token
        self.chat_id = chat_id
        # httpx logga l'URL completo, che per Telegram contiene il token
        logging.getLogger("httpx").setLevel(logging.WARNING)

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    @classmethod
    async def from_supabase(cls, db: SupabaseManager) -> "TelegramNotifier":
        try:
            token = await db.resolve_secret("TELEGRAM_BOT_TOKEN")
            chat_id = await db.resolve_secret("TELEGRAM_CHAT_ID")
        except ValueError:
            logger.warning("Telegram non configurato: notifiche disattivate")
            return cls(None, None)
        return cls(token, chat_id)

    async def send(self, text: str) -> None:
        if not self.enabled:
            raise RuntimeError("Telegram non configurato (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        if len(text) > TELEGRAM_MAX_LEN:
            text = text[: TELEGRAM_MAX_LEN - 20].rsplit("\n", 1)[0] + "\n(troncato)"
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
            )
        # Niente raise_for_status: il messaggio d'errore conterrebbe l'URL con il token
        if res.status_code != 200:
            raise RuntimeError(f"Telegram HTTP {res.status_code}: {res.text[:200]}")

    async def notify(self, text: str) -> None:
        """Come send(), ma non solleva mai: una notifica fallita non deve fermare il bot."""
        if not self.enabled:
            return
        try:
            await self.send(text)
        except Exception as exc:  # noqa: BLE001
            detail = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
            logger.error("Notifica Telegram fallita: %s", detail)
