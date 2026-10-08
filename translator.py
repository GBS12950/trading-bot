"""Traduzione EN -> IT senza chiavi API (Google, con ripiego su MyMemory). Non solleva mai eccezioni."""
from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger("translator")

MAX_CHARS = 450  # limite prudente per richieste GET
_cache: dict[str, str] = {}


def _clip(text: str, limit: int = MAX_CHARS) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0] + "..."


async def _google(client: httpx.AsyncClient, text: str) -> Optional[str]:
    res = await client.get(
        "https://translate.googleapis.com/translate_a/single",
        params={"client": "gtx", "sl": "en", "tl": "it", "dt": "t", "q": text},
    )
    res.raise_for_status()
    segments = res.json()[0] or []
    translated = "".join(seg[0] for seg in segments if seg and seg[0])
    return translated or None


async def _mymemory(client: httpx.AsyncClient, text: str) -> Optional[str]:
    res = await client.get(
        "https://api.mymemory.translated.net/get",
        params={"q": text, "langpair": "en|it"},
    )
    res.raise_for_status()
    translated = (res.json().get("responseData") or {}).get("translatedText") or ""
    # Quando la quota gratuita è esaurita restituisce un avviso al posto della traduzione
    if not translated or "MYMEMORY WARNING" in translated.upper():
        return None
    return translated


async def translate_to_italian(text: Optional[str], timeout: float = 8.0) -> Optional[str]:
    """Traduce `text` in italiano; ritorna None se non riesce (il chiamante usa l'originale)."""
    if not text or not text.strip():
        return None
    text = _clip(text)
    if text in _cache:
        return _cache[text]

    async with httpx.AsyncClient(timeout=timeout) as client:
        for provider in (_google, _mymemory):
            try:
                translated = await provider(client, text)
            except Exception as exc:  # noqa: BLE001
                # Il messaggio dell'eccezione può contenere l'URL con il testo: si logga solo il tipo
                logger.warning("Traduzione fallita con %s: %s", provider.__name__.strip("_"), type(exc).__name__)
                continue
            if translated:
                _cache[text] = translated
                return translated
    return None
