"""
SupabaseManager - Livello di accesso asincrono a Supabase per il News Sentiment Bot.

Variabili d'ambiente richieste:
    SUPABASE_URL                 es. https://xxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY    chiave service_role (MAI esporla lato client)
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import uuid
from dataclasses import dataclass
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, TypeVar

import httpx
from postgrest.exceptions import APIError
from supabase import AsyncClient, acreate_client

logger = logging.getLogger("supabase_manager")
T = TypeVar("T")

# Codici PostgreSQL/PostgREST da NON ritentare (errori logici, non transitori)
_NON_RETRYABLE_PG_CODES = {
    "23505", "23503", "23514", "22P02", "42501", "42P01", "42883",
    "PGRST116", "PGRST202", "PGRST204", "PGRST205", "PGRST301",
}

VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
VALID_NEWS_STATUS = {"CLAIMED", "SKIPPED", "ORDERED", "FAILED"}


class SupabaseConnectionError(RuntimeError):
    """Sollevata quando Supabase resta irraggiungibile dopo tutti i tentativi."""


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.5   # secondi
    max_delay: float = 15.0
    jitter: float = 0.3       # frazione del delay


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError,
                        ConnectionError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {408, 425, 429, 500, 502, 503, 504}
    if isinstance(exc, APIError):
        return getattr(exc, "code", None) not in _NON_RETRYABLE_PG_CODES
    return False


def with_retry(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Decoratore: exponential backoff + jitter sui soli errori transitori."""
    @wraps(func)
    async def wrapper(self: "SupabaseManager", *args: Any, **kwargs: Any) -> T:
        policy = self.retry_policy
        last_exc: Optional[BaseException] = None
        for attempt in range(1, policy.max_attempts + 1):
            try:
                return await asyncio.wait_for(func(self, *args, **kwargs), timeout=self.op_timeout)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if not _is_retryable(exc) or attempt == policy.max_attempts:
                    break
                delay = min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1))
                delay += random.uniform(0, delay * policy.jitter)
                logger.warning("[%s] tentativo %d/%d fallito (%s: %s). Retry tra %.2fs",
                               func.__name__, attempt, policy.max_attempts,
                               type(exc).__name__, exc, delay)
                await asyncio.sleep(delay)
        if last_exc is not None and _is_retryable(last_exc):
            raise SupabaseConnectionError(
                f"{func.__name__}: Supabase non raggiungibile dopo {policy.max_attempts} tentativi"
            ) from last_exc
        raise last_exc  # type: ignore[misc]
    return wrapper


class SupabaseManager:
    """Gestore asincrono delle operazioni I/O su Supabase. Usare come async context manager."""

    def __init__(
        self,
        url: Optional[str] = None,
        key: Optional[str] = None,
        retry_policy: RetryPolicy = RetryPolicy(),
        op_timeout: float = 20.0,
    ) -> None:
        self.url = url or os.getenv("SUPABASE_URL")
        self.key = key or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devono essere impostate.")
        self.retry_policy = retry_policy
        self.op_timeout = op_timeout
        self.run_id = str(uuid.uuid4())
        self._client: Optional[AsyncClient] = None

    # ------------------------------------------------------------------ lifecycle
    async def connect(self) -> "SupabaseManager":
        policy = self.retry_policy
        for attempt in range(1, policy.max_attempts + 1):
            try:
                self._client = await acreate_client(self.url, self.key)
                await self.health_check()
                logger.info("Connessione a Supabase stabilita (run_id=%s)", self.run_id)
                return self
            except Exception as exc:  # noqa: BLE001
                await self.close()
                if attempt == policy.max_attempts or not _is_retryable(exc):
                    raise SupabaseConnectionError(f"Connessione fallita: {exc}") from exc
                delay = min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1))
                logger.warning("Connessione fallita (%d/%d): %s. Retry tra %.2fs",
                               attempt, policy.max_attempts, exc, delay)
                await asyncio.sleep(delay)
        raise SupabaseConnectionError("Connessione fallita")  # pragma: no cover

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.postgrest.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._client = None

    async def __aenter__(self) -> "SupabaseManager":
        return await self.connect()

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    @property
    def client(self) -> AsyncClient:
        if self._client is None:
            raise RuntimeError("SupabaseManager non connesso: chiamare connect() o usare 'async with'.")
        return self._client

    # ------------------------------------------------------------------ health
    async def health_check(self) -> bool:
        """Query leggera per validare URL, chiave e presenza dello schema."""
        await asyncio.wait_for(
            self.client.table("active_tickers").select("ticker").limit(1).execute(),
            timeout=self.op_timeout,
        )
        return True

    # ------------------------------------------------------------------ logs
    @with_retry
    async def _insert_log(self, row: dict[str, Any]) -> None:
        await self.client.table("bot_logs").insert(row).execute()

    async def log(self, level: str, message: str, ticker: Optional[str] = None,
                  context: Optional[dict[str, Any]] = None) -> None:
        """Scrive un log su DB. Non solleva mai: un errore di logging non deve fermare il bot."""
        level = level.upper()
        if level not in VALID_LEVELS:
            level = "INFO"
        getattr(logger, level.lower(), logger.info)("%s%s", f"[{ticker}] " if ticker else "", message)
        row = {"level": level, "message": message[:5000], "ticker": ticker.upper() if ticker else None,
               "context": context or {}, "run_id": self.run_id}
        try:
            await self._insert_log(row)
        except Exception as exc:  # noqa: BLE001
            logger.error("Impossibile persistere il log su Supabase: %s", exc)

    # ------------------------------------------------------------------ tickers
    @with_retry
    async def get_active_tickers(self) -> list[str]:
        res = await self.client.table("active_tickers").select("ticker").eq("is_active", True).execute()
        return [r["ticker"] for r in (res.data or [])]

    @with_retry
    async def get_active_ticker_limits(self) -> dict[str, float]:
        res = await (self.client.table("active_tickers").select("ticker,max_notional")
                     .eq("is_active", True).execute())
        return {r["ticker"].upper(): float(r["max_notional"]) for r in (res.data or [])}

    # ------------------------------------------------------------------ secrets
    @with_retry
    async def get_secret(self, name: str) -> Optional[str]:
        """Legge un segreto da Supabase Vault tramite RPC riservata al service_role."""
        res = await self.client.rpc("get_secret", {"p_name": name}).execute()
        return res.data or None

    async def resolve_secret(self, name: str) -> str:
        """Env var (es. GitHub Actions secrets) con fallback su Vault."""
        value = os.getenv(name) or await self.get_secret(name)
        if not value:
            raise ValueError(f"Segreto '{name}' non trovato né in env né in Supabase Vault.")
        # Il repo è pubblico: i valori letti dal Vault vanno oscurati nei log di Actions
        if os.getenv("GITHUB_ACTIONS") == "true":
            print(f"::add-mask::{value}", flush=True)
        return value

    # ------------------------------------------------------------------ news
    @with_retry
    async def is_news_processed(self, news_id: str) -> bool:
        res = await (self.client.table("processed_news").select("news_id")
                     .eq("news_id", str(news_id)).limit(1).execute())
        return bool(res.data)

    @with_retry
    async def get_processed_ids_since(self, since_iso: str) -> set[str]:
        """news_id già prenotati dalla data indicata (PostgREST restituisce max 1000 righe per pagina)."""
        ids: set[str] = set()
        offset, page = 0, 1000
        while True:
            res = await (self.client.table("processed_news").select("news_id")
                         .gte("processed_at", since_iso)
                         .range(offset, offset + page - 1).execute())
            rows = res.data or []
            ids.update(r["news_id"] for r in rows)
            if len(rows) < page:
                return ids
            offset += page

    @with_retry
    async def claim_news(self, news_id: str, ticker: str, headline: Optional[str] = None) -> bool:
        """Prenotazione atomica: True solo se questa esecuzione è la prima a vedere la notizia."""
        res = await self.client.rpc("claim_news", {
            "p_news_id": str(news_id), "p_ticker": ticker, "p_headline": headline,
        }).execute()
        return bool(res.data)

    @with_retry
    async def update_news_status(self, news_id: str, status: str,
                                 sentiment_score: Optional[float] = None) -> None:
        status = status.upper()
        if status not in VALID_NEWS_STATUS:
            raise ValueError(f"Status non valido: {status}")
        payload: dict[str, Any] = {"status": status}
        if sentiment_score is not None:
            payload["sentiment_score"] = round(max(-1.0, min(1.0, float(sentiment_score))), 4)
        await self.client.table("processed_news").update(payload).eq("news_id", str(news_id)).execute()

    # ------------------------------------------------------------------ trades
    @with_retry
    async def record_trade(self, *, news_id: Optional[str], ticker: str, side: str, qty: float,
                           entry_price: Optional[float], stop_loss: Optional[float],
                           take_profit: Optional[float], alpaca_order_id: Optional[str],
                           status: str = "submitted") -> None:
        row = {"news_id": news_id, "ticker": ticker.upper(), "side": side.lower(), "qty": qty,
               "entry_price": entry_price, "stop_loss": stop_loss, "take_profit": take_profit,
               "alpaca_order_id": alpaca_order_id, "status": status}
        await self.client.table("trades").upsert(row, on_conflict="alpaca_order_id").execute()

    # ------------------------------------------------------------------ report
    @with_retry
    async def get_rows_since(self, table: str, ts_column: str, since_iso: str,
                             columns: str = "*") -> list[dict[str, Any]]:
        res = await self.client.table(table).select(columns).gte(ts_column, since_iso).execute()
        return res.data or []

    @with_retry
    async def has_log_since(self, message_prefix: str, since_iso: str) -> bool:
        res = await (self.client.table("bot_logs").select("id")
                     .like("message", f"{message_prefix}%")
                     .gte("timestamp", since_iso).limit(1).execute())
        return bool(res.data)


# ---------------------------------------------------------------------- smoke test
async def _smoke_test() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    async with SupabaseManager() as db:
        tickers = await db.get_active_tickers()
        await db.log("INFO", "Smoke test connessione OK", context={"tickers": tickers})

        test_id = f"smoke-{db.run_id}"
        first = await db.claim_news(test_id, "AAPL", "Test headline")
        second = await db.claim_news(test_id, "AAPL", "Test headline")
        assert first and not second, "Deduplica non funzionante!"
        await db.update_news_status(test_id, "SKIPPED", sentiment_score=0.0)
        logger.info("Ticker attivi: %s | Deduplica OK", tickers)

        for name in ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"):
            found = bool(await db.get_secret(name))
            logger.info("Vault %s: %s", name, "presente" if found else "MANCANTE")


if __name__ == "__main__":
    asyncio.run(_smoke_test())
