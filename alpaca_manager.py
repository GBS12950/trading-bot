"""
AlpacaManager - Integrazione con Alpaca Markets API per Paper Trading.

Variabili d'ambiente richieste:
    ALPACA_API_KEY              Alpaca API Key
    ALPACA_SECRET_KEY           Alpaca Secret Key
    ALPACA_BASE_URL             https://paper-api.alpaca.markets (paper trading)
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx

logger = logging.getLogger("alpaca_manager")

# Endpoint Alpaca
ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_URL = "https://data.alpaca.markets"


@dataclass
class NewsItem:
    """Rappresenta una notizia da Alpaca."""
    id: str
    headline: str
    summary: str
    source: str
    url: str
    created_at: str
    updated_at: str
    symbols: list[str]


@dataclass
class Order:
    """Rappresenta un ordine inviato ad Alpaca."""
    order_id: str
    symbol: str
    qty: float
    side: str
    status: str
    filled_qty: float
    filled_avg_price: Optional[float]


class AlpacaConnectionError(RuntimeError):
    """Sollevata quando Alpaca è irraggiungibile."""


class AlpacaManager:
    """Gestore asincrono delle operazioni su Alpaca Markets."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        base_url: str = ALPACA_BASE_URL,
        data_url: str = ALPACA_DATA_URL,
    ) -> None:
        import os
        self.api_key = api_key or os.getenv("ALPACA_API_KEY")
        self.secret_key = secret_key or os.getenv("ALPACA_SECRET_KEY")
        self.base_url = base_url
        self.data_url = data_url

        if not self.api_key or not self.secret_key:
            raise ValueError("ALPACA_API_KEY e ALPACA_SECRET_KEY devono essere impostate.")

        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------ lifecycle
    async def connect(self) -> "AlpacaManager":
        """Crea il client HTTP asincrono."""
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=20.0,
        )
        logger.info("Connessione ad Alpaca stabilita")
        return self

    async def close(self) -> None:
        """Chiude il client HTTP."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "AlpacaManager":
        return await self.connect()

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("AlpacaManager non connesso: chiamare connect() o usare 'async with'.")
        return self._client

    # ------------------------------------------------------------------ account
    async def get_account(self) -> dict[str, Any]:
        """Recupera lo stato dell'account."""
        res = await self.client.get("/v2/account")
        res.raise_for_status()
        return res.json()

    async def get_json(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        res = await self.client.get(path, params=params)
        res.raise_for_status()
        return res.json()

    async def is_market_open(self) -> bool:
        res = await self.client.get("/v2/clock")
        res.raise_for_status()
        return bool(res.json().get("is_open"))

    async def has_exposure(self, symbol: str) -> bool:
        """True se esiste già una posizione o un ordine aperto sul simbolo."""
        symbol = symbol.upper()
        res = await self.client.get(f"/v2/positions/{symbol}")
        if res.status_code == 200:
            return True
        if res.status_code != 404:
            res.raise_for_status()
        orders = await self.get_orders(status="open")
        return any(o.symbol == symbol for o in orders)

    # ------------------------------------------------------------------ news
    async def get_news(
        self,
        symbols: Optional[list[str]] = None,
        limit: int = 50,
        sort: str = "desc",
        include_content: bool = True,
        start: Optional[str] = None,
        max_pages: int = 10,
    ) -> list[NewsItem]:
        """
        Recupera le notizie per i simboli specificati (endpoint /v1beta1/news).

        Args:
            symbols: Lista di ticker (es. ['AAPL', 'MSFT'])
            limit: Notizie per pagina (max 50 per Alpaca)
            sort: 'asc' o 'desc'
            include_content: Se True, include headline e summary
            start: Timestamp RFC3339: se indicato scarica tutte le notizie da quel momento
            max_pages: Limite di pagine quando start è indicato

        Returns:
            Lista di NewsItem
        """
        if not symbols:
            symbols = ["*"]  # Tutte le notizie

        params: dict[str, Any] = {
            "symbols": ",".join(symbols),
            "limit": min(limit, 50),
            "sort": sort,
        }
        if start:
            params["start"] = start

        try:
            headers = {
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.secret_key,
            }
            items: list[dict[str, Any]] = []
            async with httpx.AsyncClient(headers=headers, timeout=20.0) as client:
                for _ in range(max_pages if start else 1):
                    res = await client.get(f"{self.data_url}/v1beta1/news", params=params)
                    res.raise_for_status()
                    data = res.json()
                    items.extend(data.get("news", []))
                    token = data.get("next_page_token")
                    if not token:
                        break
                    params["page_token"] = token

            news_list = []
            for item in items:
                news_list.append(
                    NewsItem(
                        id=item.get("id", ""),
                        headline=item.get("headline", ""),
                        summary=item.get("summary", ""),
                        source=item.get("source", ""),
                        url=item.get("url", ""),
                        created_at=item.get("created_at", ""),
                        updated_at=item.get("updated_at", ""),
                        symbols=item.get("symbols", []),
                    )
                )
            logger.info("Recuperate %d notizie da Alpaca", len(news_list))
            return news_list

        except httpx.HTTPError as exc:
            logger.error("Errore nel recupero notizie da Alpaca: %s", exc)
            raise AlpacaConnectionError(f"Impossibile recuperare notizie: {exc}") from exc

    # ------------------------------------------------------------------ orders
    async def place_bracket_order(
        self,
        symbol: str,
        qty: float,
        side: str = "buy",
        entry_price: Optional[float] = None,
        stop_loss_pct: float = 0.02,  # -2%
        take_profit_pct: float = 0.05,  # +5%
    ) -> Order:
        """
        Invia un ordine Market con Stop Loss e Take Profit (ordine bracket).

        Args:
            symbol: Ticker (es. 'AAPL')
            qty: Quantità da comprare
            side: 'buy' o 'sell'
            entry_price: Prezzo di entry (per calcolare SL/TP); se None, usa il prezzo di mercato
            stop_loss_pct: Percentuale di stop loss (es. 0.02 = -2%)
            take_profit_pct: Percentuale di take profit (es. 0.05 = +5%)

        Returns:
            Order con i dettagli dell'ordine principale inviato
        """
        try:
            # Se entry_price non è fornito, recupera il prezzo di mercato
            if entry_price is None:
                quote = await self.get_quote(symbol)
                entry_price = quote.get("ask_price", quote.get("bid_price", 100.0))

            # Calcola SL e TP
            if side.lower() == "buy":
                stop_loss_price = entry_price * (1 - stop_loss_pct)
                take_profit_price = entry_price * (1 + take_profit_pct)
            else:  # sell
                stop_loss_price = entry_price * (1 + stop_loss_pct)
                take_profit_price = entry_price * (1 - take_profit_pct)

            # Ordine principale (Market)
            order_payload = {
                "symbol": symbol.upper(),
                "qty": qty,
                "side": side.lower(),
                "type": "market",
                "time_in_force": "day",
                "order_class": "bracket",
                "take_profit": {
                    "limit_price": round(take_profit_price, 2),
                },
                "stop_loss": {
                    "stop_price": round(stop_loss_price, 2),
                },
            }

            res = await self.client.post("/v2/orders", json=order_payload)
            res.raise_for_status()
            data = res.json()

            order = Order(
                order_id=data.get("id", ""),
                symbol=data.get("symbol", ""),
                qty=float(data.get("qty", 0)),
                side=data.get("side", ""),
                status=data.get("status", ""),
                filled_qty=float(data.get("filled_qty", 0)),
                filled_avg_price=float(data.get("filled_avg_price") or 0) or None,
            )

            logger.info(
                "Ordine bracket inviato: %s %d %s @ %s (SL=%.2f, TP=%.2f)",
                symbol, qty, side, entry_price, stop_loss_price, take_profit_price,
            )
            return order

        except httpx.HTTPError as exc:
            logger.error("Errore nell'invio ordine a Alpaca: %s", exc)
            raise AlpacaConnectionError(f"Impossibile inviare ordine: {exc}") from exc

    async def get_orders(self, status: str = "open", limit: int = 100) -> list[Order]:
        """Recupera gli ordini aperti."""
        try:
            res = await self.client.get(
                "/v2/orders",
                params={"status": status, "limit": limit},
            )
            res.raise_for_status()
            orders = []
            for data in res.json():
                orders.append(
                    Order(
                        order_id=data.get("id", ""),
                        symbol=data.get("symbol", ""),
                        qty=float(data.get("qty", 0)),
                        side=data.get("side", ""),
                        status=data.get("status", ""),
                        filled_qty=float(data.get("filled_qty", 0)),
                        filled_avg_price=float(data.get("filled_avg_price") or 0) or None,
                    )
                )
            return orders
        except httpx.HTTPError as exc:
            logger.error("Errore nel recupero ordini: %s", exc)
            raise AlpacaConnectionError(f"Impossibile recuperare ordini: {exc}") from exc

    async def cancel_order(self, order_id: str) -> bool:
        """Cancella un ordine."""
        try:
            res = await self.client.delete(f"/v2/orders/{order_id}")
            res.raise_for_status()
            logger.info("Ordine %s cancellato", order_id)
            return True
        except httpx.HTTPError as exc:
            logger.error("Errore nel cancellare ordine: %s", exc)
            return False

    # ------------------------------------------------------------------ market data
    async def get_quote(self, symbol: str) -> dict[str, Any]:
        """Recupera il quote (bid/ask) per un simbolo."""
        try:
            headers = {
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.secret_key,
            }
            async with httpx.AsyncClient(headers=headers, timeout=20.0) as client:
                res = await client.get(
                    f"{self.data_url}/v2/stocks/{symbol}/latest/quote",
                )
                res.raise_for_status()
                data = res.json()
                quote = data.get("quote", {})
                return {
                    "bid_price": quote.get("bp"),
                    "ask_price": quote.get("ap"),
                    "bid_size": quote.get("bs"),
                    "ask_size": quote.get("as"),
                }
        except httpx.HTTPError as exc:
            logger.error("Errore nel recupero quote: %s", exc)
            return {}


# ---------------------------------------------------------------------- smoke test
async def _smoke_test() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    async with AlpacaManager() as alpaca:
        account = await alpaca.get_account()
        logger.info("Account: %s (Portfolio Value: $%.2f)", account.get("account_number"), float(account.get("portfolio_value", 0)))

        # Recupera ultime notizie
        news = await alpaca.get_news(symbols=["AAPL", "MSFT"], limit=5)
        for item in news[:2]:
            logger.info("News: %s - %s", item.symbols, item.headline)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
