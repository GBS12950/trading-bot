"""
Main Bot Orchestrator - News Sentiment Trading Bot

Flusso principale:
1. Recupera le notizie da Alpaca
2. Per ogni notizia e ticker, prova a "prenotarla" su Supabase
3. Se prenotata, analizza il sentiment
4. Se sentiment è decisivo (>0.3 o <-0.3), invia un ordine bracket
5. Registra il trade su Supabase
6. Aggiorna lo stato della notizia
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from alpaca_manager import AlpacaManager, AlpacaConnectionError
from sentiment_analyzer import SentimentAnalyzer
from supabase_manager import SupabaseManager, SupabaseConnectionError

logger = logging.getLogger("news_sentiment_bot")

# Configurazione
SENTIMENT_BUY_THRESHOLD = 0.3   # Buy se sentiment > 0.3
SENTIMENT_SELL_THRESHOLD = -0.3  # Sell se sentiment < -0.3
MIN_CONFIDENCE = 0.5             # Ignora se confidence < 0.5
POSITION_SIZE_PCT = 0.1          # 10% del portfolio per trade
MAX_NOTIONAL = 1000.0            # Max notional per trade
MAX_NEWS_AGE_MINUTES = 60        # Ignora notizie più vecchie


def _news_age_minutes(created_at: str) -> float:
    try:
        ts = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return float("inf")
    return (datetime.now(timezone.utc) - ts).total_seconds() / 60


class NewsBot:
    """Orchestratore principale del bot."""

    def __init__(
        self,
        sentiment_mode: str = "vader",
        dry_run: bool = False,
    ) -> None:
        self.sentiment_mode = sentiment_mode
        self.dry_run = dry_run
        self.supabase: Optional[SupabaseManager] = None
        self.alpaca: Optional[AlpacaManager] = None
        self.analyzer: Optional[SentimentAnalyzer] = None

    async def connect(self) -> "NewsBot":
        """Inizializza le connessioni a Supabase e Alpaca."""
        try:
            self.supabase = await SupabaseManager().connect()
            api_key = await self.supabase.resolve_secret("ALPACA_API_KEY")
            secret_key = await self.supabase.resolve_secret("ALPACA_SECRET_KEY")
            self.alpaca = await AlpacaManager(api_key=api_key, secret_key=secret_key).connect()
            self.analyzer = SentimentAnalyzer(mode=self.sentiment_mode)
            logger.info("Bot connesso a Supabase e Alpaca (dry_run=%s)", self.dry_run)
            return self
        except Exception as exc:  # noqa: BLE001
            logger.error("Errore di connessione: %s", exc)
            await self.close()
            raise

    async def close(self) -> None:
        """Chiude tutte le connessioni."""
        if self.alpaca:
            await self.alpaca.close()
        if self.supabase:
            await self.supabase.close()

    async def __aenter__(self) -> "NewsBot":
        return await self.connect()

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    # ------------------------------------------------------------------ main loop
    async def run_once(self) -> None:
        """Esegue una singola iterazione del bot."""
        if not self.supabase or not self.alpaca or not self.analyzer:
            raise RuntimeError("Bot non connesso")

        await self.supabase.log("INFO", "Bot loop started")

        try:
            if not await self.alpaca.is_market_open():
                await self.supabase.log("INFO", "Mercato chiuso: nessuna operazione")
                return

            # 1. Recupera i ticker attivi
            tickers = await self.supabase.get_active_tickers()
            if not tickers:
                await self.supabase.log("WARNING", "Nessun ticker attivo trovato")
                return

            logger.info("Ticker attivi: %s", tickers)

            # 2. Recupera le notizie
            try:
                news_items = await self.alpaca.get_news(symbols=tickers, limit=50)
            except AlpacaConnectionError as exc:
                await self.supabase.log("ERROR", f"Impossibile recuperare notizie: {exc}")
                return

            if not news_items:
                await self.supabase.log("INFO", "Nessuna notizia trovata")
                return

            logger.info("Recuperate %d notizie", len(news_items))

            # 3. Processa ogni notizia
            active = {t.upper() for t in tickers}
            for news in news_items:
                await self._process_news(news, active)

            await self.supabase.log("INFO", "Bot loop completed")

        except Exception as exc:  # noqa: BLE001
            logger.exception("Errore nel loop del bot")
            await self.supabase.log("ERROR", f"Errore nel bot loop: {exc}")

    async def _process_news(self, news, active: set[str]) -> None:
        """Processa una singola notizia."""
        if not self.supabase or not self.alpaca or not self.analyzer:
            return

        headline = news.headline
        summary = news.summary
        # Le notizie Alpaca citano spesso anche ticker non monitorati
        tickers = sorted({s.upper() for s in (news.symbols or [])} & active)
        if not tickers:
            return

        age = _news_age_minutes(news.created_at)
        if age > MAX_NEWS_AGE_MINUTES:
            logger.debug("News %s troppo vecchia (%.0f min)", news.id, age)
            return

        logger.info("Processing news %s: %s", news.id, headline[:60])
        sentiment = None

        for ticker in tickers:
            # Chiave per (notizia, ticker): una notizia su più ticker va valutata per ciascuno
            news_id = f"{news.id}:{ticker}"
            try:
                claimed = await self.supabase.claim_news(news_id, ticker, headline)
                if not claimed:
                    logger.debug("News %s già processata", news_id)
                    continue

                logger.info("Claimed news %s", news_id)

                if sentiment is None:
                    sentiment = await self.analyzer.analyze(summary or headline, headline)
                logger.info("Sentiment for %s: %s", ticker, sentiment)

                # Aggiorna il sentiment sulla news
                await self.supabase.update_news_status(
                    news_id, "CLAIMED", sentiment_score=sentiment.score
                )

                # Se confidence è bassa, salta
                if sentiment.confidence < MIN_CONFIDENCE:
                    logger.info("Confidence troppo bassa per %s: %.2f", ticker, sentiment.confidence)
                    await self.supabase.update_news_status(news_id, "SKIPPED")
                    continue

                # Decidi se comprare, vendere, o skippare
                if sentiment.score > SENTIMENT_BUY_THRESHOLD:
                    await self._execute_trade(news_id, ticker, "buy", sentiment.score, headline)
                elif sentiment.score < SENTIMENT_SELL_THRESHOLD:
                    await self._execute_trade(news_id, ticker, "sell", sentiment.score, headline)
                else:
                    logger.info("Sentiment neutrale per %s: %.4f", ticker, sentiment.score)
                    await self.supabase.update_news_status(news_id, "SKIPPED")

            except Exception as exc:  # noqa: BLE001
                logger.exception("Errore nel processare news %s per %s", news_id, ticker)
                await self.supabase.update_news_status(news_id, "FAILED")

    async def _execute_trade(self, news_id: str, ticker: str, side: str, sentiment: float, headline: str) -> None:
        """Esegue un trade basato sul sentiment."""
        if not self.supabase or not self.alpaca:
            return

        try:
            if await self.alpaca.has_exposure(ticker):
                logger.info("Posizione/ordine già aperto su %s: skip", ticker)
                await self.supabase.update_news_status(news_id, "SKIPPED")
                return

            quote = await self.alpaca.get_quote(ticker)
            entry_price = quote.get("bid_price") if side == "sell" else quote.get("ask_price")
            if not entry_price or entry_price <= 0:
                logger.warning("Quote non disponibile per %s", ticker)
                await self.supabase.log("WARNING", f"Quote non disponibile per {ticker}", ticker=ticker)
                await self.supabase.update_news_status(news_id, "SKIPPED")
                return

            # Calcola la quantità basato sul portfolio size
            account = await self.alpaca.get_account()
            portfolio_value = float(account.get("portfolio_value", 0))
            notional = min(portfolio_value * POSITION_SIZE_PCT, MAX_NOTIONAL)
            qty = int(notional / entry_price)
            if qty < 1:
                logger.info("Prezzo di %s troppo alto per il notional massimo: skip", ticker)
                await self.supabase.update_news_status(news_id, "SKIPPED")
                return

            logger.info("Executing %s order for %s: %d @ %.2f (sentiment=%.4f)",
                       side, ticker, qty, entry_price, sentiment)

            if self.dry_run:
                logger.info("DRY RUN: Ordine non inviato")
                await self.supabase.log(
                    "INFO",
                    f"DRY RUN: {side.upper()} {qty} {ticker} @ ${entry_price:.2f} (sentiment={sentiment:.4f})",
                    ticker=ticker,
                )
                await self.supabase.record_trade(
                    news_id=news_id,
                    ticker=ticker,
                    side=side,
                    qty=qty,
                    entry_price=entry_price,
                    stop_loss=entry_price * (1 - 0.02) if side == "buy" else entry_price * (1 + 0.02),
                    take_profit=entry_price * (1 + 0.05) if side == "buy" else entry_price * (1 - 0.05),
                    alpaca_order_id=f"dry-run-{news_id}",
                    status="dry_run",
                )
                await self.supabase.update_news_status(news_id, "ORDERED")
                return

            # Invia l'ordine bracket
            order = await self.alpaca.place_bracket_order(
                symbol=ticker,
                qty=qty,
                side=side,
                entry_price=entry_price,
                stop_loss_pct=0.02,
                take_profit_pct=0.05,
            )

            logger.info("Order placed: %s (status=%s)", order.order_id, order.status)

            # Registra il trade su Supabase
            sl_price = entry_price * (1 - 0.02) if side == "buy" else entry_price * (1 + 0.02)
            tp_price = entry_price * (1 + 0.05) if side == "buy" else entry_price * (1 - 0.05)

            await self.supabase.record_trade(
                news_id=news_id,
                ticker=ticker,
                side=side,
                qty=qty,
                entry_price=entry_price,
                stop_loss=sl_price,
                take_profit=tp_price,
                alpaca_order_id=order.order_id,
                status="submitted",
            )

            # Aggiorna lo stato della notizia
            await self.supabase.update_news_status(news_id, "ORDERED")

            await self.supabase.log(
                "INFO",
                f"Trade executed: {side.upper()} {qty} {ticker} @ ${entry_price:.2f} | SL=${sl_price:.2f} | TP=${tp_price:.2f}",
                ticker=ticker,
                context={"order_id": order.order_id, "sentiment": sentiment},
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("Errore nell'esecuzione del trade")
            await self.supabase.log("ERROR", f"Errore nell'esecuzione trade: {exc}", ticker=ticker)
            await self.supabase.update_news_status(news_id, "FAILED")

    async def run_scheduled(self, interval_seconds: int = 300) -> None:
        """Esegue il bot periodicamente (intervallo in secondi)."""
        logger.info("Bot scheduled loop avviato (intervallo=%ds)", interval_seconds)
        try:
            while True:
                await self.run_once()
                await asyncio.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Bot fermato dall'utente")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Errore fatale nel scheduled loop")


# ---------------------------------------------------------------------- entry point
async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    mode = os.getenv("SENTIMENT_MODE", "vader")

    async with NewsBot(sentiment_mode=mode, dry_run=dry_run) as bot:
        await bot.run_once()


if __name__ == "__main__":
    asyncio.run(_main())
