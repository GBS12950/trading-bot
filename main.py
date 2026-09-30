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
from datetime import datetime
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
            self.alpaca = await AlpacaManager().connect()
            self.analyzer = SentimentAnalyzer(mode=self.sentiment_mode)
            logger.info("Bot connesso a Supabase e Alpaca")
            return self
        except (SupabaseConnectionError, AlpacaConnectionError) as exc:
            logger.error("Errore di connessione: %s", exc)
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
            for news in news_items:
                await self._process_news(news)

            await self.supabase.log("INFO", "Bot loop completed")

        except Exception as exc:  # noqa: BLE001
            logger.exception("Errore nel loop del bot")
            await self.supabase.log("ERROR", f"Errore nel bot loop: {exc}")

    async def _process_news(self, news) -> None:
        """Processa una singola notizia."""
        if not self.supabase or not self.alpaca or not self.analyzer:
            return

        news_id = news.id
        headline = news.headline
        summary = news.summary
        symbols = news.symbols or []

        logger.info("Processing news %s: %s", news_id, headline[:60])

        for ticker in symbols:
            try:
                ticker = ticker.upper()

                # Prova a "prenotare" la notizia
                claimed = await self.supabase.claim_news(news_id, ticker, headline)
                if not claimed:
                    logger.debug("News %s già processata per %s", news_id, ticker)
                    continue

                logger.info("Claimed news %s for %s", news_id, ticker)

                # Analizza il sentiment
                sentiment = await self.analyzer.analyze(summary, headline)
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
            # Recupera il quote
            quote = await self.alpaca.get_quote(ticker)
            if not quote or not quote.get("bid_price"):
                logger.warning("Quote non disponibile per %s", ticker)
                await self.supabase.log("WARNING", f"Quote non disponibile per {ticker}", ticker=ticker)
                return

            entry_price = quote["bid_price"] if side == "sell" else quote["ask_price"]

            # Calcola la quantità basato sul portfolio size
            account = await self.alpaca.get_account()
            portfolio_value = float(account.get("portfolio_value", 0))
            notional = min(portfolio_value * POSITION_SIZE_PCT, MAX_NOTIONAL)
            qty = max(1, int(notional / entry_price))

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


# ---------------------------------------------------------------------- smoke test
async def _smoke_test() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    async with NewsBot(sentiment_mode="vader", dry_run=True) as bot:
        logger.info("=== Running single bot iteration (DRY RUN) ===")
        await bot.run_once()


if __name__ == "__main__":
    asyncio.run(_smoke_test())
