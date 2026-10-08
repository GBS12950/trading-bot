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
import socket
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from alpaca_manager import AlpacaManager, AlpacaConnectionError
from sentiment_analyzer import SentimentAnalyzer
from supabase_manager import SupabaseManager
from telegram_notifier import TelegramNotifier, esc
from translator import translate_to_italian

logger = logging.getLogger("news_sentiment_bot")

# Configurazione
SENTIMENT_BUY_THRESHOLD = 0.3   # Buy se sentiment > 0.3
SENTIMENT_SELL_THRESHOLD = -0.3  # Sell se sentiment < -0.3
MIN_CONFIDENCE = 0.5             # Ignora se confidence < 0.5
POSITION_SIZE_PCT = 0.1          # 10% del portfolio per trade
MAX_NOTIONAL = 1000.0            # Default se il ticker non ha max_notional
# Finestra notizie: "since_close" = dalla chiusura della sessione precedente,
# "minutes" = solo le ultime NEWS_MAX_AGE_MINUTES
NEWS_WINDOW = os.getenv("NEWS_WINDOW", "since_close").lower()
MAX_NEWS_AGE_MINUTES = int(os.getenv("NEWS_MAX_AGE_MINUTES", "60"))
MAX_DAILY_LOSS_PCT = 0.02        # Stop nuove operazioni se il portafoglio perde il 2% nel giorno
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "20"))
LOSS_LIMIT_MSG = "Limite di perdita giornaliera raggiunto"
NY = ZoneInfo("America/New_York")
ROME = ZoneInfo("Europe/Rome")
STOP_LOSS_PCT = 0.02
TAKE_PROFIT_PCT = 0.05
MAX_CLOSED_SLEEP_SECONDS = 300   # A mercato chiuso si risveglia almeno ogni 5 minuti per il segnale di vita
NEWS_OVERLAP_MINUTES = 10        # In modalità continua rilegge gli ultimi minuti per non perdere notizie in ritardo
ALERT_COOLDOWN_MINUTES = 30      # Un avviso Telegram di errore ogni 30 minuti al massimo


def _parse_ts(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _clip(text: str, limit: int = 400) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[:limit].rsplit(" ", 1)[0] + "..."


class NewsBot:
    """Orchestratore principale del bot."""

    def __init__(
        self,
        sentiment_mode: str = "vader",
        dry_run: bool = False,
        continuous: bool = False,
    ) -> None:
        self.sentiment_mode = sentiment_mode
        self.dry_run = dry_run
        self.continuous = continuous
        self.supabase: Optional[SupabaseManager] = None
        self.alpaca: Optional[AlpacaManager] = None
        self.analyzer: Optional[SentimentAnalyzer] = None
        self.notifier = TelegramNotifier(None, None)
        self._limits: dict[str, float] = {}
        self._last_news_fetch: Optional[datetime] = None
        self._last_alert: dict[str, datetime] = {}

    async def connect(self) -> "NewsBot":
        """Inizializza le connessioni a Supabase e Alpaca."""
        try:
            self.supabase = await SupabaseManager().connect()
            api_key = await self.supabase.resolve_secret("ALPACA_API_KEY")
            secret_key = await self.supabase.resolve_secret("ALPACA_SECRET_KEY")
            self.alpaca = await AlpacaManager(api_key=api_key, secret_key=secret_key).connect()
            self.analyzer = SentimentAnalyzer(mode=self.sentiment_mode)
            self.notifier = await TelegramNotifier.from_supabase(self.supabase)
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

        if not self.continuous:
            await self.supabase.log("INFO", "Bot loop started")

        try:
            if not await self.alpaca.is_market_open():
                if not self.continuous:
                    await self.supabase.log("INFO", "Mercato chiuso: nessuna operazione")
                return

            if await self._daily_loss_limit_hit(await self.alpaca.get_account()):
                return

            # 1. Recupera i ticker attivi e i relativi limiti
            self._limits = await self.supabase.get_active_ticker_limits()
            tickers = list(self._limits)
            if not tickers:
                await self.supabase.log("WARNING", "Nessun ticker attivo trovato")
                return

            logger.info("Ticker attivi: %s", tickers)

            # 2. Recupera le notizie della finestra configurata
            window_start, window_label = await self._news_window_start()
            if self.continuous and self._last_news_fetch:
                # Dal secondo ciclo in poi basta rileggere dall'ultimo controllo
                narrowed = self._last_news_fetch - timedelta(minutes=NEWS_OVERLAP_MINUTES)
                if narrowed > window_start:
                    window_start, window_label = narrowed, "dall'ultimo controllo"

            fetch_started = datetime.now(timezone.utc)
            try:
                news_items = await self.alpaca.get_news(
                    symbols=tickers, start=window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            except AlpacaConnectionError as exc:
                logger.error("Impossibile recuperare notizie: %s", exc)
                if self._should_alert("news"):
                    await self.supabase.log("ERROR", f"Impossibile recuperare notizie: {exc}")
                    await self.notifier.notify(f"<b>Notizie non disponibili</b>\n{esc(exc)}")
                return

            self._last_news_fetch = fetch_started
            if not news_items:
                if self.continuous:
                    logger.info("Nessuna notizia trovata")
                else:
                    await self.supabase.log("INFO", "Nessuna notizia trovata")
                return

            logger.info("Recuperate %d notizie", len(news_items))

            # 3. Processa ogni notizia, saltando in blocco quelle già prenotate
            seen = await self.supabase.get_processed_ids_since(window_start.isoformat())
            active = {t.upper() for t in tickers}
            stats: Counter[str] = Counter()
            for news in news_items:
                stats[await self._process_news(news, active, window_start, seen)] += 1

            summary_msg = (
                f"Notizie ({window_label}): {len(news_items)} scaricate, {stats['old']} fuori finestra, "
                f"{stats['seen']} già elaborate, {stats['new']} nuove"
            )
            # In modalità continua scrive su Supabase solo quando c'è qualcosa di nuovo
            if stats["new"] or not self.continuous:
                await self.supabase.log("INFO", summary_msg, context=dict(stats))
            else:
                logger.info(summary_msg)

        except Exception as exc:  # noqa: BLE001
            logger.exception("Errore nel loop del bot")
            if self._should_alert("loop"):
                await self.supabase.log("ERROR", f"Errore nel bot loop: {exc}")
                await self.notifier.notify(f"<b>Errore nel bot</b>\n{esc(exc)}")

    def _should_alert(self, key: str) -> bool:
        """Evita di ripetere lo stesso avviso ogni minuto durante un'interruzione prolungata."""
        now = datetime.now(timezone.utc)
        last = self._last_alert.get(key)
        if last and now - last < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
            return False
        self._last_alert[key] = now
        return True

    async def _daily_loss_limit_hit(self, account: dict[str, Any]) -> bool:
        start = float(account.get("last_equity") or 0)
        now = float(account.get("equity") or 0)
        if start <= 0 or (now - start) / start > -MAX_DAILY_LOSS_PCT:
            return False

        msg = f"{LOSS_LIMIT_MSG} ({(now - start) / start:+.2%}): nuove operazioni sospese fino a domani"
        day_start = datetime.now(NY).replace(hour=0, minute=0, second=0, microsecond=0)
        already_notified = await self.supabase.has_log_since(
            LOSS_LIMIT_MSG, day_start.astimezone(timezone.utc).isoformat()
        )
        if already_notified:
            logger.warning(msg)
        else:
            await self.supabase.log("WARNING", msg)
            await self.notifier.notify(f"<b>Stop giornaliero</b>\n{esc(msg)}")
        return True

    async def _news_window_start(self) -> tuple[datetime, str]:
        """Inizio della finestra notizie secondo NEWS_WINDOW."""
        now = datetime.now(timezone.utc)
        if NEWS_WINDOW == "minutes":
            return now - timedelta(minutes=MAX_NEWS_AGE_MINUTES), f"ultimi {MAX_NEWS_AGE_MINUTES} min"

        today = datetime.now(NY).date()
        calendar = await self.alpaca.get_json(
            "/v2/calendar",
            {"start": (today - timedelta(days=10)).isoformat(), "end": today.isoformat()},
        )
        previous = [d for d in calendar or [] if d.get("date", "") < today.isoformat()]
        if not previous:
            return now - timedelta(hours=24), "ultime 24 ore"
        hour, minute = map(int, previous[-1]["close"].split(":")[:2])
        close_ny = datetime.fromisoformat(previous[-1]["date"]).replace(hour=hour, minute=minute, tzinfo=NY)
        return close_ny.astimezone(timezone.utc), f"dalla chiusura del {close_ny:%d/%m %H:%M} NY"

    async def _process_news(self, news, active: set[str], window_start: datetime,
                            seen: set[str]) -> str:
        """Processa una notizia e ritorna l'esito: untracked, old, seen o new."""
        if not self.supabase or not self.alpaca or not self.analyzer:
            return "untracked"

        headline = news.headline
        summary = news.summary
        # Le notizie Alpaca citano spesso anche ticker non monitorati
        tickers = sorted({s.upper() for s in (news.symbols or [])} & active)
        if not tickers:
            return "untracked"

        created = _parse_ts(news.created_at)
        if created is None or created < window_start:
            return "old"

        logger.info("Processing news %s: %s", news.id, headline[:60])
        sentiment = None
        processed = False

        for ticker in tickers:
            # Chiave per (notizia, ticker): una notizia su più ticker va valutata per ciascuno
            news_id = f"{news.id}:{ticker}"
            if news_id in seen:
                continue
            try:
                claimed = await self.supabase.claim_news(news_id, ticker, headline)
                if not claimed:
                    logger.debug("News %s già processata", news_id)
                    continue

                logger.info("Claimed news %s", news_id)
                processed = True

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
                    await self._execute_trade(news_id, ticker, "buy", sentiment.score, headline, news)
                elif sentiment.score < SENTIMENT_SELL_THRESHOLD:
                    await self._execute_trade(news_id, ticker, "sell", sentiment.score, headline, news)
                else:
                    logger.info("Sentiment neutrale per %s: %.4f", ticker, sentiment.score)
                    await self.supabase.update_news_status(news_id, "SKIPPED")

            except Exception as exc:  # noqa: BLE001
                logger.exception("Errore nel processare news %s per %s", news_id, ticker)
                await self.supabase.update_news_status(news_id, "FAILED")

        return "new" if processed else "seen"

    async def _execute_trade(self, news_id: str, ticker: str, side: str, sentiment: float,
                             headline: str, news=None) -> None:
        """Esegue un trade basato sul sentiment."""
        if not self.supabase or not self.alpaca:
            return

        try:
            if await self.alpaca.has_exposure(ticker):
                logger.info("Posizione/ordine già aperto su %s: skip", ticker)
                await self.supabase.update_news_status(news_id, "SKIPPED")
                return

            open_positions = await self.alpaca.get_json("/v2/positions") or []
            if len(open_positions) >= MAX_OPEN_POSITIONS:
                logger.info("Raggiunto il massimo di %d posizioni aperte: skip %s", MAX_OPEN_POSITIONS, ticker)
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
            notional = min(portfolio_value * POSITION_SIZE_PCT, self._limits.get(ticker, MAX_NOTIONAL))
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
                stop_loss_pct=STOP_LOSS_PCT,
                take_profit_pct=TAKE_PROFIT_PCT,
            )

            logger.info("Order placed: %s (status=%s)", order.order_id, order.status)

            # Registra il trade con i livelli effettivamente inviati ad Alpaca
            entry_price = order.entry_price or entry_price
            sl_price = order.stop_loss
            tp_price = order.take_profit

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
            await self._notify_trade(
                side=side, qty=qty, ticker=ticker, entry_price=entry_price,
                sl_price=sl_price, tp_price=tp_price, sentiment=sentiment,
                headline=headline, news=news,
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("Errore nell'esecuzione del trade")
            await self.supabase.log("ERROR", f"Errore nell'esecuzione trade: {exc}", ticker=ticker)
            await self.supabase.update_news_status(news_id, "FAILED")
            await self.notifier.notify(
                f"<b>Ordine fallito su {esc(ticker)}</b> ({'acquisto' if side == 'buy' else 'vendita'})\n"
                f"{esc(exc)}\n<i>{esc(headline[:200])}</i>"
            )

    async def _notify_trade(self, *, side: str, qty: int, ticker: str, entry_price: float,
                            sl_price: float, tp_price: float, sentiment: float,
                            headline: str, news=None) -> None:
        """Avviso Telegram: tipo di operazione, livelli e notizia che l'ha generata (tradotta)."""
        summary = (getattr(news, "summary", "") or "").strip()
        it_headline, it_summary = await asyncio.gather(
            translate_to_italian(headline), translate_to_italian(summary)
        )

        is_buy = side == "buy"
        threshold = SENTIMENT_BUY_THRESHOLD if is_buy else SENTIMENT_SELL_THRESHOLD
        sl_sign, tp_sign = ("-", "+") if is_buy else ("+", "-")
        lines = [
            f"<b>{'ACQUISTO' if is_buy else 'VENDITA ALLO SCOPERTO (short)'}: {esc(ticker)}</b>",
            f"{qty} azioni a ${entry_price:,.2f} (circa ${qty * entry_price:,.0f})",
            f"Stop Loss ${sl_price:,.2f} ({sl_sign}{STOP_LOSS_PCT:.0%}) | "
            f"Take Profit ${tp_price:,.2f} ({tp_sign}{TAKE_PROFIT_PCT:.0%})",
            f"Motivo: sentiment {'positivo' if sentiment > 0 else 'negativo'} {sentiment:+.2f} "
            f"(soglia {threshold:+.2f})",
            "",
            "<b>Notizia che ha generato l'operazione</b>",
        ]

        meta = []
        if getattr(news, "source", ""):
            meta.append(esc(news.source))
        created = _parse_ts(getattr(news, "created_at", ""))
        if created:
            meta.append(created.astimezone(ROME).strftime("%d/%m %H:%M"))
        if meta:
            lines.append(" - ".join(meta))

        lines.append(f"<i>{esc(it_headline or headline)}</i>")
        if summary:
            lines.append(esc(it_summary or _clip(summary)))
        lines.append(f"Originale: {esc(headline)}" if it_headline else "(traduzione non disponibile)")

        url = getattr(news, "url", "") or ""
        if url.startswith(("http://", "https://")):
            lines.append(f'<a href="{esc(url)}">Leggi la notizia</a>')

        await self.notifier.notify("\n".join(lines))

    async def heartbeat(self, started: bool = False) -> None:
        """Segnale di vita su Supabase; avvisa su Telegram di avvii, riavvii e ritorni online."""
        try:
            prev = await self.supabase.bot_ping(started=started)
        except Exception as exc:  # noqa: BLE001
            if self._should_alert("heartbeat"):
                logger.warning("Segnale di vita non registrato (schema.sql aggiornato?): %s", exc)
            return

        host = esc(socket.gethostname())
        prev_seen = _parse_ts((prev or {}).get("prev_seen"))
        silent = int((datetime.now(timezone.utc) - prev_seen).total_seconds() // 60) if prev_seen else 0
        last_txt = prev_seen.astimezone(ROME).strftime("%d/%m %H:%M") if prev_seen else ""

        if (prev or {}).get("prev_alerted"):
            await self.notifier.notify(
                f"<b>Bot di nuovo online</b> su {host}\n"
                f"Nessun segnale per circa {silent} minuti (dalle {last_txt})."
            )
        elif started and prev_seen is None:
            await self.notifier.notify(f"<b>Bot avviato</b> su {host} (primo avvio)")
        elif started:
            await self.notifier.notify(
                f"<b>Bot riavviato</b> su {host}\nUltimo segnale: {last_txt} ({silent} minuti fa)."
            )

    async def seconds_until_open(self) -> float:
        """0 se il mercato è aperto, altrimenti i secondi alla prossima apertura."""
        clock = await self.alpaca.get_json("/v2/clock")
        if clock.get("is_open"):
            return 0.0
        next_open = datetime.fromisoformat(clock["next_open"].replace("Z", "+00:00"))
        return max(0.0, (next_open - datetime.now(timezone.utc)).total_seconds())


# ---------------------------------------------------------------------- entry point
async def _run_forever(mode: str, dry_run: bool, interval: int) -> None:
    """Modalità continua (Raspberry Pi): un ciclo ogni `interval` secondi a mercato aperto."""
    logger.info("Modalità continua avviata (intervallo=%ds, dry_run=%s)", interval, dry_run)
    bot: Optional[NewsBot] = None
    started_announced = False
    while True:
        wait: float = interval
        try:
            if bot is None:
                bot = await NewsBot(sentiment_mode=mode, dry_run=dry_run, continuous=True).connect()

            # Segnale di vita ad ogni giro, anche a mercato chiuso: lo controlla il watchdog su Supabase
            await bot.heartbeat(started=not started_announced)
            started_announced = True

            until_open = await bot.seconds_until_open()
            if until_open > 0:
                wait = min(until_open + 5, MAX_CLOSED_SLEEP_SECONDS)
                logger.info("Mercato chiuso, riapre tra %.0f min: attendo %.0f min",
                            until_open / 60, wait / 60)
            else:
                await bot.run_once()
        except Exception:  # noqa: BLE001
            logger.exception("Ciclo fallito, riconnessione al prossimo giro")
            if bot is not None:
                await bot.close()
                bot = None
        await asyncio.sleep(wait)


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    mode = os.getenv("SENTIMENT_MODE", "vader")

    if os.getenv("RUN_FOREVER", "false").lower() == "true":
        await _run_forever(mode, dry_run, int(os.getenv("LOOP_INTERVAL_SECONDS", "60")))
        return

    async with NewsBot(sentiment_mode=mode, dry_run=dry_run) as bot:
        await bot.run_once()


if __name__ == "__main__":
    asyncio.run(_main())
