"""
Riepilogo giornaliero su Telegram alla chiusura del mercato USA.

Segreti (env o Supabase Vault): TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""
from __future__ import annotations

import asyncio
import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from alpaca_manager import AlpacaManager
from supabase_manager import SupabaseManager
from telegram_notifier import TelegramNotifier, esc as _esc

logger = logging.getLogger("daily_report")

NY = ZoneInfo("America/New_York")
REPORT_WINDOW_MINUTES = 60
ORDER_KIND = {"market": "entrata", "limit": "take profit", "stop": "stop loss"}


def _f(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _ny_time(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(NY).strftime("%H:%M")
    except (ValueError, AttributeError):
        return "--:--"


async def market_just_closed(alpaca: AlpacaManager, now_ny: datetime) -> bool:
    day = now_ny.date().isoformat()
    calendar = await alpaca.get_json("/v2/calendar", {"start": day, "end": day})
    if not calendar:
        return False  # giorno festivo
    hour, minute = map(int, calendar[0]["close"].split(":")[:2])
    close_ny = now_ny.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return timedelta(0) <= now_ny - close_ny <= timedelta(minutes=REPORT_WINDOW_MINUTES)


async def build_report(db: SupabaseManager, alpaca: AlpacaManager, now_ny: datetime) -> str:
    day_start = now_ny.replace(hour=0, minute=0, second=0, microsecond=0)
    since_iso = day_start.astimezone(timezone.utc).isoformat()
    # Le gambe TP/SL vengono create insieme all'ordine padre, anche giorni prima
    orders_after = (day_start - timedelta(days=7)).astimezone(timezone.utc).isoformat()

    account = await alpaca.get_account()
    positions = await alpaca.get_json("/v2/positions") or []
    open_orders = await alpaca.get_json("/v2/orders", {"status": "open", "limit": 500}) or []
    closed_orders = await alpaca.get_json(
        "/v2/orders", {"status": "closed", "after": orders_after, "limit": 500, "direction": "desc"}
    ) or []

    trades = await db.get_rows_since("trades", "created_at", since_iso, "id")
    news = await db.get_rows_since("processed_news", "processed_at", since_iso, "status")
    logs = await db.get_rows_since("bot_logs", "timestamp", since_iso, "level")

    start_eq = _f(account.get("last_equity"))
    end_eq = _f(account.get("equity"))
    pnl = end_eq - start_eq
    pnl_pct = pnl / start_eq * 100 if start_eq else 0.0

    news_status = Counter(r.get("status") for r in news)
    errors = sum(1 for r in logs if r.get("level") in ("ERROR", "CRITICAL"))

    fills = sorted(
        (o for o in closed_orders if o.get("filled_at") and o["filled_at"] >= since_iso),
        key=lambda o: o["filled_at"],
    )

    legs: dict[str, dict[str, float]] = {}
    for o in open_orders:
        leg = legs.setdefault(o.get("symbol", ""), {})
        if o.get("type") == "stop" and o.get("stop_price"):
            leg["sl"] = _f(o["stop_price"])
        elif o.get("type") == "limit" and o.get("limit_price"):
            leg["tp"] = _f(o["limit_price"])

    lines = [
        f"<b>Riepilogo {now_ny:%d/%m/%Y}</b>",
        "",
        "<b>Portafoglio</b>",
        f"Inizio giornata: ${start_eq:,.2f}",
        f"Fine giornata: ${end_eq:,.2f}",
        f"P&amp;L: {pnl:+,.2f} $ ({pnl_pct:+.2f}%)",
        f"Liquidit\u00e0: ${_f(account.get('cash')):,.2f}",
        "",
        "<b>Attivit\u00e0 del bot</b>",
        f"Notizie valutate: {len(news)} (ordinate {news_status.get('ORDERED', 0)}, "
        f"scartate {news_status.get('SKIPPED', 0)}, fallite {news_status.get('FAILED', 0)})",
        f"Ordini inviati dal bot: {len(trades)}",
        f"Errori nei log: {errors}",
        "",
        f"<b>Eseguiti oggi ({len(fills)})</b>",
    ]
    if not fills:
        lines.append("Nessuno")
    for o in fills:
        kind = ORDER_KIND.get(o.get("type"), o.get("type"))
        lines.append(
            f"\u2022 {_ny_time(o['filled_at'])} {_esc(o.get('side', '')).upper()} "
            f"{_esc(o.get('filled_qty'))} {_esc(o.get('symbol'))} @ ${_f(o.get('filled_avg_price')):,.2f} ({_esc(kind)})"
        )

    lines += ["", f"<b>Posizioni aperte ({len(positions)})</b>"]
    if not positions:
        lines.append("Nessuna")
    for p in positions:
        sym = p.get("symbol", "")
        leg = legs.get(sym, {})
        sl = f"${leg['sl']:,.2f}" if "sl" in leg else "n/d"
        tp = f"${leg['tp']:,.2f}" if "tp" in leg else "n/d"
        lines.append(
            f"\u2022 <b>{_esc(sym)}</b> {_esc(p.get('side'))} {_esc(p.get('qty'))} "
            f"@ ${_f(p.get('avg_entry_price')):,.2f} \u2192 ${_f(p.get('current_price')):,.2f}\n"
            f"   P&amp;L {_f(p.get('unrealized_pl')):+,.2f} $ ({_f(p.get('unrealized_plpc')) * 100:+.2f}%) "
            f"| SL {sl} | TP {tp}"
        )

    week_fills = [o for o in closed_orders if o.get("filled_at")]
    tp_hits = sum(1 for o in week_fills if o.get("type") == "limit")
    sl_hits = sum(1 for o in week_fills if o.get("type") == "stop")
    exits = tp_hits + sl_hits
    lines += [
        "",
        "<b>Ultimi 7 giorni</b>",
        f"Entrate: {sum(1 for o in week_fills if o.get('type') == 'market')}",
        f"Chiusure: {exits} (take profit {tp_hits}, stop loss {sl_hits})"
        + (f" | win rate {tp_hits / exits:.0%}" if exits else ""),
    ]

    return "\n".join(lines)


async def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    force = os.getenv("FORCE_REPORT", "false").lower() == "true"
    now_ny = datetime.now(NY)

    async with SupabaseManager() as db:
        api_key = await db.resolve_secret("ALPACA_API_KEY")
        secret_key = await db.resolve_secret("ALPACA_SECRET_KEY")
        async with AlpacaManager(api_key=api_key, secret_key=secret_key) as alpaca:
            if not force and not await market_just_closed(alpaca, now_ny):
                logger.info("Mercato non appena chiuso: nessun riepilogo")
                return
            report = await build_report(db, alpaca, now_ny)

        notifier = await TelegramNotifier.from_supabase(db)
        try:
            await notifier.send(report)
        except Exception as exc:  # noqa: BLE001
            await db.log("ERROR", f"Invio riepilogo Telegram fallito: {exc}")
            raise
        await db.log("INFO", "Riepilogo giornaliero inviato su Telegram")
        logger.info("Riepilogo inviato")


if __name__ == "__main__":
    asyncio.run(_main())
