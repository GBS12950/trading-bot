"""
Ordine di prova su Alpaca paper: verifica la catena ordine -> Supabase -> Telegram
senza passare dall'analisi del sentiment.

Variabili: TEST_SYMBOL (default F), TEST_SIDE (buy/sell), TEST_QTY (default 1)
"""
from __future__ import annotations

import asyncio
import logging
import os

from alpaca_manager import AlpacaManager
from supabase_manager import SupabaseManager
from telegram_notifier import TelegramNotifier, esc

logger = logging.getLogger("test_order")

STOP_LOSS_PCT = 0.02
TAKE_PROFIT_PCT = 0.05


async def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    symbol = os.getenv("TEST_SYMBOL", "F").strip().upper()
    side = os.getenv("TEST_SIDE", "buy").strip().lower()
    qty = int(os.getenv("TEST_QTY", "1"))
    if side not in ("buy", "sell") or qty < 1 or not symbol.isalpha():
        raise ValueError(f"Parametri non validi: symbol={symbol} side={side} qty={qty}")

    async with SupabaseManager() as db:
        api_key = await db.resolve_secret("ALPACA_API_KEY")
        secret_key = await db.resolve_secret("ALPACA_SECRET_KEY")
        notifier = await TelegramNotifier.from_supabase(db)

        async with AlpacaManager(api_key=api_key, secret_key=secret_key) as alpaca:
            if not await alpaca.is_market_open():
                raise RuntimeError("Mercato chiuso: lancia il test tra le 15:30 e le 22:00 italiane")

            quote = await alpaca.get_quote(symbol)
            price = quote.get("ask_price") if side == "buy" else quote.get("bid_price")
            if not price or price <= 0:
                raise RuntimeError(f"Prezzo non disponibile per {symbol}: {quote}")

            order = await alpaca.place_bracket_order(
                symbol=symbol, qty=qty, side=side, entry_price=price,
                stop_loss_pct=STOP_LOSS_PCT, take_profit_pct=TAKE_PROFIT_PCT,
            )

        sign = 1 if side == "buy" else -1
        sl = price * (1 - sign * STOP_LOSS_PCT)
        tp = price * (1 + sign * TAKE_PROFIT_PCT)

        await db.record_trade(
            news_id=None, ticker=symbol, side=side, qty=qty, entry_price=price,
            stop_loss=sl, take_profit=tp, alpaca_order_id=order.order_id, status="test",
        )
        msg = f"Ordine di prova: {side.upper()} {qty} {symbol} @ ${price:,.2f} | SL ${sl:,.2f} | TP ${tp:,.2f}"
        await db.log("INFO", msg, ticker=symbol, context={"order_id": order.order_id})
        await notifier.notify(f"<b>TEST</b> {esc(msg)}\nOrdine Alpaca: {esc(order.order_id)} ({esc(order.status)})")
        logger.info("%s | order_id=%s status=%s", msg, order.order_id, order.status)


if __name__ == "__main__":
    asyncio.run(_main())
