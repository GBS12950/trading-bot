"""
Invia un avviso Telegram da riga di comando.

Usato da systemd (ExecStopPost) quando il servizio si ferma:  python notify_event.py stop
Legge SERVICE_RESULT / EXIT_CODE / EXIT_STATUS dall'ambiente impostato da systemd.
Esce sempre con codice 0 e con tempi stretti, per non rallentare spegnimenti e riavvii.
"""
from __future__ import annotations

import asyncio
import os
import socket
import sys

from supabase_manager import RetryPolicy, SupabaseManager
from telegram_notifier import TelegramNotifier, esc

TOTAL_TIMEOUT = 25.0

STOP_REASONS = {
    "success": "arresto normale (stop o riavvio manuale, oppure spegnimento del Raspberry)",
    "exit-code": "il programma si è chiuso con un errore",
    "signal": "il programma è stato terminato da un segnale",
    "core-dump": "il programma è andato in crash",
    "timeout": "il programma non ha risposto in tempo",
    "start-limit-hit": "troppi riavvii ravvicinati: il servizio non riparte più da solo",
}


def _stop_message() -> str:
    result = os.getenv("SERVICE_RESULT", "")
    detail = " / ".join(v for v in (os.getenv("EXIT_CODE", ""), os.getenv("EXIT_STATUS", "")) if v)
    reason = STOP_REASONS.get(result, result or "motivo sconosciuto")
    lines = [f"<b>Bot fermato</b> su {esc(socket.gethostname())}", f"Motivo: {esc(reason)}"]
    if detail:
        lines.append(f"Dettaglio: {esc(detail)}")
    if result and result != "success":
        lines.append("Il servizio prova a riavviarsi da solo entro 30 secondi.")
    return "\n".join(lines)


async def _send(text: str) -> None:
    # Un solo tentativo: durante uno spegnimento la rete può essere già caduta
    async with SupabaseManager(retry_policy=RetryPolicy(max_attempts=1), op_timeout=8.0) as db:
        notifier = await TelegramNotifier.from_supabase(db)
        await notifier.send(text)


async def _main() -> None:
    kind = sys.argv[1] if len(sys.argv) > 1 else "stop"
    text = _stop_message() if kind == "stop" else " ".join(sys.argv[1:])
    try:
        await asyncio.wait_for(_send(text), timeout=TOTAL_TIMEOUT)
    except Exception as exc:  # noqa: BLE001
        print(f"Avviso non inviato: {type(exc).__name__}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(_main())
