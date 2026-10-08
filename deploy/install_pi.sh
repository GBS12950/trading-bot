#!/usr/bin/env bash
# Installa il bot come servizio systemd (Raspberry Pi OS / Debian).
# Uso, dalla cartella del repo:  bash deploy/install_pi.sh
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
    echo "Esegui lo script come utente normale, senza sudo." >&2
    exit 1
fi

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_USER="$(id -un)"
cd "$APP_DIR"

echo "== Fuso orario (Europe/Rome) =="
sudo timedatectl set-timezone Europe/Rome

echo "== Pacchetti di sistema =="
sudo apt-get update
sudo apt-get install -y python3-venv python3-pip git

echo "== Ambiente Python =="
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

if [ ! -f .env ]; then
    echo "== Credenziali Supabase =="
    read -rp "SUPABASE_URL (es. https://xxxx.supabase.co): " SUPABASE_URL
    read -rsp "SUPABASE_SERVICE_ROLE_KEY (la digitazione non viene mostrata): " SUPABASE_KEY
    echo
    (
        umask 077
        printf 'SUPABASE_URL=%s\nSUPABASE_SERVICE_ROLE_KEY=%s\n' "$SUPABASE_URL" "$SUPABASE_KEY" > .env
    )
fi
chmod 600 .env

echo "== Test connessione a Supabase =="
set -a
. ./.env
set +a
.venv/bin/python supabase_manager.py

echo "== Servizi systemd =="
sudo tee /etc/systemd/system/trading-bot.service > /dev/null <<EOF
[Unit]
Description=News Sentiment Trading Bot
After=network-online.target
Wants=network-online.target

[Service]
User=${APP_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
Environment=RUN_FOREVER=true
Environment=LOOP_INTERVAL_SECONDS=60
Environment=PYTHONUNBUFFERED=1
ExecStart=${APP_DIR}/.venv/bin/python main.py
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/trading-bot-report.service > /dev/null <<EOF
[Unit]
Description=Trading bot - riepilogo giornaliero
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${APP_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
Environment=PYTHONUNBUFFERED=1
ExecStart=${APP_DIR}/.venv/bin/python daily_report.py
EOF

# Ore italiane: copre la chiusura NYSE (21:00 o 22:00) e le chiusure anticipate (19:00 o 20:00).
# Lo script invia solo nell'ora successiva alla chiusura effettiva.
sudo tee /etc/systemd/system/trading-bot-report.timer > /dev/null <<EOF
[Unit]
Description=Trading bot - timer riepilogo giornaliero

[Timer]
OnCalendar=Mon..Fri *-*-* 19,20,21,22,23:10:00

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable trading-bot.service trading-bot-report.timer
sudo systemctl restart trading-bot.service
sudo systemctl start trading-bot-report.timer

echo
echo "Installazione completata."
systemctl --no-pager --lines=0 status trading-bot.service || true
echo "Log in tempo reale:  journalctl -u trading-bot -f"
