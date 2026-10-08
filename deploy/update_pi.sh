#!/usr/bin/env bash
# Aggiorna il bot all'ultima versione su GitHub e lo riavvia.
# Uso, dalla cartella del repo:  bash deploy/update_pi.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
git pull --ff-only
.venv/bin/pip install -r requirements.txt
sudo systemctl restart trading-bot
systemctl --no-pager --lines=0 status trading-bot || true
