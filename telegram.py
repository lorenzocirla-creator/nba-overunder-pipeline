"""
Invio messaggi Telegram — usato da tutti gli avvisi del progetto.

Credenziali, in ordine di precedenza:
  1. variabili d'ambiente TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID
  2. file dati/telegram_config.json  {"bot_token": "...", "chat_id": "..."}

Se mancano, l'invio non fallisce: stampa il messaggio e le istruzioni, così la
pipeline notturna non si interrompe per una configurazione assente.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "dati" / "telegram_config.json"
API = "https://api.telegram.org/bot{token}/sendMessage"
TIMEOUT = 20

SETUP_HELP = """
ℹ️ Telegram non configurato: messaggio non inviato.

Per attivarlo (una volta sola):
  1. Su Telegram apri @BotFather → /newbot → scegli nome e username.
     Ti risponde con un token tipo 123456789:AAG...
  2. Scrivi un messaggio qualsiasi al tuo bot (serve ad aprire la chat).
  3. Apri nel browser:  https://api.telegram.org/bot<TOKEN>/getUpdates
     e copia il numero in  "chat":{"id": ...}
  4. Crea il file dati/telegram_config.json:
     {"bot_token": "IL_TUO_TOKEN", "chat_id": "IL_TUO_CHAT_ID"}
"""


def load_credentials() -> tuple[str | None, str | None]:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        return token, chat_id

    if CONFIG.exists():
        try:
            cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
            token = cfg.get("bot_token") or token
            chat_id = str(cfg.get("chat_id") or chat_id or "") or None
        except Exception as e:
            print(f"⚠️ {CONFIG} non leggibile: {e}")

    return token, chat_id


def is_configured() -> bool:
    token, chat_id = load_credentials()
    return bool(token and chat_id)


def send_message(text: str, dry_run: bool = False) -> bool:
    """Invia il messaggio. Ritorna True se è partito davvero."""
    if dry_run:
        print("--- ANTEPRIMA MESSAGGIO ---")
        print(text)
        return False

    token, chat_id = load_credentials()
    if not token or not chat_id:
        print(SETUP_HELP)
        print("--- Messaggio che sarebbe stato inviato ---")
        print(text)
        return False

    try:
        r = requests.post(
            API.format(token=token),
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            print("✅ Messaggio Telegram inviato.")
            return True
        print(f"❌ Telegram ha risposto {r.status_code}: {r.text[:300]}")
    except requests.RequestException as e:
        print(f"❌ Invio Telegram fallito: {type(e).__name__}: {e}")
    return False


if __name__ == "__main__":
    import sys
    msg = " ".join(sys.argv[1:]) or "🏀 Test dal progetto NBA: la connessione funziona."
    send_message(msg)
