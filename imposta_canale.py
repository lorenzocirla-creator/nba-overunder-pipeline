"""Collega il canale pubblico delle previsioni al bot dei totali.
Salva l'id del canale sul Mac (dati/telegram_config.json) e nei secrets di GitHub
(TELEGRAM_CHANNEL_ID), dopo aver verificato che il bot riesca a scriverci.

Prima di lanciarlo, su Telegram:
  1. Nuovo canale → Pubblico → scegli il link (es. t.me/nba_totali)
  2. Impostazioni canale → Amministratori → aggiungi il bot dei totali
     (basta il permesso "Pubblica messaggi")

Uso: python3 imposta_canale.py
"""
import getpass
import json
import os
import subprocess
import sys
from pathlib import Path

import requests

CONFIG = Path(__file__).parent / "dati" / "telegram_config.json"   # escluso da git (.gitignore)
REPO = "lorenzocirla-creator/nba-overunder-pipeline"
API = "https://api.telegram.org/bot{token}/{metodo}"


def chiama(token: str, metodo: str, **dati) -> dict:
    r = requests.post(API.format(token=token, metodo=metodo), json=dati, timeout=20)
    return r.json()


cfg = json.loads(CONFIG.read_text()) if CONFIG.exists() else {}
token = cfg.get("bot_token")
if not token or not chiama(token, "getMe").get("ok"):
    token = getpass.getpass("Incolla il token del bot dei totali e premi Invio (non vedi nulla, è normale): ").strip()
bot = chiama(token, "getMe")
if not bot.get("ok"):
    print("❌ Token rifiutato da Telegram: non ho salvato niente.")
    sys.exit(1)
print(f"✅ Bot: @{bot['result']['username']}")

nome = input("Scrivi il link del canale (es. @nba_totali oppure t.me/nba_totali): ").strip()
nome = "@" + nome.rsplit("/", 1)[-1].lstrip("@")
canale = chiama(token, "getChat", chat_id=nome)
if not canale.get("ok"):
    print(f"❌ Non trovo il canale {nome}: controlla il link e che sia pubblico.")
    sys.exit(1)
channel_id = str(canale["result"]["id"])   # l'id numerico resta valido anche se cambi il link

prova = chiama(token, "sendMessage", chat_id=channel_id, parse_mode="HTML",
               text="🏀 <b>NBA — totali stimati</b>\n\n"
                    "Ogni mattina le partite del giorno con il totale punti stimato dal modello, "
                    "la sera le linee che si sono mosse, il giorno dopo il consuntivo.")
if not prova.get("ok"):
    print(f"❌ Il bot non riesce a scrivere nel canale ({prova.get('description')}).\n"
          "   Aggiungilo come amministratore con il permesso 'Pubblica messaggi' e rilancia.")
    sys.exit(1)
print(f"✅ Messaggio di benvenuto pubblicato in {nome}")

cfg.update({"bot_token": token, "channel_id": channel_id})
CONFIG.write_text(json.dumps(cfg, indent=2))
os.chmod(CONFIG, 0o600)
subprocess.run(["gh", "secret", "set", "TELEGRAM_CHANNEL_ID", "-R", REPO], input=channel_id.encode(), check=True)
print("✅ Salvato sul Mac e nei secrets di GitHub: da domani le previsioni vanno nel canale,\n"
      "   gli avvisi tecnici restano nella tua chat privata.\n")
