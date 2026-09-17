#!/usr/bin/env python3
"""
Configurazione guidata di Telegram.

Chiede il token del bot, trova da solo il chat_id, salva il file di
configurazione e manda un messaggio di prova.

Uso:
    python setup_telegram.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "dati" / "telegram_config.json"


def chiedi_token() -> str:
    print("""
┌──────────────────────────────────────────────────────────────┐
│  PASSO 1 — Crea il bot                                       │
└──────────────────────────────────────────────────────────────┘

  1. Apri Telegram e cerca:  @BotFather
  2. Scrivigli:  /newbot
  3. Ti chiede un nome  (es: NBA Totali)
  4. Poi uno username, che deve finire per "bot"
     (es: nba_totali_lorenzo_bot)
  5. Ti risponde con un token, una riga tipo:
     8123456789:AAHk3l-ExAmPlE_tOkEn_Qui

""")
    token = input("Incolla qui il token e premi Invio:\n> ").strip()
    if ":" not in token or len(token) < 20:
        print("\n❌ Non sembra un token valido (deve contenere ':' ed essere lungo).")
        sys.exit(1)
    return token


def verifica_token(token: str) -> str:
    r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=20)
    if r.status_code != 200 or not r.json().get("ok"):
        print(f"\n❌ Token rifiutato da Telegram: {r.text[:200]}")
        sys.exit(1)
    nome = r.json()["result"].get("username", "?")
    print(f"\n✅ Bot riconosciuto: @{nome}")
    return nome


def trova_chat_id(token: str, nome_bot: str) -> str:
    print(f"""
┌──────────────────────────────────────────────────────────────┐
│  PASSO 2 — Apri la conversazione                             │
└──────────────────────────────────────────────────────────────┘

  1. Su Telegram cerca:  @{nome_bot}
  2. Aprilo e premi  AVVIA  (oppure scrivigli "ciao")

  Serve perché un bot non può scrivere per primo: la conversazione
  deve essere aperta da te.
""")
    input("Quando l'hai fatto premi Invio...")

    r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=20)
    updates = r.json().get("result", [])

    chat_ids = []
    for u in updates:
        msg = u.get("message") or u.get("channel_post") or {}
        chat = msg.get("chat") or {}
        if chat.get("id") and chat["id"] not in chat_ids:
            chat_ids.append(chat["id"])

    if not chat_ids:
        print("""
❌ Non vedo ancora nessun messaggio.

   Controlla di aver premuto AVVIA nella chat del bot, poi rilancia:
       python setup_telegram.py
""")
        sys.exit(1)

    if len(chat_ids) == 1:
        print(f"✅ Chat trovata: {chat_ids[0]}")
        return str(chat_ids[0])

    print("\nHo trovato più chat:")
    for i, cid in enumerate(chat_ids, 1):
        print(f"  {i}) {cid}")
    scelta = input("Quale vuoi usare? (numero) > ").strip()
    try:
        return str(chat_ids[int(scelta) - 1])
    except Exception:
        print("❌ Scelta non valida.")
        sys.exit(1)


def salva(token: str, chat_id: str) -> None:
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    CONFIG.write_text(json.dumps({"bot_token": token, "chat_id": chat_id}, indent=2),
                      encoding="utf-8")
    CONFIG.chmod(0o600)
    print(f"\n💾 Configurazione salvata in {CONFIG}")
    print("   (il file non viene versionato: contiene le tue credenziali)")


def prova(token: str, chat_id: str) -> None:
    testo = ("🏀 <b>Configurazione riuscita</b>\n\n"
             "Da qui in avanti riceverai:\n"
             "• ogni mattina le partite del giorno con il totale stimato\n"
             "• il primo del mese il confronto con le quote del mercato\n"
             "• un avviso se le fonti dati smettono di rispondere")
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": testo, "parse_mode": "HTML"},
        timeout=20,
    )
    if r.status_code == 200:
        print("\n✅ Messaggio di prova inviato: guarda Telegram.")
        print("\nTutto pronto. Non devi fare altro.")
    else:
        print(f"\n❌ Invio fallito: {r.text[:300]}")
        sys.exit(1)


def main() -> None:
    print("\n🏀 Configurazione notifiche Telegram — progetto NBA")

    if CONFIG.exists():
        print(f"\n⚠️  Esiste già una configurazione in {CONFIG}")
        if input("Vuoi rifarla da capo? (s/N) > ").strip().lower() not in {"s", "si", "sì"}:
            print("Lascio tutto com'è.")
            return

    token = chiedi_token()
    nome_bot = verifica_token(token)
    chat_id = trova_chat_id(token, nome_bot)
    salva(token, chat_id)
    prova(token, chat_id)


if __name__ == "__main__":
    main()
