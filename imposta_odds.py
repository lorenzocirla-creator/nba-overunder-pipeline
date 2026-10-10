"""Salva la chiave The Odds API sul Mac e nei secrets di GitHub, dopo averla verificata.
Uso: python3 imposta_odds.py   (la chiave non compare a schermo e non finisce nei file del repository)
"""
import getpass
import json
import os
import subprocess
import sys
from pathlib import Path

import requests

CONFIG = Path(__file__).parent / "dati" / "odds_config.json"   # escluso da git (.gitignore)
REPO = "lorenzocirla-creator/nba-overunder-pipeline"

chiave = getpass.getpass("Incolla la NUOVA chiave The Odds API e premi Invio (non vedi nulla, è normale): ").strip()
print("Verifico la chiave...")
r = requests.get("https://api.the-odds-api.com/v4/sports", params={"apiKey": chiave}, timeout=20)
if r.status_code != 200:
    print(f"❌ La chiave non funziona (risposta {r.status_code}): non ho salvato niente. Ricopiala e riprova.")
    sys.exit(1)
print(f"✅ Chiave valida (crediti rimasti questo mese: {r.headers.get('x-requests-remaining', '?')})")

CONFIG.write_text(json.dumps({"api_key": chiave}))
os.chmod(CONFIG, 0o600)
subprocess.run(["gh", "secret", "set", "ODDS_API_KEY", "-R", REPO], input=chiave.encode(), check=True)
print("✅ Salvata sul Mac e nei secrets di GitHub.\n")
