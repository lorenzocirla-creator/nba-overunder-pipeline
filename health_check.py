#!/usr/bin/env python3
"""
Controllo di salute delle fonti dati, con avviso su Telegram.

Serve a sapere subito se una fonte smette di rispondere: ESPN è un'API non
ufficiale e può cambiare senza preavviso, e il resto della pipeline continuerebbe
a girare "con successo" producendo semplicemente file vuoti.

Controlla:
  - ESPN scoreboard risponde e restituisce partite in una giornata nota
  - il calendario della stagione è presente e coerente
  - il dataset ha i risultati delle partite già giocate
  - le quote di mercato sono state aggiornate di recente

Avvisa su Telegram SOLO quando qualcosa non va (o con --always).

Uso:
    python health_check.py
    python health_check.py --always     # invia anche se è tutto a posto
    python health_check.py --dry-run
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from config_season import (
    SEASON_TAG, SEASON_START, SEASON_END, in_season, season_progress,
    path_dataset_regular, path_schedule_full, path_odds,
)
import telegram

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Giornata di riferimento con partite certe: se ESPN non risponde neanche qui,
# il problema è della fonte, non del calendario.
KNOWN_GAME_DAY = date(2025, 11, 24)
KNOWN_GAME_COUNT = 10


class Check:
    def __init__(self, name: str):
        self.name = name
        self.ok = True
        self.detail = ""

    def fail(self, detail: str) -> "Check":
        self.ok = False
        self.detail = detail
        return self

    def pass_(self, detail: str = "") -> "Check":
        self.ok = True
        self.detail = detail
        return self


def check_espn_reachable() -> Check:
    c = Check("API ESPN")
    try:
        r = requests.get(SCOREBOARD.format(ymd=KNOWN_GAME_DAY.strftime("%Y%m%d")),
                         headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return c.fail(f"risposta HTTP {r.status_code}")
        events = r.json().get("events", [])
        if len(events) < KNOWN_GAME_COUNT:
            return c.fail(f"solo {len(events)} partite per il {KNOWN_GAME_DAY} "
                          f"(attese {KNOWN_GAME_COUNT}): formato cambiato?")
        return c.pass_(f"{len(events)} partite sulla giornata di controllo")
    except requests.RequestException as e:
        return c.fail(f"irraggiungibile ({type(e).__name__})")
    except ValueError:
        return c.fail("risposta non leggibile come JSON")


def check_today_coverage() -> Check:
    """ESPN conosce le partite di oggi? (solo durante la stagione)"""
    c = Check("Partite di oggi")
    today = date.today()
    if not in_season(today):
        return c.pass_(season_progress(today))
    try:
        r = requests.get(SCOREBOARD.format(ymd=today.strftime("%Y%m%d")),
                         headers=HEADERS, timeout=20)
        events = r.json().get("events", [])
        return c.pass_(f"{len(events)} partite oggi")
    except Exception as e:
        return c.fail(f"nessuna risposta per oggi ({type(e).__name__})")


def check_schedule() -> Check:
    c = Check("Calendario stagione")
    p = path_schedule_full()
    if not p.exists():
        return c.fail(f"{p.name} mancante: lancia download_full_schedule_espn.py")
    try:
        df = pd.read_csv(p)
    except Exception as e:
        return c.fail(f"{p.name} illeggibile ({e})")
    if len(df) < 900:
        return c.fail(f"solo {len(df)} partite in calendario (attese ~1200)")
    return c.pass_(f"{len(df)} partite")


def check_dataset() -> Check:
    c = Check("Dataset partite")
    p = path_dataset_regular()
    if not p.exists():
        return c.fail(f"{p.name} mancante")
    try:
        df = pd.read_csv(p, parse_dates=["GAME_DATE"], low_memory=False)
    except Exception as e:
        return c.fail(f"{p.name} illeggibile ({e})")

    if df.empty:
        return c.fail("dataset vuoto")

    dup = df.duplicated(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]).sum()
    if dup:
        return c.fail(f"{dup} partite duplicate nel dataset")

    today = date.today()
    past = df[df["GAME_DATE"].dt.date < today]
    if past.empty:
        return c.pass_(f"{len(df)} partite, stagione non ancora iniziata")

    missing = int(pd.to_numeric(past["TOTAL_POINTS"], errors="coerce").isna().sum())
    if missing > max(5, len(past) * 0.05):
        return c.fail(f"{missing} partite passate senza risultato su {len(past)}")
    return c.pass_(f"{len(df)} partite, {missing} risultati mancanti")


def check_odds() -> Check:
    c = Check("Quote di mercato")
    p = path_odds()
    if not p.exists():
        return c.fail(f"{p.name} mancante: le quote servono per capire se il "
                      f"modello batte il mercato")
    try:
        df = pd.read_csv(p, parse_dates=["GAME_DATE"])
    except Exception as e:
        return c.fail(f"{p.name} illeggibile ({e})")

    if df.empty:
        return c.fail("nessuna quota salvata")

    if not in_season(date.today()):
        return c.pass_(f"{len(df)} quote in archivio (fuori stagione)")

    recenti = df[df["GAME_DATE"].dt.date >= date.today() - timedelta(days=2)]
    if recenti.empty:
        return c.fail("nessuna quota aggiornata negli ultimi 2 giorni")
    return c.pass_(f"{len(recenti)} quote recenti")


def run_checks() -> list[Check]:
    return [
        check_espn_reachable(),
        check_today_coverage(),
        check_schedule(),
        check_dataset(),
        check_odds(),
    ]


def format_message(checks: list[Check]) -> str:
    failed = [c for c in checks if not c.ok]
    testa = ("🚨 <b>Problema nelle fonti dati NBA</b>" if failed
             else "✅ <b>Controllo fonti NBA: tutto a posto</b>")

    righe = [testa, f"<i>Stagione {SEASON_TAG} — {season_progress()}</i>", ""]
    for c in checks:
        icona = "✅" if c.ok else "❌"
        righe.append(f"{icona} <b>{c.name}</b>: {c.detail}")

    if failed:
        righe.append("")
        righe.append("La pipeline continua a girare, ma i dati potrebbero essere "
                     "incompleti: conviene controllare il log in logs/log_daily.txt.")
    return "\n".join(righe)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--always", action="store_true", help="invia anche se è tutto ok")
    ap.add_argument("--dry-run", action="store_true", help="stampa senza inviare")
    args = ap.parse_args()

    checks = run_checks()
    for c in checks:
        print(f"{'OK ' if c.ok else 'KO '} {c.name}: {c.detail}")

    failed = [c for c in checks if not c.ok]
    if failed or args.always or args.dry_run:
        telegram.send_message(format_message(checks), dry_run=args.dry_run)
    else:
        print("\nTutto a posto: nessun avviso inviato.")

    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
