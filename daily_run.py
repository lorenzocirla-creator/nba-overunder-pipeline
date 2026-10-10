#!/usr/bin/env python3
"""
DAILY RUN – NBA 2025–26
Pipeline completa giornaliera.
Esegue:

1) data_updater_2526.py                  (aggiorna raw, schedule, risultati)
2) build_dataset_regular_2025_26.py      (ricostruisce dataset base)
3) manual_results_patch.py               (se presente)
4) check_missing_results.py              (se presente)
5) build_features_2526.py                (features base: team stats, injuries, closing line, forma, roadtrip, B2B, H2H, fatigue)
6) build_features_advanced_2526.py       (rolling avanzate + matchup features)
7) main_nba.py                           (training modello, condizionale)
8) predict_today.py                      (best effort)
9) recommend_bets_today.py               (best effort)
10) update_master_and_append.py          (REAL_TOTAL + append predictions)
11) build_mae_history.py                 (MAE storico reale)
12) append_summary_to_stats.py           (aggiorna predictions_master_enriched.csv e/o report)
13) notify_telegram.py                   (messaggio con partite e totali stimati)
13b) consuntivo_telegram.py              (secondo messaggio: come sono andate quelle di ieri)

Uso:
    python daily_run.py
    python daily_run.py --full
    python daily_run.py --no-train
    python daily_run.py --min-rows 25
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import date
import pandas as pd
from config_season import SEASON_TAG

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "dati"
LOGS = ROOT / "logs"
REG_PATH = DATA / f"dataset_regular_{SEASON_TAG}.csv"

LOGS.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS / "log_daily.txt"


# -------------------------------------
# Logging
# -------------------------------------
# run_daily.sh redirige già stdout nel log: in quel caso scrivere anche sul file
# raddoppierebbe ogni riga.
_STDOUT_ALREADY_LOGGED = os.environ.get("NBA_LOG_REDIRECTED") == "1"


def log_print(msg: str):
    print(msg, flush=True)
    if _STDOUT_ALREADY_LOGGED:
        return
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def run(label, cmd_list, check=True):
    """
    Esegue uno step della pipeline. L'output dello script viene mostrato a schermo
    E scritto nel log: senza questo, un fallimento lascia nel log solo "❌ ERRORE"
    e il traceback si perde con la finestra del terminale.
    """
    log_print(f"\n▶️  {label}")
    proc = subprocess.Popen(
        [sys.executable, "-u", *cmd_list],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    # Streaming riga per riga: l'avanzamento resta visibile durante gli step lunghi
    # (backfill, download) e finisce comunque nel log.
    with proc.stdout:
        for line in proc.stdout:
            log_print(line.rstrip("\n"))
    rc = proc.wait()
    if check and rc != 0:
        log_print(f"❌ ERRORE in step: {label} (exit code {rc})")
        sys.exit(rc)
    if rc == 0:
        log_print(f"✅ {label} completato")
    else:
        log_print(f"⚠️  {label} completato con codice {rc}")
    return rc


def optional(label, script_name):
    path = ROOT / script_name
    if not path.exists():
        log_print(f"ℹ️  {script_name} non trovato: salto step '{label}'.")
        return 0
    return run(label, [str(path)], check=False)


# -------------------------------------
# Training condition
# -------------------------------------
def enough_training_rows(min_rows=20):
    try:
        df = pd.read_csv(REG_PATH, parse_dates=["GAME_DATE"])
        today = date.today()
        past = df[df["GAME_DATE"].dt.date < today]
        ok = int(past["TOTAL_POINTS"].notna().sum())
        tot = len(past)
        log_print(f"📊 Check training rows: {ok}/{tot} non-NaN (min={min_rows})")
        return ok >= min_rows
    except Exception as e:
        log_print(f"⚠️ Impossibile leggere {REG_PATH}: {e}")
        return False


# -------------------------------------
# MAIN
# -------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--no-train", action="store_true")
    parser.add_argument("--min-rows", type=int, default=20)
    args = parser.parse_args()

    log_print("\n🏀 DAILY RUN NBA 2025–26 – START")

    # 0) Linee di mercato. Vanno registrate prima della partita: dopo non
    # esistono più, e senza archivio non si può sapere se il modello le batte.
    optional("Registra linee di mercato", "mercato.py")
    optional("Scarico quote", "features/download_odds.py")

    # 1) Update raw data
    updater_args = [str(ROOT / "data_updater_2526.py")]
    if args.full:
        updater_args.append("--full")
    run("Aggiornamento partite", updater_args)

    # 2) Base dataset
    run("Rebuild dataset base", [str(ROOT / "build_dataset_regular_2025_26.py")])

    # 3–4) Fix e validazioni
    optional("Manual results patch", "manual_results_patch.py")
    optional("Check missing results", "check_missing_results.py")

    # 5) Feature base
    run("Build features base", [str(ROOT / "build_features_2526.py")])

    # 6) Feature avanzate
    run("Build features avanzate", [str(ROOT / "build_features_advanced_2526.py")])

    # 7) Training modello
    if args.no_train:
        log_print("⏭️  Training disattivato (--no-train).")
    else:
        if enough_training_rows(args.min_rows):
            run("Esecuzione modello principale", [str(ROOT / "main_nba.py")])
        else:
            log_print("⏭️  Non abbastanza partite concluse: skip training.")

    # 8) Predizioni del giorno
    # model_v2 si allena anche sulle stagioni archiviate: a inizio stagione le
    # partite nuove sono zero e predict_today non avrebbe nulla su cui allenarsi.
    run("Predizioni giornata", [str(ROOT / "model_v2.py"), "--predict"], check=False)

    # 9) Recommended bets
    run("Raccomandazioni giornata", [str(ROOT / "recommend_bets_today.py")], check=False)

    # 10) Update master
    optional("Update master (REAL_TOTAL + append today)", "update_master_and_append.py")

    # 11) MAE history
    optional("Build MAE history", "build_mae_history.py")

    # 12) Predictions master enriched / stats report
    optional("Build predictions master enriched", "append_summary_to_stats.py")
    # Se vuoi anche il report aggregato giornaliero:
    # optional("Build stats report", "build_stats_report.py")

    # 13) Notifica Telegram con partite e totali stimati
    optional("Notifica Telegram", "notify_telegram.py")

    # 13b) Consuntivo di ieri: le partite della notte sono finite e i risultati
    # sono stati appena aggiornati, quindi si può dire com'è andata.
    optional("Consuntivo di ieri", "consuntivo_telegram.py")

    # 14) Controllo fonti dati: avvisa su Telegram se qualcosa ha smesso di rispondere
    optional("Controllo fonti dati", "health_check.py")

    # 15) Il primo del mese: report sul confronto col mercato
    if date.today().day == 1:
        optional("Report mensile", "monthly_report.py")

    log_print("\n🎯 DAILY RUN COMPLETATA")


if __name__ == "__main__":
    main()