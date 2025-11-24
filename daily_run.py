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

Uso:
    python daily_run.py
    python daily_run.py --full
    python daily_run.py --no-train
    python daily_run.py --min-rows 25
"""

import sys
import argparse
import subprocess
from pathlib import Path
from datetime import date
import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "dati"
LOGS = ROOT / "logs"
REG_PATH = DATA / "dataset_regular_2025_26.csv"

LOGS.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS / "log_daily.txt"


# -------------------------------------
# Logging
# -------------------------------------
def log_print(msg: str):
    print(msg, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def run(label, cmd_list, check=True):
    log_print(f"\n▶️  {label}")
    rc = subprocess.run([sys.executable, *cmd_list]).returncode
    if check and rc != 0:
        log_print(f"❌ ERRORE in step: {label}")
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

    # 8) Prediction today
    log_print("\n▶️ Predizioni giornata")
    subprocess.run([sys.executable, str(ROOT / "predict_today.py")], check=False)
    log_print("✅ Predizioni completate")

    # 9) Recommended bets
    log_print("\n▶️ Raccomandazioni giornata")
    subprocess.run([sys.executable, str(ROOT / "recommend_bets_today.py")], check=False)
    log_print("✅ Raccomandazioni completate")

    # 10) Update master
    optional("Update master (REAL_TOTAL + append today)", "update_master_and_append.py")

    # 11) MAE history
    optional("Build MAE history", "build_mae_history.py")

    log_print("\n🎯 DAILY RUN COMPLETATA")


if __name__ == "__main__":
    main()