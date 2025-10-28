"""
Pipeline giornaliera NBA 2025–26
Usage:
  python daily_run.py                 # update ieri+oggi
  python daily_run.py --full          # backfill dall'inizio stagione a oggi
  python daily_run.py --no-train      # salta il training
  python daily_run.py --min-rows 25   # richiedi almeno 25 partite concluse

Steps:
1) data_updater_2526.py [--full]
2) build_dataset_regular_2025_26.py
3) manual_results_patch.py      (se esiste)
4) check_missing_results.py     (se esiste)
5) build_features_2526.py
6) main_nba.py                  (solo se abbastanza partite concluse, salvo --no-train)
7) predict_today.py             (best-effort)
8) recommend_bets_today.py      (best-effort)
"""

import sys
import argparse
import subprocess
from pathlib import Path
from datetime import date
import traceback

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "dati"
LOGS = ROOT / "logs"
REG_PATH = DATA / "dataset_regular_2025_26.csv"

LOGS.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS / "log_daily.txt"


# =========================
# Utility logging & runner
# =========================
def log_print(msg: str):
    """Stampa su console e scrive anche nel log giornaliero."""
    print(msg, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def run(label, cmd_list, check=True):
    """Esegue uno script Python come subprocess e gestisce errori."""
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


# =========================
# Controllo training
# =========================
def enough_training_rows(min_rows=20):
    """Conta le partite passate (GAME_DATE < oggi) con TOTAL_POINTS non-NaN."""
    try:
        import pandas as pd
        df = pd.read_csv(REG_PATH, parse_dates=["GAME_DATE"])
        today = date.today()
        past = df[df["GAME_DATE"].dt.date < today]
        ok = int(past["TOTAL_POINTS"].notna().sum())
        tot_past = len(past)
        log_print(f"📊 Check training label (solo partite passate): {ok}/{tot_past} non-NaN (min={min_rows})")
        return ok >= min_rows
    except Exception as e:
        log_print(f"⚠️  Impossibile leggere {REG_PATH}: {e}")
        return False


# =========================
# Esecuzione opzionale
# =========================
def optional(label, script_name):
    """Esegue uno script solo se esiste, senza bloccare la pipeline."""
    path = ROOT / script_name
    if not path.exists():
        log_print(f"ℹ️  {script_name} non trovato: salto step '{label}'.")
        return 0
    try:
        return run(label, [str(path)], check=False)
    except Exception:
        log_print(f"⚠️  Errore inatteso in '{label}':\n{traceback.format_exc()}")
        return 1


# =========================
# MAIN PIPELINE
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Backfill dall'inizio stagione a oggi")
    parser.add_argument("--no-train", action="store_true", help="Salta il training del modello")
    parser.add_argument("--min-rows", type=int, default=20, help="Min partite concluse richieste per il training")
    args = parser.parse_args()

    log_print("\n🏀 Avvio pipeline giornaliera NBA 2025–26")

    # 1️ Aggiornamento partite
    updater_args = [str(ROOT / "data_updater_2526.py")]
    if args.full:
        updater_args.append("--full")
    run("Aggiornamento partite", updater_args)

    # 2️ Ricostruzione dataset base
    run("Rebuild dataset base", [str(ROOT / "build_dataset_regular_2025_26.py")])

    # 3️ Re-applica risultati manuali (se presente)
    optional("Manual results patch", "manual_results_patch.py")

    # 4️ Controllo partite passate senza risultato (se presente)
    optional("Check missing results", "check_missing_results.py")

    # 5️ Costruzione feature set completo
    run("Build feature set", [str(ROOT / "build_features_2526.py")])

    # 6️ Training condizionale
    if args.no_train:
        log_print("⏭️  Flag --no-train attivo: salto training.")
    else:
        if enough_training_rows(min_rows=args.min_rows):
            run("Esecuzione modello principale", [str(ROOT / "main_nba.py")])
        else:
            log_print("⏭️  Poche partite concluse: salto il training per evitare label NaN.")

    # 7️ Predizioni del giorno
    log_print("\n▶️ Predizioni giornata")
    subprocess.run([sys.executable, str(ROOT / "predict_today.py")], check=False)
    log_print("✅ Predizioni completate")

    # 8 Raccomandazioni scommesse
    log_print("\n▶️ Raccomandazioni scommesse")
    subprocess.run([sys.executable, str(ROOT / "recommend_bets_today.py")], check=False)
    log_print("✅ Raccomandazioni completate")
    
    # 9 Aggiorna storico statistiche (backfill ultimi 4 giorni)
    run("Aggiorna statistiche predizioni vs risultati", ["build_stats_report.py"])

    log_print("\n🎯 DAILY RUN COMPLETATA")


if __name__ == "__main__":
    main()