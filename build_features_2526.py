# build_features_2526.py
"""
Orchestratore completo feature 2025–26.
Esegue in sequenza:
- build dataset base
- build/merge team stats (PACE, OFF/DEF/NET, TS, EFG)
- add_backtoback, add_roadtrip, add_forma, add_context_features,
  add_rest_days, add_h2h, add_fatigue
- (opzionale) injuries: download_injuries + build_player_stats + add_injuries
- (opzionale) add_closing_line (quando definita la sorgente quote)

Esecuzione:
    python build_features_2526.py
"""

import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FEATURES = ROOT / "features"
DATASET = ROOT / "dati_2025_2026" / "dataset_regular_2025_26.csv"

# ---- Toggle step opzionali ----
RUN_INJURIES = True       # metti False se vuoi saltare injuries
RUN_CLOSING  = True      # metti True quando add_closing_line.py è pronto e hai le odds

def run(label, cmd_list):
    print(f"\n▶️  {label}")
    rc = subprocess.run([sys.executable, *cmd_list]).returncode
    if rc != 0:
        print(f"❌ ERRORE in step: {label}")
        sys.exit(rc)
    print(f"✅ {label} completato")

def main():
    # 0) Base dataset
    if not DATASET.exists():
        run("Build dataset base", [str(ROOT / "build_dataset_regular_2025_26.py")])
    else:
        print("ℹ️ Dataset base esiste già:", DATASET)

    # 1) Team stats (PACE/OFF/DEF/NET/TS/EFG)
    run("Build team stats", [str(FEATURES / "build_team_stats_2526.py")])
    run("Add team stats",   [str(FEATURES / "add_team_stats.py")])

    # 2) Calendario e forma
    run("Add back-to-back",   [str(FEATURES / "add_backtoback.py")])
    run("Add roadtrip",       [str(FEATURES / "add_roadtrip.py")])
    run("Add forma/difesa",   [str(FEATURES / "add_forma.py")])
    run("Add context feats",  [str(FEATURES / "add_context_features.py")])
    run("Add rest days",      [str(FEATURES / "add_rest_days.py")])
    run("Add H2H",            [str(FEATURES / "add_h2h.py")])
    run("Add fatigue",        [str(FEATURES / "add_fatigue.py")])

    # 3) Injuries (opzionale ma consigliato)
    if RUN_INJURIES:
        run("Download injuries",      [str(ROOT / "download_injuries_2526.py")])
        run("Build player stats",     [str(FEATURES / "build_player_stats_2526.py")])
        run("Add injuries impact",    [str(FEATURES / "add_injuries.py")])
    else:
        print("⏭️  Injuries DISABILITATO (RUN_INJURIES=False)")

    # 4) Closing line (quando definita la sorgente odds)
    if RUN_CLOSING:
        run("Add closing line", [str(FEATURES / "add_closing_line.py")])
    else:
        print("⏭️  Closing line DISABILITATO (RUN_CLOSING=False)")

    print("\n🎯 Build features 2025–26 COMPLETATO")
    print("File aggiornato:", DATASET)

if __name__ == "__main__":
    main()
