# daily_run.py
"""
Pipeline giornaliera NBA 2025–26:
1. update partite (data_updater_2526.py)
2. build features complete (build_features_2526.py)
3. esegui modello principale (main_nba.py)
"""

import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run(label, cmd_list):
    print(f"\n▶️  {label}")
    rc = subprocess.run([sys.executable, *cmd_list]).returncode
    if rc != 0:
        print(f"❌ ERRORE in step: {label}")
        sys.exit(rc)
    print(f"✅ {label} completato")

def main():
    # 1) Aggiornamento partite
    run("Aggiornamento partite", [str(ROOT / "data_updater_2526.py")])

    # 2) Build features
    run("Build features", [str(ROOT / "build_features_2526.py")])

    # 3) Modello NBA (predizioni)
    run("Esegui modello NBA", [str(ROOT / "main_nba.py")])

    # ...
print("✅ Build features completato")

    # Predizioni del giorno
    print("\n▶️ Predizioni giornata")
    subprocess.run([sys.executable, str(ROOT / "predict_today.py")], check=False)
    print("✅ Predizioni completate")

    # Raccomandazioni scommesse
    print("\n▶️ Raccomandazioni scommesse")
    subprocess.run([sys.executable, str(ROOT / "recommend_bets_today.py")], check=False)
    print("✅ Raccomandazioni completate")


    print("\n🎯 DAILY RUN COMPLETATA")

if __name__ == "__main__":
    main()
