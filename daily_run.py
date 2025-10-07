# daily_run.py
"""
Pipeline giornaliera NBA 2025–26:
1. update partite (data_updater_2526.py)
2. build features complete (build_features_2526.py)
3. esegui modello principale (main_nba.py)
4. genera previsioni e raccomandazioni scommesse
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
    print("\n🏀 Avvio pipeline giornaliera NBA 2025–26")

    # 1️⃣ Aggiornamento partite
    run("Aggiornamento partite", [str(ROOT / "data_updater_2526.py")])

    # 2️⃣ Costruzione feature set
    run("Build feature set", [str(ROOT / "build_features_2526.py")])

    # 3️⃣ Esecuzione modello principale
    run("Esecuzione modello principale", [str(ROOT / "main_nba.py")])

    # 4️⃣ Predizioni del giorno
    print("\n▶️ Predizioni giornata")
    subprocess.run([sys.executable, str(ROOT / "predict_today.py")], check=False)
    print("✅ Predizioni completate")

    # 5️⃣ Raccomandazioni scommesse
    print("\n▶️ Raccomandazioni scommesse")
    subprocess.run([sys.executable, str(ROOT / "recommend_bets_today.py")], check=False)
    print("✅ Raccomandazioni completate")

    print("\n🎯 DAILY RUN COMPLETATA CON SUCCESSO!")


if __name__ == "__main__":
    main()

