# config_season_2526.py
"""
Configurazione stagione NBA 2025–26
"""

import datetime as dt
from pathlib import Path

# === Stagione target ===
TARGET_SEASON = "2025-26" #2025-26
SEASON_START = dt.date(2025, 10, 21) #2025, 10, 1
SEASON_END   = dt.date(2026, 4, 15) # 2026, 7, 1

# === Cartelle ===
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dati"
RAW_DIR = DATA_DIR / "raw"
OUTPUTS_DIR = ROOT / "outputs"
LOGS_DIR = ROOT / "logs"

for d in (DATA_DIR, RAW_DIR, OUTPUTS_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# === File principali ===
def path_dataset_raw() -> Path:
    return DATA_DIR / "dataset_raw_2025_26.csv"

def path_schedule_raw() -> Path:
    return DATA_DIR / "schedule_raw_2025_26.csv"

def path_dataset_regular() -> Path:
    return DATA_DIR / "dataset_regular_2025_26.csv"

# === Utilità ===
def in_season(day: dt.date) -> bool:
    """Ritorna True se la data è dentro la finestra stagione 2025–26"""
    return SEASON_START <= day <= SEASON_END

if __name__ == "__main__":
    print("Stagione target:", TARGET_SEASON)
    print("Inizio:", SEASON_START, "Fine:", SEASON_END)
    print("Dataset regular path:", path_dataset_regular())
