# config_season.py
"""
Configurazione della stagione NBA corrente.

Da qui passa TUTTO ciò che dipende dall'anno: finestra di date e nomi dei file.
Per passare alla stagione successiva basta aggiornare questo file (e lanciare
`python migrate_season.py --to 2027_28`, che archivia i dati e prepara i nuovi).

Storico: le stagioni passate restano in dati/history/ e servono ad allenare il
modello a inizio stagione, quando le partite nuove sono ancora zero.
"""

import datetime as dt
from pathlib import Path

# === Stagione corrente ===
SEASON_TAG = "2026_27"          # usato nei nomi dei file
TARGET_SEASON = "2026-27"       # usato nelle chiamate alle API NBA
SEASON_START = dt.date(2026, 10, 20)   # opening night: 3 partite il 20/10
SEASON_END = dt.date(2027, 4, 15)

# Stagioni precedenti disponibili come storico di training (dalla più recente)
PREV_SEASON_TAGS = ["2025_26"]

# === Cartelle ===
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dati"
RAW_DIR = DATA_DIR / "raw"
HISTORY_DIR = DATA_DIR / "history"
OUTPUTS_DIR = ROOT / "outputs"
LOGS_DIR = ROOT / "logs"
PRED_DIR = ROOT / "predictions"
STATE_DIR = ROOT / "state"

for d in (DATA_DIR, RAW_DIR, HISTORY_DIR, OUTPUTS_DIR, LOGS_DIR, PRED_DIR, STATE_DIR):
    d.mkdir(parents=True, exist_ok=True)


# === File della stagione corrente ===
def path_dataset_raw() -> Path:
    return DATA_DIR / f"dataset_raw_{SEASON_TAG}.csv"


def path_schedule_raw() -> Path:
    return DATA_DIR / f"schedule_raw_{SEASON_TAG}.csv"


def path_dataset_regular() -> Path:
    return DATA_DIR / f"dataset_regular_{SEASON_TAG}.csv"


def path_schedule_full() -> Path:
    return DATA_DIR / f"schedule_full_{SEASON_TAG}.csv"


def path_odds() -> Path:
    return DATA_DIR / f"odds_{SEASON_TAG}.csv"


def path_injuries() -> Path:
    return DATA_DIR / f"injuries_{SEASON_TAG}.csv"


def path_team_stats() -> Path:
    return DATA_DIR / f"team_stats_{SEASON_TAG}.csv"


def path_player_stats() -> Path:
    return DATA_DIR / f"player_stats_{SEASON_TAG}.csv"


def historical_datasets() -> list[Path]:
    """Dataset delle stagioni precedenti, se archiviati, dalla più recente."""
    out = []
    for tag in PREV_SEASON_TAGS:
        p = HISTORY_DIR / f"dataset_regular_{tag}.csv"
        if p.exists():
            out.append(p)
    return out


# === Utilità ===
def in_season(day: dt.date) -> bool:
    return SEASON_START <= day <= SEASON_END


def season_progress(day: dt.date | None = None) -> str:
    """Descrizione leggibile dello stato della stagione (usata negli avvisi)."""
    day = day or dt.date.today()
    if day < SEASON_START:
        return f"pre-stagione (inizio il {SEASON_START.strftime('%d/%m/%Y')})"
    if day > SEASON_END:
        return f"stagione conclusa il {SEASON_END.strftime('%d/%m/%Y')}"
    return "stagione in corso"


# --- Feature toggles per esperimenti modello ---
FEATURE_FLAGS = {
    "USE_CLOSING_GAP": False,
    "USE_PACE_LAST5": True,
}

if __name__ == "__main__":
    print("Stagione:", TARGET_SEASON, f"({SEASON_TAG})")
    print("Finestra:", SEASON_START, "→", SEASON_END)
    print("Stato:", season_progress())
    print("Dataset:", path_dataset_regular())
    print("Storico:", [p.name for p in historical_datasets()] or "nessuno")
