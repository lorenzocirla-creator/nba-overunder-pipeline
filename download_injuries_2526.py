# download_injuries_2526.py
"""
Scarica e aggiorna gli injury report ufficiali NBA per la stagione 2025–26.
Usa la libreria nbainjuries per scaricare i PDF e convertirli in DataFrame.

Output:
    - dati_2025_2026/injuries_2025_26.csv
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

from nbainjuries import injury

# === Import config ===
sys.path.append(str(Path(__file__).resolve().parent))
from config_season_2526 import SEASON_START, SEASON_END, DATA_DIR

# === Path output ===
OUTPUT = DATA_DIR / "injuries_2025_26.csv"


def fetch_one_day(date: datetime):
    """Scarica injury report per una data specifica (ET)."""
    ts = datetime(date.year, date.month, date.day, 17, 30)  # orario tipico ET
    try:
        df_day = injury.get_reportdata(ts, return_df=True)
        if df_day is not None and not df_day.empty:
            df_day["report_date"] = date
            print(f"✅ {date} -> {len(df_day)} record")
            return df_day
        else:
            print(f"— Nessun dato disponibile per {date}")
            return None
    except Exception as e:
        print(f"— Nessun injury report disponibile ({date}): {e}")
        return None


def main():
    # === Se esiste già un file, caricalo ===
    if OUTPUT.exists():
        old = pd.read_csv(OUTPUT)
        if old.empty:
            print("⚠️ Injury file esistente ma vuoto → parto da SEASON_START")
            start_date = SEASON_START
            all_reports = []
        else:
            old["report_date"] = pd.to_datetime(old["report_date"], format="mixed", errors="coerce")
            last_date = max(old["report_date"]).date()
            print(f"ℹ️ Injury file trovato, ultimo aggiornamento: {last_date}")
            start_date = last_date + timedelta(days=1)
            all_reports = [old]
    else:
        print("📂 Nessun injury file trovato → parto da SEASON_START")
        start_date = SEASON_START
        all_reports = []

    today = datetime.utcnow().date()
    end_date = min(SEASON_END, today)

    d = start_date
    while d <= end_date:
        df_day = fetch_one_day(d)
        if df_day is not None:
            all_reports.append(df_day)
        d += timedelta(days=1)

    # === Salvataggio finale ===
    if all_reports:
        df_all = pd.concat(all_reports, ignore_index=True)
        df_all.to_csv(OUTPUT, index=False)
        print(f"💾 Salvato {len(df_all)} record in {OUTPUT}")
    else:
        # Se nulla → crea file vuoto con header minimo
        pd.DataFrame(columns=["Team", "Player Name", "Current Status", "report_date"]).to_csv(OUTPUT, index=False)
        print(f"📂 Creato file vuoto {OUTPUT}")


if __name__ == "__main__":
    main()
