from pathlib import Path
import pandas as pd
from datetime import date

ROOT = Path(__file__).resolve().parent
DATASET = ROOT / "dati" / "dataset_regular_2025_26.csv"

def main():
    if not DATASET.exists():
        print("⚠️ dataset_regular_2025_26.csv non trovato.")
        return

    df = pd.read_csv(DATASET)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
    today = date.today()

    past_missing = df[(df["GAME_DATE"] < today) & (df["TOTAL_POINTS"].isna())][["GAME_DATE","HOME_TEAM","AWAY_TEAM"]]

    if past_missing.empty:
        print("✅ Nessuna partita passata con risultato mancante.")
    else:
        print(f"❌ {len(past_missing)} partite passate senza risultato:")
        print(past_missing.head(20).to_string(index=False))

if __name__ == "__main__":
    main()
