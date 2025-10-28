# backfill_sum_pts.py
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "dati"
P_REG = DATA_DIR / "dataset_regular_2025_26.csv"
P_LS  = DATA_DIR / "schedule_raw_2025_26.csv"

def main():
    reg = pd.read_csv(P_REG)
    ls = pd.read_csv(P_LS)

    # Normalizza
    ls["PTS"] = pd.to_numeric(ls["PTS"], errors="coerce")
    ls["GAME_ID"] = pd.to_numeric(ls["GAME_ID"], errors="coerce").astype("Int64")

    # Calcola punti aggregati per game_id
    grouped = ls.groupby("GAME_ID")["PTS"].sum(min_count=1).rename("PTS_SUM").reset_index()

    # Merge con dataset principale
    merged = reg.merge(grouped, on="GAME_ID", how="left")

    # Aggiorna solo se TOTAL_POINTS è NaN e abbiamo PTS_SUM valido
    mask = merged["TOTAL_POINTS"].isna() & merged["PTS_SUM"].notna()
    merged.loc[mask, "TOTAL_POINTS"] = merged.loc[mask, "PTS_SUM"]

    print(f"🎯 Partite aggiornate: {mask.sum()} su {len(merged)}")
    merged.to_csv(P_REG, index=False)
    print(f"💾 Salvato {P_REG}")

    # Mostra riepilogo
    filled = merged[merged["TOTAL_POINTS"].notna()]
    print("\n📊 Ultime partite con TOTAL_POINTS:")
    print(filled[["GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS"]].tail(10))

if __name__ == "__main__":
    main()