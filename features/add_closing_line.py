import sys
from pathlib import Path
import pandas as pd

# aggiungo la cartella padre (2025_2026) a sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR

INPUT_PATH = DATA_DIR / "dataset_regular_2025_26.csv"
ODDS_PATH = DATA_DIR / "odds_2025_26.csv"
OUTPUT_REGULAR = DATA_DIR / "dataset_regular_2025_26.csv"
OUTPUT_CLOSING = DATA_DIR / "dataset_closing.csv"


def add_closing_line(window=10):
    """Integra current/closing line nel dataset regular 2025-26"""

    # Dataset principale
    df = pd.read_csv(INPUT_PATH)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # Odds scaricate
    odds = pd.read_csv(ODDS_PATH)
    odds["GAME_DATE"] = pd.to_datetime(odds["GAME_DATE"])

    # Merge con odds
    df = df.merge(
        odds[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "CURRENT_LINE", "CLOSING_LINE"]],
        on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"],
        how="left"
    )

    # Crea colonna FINAL_LINE
    final_lines = []
    for idx, row in df.iterrows():
        if not pd.isna(row.get("CLOSING_LINE")):   # priorità 1
            final_lines.append(row["CLOSING_LINE"])
        elif not pd.isna(row.get("CURRENT_LINE")): # priorità 2
            final_lines.append(row["CURRENT_LINE"])
        else:  # fallback proxy
            home, away, date = row["HOME_TEAM"], row["AWAY_TEAM"], row["GAME_DATE"]

            mask_home = (
                ((df["HOME_TEAM"] == home) | (df["AWAY_TEAM"] == home))
                & (df["GAME_DATE"] < date)
            )
            recent_home = df.loc[mask_home].tail(window)

            mask_away = (
                ((df["HOME_TEAM"] == away) | (df["AWAY_TEAM"] == away))
                & (df["GAME_DATE"] < date)
            )
            recent_away = df.loc[mask_away].tail(window)

            mean_home = recent_home["TOTAL_POINTS"].mean() if not recent_home.empty else df["TOTAL_POINTS"].mean()
            mean_away = recent_away["TOTAL_POINTS"].mean() if not recent_away.empty else df["TOTAL_POINTS"].mean()
            proxy = (mean_home + mean_away) / 2
            final_lines.append(proxy)

    df["FINAL_LINE"] = final_lines

    # 🔹 Aggiorna dataset principale
    df.to_csv(OUTPUT_REGULAR, index=False)
    print(f"✅ Aggiornato {OUTPUT_REGULAR} con colonna FINAL_LINE")

    # 🔹 Salva storico dedicato (solo colonne odds)
    closing_cols = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "CURRENT_LINE", "CLOSING_LINE", "FINAL_LINE"]
    df[closing_cols].to_csv(OUTPUT_CLOSING, index=False)
    print(f"✅ Salvato storico closing line in {OUTPUT_CLOSING}")
    
    return df


if __name__ == "__main__":
    add_closing_line()
