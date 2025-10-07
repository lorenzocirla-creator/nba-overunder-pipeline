# expand_closing_lines.py
"""
Genera linee alternative a partire da FINAL_LINE.
Esempio: se FINAL_LINE = 225.5 → genera linee da 215.5 a 235.5 (step 1.0).
"""

import pandas as pd
from pathlib import Path

# === Config ===
DATA_DIR = Path("../dati")
INPUT_FILE = DATA_DIR / "predictions.csv"       # output di main_nba.py
OUTPUT_FILE = DATA_DIR / "expanded_lines.csv"   # file con linee estese
LINE_RANGE = 10     # +/- range rispetto a FINAL_LINE
STEP = 1.0          # incremento delle linee (puoi usare 0.5 se Snai lo consente)

def expand_lines():
    df = pd.read_csv(INPUT_FILE)

    if "FINAL_LINE" not in df.columns:
        raise ValueError("❌ Colonna FINAL_LINE mancante in predictions.csv")

    expanded_rows = []

    for _, row in df.iterrows():
        base_line = row["FINAL_LINE"]
        predicted = row["PREDICTED_POINTS"]

        for offset in range(-LINE_RANGE, LINE_RANGE + 1):
            line_value = base_line + (offset * STEP)

            diff = predicted - line_value
            if abs(diff) >= 10:
                bet = "OVER" if diff > 0 else "UNDER"
            else:
                bet = "NO BET"

            expanded_rows.append({
                "GAME_DATE": row["GAME_DATE"],
                "HOME_TEAM": row["HOME_TEAM"],
                "AWAY_TEAM": row["AWAY_TEAM"],
                "FINAL_LINE": base_line,
                "PREDICTED_POINTS": predicted,
                "TEST_LINE": line_value,
                "DIFF": round(diff, 2),
                "BET_DECISION": bet
            })

    expanded_df = pd.DataFrame(expanded_rows)
    expanded_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ File salvato con linee estese: {OUTPUT_FILE}")
    return expanded_df


if __name__ == "__main__":
    expand_lines()
