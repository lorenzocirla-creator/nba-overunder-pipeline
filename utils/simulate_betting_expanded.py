# simulate_betting_expanded.py
"""
Simula scommesse su linee alternative generate da expand_closing_lines.py.
Calcola Winrate e ROI.
"""

import pandas as pd
from pathlib import Path

# === Config ===
DATA_DIR = Path("../dati")
INPUT_FILE = DATA_DIR / "expanded_lines.csv"
OUTPUT_FILE = DATA_DIR / "betting_results.csv"
ODDS = 1.91       # quota bookmaker standard
STAKE = 1.0       # puntata fissa per bet

def simulate_bets():
    df = pd.read_csv(INPUT_FILE)

    if "TOTAL_POINTS" not in df.columns:
        raise ValueError("❌ Colonna TOTAL_POINTS mancante: serve il punteggio reale della partita.")

    results = []

    for _, row in df.iterrows():
        decision = row["BET_DECISION"]
        if decision == "NO BET":
            continue

        total_points = row["TOTAL_POINTS"]
        line = row["TEST_LINE"]

        if decision == "OVER":
            win = total_points > line
        elif decision == "UNDER":
            win = total_points < line
        else:
            win = False

        profit = (ODDS * STAKE - STAKE) if win else -STAKE

        results.append({
            "GAME_DATE": row["GAME_DATE"],
            "HOME_TEAM": row["HOME_TEAM"],
            "AWAY_TEAM": row["AWAY_TEAM"],
            "TEST_LINE": line,
            "PREDICTED_POINTS": row["PREDICTED_POINTS"],
            "TOTAL_POINTS": total_points,
            "BET_DECISION": decision,
            "WIN": int(win),
            "PROFIT": profit
        })

    out_df = pd.DataFrame(results)

    # KPI finali
    total_bets = len(out_df)
    wins = out_df["WIN"].sum()
    winrate = wins / total_bets * 100 if total_bets > 0 else 0
    total_profit = out_df["PROFIT"].sum()
    roi = total_profit / (total_bets * STAKE) * 100 if total_bets > 0 else 0

    print(f"🎲 Totale bets: {total_bets}")
    print(f"✅ Vinte: {wins} ({winrate:.2f}%)")
    print(f"💰 ROI: {roi:.2f}%")

    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n💾 Risultati salvati in {OUTPUT_FILE}")

    return out_df


if __name__ == "__main__":
    simulate_bets()
