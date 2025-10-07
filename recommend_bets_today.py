# recommend_bets_today.py
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
from config_season_2526 import DATA_DIR

TODAY = date.today()
PRED_FILE = DATA_DIR / f"predictions_today_{TODAY.strftime('%Y%m%d')}.csv"
OUT_FILE  = DATA_DIR / f"recommended_bets_today_{TODAY.strftime('%Y%m%d')}.csv"

THRESH = 10.0  # differenza minima per piazzare un bet
SPAN = 10.0    # +/- attorno alla final line
STEP = 0.5     # granularità linee alternative

def half_round(x: float) -> float:
    # arrotonda al .0/.5 (come quote totals tipiche)
    return np.round(x * 2) / 2.0

def build_lines(base: float) -> np.ndarray:
    lo = base - SPAN
    hi = base + SPAN
    n = int((hi - lo)/STEP) + 1
    return np.round(np.linspace(lo, hi, n) * 2) / 2.0

def decide(pred: float, base_line: float):
    # Trova OVER/UNDER e linea consigliata edge (margine = THRESH)
    # OVER se pred >= line + THRESH -> line <= pred - THRESH
    # UNDER se pred <= line - THRESH -> line >= pred + THRESH
    # Scegliamo contro-linea "di bordo" (più alta per OVER, più bassa per UNDER)
    over_edge  = half_round(pred - THRESH)
    under_edge = half_round(pred + THRESH)
    if pred >= base_line + THRESH:
        return "OVER", over_edge
    if pred <= base_line - THRESH:
        return "UNDER", under_edge
    return "NO BET", np.nan

def main():
    if not PRED_FILE.exists():
        print(f"⚠️ File predizioni non trovato: {PRED_FILE}")
        return

    df = pd.read_csv(PRED_FILE)
    # Usa FINAL_LINE, fallback su CURRENT_LINE
    line = df["FINAL_LINE"].copy()
    if "CURRENT_LINE" in df.columns:
        line = line.fillna(df["CURRENT_LINE"])

    df["BASE_LINE"] = line
    recs = []

    for _, r in df.iterrows():
        base = r["BASE_LINE"]
        pred = r["PREDICTED_POINTS"]
        if pd.isna(base) or pd.isna(pred):
            recs.append({
                "GAME_DATE": r["GAME_DATE"],
                "HOME_TEAM": r["HOME_TEAM"],
                "AWAY_TEAM": r["AWAY_TEAM"],
                "PREDICTED_POINTS": pred,
                "BASE_LINE": base,
                "RECOMMENDATION": "NO DATA",
                "SUGGESTED_LINE": np.nan,
                "MARGIN": np.nan
            })
            continue

        side, edge_line = decide(pred, base)
        if side == "NO BET":
            recs.append({
                "GAME_DATE": r["GAME_DATE"],
                "HOME_TEAM": r["HOME_TEAM"],
                "AWAY_TEAM": r["AWAY_TEAM"],
                "PREDICTED_POINTS": pred,
                "BASE_LINE": base,
                "RECOMMENDATION": "NO BET",
                "SUGGESTED_LINE": np.nan,
                "MARGIN": np.nan
            })
        else:
            # clamp edge_line entro ±SPAN rispetto a base
            lo = base - SPAN
            hi = base + SPAN
            edge_line = min(max(edge_line, lo), hi)
            # margine informativo (quanto superiamo la soglia sulla linea consigliata)
            margin = abs(pred - edge_line)
            recs.append({
                "GAME_DATE": r["GAME_DATE"],
                "HOME_TEAM": r["HOME_TEAM"],
                "AWAY_TEAM": r["AWAY_TEAM"],
                "PREDICTED_POINTS": pred,
                "BASE_LINE": base,
                "RECOMMENDATION": side,
                "SUGGESTED_LINE": edge_line,
                "MARGIN": margin
            })

    out = pd.DataFrame(recs)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Raccomandazioni salvate in {OUT_FILE}")

if __name__ == "__main__":
    main()
