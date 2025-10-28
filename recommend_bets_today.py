# recommend_bets_today.py
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np
import re

ROOT = Path(__file__).resolve().parent
PRED_DIR = ROOT / "predictions"
PRED_DIR.mkdir(parents=True, exist_ok=True)

EDGE_TH = 5.5  # punti

def list_prediction_files():
    return sorted(PRED_DIR.glob("predictions_today_*.csv"))

def ymd_from_name(p: Path) -> int:
    m = re.search(r"(\d{8})", p.name)
    return int(m.group(1)) if m else -1

def latest_nonempty_predictions_file() -> Path | None:
    files = list_prediction_files()
    if not files:
        return None
    # ordina per data nel nome (crescente) e scorri al contrario
    files.sort(key=ymd_from_name)
    for p in reversed(files):
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if "PREDICTED_POINTS" not in df.columns:
            continue
        # almeno una riga valida
        if pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce").notna().any():
            return p
    return None

def save_empty(out_path: Path, reason: str):
    cols = ["GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS","USED_LINE","RECOMMENDATION","MARGIN"]
    pd.DataFrame(columns=cols).to_csv(out_path, index=False)
    print(f"ℹ️ Nessuna raccomandazione: {reason}")
    print(f"✅ File creato (vuoto): {out_path}")

def pick_line(df: pd.DataFrame) -> pd.Series:
    # FINAL -> CLOSING -> CURRENT -> BASE
    candidates = [c for c in ["FINAL_LINE","CLOSING_LINE","CURRENT_LINE","BASE_LINE"] if c in df.columns]
    if not candidates:
        return pd.Series(np.nan, index=df.index)
    line = pd.Series(np.nan, index=df.index, dtype=float)
    for c in candidates:
        vals = pd.to_numeric(df[c], errors="coerce")
        line = line.fillna(vals)
    return line

def main():
    pred_file = latest_nonempty_predictions_file()
    if pred_file is None:
        out_today = PRED_DIR / f"recommended_bets_today_{date.today().strftime('%Y%m%d')}.csv"
        save_empty(out_today, "nessun file di predizioni non vuoto disponibile.")
        return

    # output coerente con la data del file scelto
    ymd = re.search(r"(\d{8})", pred_file.name).group(1)
    out_file = PRED_DIR / f"recommended_bets_today_{ymd}.csv"

    df = pd.read_csv(pred_file)
    line = pick_line(df)
    if line.isna().all():
        save_empty(out_file, "nessuna linea disponibile (FINAL/CLOSING/CURRENT/BASE).")
        return

    pred = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
    out = df[["GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS"]].copy()
    out["USED_LINE"] = line

    diff = pred - line
    out["MARGIN"] = diff.abs()
    out["RECOMMENDATION"] = np.where(diff >= EDGE_TH, "OVER",
                              np.where(diff <= -EDGE_TH, "UNDER", "NO BET"))

    out = out.sort_values("MARGIN", ascending=False).reset_index(drop=True)
    out.to_csv(out_file, index=False)
    print(f"✅ Raccomandazioni salvate in {out_file} (sorgente: {pred_file.name})")

if __name__ == "__main__":
    main()