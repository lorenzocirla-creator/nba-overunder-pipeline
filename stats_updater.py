# stats_updater.py
"""
Crea/aggiorna il file cumulativo con statistiche:
DATE, GAME, PREDICTED_POINTS, TOTAL_POINTS, DIFF, GAME_ID

• Legge le predizioni: predictions/predictions_today_YYYYMMDD.csv
• Incrocia con i risultati finali: dati/dataset_regular_2025_26.csv
• Aggiunge solo partite IS_FINAL=True, senza duplicati (chiave GAME_ID)
• DIFF = PREDICTED_POINTS - TOTAL_POINTS
• Scrive un riepilogo in Markdown con:
  (riga in grassetto, dopo due righe vuote) “X partite (<5 pt) = Y%”
"""

from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta, datetime
import argparse
import pandas as pd
import numpy as np

# --- PATHS ---
ROOT = Path(__file__).resolve().parent
PRED_DIR = ROOT / "predictions"
DATASET_REG = ROOT / "dati" / "dataset_regular_2025_26.csv"
OUT_FILE = PRED_DIR / "stats_predictions_vs_results.csv"
SUMMARY_MD = PRED_DIR / "stats_predictions_vs_results_summary.csv"

# --- COLONNE OUTPUT ---
OUT_COLS = ["DATE", "GAME", "PREDICTED_POINTS", "TOTAL_POINTS", "DIFF", "GAME_ID"]

def _load_predictions_for_day(d: date) -> pd.DataFrame:
    fname = PRED_DIR / f"predictions_today_{d.strftime('%Y%m%d')}.csv"
    if not fname.exists():
        return pd.DataFrame(columns=["GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS"])
    df = pd.read_csv(fname)
    for c in ["GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS"]:
        if c not in df.columns: df[c] = pd.NA
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
    df = df.loc[df["GAME_DATE"] == d, ["GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS"]].copy()
    df["PREDICTED_POINTS"] = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
    return df

def _load_results_final_for_day(d: date) -> pd.DataFrame:
    if not DATASET_REG.exists():
        return pd.DataFrame(columns=["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS","IS_FINAL"])
    reg = pd.read_csv(DATASET_REG)
    for c in ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS","IS_FINAL"]:
        if c not in reg.columns: reg[c] = pd.NA
    reg["GAME_ID"] = pd.to_numeric(reg["GAME_ID"], errors="coerce").astype("Int64")
    reg["GAME_DATE"] = pd.to_datetime(reg["GAME_DATE"], errors="coerce").dt.date
    reg["TOTAL_POINTS"] = pd.to_numeric(reg["TOTAL_POINTS"], errors="coerce")
    reg["IS_FINAL"] = reg["IS_FINAL"].astype(bool)
    return reg.loc[
        (reg["GAME_DATE"] == d) & (reg["IS_FINAL"]),
        ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS","IS_FINAL"]
    ].copy()

def _merge_pred_vs_final(pred: pd.DataFrame, fin: pd.DataFrame) -> pd.DataFrame:
    if pred.empty or fin.empty:
        return pd.DataFrame(columns=OUT_COLS)
    m = fin.merge(
        pred,
        on=["GAME_DATE","HOME_TEAM","AWAY_TEAM"],
        how="left"
    )
    out = pd.DataFrame({
        "DATE": m["GAME_DATE"].astype(str),
        "GAME": m["AWAY_TEAM"].astype(str) + " @ " + m["HOME_TEAM"].astype(str),
        "PREDICTED_POINTS": pd.to_numeric(m["PREDICTED_POINTS"], errors="coerce"),
        "TOTAL_POINTS": pd.to_numeric(m["TOTAL_POINTS"], errors="coerce"),
        "GAME_ID": m["GAME_ID"].astype("Int64")
    })
    out["DIFF"] = out["PREDICTED_POINTS"] - out["TOTAL_POINTS"]
    return out[OUT_COLS]

def _write_summary_md(df_all: pd.DataFrame) -> None:
    """Scrive/aggiorna il file Markdown con la riga in grassetto, staccata da due righe vuote."""
    total = len(df_all)
    if total == 0:
        text = "**Nessuna partita in archivio.**\n"
    else:
        close_count = int((df_all["DIFF"].abs() < 5).sum())
        pct = (close_count / total) * 100.0
        # due righe vuote prima, poi riga in grassetto
        text = "\n\n" + f"**Partite con |diff| < 5 pt: {close_count} / {total} ({pct:.1f}%)**\n"
    # append o crea
    if SUMMARY_MD.exists():
        # sovrascrivo per avere sempre una sola riga finale coerente
        SUMMARY_MD.write_text(text, encoding="utf-8")
    else:
        SUMMARY_MD.write_text(text, encoding="utf-8")

    # stampa anche in console
    print(text.strip())

def update_stats(days_back: int = 4, specific_date: date | None = None) -> Path:
    if specific_date:
        dates = [specific_date]
    else:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(days_back+1)]

    if OUT_FILE.exists():
        hist = pd.read_csv(OUT_FILE)
        for c in OUT_COLS:
            if c not in hist.columns: hist[c] = pd.NA
        hist["GAME_ID"] = pd.to_numeric(hist["GAME_ID"], errors="coerce").astype("Int64")
        hist["PREDICTED_POINTS"] = pd.to_numeric(hist["PREDICTED_POINTS"], errors="coerce")
        hist["TOTAL_POINTS"] = pd.to_numeric(hist["TOTAL_POINTS"], errors="coerce")
        hist["DIFF"] = pd.to_numeric(hist["DIFF"], errors="coerce")
    else:
        hist = pd.DataFrame(columns=OUT_COLS)

    new_rows = []
    for d in sorted(dates):
        pred = _load_predictions_for_day(d)
        fin  = _load_results_final_for_day(d)
        merged = _merge_pred_vs_final(pred, fin)
        if not merged.empty:
            new_rows.append(merged)

    if new_rows:
        add = pd.concat(new_rows, ignore_index=True)
        combo = pd.concat([hist, add], ignore_index=True)
        if combo["GAME_ID"].notna().any():
            combo = combo.drop_duplicates(subset=["GAME_ID"], keep="last")
        else:
            combo = combo.drop_duplicates(subset=["DATE","GAME"], keep="last")
        combo["DATE"] = pd.to_datetime(combo["DATE"], errors="coerce")
        combo = combo.sort_values(["DATE","GAME_ID"], na_position="last").reset_index(drop=True)
        combo["DATE"] = combo["DATE"].dt.date.astype(str)
    else:
        combo = hist
        print("ℹ️ Nessuna nuova riga finale da aggiungere.")

    combo.to_csv(OUT_FILE, index=False)
    print(f"✅ Statistiche aggiornate: {OUT_FILE} (righe totali: {len(combo)})")

    # Scrivi riepilogo Markdown con riga in grassetto (staccata da due righe)
    _write_summary_md(combo)

    return OUT_FILE

def main():
    ap = argparse.ArgumentParser(description="Aggiorna storico statistiche predetto vs finale.")
    ap.add_argument("--days", type=int, default=4, help="backfill ultimi N giorni (default 4)")
    ap.add_argument("--date", type=str, help="aggiorna solo una data specifica YYYY-MM-DD")
    args = ap.parse_args()

    d = None
    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").date()

    update_stats(days_back=args.days, specific_date=d)

if __name__ == "__main__":
    main()