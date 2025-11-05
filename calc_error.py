#!/usr/bin/env python3
"""
Calcolo errori sulle predizioni Over/Under.

Input atteso: CSV con almeno queste colonne:
- GAME_DATE (YYYY-MM-DD)
- HOME_TEAM
- AWAY_TEAM
- PREDICTED_POINTS
- REAL_TOTAL  (può essere NaN per partite future)

Output:
- stampa a video MAE / RMSE / MAPE complessivi e per data
- file CSV in outputs/:
  - errors_summary.csv
  - errors_by_date.csv
  - errors_detailed.csv (una riga per partita con DIFF/ABS_ERR)

Uso:
  python3 calc_error.py --input dati/predictions_master_enriched.csv
  python3 calc_error.py -i dati/predictions_master_enriched.csv --from 2025-10-26 --to 2025-11-03
  python3 calc_error.py -i ... --by-date-only   # stampa solo il per-data
"""
import argparse
from pathlib import Path
import sys
import pandas as pd
import numpy as np

def mape_safe(y_true, y_pred):
    # Evita divisioni per zero: ignora le righe con REAL_TOTAL == 0
    mask = (y_true != 0) & (~pd.isna(y_true)) & (~pd.isna(y_pred))
    if mask.sum() == 0:
        return np.nan
    return (np.abs((y_true[mask] - y_pred[mask]) / y_true[mask]) * 100.0).mean()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("-i","--input", required=True, help="Percorso al CSV (es. dati/predictions_master_enriched.csv)")
    p.add_argument("--from", dest="date_from", default=None, help="Filtro data minima (YYYY-MM-DD)")
    p.add_argument("--to", dest="date_to", default=None, help="Filtro data massima (YYYY-MM-DD)")
    p.add_argument("--by-date-only", action="store_true", help="Stampa solo le metriche per data (no global)")
    p.add_argument("--outdir", default="outputs", help="Cartella per salvare i CSV (default: outputs)")
    args = p.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"❌ Input non trovato: {in_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(in_path)

    required = {"GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS","REAL_TOTAL"}
    missing = required - set(df.columns)
    if missing:
        print(f"❌ Colonne mancanti nel CSV: {sorted(missing)}", file=sys.stderr)
        sys.exit(1)

    # Cast e pulizie
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
    df["PREDICTED_POINTS"] = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
    df["REAL_TOTAL"] = pd.to_numeric(df["REAL_TOTAL"], errors="coerce")

    # Filtro date se richiesto
    if args.date_from:
        d0 = pd.to_datetime(args.date_from).date()
        df = df[df["GAME_DATE"] >= d0]
    if args.date_to:
        d1 = pd.to_datetime(args.date_to).date()
        df = df[df["GAME_DATE"] <= d1]

    # Consideriamo solo righe con REAL_TOTAL valorizzato (partite concluse) per le metriche di errore
    concluded = df[df["REAL_TOTAL"].notna()].copy()

    # DIFF = pred - real
    concluded["DIFF"] = concluded["PREDICTED_POINTS"] - concluded["REAL_TOTAL"]
    concluded["ABS_ERR"] = concluded["DIFF"].abs()
    concluded["SQ_ERR"] = concluded["DIFF"] ** 2

    # --- Metriche globali
    if not args.by_date_only:
        n_games = len(concluded)
        mae = concluded["ABS_ERR"].mean() if n_games else np.nan
        rmse = np.sqrt(concluded["SQ_ERR"].mean()) if n_games else np.nan
        mape = mape_safe(concluded["REAL_TOTAL"], concluded["PREDICTED_POINTS"])

        print("========= METRICHE GLOBALI =========")
        print(f"Partite concluse: {n_games}")
        print(f"MAE : {mae:.3f}" if pd.notna(mae) else "MAE : n/a")
        print(f"RMSE: {rmse:.3f}" if pd.notna(rmse) else "RMSE: n/a")
        print(f"MAPE: {mape:.3f} %" if pd.notna(mape) else "MAPE: n/a")
        print("====================================\n")

    # --- Metriche per data
    by_date = None
    if len(concluded):
        grp = concluded.groupby("GAME_DATE")
        by_date = grp.apply(
            lambda g: pd.Series({
                "N": len(g),
                "MAE": g["ABS_ERR"].mean(),
                "RMSE": np.sqrt(g["SQ_ERR"].mean()),
                "MAPE": mape_safe(g["REAL_TOTAL"], g["PREDICTED_POINTS"])
            })
        ).reset_index().sort_values("GAME_DATE")
        print("==== METRICHE PER DATA ====")
        # stampa compatta
        for _, r in by_date.iterrows():
            gd = r["GAME_DATE"]
            print(f"{gd}: N={int(r['N'])}  MAE={r['MAE']:.2f}  RMSE={r['RMSE']:.2f}  MAPE={r['MAPE']:.2f}%")
        print("===========================\n")
    else:
        print("Nessuna partita conclusa nel range selezionato.\n")

    # --- Metriche per matchup (opzionale: utile per audit)
    by_matchup = None
    if len(concluded):
        concluded["MATCHUP"] = concluded["AWAY_TEAM"].astype(str) + " @ " + concluded["HOME_TEAM"].astype(str)
        by_matchup = concluded.groupby(["GAME_DATE","MATCHUP"]).agg(
            PRED=("PREDICTED_POINTS","mean"),
            REAL=("REAL_TOTAL","mean"),
            DIFF=("DIFF","mean"),
            ABS_ERR=("ABS_ERR","mean")
        ).reset_index().sort_values(["GAME_DATE","MATCHUP"])

    # --- Salvataggi
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # sommario globale
    if not args.by_date_only:
        summary = pd.DataFrame([{
            "FROM": args.date_from or "",
            "TO": args.date_to or "",
            "N_GAMES": len(concluded),
            "MAE": None if not len(concluded) else concluded["ABS_ERR"].mean(),
            "RMSE": None if not len(concluded) else np.sqrt(concluded["SQ_ERR"].mean()),
            "MAPE": None if not len(concluded) else mape_safe(concluded["REAL_TOTAL"], concluded["PREDICTED_POINTS"])
        }])
        summary.to_csv(outdir / "errors_summary.csv", index=False)

    if by_date is not None:
        by_date.to_csv(outdir / "errors_by_date.csv", index=False)

    # dettagli per partita (utile per debug)
    detailed_cols = ["GAME_DATE","HOME_TEAM","AWAY_TEAM","PREDICTED_POINTS","REAL_TOTAL","DIFF","ABS_ERR"]
    if len(concluded):
        concluded[detailed_cols].sort_values(["GAME_DATE","HOME_TEAM","AWAY_TEAM"]).to_csv(
            outdir / "errors_detailed.csv", index=False
        )

    print(f"💾 Salvati CSV in: {outdir}/")
    if not args.by_date_only:
        print(f" - errors_summary.csv")
    if by_date is not None:
        print(f" - errors_by_date.csv")
    if len(concluded):
        print(f" - errors_detailed.csv")

if __name__ == "__main__":
    main()