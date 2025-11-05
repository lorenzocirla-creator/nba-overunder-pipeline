#!/usr/bin/env python3
# build_mae_history.py
#
# Crea un report giornaliero di accuratezza del modello (MAE):
# colonne e ordine:
#   data, n_games, Mae day, Last 15 start, last 15 end, last 15_N, mae rolling 15, mae season
#
# Input  (default): dati/predictions_master_enriched.csv
# Output (default): predictions/mae_history_real.csv

from pathlib import Path
import argparse
import pandas as pd
import numpy as np

DEFAULT_IN  = "dati/predictions_master_enriched.csv"
DEFAULT_OUT = "predictions/mae_history_real.csv"

def main(inp: str, outp: str):
    inp_p  = Path(inp)
    out_p  = Path(outp)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(inp_p)
    # Normalizza colonne attese
    required = ["GAME_DATE", "PREDICTED_POINTS", "REAL_TOTAL"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mancano colonne nel file di input: {missing}")

    # parse data e filtra solo righe con REAL_TOTAL disponibile (per il calcolo MAE day/rolling/season)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date

    # MAE DAY per data (solo partite con real disponibili quel giorno)
    day_grp = (
        df[df["REAL_TOTAL"].notna()]
        .groupby("GAME_DATE")
        .apply(lambda g: np.mean(np.abs(g["PREDICTED_POINTS"] - g["REAL_TOTAL"])))
        .rename("Mae day")
        .reset_index()
    )

    # n_games per data: quante partite con REAL_TOTAL presenti quel giorno
    n_games = (
        df[df["REAL_TOTAL"].notna()]
        .groupby("GAME_DATE")
        .size()
        .rename("n_games")
        .reset_index()
    )

    # Unisci base (tutte le date che hanno almeno un real)
    base = (
        pd.merge(day_grp, n_games, on="GAME_DATE", how="outer")
        .sort_values("GAME_DATE")
        .reset_index(drop=True)
    )

    # Calcolo MAE SEASON cumulativo
    # Pre-aggrego a livello di partita (abs error)
    games = df[df["REAL_TOTAL"].notna()].copy()
    games["abs_err"] = (games["PREDICTED_POINTS"] - games["REAL_TOTAL"]).abs()
    # Somme e conteggi cumulativi per data
    daily_sum = games.groupby("GAME_DATE")["abs_err"].sum().rename("abs_sum_day")
    daily_cnt = games.groupby("GAME_DATE")["abs_err"].count().rename("cnt_day")
    tmp = pd.concat([daily_sum, daily_cnt], axis=1).reset_index().sort_values("GAME_DATE")
    tmp["cum_sum"] = tmp["abs_sum_day"].cumsum()
    tmp["cum_cnt"] = tmp["cnt_day"].cumsum()
    tmp["mae_season"] = tmp["cum_sum"] / tmp["cum_cnt"]

    base = pd.merge(base, tmp[["GAME_DATE", "mae_season"]], on="GAME_DATE", how="left")

    # Calcolo MAE rolling 15 giorni (ultimi 15 giorni di calendario, inclusa la data)
    # Per ogni data d, finestra: [d-14, d]
    dates = base["GAME_DATE"].tolist()
    records = []
    # Prepara df con date in datetime per filtro comodo
    games["GAME_DATE_DT"] = pd.to_datetime(games["GAME_DATE"])
    for d in dates:
        d_ts = pd.to_datetime(d)
        start = (d_ts - pd.Timedelta(days=14)).date()
        end = d
        mask = (games["GAME_DATE_DT"] >= pd.to_datetime(start)) & (games["GAME_DATE_DT"] <= pd.to_datetime(end))
        win = games.loc[mask, "abs_err"]
        last15_n = int(win.count())
        mae15 = float(win.mean()) if last15_n > 0 else np.nan
        records.append({
            "GAME_DATE": d,
            "Last 15 start": start.isoformat(),
            "last 15 end": end.isoformat(),
            "last 15_N": last15_n,
            "mae rolling 15": mae15
        })
    roll = pd.DataFrame(records)

    # Merge finale
    out = (
        base.merge(roll, on="GAME_DATE", how="left")
            .rename(columns={
                "GAME_DATE": "data",
                "mae_season": "mae season"
            })
    )

    # Ordine colonne richiesto
    cols = ["data", "n_games", "Mae day", "Last 15 start", "last 15 end", "last 15_N", "mae rolling 15", "mae season"]
    out = out[cols].sort_values("data")

    # Salva
    out.to_csv(out_p, index=False)
    print(f"✅ Scritto {out_p} con {len(out)} righe.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Crea MAE giornaliero con rolling 15 giorni e stagionale.")
    ap.add_argument("-i", "--input", default=DEFAULT_IN, help="CSV predictions_master_enriched.csv")
    ap.add_argument("-o", "--out",   default=DEFAULT_OUT, help="Percorso output CSV (mae_history)")
    args = ap.parse_args()
    main(args.input, args.out)