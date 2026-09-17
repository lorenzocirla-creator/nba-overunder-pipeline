#!/usr/bin/env python3
"""
Modello v2 per il totale punti — feature point-in-time e valutazione onesta.

Perché esiste:
Il modello storico (main_nba.py) usava ~94 colonne, molte delle quali derivate da
`team_stats_2025_26.csv`, che contiene UN SOLO snapshot per squadra applicato a tutte
le partite della stagione (PACE_HOME ha 1 valore distinto per squadra). Sono quindi
costanti-per-squadra che nel backtest portano informazione dal futuro, e le rolling
calcolate su di esse ereditano lo stesso difetto. Risultato misurato: MAE 16.04,
peggio del predire sempre la media di lega (15.98).

Qui le feature sono ricostruite SOLO dai risultati già giocati, con shift(1) esplicito:
nessun valore entra nella riga di una partita se non era noto prima di quella partita.

Uso:
    python model_v2.py                 # walk-forward + confronto con le baseline
    python model_v2.py --save          # salva anche le predizioni out-of-sample
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "dati" / "dataset_regular_2025_26.csv"
OUT_DIR = ROOT / "outputs"

# Finestre delle medie mobili per squadra
WINDOWS = (5, 10)

# Colonne di contesto già presenti nel dataset che NON derivano dalle team stats statiche
CONTEXT_COLS = [
    "HOME_REST_DAYS", "AWAY_REST_DAYS", "REST_DIFF",
    "HOME_B2B", "AWAY_B2B",
    "ROAD_TRIP_LEN_HOME", "ROAD_TRIP_LEN_AWAY",
    "3IN4_HOME", "3IN4_AWAY",
    "KEY_PLAYERS_OUT_HOME", "KEY_PLAYERS_OUT_AWAY",
    "IMPACT_HOME", "IMPACT_AWAY",
]

# Squadre in altitudine: effetto noto su ritmo e stanchezza degli ospiti
ALTITUDE_TEAMS = {"DEN", "UTA"}


def load_games() -> pd.DataFrame:
    df = pd.read_csv(DATA, parse_dates=["GAME_DATE"])
    df["TOTAL_POINTS"] = pd.to_numeric(df["TOTAL_POINTS"], errors="coerce")
    for c in ["PTS_HOME", "PTS_AWAY"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)
    return df


def build_team_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Frame "long": una riga per squadra per partita, con punti fatti e subiti.
    È la base da cui si ricavano tutte le medie mobili.
    """
    home = pd.DataFrame({
        "GAME_ID": df["GAME_ID"], "GAME_DATE": df["GAME_DATE"],
        "TEAM": df["HOME_TEAM"], "IS_HOME": 1,
        "PTS_FOR": df["PTS_HOME"], "PTS_AGAINST": df["PTS_AWAY"],
        "TOTAL": df["TOTAL_POINTS"],
    })
    away = pd.DataFrame({
        "GAME_ID": df["GAME_ID"], "GAME_DATE": df["GAME_DATE"],
        "TEAM": df["AWAY_TEAM"], "IS_HOME": 0,
        "PTS_FOR": df["PTS_AWAY"], "PTS_AGAINST": df["PTS_HOME"],
        "TOTAL": df["TOTAL_POINTS"],
    })
    long_df = pd.concat([home, away], ignore_index=True)
    return long_df.sort_values(["TEAM", "GAME_DATE", "GAME_ID"]).reset_index(drop=True)


def add_point_in_time_features(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Medie mobili per squadra calcolate SOLO sulle partite precedenti (shift(1)).
    Vengono lasciate NaN finché la squadra non ha giocato: il modello le imputa,
    ma nessun dato futuro entra nella riga.
    """
    g = long_df.groupby("TEAM", sort=False)

    for w in WINDOWS:
        long_df[f"PF_{w}"] = g["PTS_FOR"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=2).mean())
        long_df[f"PA_{w}"] = g["PTS_AGAINST"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=2).mean())
        long_df[f"TOT_{w}"] = g["TOTAL"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=2).mean())
        # Volatilità: squadre "irregolari" spostano la varianza del totale
        long_df[f"TOT_STD_{w}"] = g["TOTAL"].transform(
            lambda s: s.shift(1).rolling(w, min_periods=3).std())

    # Medie da inizio stagione (più stabili delle finestre corte)
    long_df["PF_SEASON"] = g["PTS_FOR"].transform(lambda s: s.shift(1).expanding().mean())
    long_df["PA_SEASON"] = g["PTS_AGAINST"].transform(lambda s: s.shift(1).expanding().mean())
    long_df["GAMES_PLAYED"] = g.cumcount()

    return long_df


def league_average_to_date(df: pd.DataFrame) -> pd.Series:
    """
    Media di lega dei totali fino al giorno prima. Cattura la deriva stagionale del
    livello di punteggio (a inizio stagione si segna diversamente che a marzo).
    """
    daily = df.groupby("GAME_DATE")["TOTAL_POINTS"].agg(["sum", "count"]).sort_index()
    cum_sum = daily["sum"].cumsum().shift(1)
    cum_cnt = daily["count"].cumsum().shift(1)
    league = (cum_sum / cum_cnt).rename("LEAGUE_AVG")
    return df["GAME_DATE"].map(league)


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    long_df = add_point_in_time_features(build_team_history(df))

    home = long_df[long_df["IS_HOME"] == 1].set_index("GAME_ID")
    away = long_df[long_df["IS_HOME"] == 0].set_index("GAME_ID")

    feat = pd.DataFrame(index=df["GAME_ID"])
    stat_cols = [c for c in long_df.columns
                 if c.startswith(("PF_", "PA_", "TOT_", "GAMES_"))]
    for c in stat_cols:
        feat[f"{c}_H"] = home[c]
        feat[f"{c}_A"] = away[c]

    feat = feat.reset_index(drop=True)

    # --- Feature di incontro: la stima "fisica" del totale ---
    # Attacco di una squadra contro difesa dell'altra, mediati.
    for w in WINDOWS:
        atk_home = (feat[f"PF_{w}_H"] + feat[f"PA_{w}_A"]) / 2
        atk_away = (feat[f"PF_{w}_A"] + feat[f"PA_{w}_H"]) / 2
        feat[f"EXP_TOTAL_{w}"] = atk_home + atk_away
        feat[f"TOT_SUM_{w}"] = feat[f"TOT_{w}_H"] + feat[f"TOT_{w}_A"]
        feat[f"TOT_GAP_{w}"] = (feat[f"TOT_{w}_H"] - feat[f"TOT_{w}_A"]).abs()

    feat["EXP_TOTAL_SEASON"] = (
        (feat["PF_SEASON_H"] + feat["PA_SEASON_A"]) / 2
        + (feat["PF_SEASON_A"] + feat["PA_SEASON_H"]) / 2
    )

    # --- Stima ancora (shrinkage verso la media di lega) ---
    # Una media su 5-10 partite è rumorosa, soprattutto a inizio stagione: si pesa
    # la media della squadra con la media di lega in proporzione alle partite giocate
    # (K = partite "virtuali" di prior). Questo è il predittore di riferimento.
    league = league_average_to_date(df).values
    gp_h = feat["GAMES_PLAYED_H"].fillna(0).values
    gp_a = feat["GAMES_PLAYED_A"].fillna(0).values
    K = 6.0

    exp10 = feat["EXP_TOTAL_10"].values
    exp_season = feat["EXP_TOTAL_SEASON"].values
    raw = np.where(np.isnan(exp10), exp_season, exp10)
    league_filled = pd.Series(league).ffill().bfill().fillna(np.nanmean(df["TOTAL_POINTS"])).values
    raw = np.where(np.isnan(raw), league_filled, raw)

    weight = np.minimum(gp_h, gp_a) / (np.minimum(gp_h, gp_a) + K)
    feat["ANCHOR"] = weight * raw + (1 - weight) * league_filled

    # --- Contesto ---
    feat["LEAGUE_AVG"] = league_average_to_date(df).values
    feat["MONTH"] = df["GAME_DATE"].dt.month.values
    feat["ALTITUDE"] = df["HOME_TEAM"].isin(ALTITUDE_TEAMS).astype(int).values

    for c in CONTEXT_COLS:
        if c in df.columns:
            feat[c] = pd.to_numeric(df[c], errors="coerce").values

    feature_names = feat.columns.tolist()
    feat["TOTAL_POINTS"] = df["TOTAL_POINTS"].values
    feat["GAME_DATE"] = df["GAME_DATE"].values
    feat["HOME_TEAM"] = df["HOME_TEAM"].values
    feat["AWAY_TEAM"] = df["AWAY_TEAM"].values
    if "FINAL_LINE" in df.columns:
        feat["FINAL_LINE"] = pd.to_numeric(df["FINAL_LINE"], errors="coerce").values

    return feat, feature_names


def make_models() -> dict:
    return {
        "Ridge": make_pipeline(
            SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=10.0)
        ),
        "GBM": make_pipeline(
            SimpleImputer(strategy="median"),
            HistGradientBoostingRegressor(
                max_depth=3, learning_rate=0.05, max_iter=300,
                min_samples_leaf=25, l2_regularization=1.0, random_state=42,
            ),
        ),
    }


def walk_forward(feat: pd.DataFrame, feature_names: list[str], n_folds: int = 5) -> pd.DataFrame:
    """
    Validazione temporale: si allena sul passato e si predice il blocco successivo.
    Le baseline sono valutate sulle STESSE righe, altrimenti il confronto non vale.
    """
    data = feat[feat["TOTAL_POINTS"].notna()].reset_index(drop=True)
    n = len(data)
    fold_size = n // (n_folds + 1)

    rows = []
    preds_out = []

    for k in range(1, n_folds + 1):
        tr_end = fold_size * k
        te_end = fold_size * (k + 1) if k < n_folds else n
        train, test = data.iloc[:tr_end], data.iloc[tr_end:te_end]
        if train.empty or test.empty:
            continue

        X_tr, y_tr = train[feature_names], train["TOTAL_POINTS"]
        X_te, y_te = test[feature_names], test["TOTAL_POINTS"]

        fold_preds = {}
        for name, model in make_models().items():
            model.fit(X_tr, y_tr)
            fold_preds[name] = model.predict(X_te)

        # Media dei due modelli: riduce la varianza dei singoli
        fold_preds["Ridge+GBM"] = (fold_preds["Ridge"] + fold_preds["GBM"]) / 2

        # --- Modelli sul RESIDUO ---
        # Invece di imparare il totale da zero, si parte dalla stima ANCHOR
        # (attacco vs difesa con shrinkage) e si impara solo di quanto correggerla.
        # Il modello non deve più "riscoprire" il livello di scoring a ogni fold.
        anchor_tr = train["ANCHOR"].values
        anchor_te = test["ANCHOR"].values
        resid_tr = y_tr.values - anchor_tr
        for name, model in make_models().items():
            model.fit(X_tr, resid_tr)
            fold_preds[f"{name} su residuo"] = anchor_te + model.predict(X_te)
        fold_preds["Residuo Ridge+GBM"] = (
            fold_preds["Ridge su residuo"] + fold_preds["GBM su residuo"]) / 2
        fold_preds["baseline: ANCHOR"] = anchor_te

        # Baseline
        fold_preds["baseline: media storica"] = np.full(len(test), y_tr.mean())
        if "EXP_TOTAL_10" in test.columns:
            exp10 = test["EXP_TOTAL_10"].fillna(y_tr.mean()).values
            fold_preds["baseline: attacco vs difesa"] = exp10
        if "FINAL_LINE" in test.columns and test["FINAL_LINE"].notna().any():
            fold_preds["baseline: FINAL_LINE"] = test["FINAL_LINE"].fillna(y_tr.mean()).values

        for name, p in fold_preds.items():
            rows.append({
                "fold": k, "model": name, "n": len(test),
                "MAE": mean_absolute_error(y_te, p),
                "RMSE": float(np.sqrt(mean_squared_error(y_te, p))),
            })

        out = test[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS"]].copy()
        out["PREDICTED_POINTS"] = fold_preds["Residuo Ridge+GBM"]
        out["ANCHOR"] = anchor_te
        out["fold"] = k
        preds_out.append(out)

    results = pd.DataFrame(rows)
    walk_forward.predictions = pd.concat(preds_out, ignore_index=True) if preds_out else pd.DataFrame()
    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--save", action="store_true", help="salva le predizioni out-of-sample")
    args = ap.parse_args()

    df = load_games()
    feat, feature_names = build_features(df)
    labeled = feat["TOTAL_POINTS"].notna().sum()
    print(f"📊 Partite con risultato: {labeled} | feature costruite: {len(feature_names)}")

    results = walk_forward(feat, feature_names)
    if results.empty:
        print("⛔ Dati insufficienti per la validazione.")
        return

    summary = (results.groupby("model")[["MAE", "RMSE"]]
               .mean().sort_values("MAE"))

    print("\n📈 Walk-forward (media sui fold, ordinati per MAE)\n")
    print(f"  {'modello':32} {'MAE':>7} {'RMSE':>8}")
    print("  " + "-" * 49)
    for name, r in summary.iterrows():
        marker = "  ←" if not name.startswith("baseline") and name == summary.index[0] else ""
        print(f"  {name:32} {r['MAE']:7.2f} {r['RMSE']:8.2f}{marker}")

    best_model = summary[~summary.index.str.startswith("baseline")].index[0]
    best_base = summary[summary.index.str.startswith("baseline")].index[0]
    delta = summary.loc[best_base, "MAE"] - summary.loc[best_model, "MAE"]
    print(f"\n  Miglior modello: {best_model} | miglior baseline: {best_base}")
    print(f"  Guadagno sul baseline: {delta:+.2f} punti di MAE")

    if args.save:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        results.to_csv(OUT_DIR / "model_v2_folds.csv", index=False)
        walk_forward.predictions.to_csv(OUT_DIR / "model_v2_predictions.csv", index=False)
        print(f"\n💾 Salvati {OUT_DIR/'model_v2_folds.csv'} e {OUT_DIR/'model_v2_predictions.csv'}")


if __name__ == "__main__":
    main()
