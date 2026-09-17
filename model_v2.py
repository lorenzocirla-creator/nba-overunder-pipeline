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
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from config_season import SEASON_TAG

from config_season import (
    SEASON_TAG, PRED_DIR, historical_datasets, path_dataset_regular, path_odds,
)
from team_names import to_abbr

ROOT = Path(__file__).resolve().parent
DATA = path_dataset_regular()
OUT_DIR = ROOT / "outputs"

# Finestre delle medie mobili per squadra
WINDOWS = (5, 10)

# Quanto pesare la correzione del modello sopra la stima base (attacco vs difesa).
# Misurato in walk-forward sulla stagione 2025-26:
#   0.00 (solo stima base) 15.47 | 0.25 -> 15.41 | 0.50 -> 15.42 | 1.00 (solo modello) 15.74
# La correzione contiene segnale, ma è rumorosa: va usata a peso ridotto.
CORRECTION_WEIGHT = 0.25

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


KEEP_COLS = ["GAME_ID", "GAME_DATE", "HOME_TEAM", "AWAY_TEAM",
             "PTS_HOME", "PTS_AWAY", "TOTAL_POINTS"]


def _read_season(path: Path, season: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["GAME_DATE"], low_memory=False)
    for c in KEEP_COLS:
        if c not in df.columns:
            df[c] = np.nan
    keep = KEEP_COLS + [c for c in CONTEXT_COLS + ["FINAL_LINE", "CURRENT_LINE", "CLOSING_LINE"]
                        if c in df.columns]
    df = df[keep].copy()
    df["SEASON"] = season
    for c in ["PTS_HOME", "PTS_AWAY", "TOTAL_POINTS"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["HOME_TEAM", "AWAY_TEAM"]:
        df[c] = df[c].map(to_abbr)
    return df.dropna(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"])


def load_games(include_history: bool = True) -> pd.DataFrame:
    """
    Stagione corrente più le stagioni archiviate.

    A inizio stagione le partite giocate sono zero: senza lo storico il modello
    non avrebbe nulla su cui allenarsi per le prime settimane.
    """
    frames = []
    if include_history:
        for h in historical_datasets():
            tag = h.stem.replace("dataset_regular_", "")
            hist = _read_season(h, tag)
            frames.append(hist)
            print(f"📚 Storico {tag}: {len(hist)} partite "
                  f"({hist['TOTAL_POINTS'].notna().sum()} con risultato)")

    current = _read_season(DATA, SEASON_TAG)
    print(f"📅 Stagione {SEASON_TAG}: {len(current)} partite "
          f"({current['TOTAL_POINTS'].notna().sum()} con risultato)")
    frames.append(current)

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)
    # I GAME_ID non sono confrontabili tra fonti diverse (NBA vs ESPN) né tra
    # stagioni: serve una chiave di riga univoca per allineare home e away.
    df["ROW_KEY"] = np.arange(len(df))
    return df


def build_team_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Frame "long": una riga per squadra per partita, con punti fatti e subiti.
    È la base da cui si ricavano tutte le medie mobili.
    """
    home = pd.DataFrame({
        "ROW_KEY": df["ROW_KEY"], "GAME_ID": df["GAME_ID"], "GAME_DATE": df["GAME_DATE"],
        "TEAM": df["HOME_TEAM"], "IS_HOME": 1, "SEASON": df["SEASON"],
        "PTS_FOR": df["PTS_HOME"], "PTS_AGAINST": df["PTS_AWAY"],
        "TOTAL": df["TOTAL_POINTS"],
    })
    away = pd.DataFrame({
        "ROW_KEY": df["ROW_KEY"], "GAME_ID": df["GAME_ID"], "GAME_DATE": df["GAME_DATE"],
        "TEAM": df["AWAY_TEAM"], "IS_HOME": 0, "SEASON": df["SEASON"],
        "PTS_FOR": df["PTS_AWAY"], "PTS_AGAINST": df["PTS_HOME"],
        "TOTAL": df["TOTAL_POINTS"],
    })
    long_df = pd.concat([home, away], ignore_index=True)
    # Il raggruppamento include SEASON: una squadra cambia roster tra una stagione
    # e l'altra, quindi le medie mobili ripartono da zero a ogni stagione.
    return long_df.sort_values(["TEAM", "GAME_DATE", "ROW_KEY"]).reset_index(drop=True)


def add_point_in_time_features(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Medie mobili per squadra calcolate SOLO sulle partite precedenti (shift(1)).
    Vengono lasciate NaN finché la squadra non ha giocato: il modello le imputa,
    ma nessun dato futuro entra nella riga.
    """
    g = long_df.groupby(["TEAM", "SEASON"], sort=False)

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


def team_priors(long_df: pd.DataFrame, all_seasons: list[str] | None = None) -> pd.DataFrame:
    """
    Rendimento di fine stagione di ogni squadra, usato come punto di partenza
    per la stagione successiva.

    Senza questo, alla prima giornata nessuna squadra ha partite giocate e la
    stima è identica per tutti: Lakers-Warriors riceverebbe lo stesso totale di
    Wizards-Hornets. Il roster cambia, ma il profilo di squadra (ritmo, difesa)
    è molto più informativo del nulla.
    """
    played = long_df[long_df["PTS_FOR"].notna()]
    if played.empty:
        return pd.DataFrame(columns=["TEAM", "SEASON", "PRIOR_PF", "PRIOR_PA"])

    agg = (played.groupby(["TEAM", "SEASON"])
                 .agg(PRIOR_PF=("PTS_FOR", "mean"), PRIOR_PA=("PTS_AGAINST", "mean"))
                 .reset_index())

    # Il prior di una stagione è il rendimento della stagione precedente.
    # L'elenco deve comprendere anche le stagioni ancora senza partite giocate:
    # sono proprio quelle che hanno bisogno del prior.
    seasons = sorted(set(agg["SEASON"]).union(all_seasons or []))
    next_of = {s: seasons[i + 1] for i, s in enumerate(seasons[:-1])}
    agg["SEASON"] = agg["SEASON"].map(lambda s: next_of.get(s))
    return agg.dropna(subset=["SEASON"])


def league_average_to_date(df: pd.DataFrame) -> pd.Series:
    """
    Media di lega dei totali fino al giorno prima. Cattura la deriva stagionale del
    livello di punteggio (a inizio stagione si segna diversamente che a marzo).
    """
    daily = df.groupby("GAME_DATE")["TOTAL_POINTS"].agg(["sum", "count"]).sort_index()  # cumulata su tutto lo storico
    cum_sum = daily["sum"].cumsum().shift(1)
    cum_cnt = daily["count"].cumsum().shift(1)
    league = (cum_sum / cum_cnt).rename("LEAGUE_AVG")
    return df["GAME_DATE"].map(league)


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    long_df = add_point_in_time_features(build_team_history(df))

    home = long_df[long_df["IS_HOME"] == 1].set_index("ROW_KEY")
    away = long_df[long_df["IS_HOME"] == 0].set_index("ROW_KEY")

    feat = pd.DataFrame(index=df["ROW_KEY"])
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

    # --- Prior di inizio stagione ---
    priors = team_priors(long_df, all_seasons=sorted(df["SEASON"].unique()))
    if not priors.empty:
        ph = priors.rename(columns={"TEAM": "HOME_TEAM"})
        pa = priors.rename(columns={"TEAM": "AWAY_TEAM",
                                    "PRIOR_PF": "PRIOR_PF_A", "PRIOR_PA": "PRIOR_PA_A"})
        key = df[["HOME_TEAM", "AWAY_TEAM", "SEASON"]].copy()
        key = key.merge(ph, on=["HOME_TEAM", "SEASON"], how="left")
        key = key.merge(pa, on=["AWAY_TEAM", "SEASON"], how="left")

        gp_h_raw = feat["GAMES_PLAYED_H"].fillna(0).values
        gp_a_raw = feat["GAMES_PLAYED_A"].fillna(0).values
        K_PRIOR = 8.0  # dopo ~8 partite la stagione in corso pesa più del passato

        def blend(cur, prior, gp):
            cur = pd.to_numeric(cur, errors="coerce").values
            prior = pd.to_numeric(prior, errors="coerce").values
            w = gp / (gp + K_PRIOR)
            out = np.where(np.isnan(cur), prior, w * cur + (1 - w) * np.where(np.isnan(prior), cur, prior))
            return np.where(np.isnan(out), cur, out)

        feat["PF_BLEND_H"] = blend(feat["PF_SEASON_H"], key["PRIOR_PF"], gp_h_raw)
        feat["PA_BLEND_H"] = blend(feat["PA_SEASON_H"], key["PRIOR_PA"], gp_h_raw)
        feat["PF_BLEND_A"] = blend(feat["PF_SEASON_A"], key["PRIOR_PF_A"], gp_a_raw)
        feat["PA_BLEND_A"] = blend(feat["PA_SEASON_A"], key["PRIOR_PA_A"], gp_a_raw)

        feat["EXP_TOTAL_BLEND"] = (
            (feat["PF_BLEND_H"] + feat["PA_BLEND_A"]) / 2
            + (feat["PF_BLEND_A"] + feat["PA_BLEND_H"]) / 2
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
    exp_blend = feat["EXP_TOTAL_BLEND"].values if "EXP_TOTAL_BLEND" in feat.columns else exp_season
    # ordine di preferenza: ultime 10 → stagione → prior della stagione scorsa
    raw = np.where(np.isnan(exp10), np.where(np.isnan(exp_season), exp_blend, exp_season), exp10)
    raw = np.where(np.isnan(raw), exp_blend, raw)
    league_filled = pd.Series(league).ffill().bfill().fillna(np.nanmean(df["TOTAL_POINTS"])).values
    raw = np.where(np.isnan(raw), league_filled, raw)

    weight = np.minimum(gp_h, gp_a) / (np.minimum(gp_h, gp_a) + K)
    if "EXP_TOTAL_BLEND" in feat.columns:
        # Con il prior della stagione precedente la stima è informativa fin dalla
        # prima giornata: non va diluita nella media di lega come se fosse ignota.
        has_prior = ~np.isnan(feat["EXP_TOTAL_BLEND"].values)
        weight = np.where(has_prior, np.maximum(weight, 0.6), weight)
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

        # Combinazione usata in produzione
        mean_corr = ((fold_preds["Ridge su residuo"] - anchor_te)
                     + (fold_preds["GBM su residuo"] - anchor_te)) / 2
        fold_preds[f"PRODUZIONE (stima + {CORRECTION_WEIGHT:.0%} correzione)"] = (
            anchor_te + CORRECTION_WEIGHT * mean_corr)

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


def load_market_lines(target: "date") -> pd.DataFrame:
    """Quote reali di mercato per la data indicata (se scaricate)."""
    odds_path = path_odds()
    if not odds_path.exists():
        return pd.DataFrame(columns=["HOME_TEAM", "AWAY_TEAM", "MARKET_LINE"])
    try:
        o = pd.read_csv(odds_path, parse_dates=["GAME_DATE"])
    except Exception:
        return pd.DataFrame(columns=["HOME_TEAM", "AWAY_TEAM", "MARKET_LINE"])

    for c in ["HOME_TEAM", "AWAY_TEAM"]:
        o[c] = o[c].map(to_abbr)
    o = o[o["GAME_DATE"].dt.date == target]
    if o.empty:
        return pd.DataFrame(columns=["HOME_TEAM", "AWAY_TEAM", "MARKET_LINE"])

    line = pd.to_numeric(o.get("CLOSING_LINE"), errors="coerce")
    line = line.fillna(pd.to_numeric(o.get("CURRENT_LINE"), errors="coerce"))
    out = o[["HOME_TEAM", "AWAY_TEAM"]].copy()
    out["MARKET_LINE"] = line.values
    return out.dropna(subset=["HOME_TEAM", "AWAY_TEAM"])


def predict_for_date(target: "date") -> Path | None:
    """
    Allena su tutte le partite con risultato (storico incluso) e predice quelle
    della data indicata. Scrive il file letto da notify_telegram e dal report.
    """
    df = load_games()
    feat, feature_names = build_features(df)

    train = feat[feat["TOTAL_POINTS"].notna()]
    mask_day = (pd.to_datetime(feat["GAME_DATE"]).dt.date == target) & feat["TOTAL_POINTS"].isna()
    test = feat[mask_day]

    out_file = PRED_DIR / f"predictions_today_{target.strftime('%Y%m%d')}.csv"

    if test.empty:
        print(f"ℹ️ Nessuna partita da predire per {target}.")
        pd.DataFrame(columns=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM",
                              "PREDICTED_POINTS", "MARKET_LINE"]).to_csv(out_file, index=False)
        return out_file

    if len(train) < 100:
        print(f"⛔ Storico insufficiente per allenare ({len(train)} partite).")
        return None

    print(f"🎓 Training su {len(train)} partite → predico {len(test)} partite del {target}")

    X_tr, y_tr = train[feature_names], train["TOTAL_POINTS"]
    anchor_tr = train["ANCHOR"].values
    anchor_te = test["ANCHOR"].values

    corrections = []
    for name, model in make_models().items():
        model.fit(X_tr, y_tr.values - anchor_tr)
        corrections.append(model.predict(test[feature_names]))
    correction = np.mean(corrections, axis=0)
    prediction = anchor_te + CORRECTION_WEIGHT * correction

    out = test[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]].copy()
    out["PREDICTED_POINTS"] = prediction.round(1)
    out["ANCHOR"] = np.round(anchor_te, 1)

    market = load_market_lines(target)
    out = out.merge(market, on=["HOME_TEAM", "AWAY_TEAM"], how="left")
    if "MARKET_LINE" not in out.columns:
        out["MARKET_LINE"] = np.nan
    out["MARKET_LINE"] = pd.to_numeric(out["MARKET_LINE"], errors="coerce")
    out["EDGE"] = (out["PREDICTED_POINTS"] - out["MARKET_LINE"]).round(1)
    out["RUN_TS"] = pd.Timestamp.now().isoformat(timespec="seconds")

    out.to_csv(out_file, index=False)
    n_market = int(out["MARKET_LINE"].notna().sum())
    print(f"✅ Salvato {out_file.name}: {len(out)} partite "
          f"({n_market} con quota di mercato)")
    return out_file


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--save", action="store_true", help="salva le predizioni out-of-sample")
    ap.add_argument("--predict", action="store_true",
                    help="allena sullo storico e predice le partite di una data")
    ap.add_argument("--date", help="YYYY-MM-DD (default: oggi), usato con --predict")
    args = ap.parse_args()

    if args.predict:
        from datetime import datetime as _dt
        target = (_dt.strptime(args.date, "%Y-%m-%d").date() if args.date
                  else pd.Timestamp.today().date())
        predict_for_date(target)
        return

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
