# build_features_advanced_2526.py
"""
Costruisce feature avanzate per il modello Over/Under NBA 2025-26.

Operazioni:
- Converte il dataset in formato "long" per TEAM (home+away)
- Calcola rolling stats per squadra sulle ultime 3 e 5 partite (solo partite già giocate):
  * TOT_POINTS (se disponibile)
  * PACE, OFFRTG, DEFRTG, NETRTG (se presenti)
- Calcola REST_DAYS e flag B2B per squadra
- Riporta tutto in formato wide:
  * <METRICA>_ROLL3_HOME / _AWAY
  * <METRICA>_ROLL5_HOME / _AWAY
  * REST_DAYS_HOME / AWAY
  * B2B_HOME / AWAY
- Aggiunge alcune feature di matchup (differenze rolling)
- Sovrascrive dati/dataset_regular_2025_26.csv con le nuove colonne aggiuntive
"""

from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

# Scegli cartella dati
for cand in [ROOT / "dati", ROOT / "dati_2025_2026"]:
    if cand.exists():
        DATA_DIR = cand
        break
else:
    DATA_DIR = ROOT / "dati"

REG_PATH = DATA_DIR / "dataset_regular_2025_26.csv"


def detect_total_points(df: pd.DataFrame) -> pd.Series:
    """
    Trova/crea una colonna TOT_POINTS:
    - se esiste TOTAL_POINTS -> usa quella
    - altrimenti se REAL_TOTAL -> usa quella
    - altrimenti se PTS_HOME e PTS_AWAY -> somma
    - altrimenti ritorna Series di NaN
    """
    if "TOTAL_POINTS" in df.columns:
        return pd.to_numeric(df["TOTAL_POINTS"], errors="coerce")
    if "REAL_TOTAL" in df.columns:
        return pd.to_numeric(df["REAL_TOTAL"], errors="coerce")
    if {"PTS_HOME", "PTS_AWAY"}.issubset(df.columns):
        return pd.to_numeric(df["PTS_HOME"], errors="coerce") + pd.to_numeric(
            df["PTS_AWAY"], errors="coerce"
        )
    return pd.Series(np.nan, index=df.index, dtype="float64")


def build_long_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Costruisce un dataframe "long" con una riga per TEAM per partita.
    Colonne minime richieste nel df originale:
    - GAME_ID
    - GAME_DATE
    - HOME_TEAM, AWAY_TEAM
    - PACE_HOME/AWAY, OFFRTG_HOME/AWAY, DEFRTG_HOME/AWAY, NETRTG_HOME/AWAY (se presenti)
    """
    if "GAME_DATE" not in df.columns:
        raise ValueError("GAME_DATE non trovato nel dataset_regular_2025_26.csv")

    df = df.copy()
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    if "GAME_ID" not in df.columns:
        df["GAME_ID"] = np.arange(len(df))

    tot_points = detect_total_points(df)

    base_metrics = ["PACE", "OFFRTG", "DEFRTG", "NETRTG"]
    metrics_home = [m + "_HOME" for m in base_metrics if m + "_HOME" in df.columns]
    metrics_away = [m + "_AWAY" for m in base_metrics if m + "_AWAY" in df.columns]

    records = []

    for idx, row in df.iterrows():
        game_id = row["GAME_ID"]
        gdate = row["GAME_DATE"]

        # HOME row
        home_team = row["HOME_TEAM"] if "HOME_TEAM" in df.columns else None
        home_data = {
            "GAME_ID": game_id,
            "GAME_DATE": gdate,
            "TEAM": home_team,
            "IS_HOME": 1,
            "TOT_POINTS": tot_points.iloc[idx],
        }
        for col in metrics_home:
            metric_name = col.replace("_HOME", "")
            home_data[metric_name] = row.get(col, np.nan)
        records.append(home_data)

        # AWAY row
        away_team = row["AWAY_TEAM"] if "AWAY_TEAM" in df.columns else None
        away_data = {
            "GAME_ID": game_id,
            "GAME_DATE": gdate,
            "TEAM": away_team,
            "IS_HOME": 0,
            "TOT_POINTS": tot_points.iloc[idx],
        }
        for col in metrics_away:
            metric_name = col.replace("_AWAY", "")
            away_data[metric_name] = row.get(col, np.nan)
        records.append(away_data)

    long_df = pd.DataFrame.from_records(records)
    long_df = long_df[long_df["TEAM"].notna()].copy()
    long_df = long_df.sort_values(["TEAM", "GAME_DATE", "GAME_ID"]).reset_index(drop=True)
    return long_df


def add_rolling_features(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggiunge rolling mean sulle ultime 3 e 5 partite per team, escludendo la partita corrente:
    - TOT_POINTS
    - PACE, OFFRTG, DEFRTG, NETRTG (se presenti)
    """
    long_df = long_df.copy()
    metrics = ["TOT_POINTS", "PACE", "OFFRTG", "DEFRTG", "NETRTG"]
    metrics_present = [m for m in metrics if m in long_df.columns]

    for m in metrics_present:
        for window in (3, 5):
            col_name = f"{m}_ROLL{window}"
            long_df[col_name] = (
                long_df.groupby("TEAM")[m]
                .apply(lambda s: s.shift(1).rolling(window=window, min_periods=1).mean())
                .reset_index(level=0, drop=True)
            )

    return long_df


def add_rest_features(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggiunge REST_DAYS e flag B2B calcolati per TEAM, basati su GAME_DATE.
    """
    long_df = long_df.copy()
    long_df["REST_DAYS"] = (
        long_df.sort_values(["TEAM", "GAME_DATE"])
        .groupby("TEAM")["GAME_DATE"]
        .diff()
        .dt.days
    )
    long_df["B2B"] = (long_df["REST_DAYS"] == 1).astype("Int64")
    return long_df


def back_to_wide(df: pd.DataFrame, long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Riporta le rolling e rest features in formato wide:
    - <METRICA>_ROLL{3,5}_HOME / _AWAY
    - REST_DAYS_HOME / _AWAY
    - B2B_HOME / _AWAY
    """
    df = df.copy()

    # HOME
    home = long_df[long_df["IS_HOME"] == 1].copy()
    home_cols = ["GAME_ID", "TEAM", "REST_DAYS", "B2B"] + [
        c for c in long_df.columns if c.endswith("_ROLL3") or c.endswith("_ROLL5")
    ]
    home = home[home_cols]

    rename_home = {"TEAM": "HOME_TEAM", "REST_DAYS": "REST_DAYS_HOME", "B2B": "B2B_HOME"}
    for c in home.columns:
        if c.endswith("_ROLL3") or c.endswith("_ROLL5"):
            rename_home[c] = c + "_HOME"
    home = home.rename(columns=rename_home)

    # AWAY
    away = long_df[long_df["IS_HOME"] == 0].copy()
    away_cols = ["GAME_ID", "TEAM", "REST_DAYS", "B2B"] + [
        c for c in long_df.columns if c.endswith("_ROLL3") or c.endswith("_ROLL5")
    ]
    away = away[away_cols]

    rename_away = {"TEAM": "AWAY_TEAM", "REST_DAYS": "REST_DAYS_AWAY", "B2B": "B2B_AWAY"}
    for c in away.columns:
        if c.endswith("_ROLL3") or c.endswith("_ROLL5"):
            rename_away[c] = c + "_AWAY"
    away = away.rename(columns=rename_away)

    # Merge
    df = df.merge(home, on=["GAME_ID", "HOME_TEAM"], how="left")
    df = df.merge(away, on=["GAME_ID", "AWAY_TEAM"], how="left")

    return df


def add_matchup_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggiunge alcune feature di matchup basate sulle rolling:
    - OFFRTG_ROLL3_DIFF = OFFRTG_ROLL3_HOME - OFFRTG_ROLL3_AWAY
    - OFFRTG_ROLL5_DIFF
    - PACE_ROLL3_EXPECTED = media di PACE_ROLL3_HOME/AWAY
    - PACE_ROLL5_EXPECTED
    - TOT_POINTS_ROLL3_EXPECTED, TOT_POINTS_ROLL5_EXPECTED
    """
    df = df.copy()

    def safe_diff(col_h, col_a, out):
        if col_h in df.columns and col_a in df.columns:
            df[out] = pd.to_numeric(df[col_h], errors="coerce") - pd.to_numeric(
                df[col_a], errors="coerce"
            )

    def safe_mean(col_h, col_a, out):
        if col_h in df.columns and col_a in df.columns:
            df[out] = (
                pd.to_numeric(df[col_h], errors="coerce")
                + pd.to_numeric(df[col_a], errors="coerce")
            ) / 2.0

    # OFFRTG diff
    safe_diff("OFFRTG_ROLL3_HOME", "OFFRTG_ROLL3_AWAY", "OFFRTG_ROLL3_DIFF")
    safe_diff("OFFRTG_ROLL5_HOME", "OFFRTG_ROLL5_AWAY", "OFFRTG_ROLL5_DIFF")

    # PACE expected
    safe_mean("PACE_ROLL3_HOME", "PACE_ROLL3_AWAY", "PACE_ROLL3_EXPECTED")
    safe_mean("PACE_ROLL5_HOME", "PACE_ROLL5_AWAY", "PACE_ROLL5_EXPECTED")

    # TOT_POINTS expected (se presenti)
    safe_mean("TOT_POINTS_ROLL3_HOME", "TOT_POINTS_ROLL3_AWAY", "TOT_POINTS_ROLL3_EXPECTED")
    safe_mean("TOT_POINTS_ROLL5_HOME", "TOT_POINTS_ROLL5_AWAY", "TOT_POINTS_ROLL5_EXPECTED")

    return df


def main():
    if not REG_PATH.exists():
        raise SystemExit(f"Dataset non trovato: {REG_PATH}")

    print(f"📂 Carico dataset: {REG_PATH}")
    df = pd.read_csv(REG_PATH)

    # 🔥 PATCH: pulizia colonne avanzate pre-esistenti (idempotenza)
    advanced_cols_prefixes = [
        "TOT_POINTS_ROLL3_", "TOT_POINTS_ROLL5_",
        "PACE_ROLL3_", "PACE_ROLL5_",
        "OFFRTG_ROLL3_", "OFFRTG_ROLL5_",
        "DEFRTG_ROLL3_", "DEFRTG_ROLL5_",
        "NETRTG_ROLL3_", "NETRTG_ROLL5_",
        "REST_DAYS_", "B2B_",
    ]
    advanced_cols_exact = [
        "OFFRTG_ROLL3_DIFF", "OFFRTG_ROLL5_DIFF",
        "PACE_ROLL3_EXPECTED", "PACE_ROLL5_EXPECTED",
        "TOT_POINTS_ROLL3_EXPECTED", "TOT_POINTS_ROLL5_EXPECTED",
    ]

    cols_to_drop = []
    for c in df.columns:
        if any(c.startswith(pref) for pref in advanced_cols_prefixes) or c in advanced_cols_exact:
            cols_to_drop.append(c)

    if cols_to_drop:
        df = df.drop(columns=cols_to_drop, errors="ignore")

    # Costruisci long frame
    print("🔧 Costruisco frame long per TEAM (home/away)...")
    long_df = build_long_frame(df)

    # Rolling stats
    print("📈 Calcolo rolling stats (3 e 5 partite)...")
    long_df = add_rolling_features(long_df)

    # Rest & B2B
    print("😴 Calcolo REST_DAYS e B2B...")
    long_df = add_rest_features(long_df)

    # Torna a wide
    print("📦 Riporto le feature in formato wide (HOME/AWAY)...")
    df = back_to_wide(df, long_df)

    # Matchup-based features
    print("🤝 Aggiungo feature di matchup basate sulle rolling...")
    df = add_matchup_features(df)

    # Salva sovrascrivendo il dataset regular
    df.to_csv(REG_PATH, index=False)
    print(f"✅ Dataset aggiornato con feature avanzate: {REG_PATH}")
    print(f"🔢 Nuovo numero di colonne: {df.shape[1]}")


if __name__ == "__main__":
    main()