# features/add_team_stats.py
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]

# Scegli cartella dati
for cand in [ROOT / "dati", ROOT / "dati_2025_2026"]:
    if cand.exists():
        DATA_DIR = cand
        break
else:
    DATA_DIR = ROOT / "dati"

REG_PATH        = DATA_DIR / "dataset_regular_2025_26.csv"
RAW_GH_PATH     = DATA_DIR / "dataset_raw_2025_26.csv"
TEAMSTATS_PATH  = DATA_DIR / "team_stats_2025_26.csv"
TEAMEXTRA_PATH  = DATA_DIR / "team_stats_extra_2025_26.csv"

TEAM_ID_TO_ABBR = {
    1610612737:"ATL",1610612738:"BOS",1610612739:"CLE",1610612740:"NOP",
    1610612741:"CHI",1610612742:"DAL",1610612743:"DEN",1610612744:"GSW",
    1610612745:"HOU",1610612746:"LAC",1610612747:"LAL",1610612748:"MIA",
    1610612749:"MIL",1610612750:"MIN",1610612751:"BKN",1610612752:"NYK",
    1610612753:"ORL",1610612754:"IND",1610612755:"PHI",1610612756:"PHX",
    1610612757:"POR",1610612758:"SAC",1610612759:"SAS",1610612760:"OKC",
    1610612761:"TOR",1610612762:"UTA",1610612763:"MEM",1610612764:"WAS",
    1610612765:"DET",1610612766:"CHA"
}

# Metriche base (team_stats_2025_26.csv)
METRICS_BASE = ["PACE", "OFFRTG", "DEFRTG", "NETRTG", "TS", "EFG"]

# Metriche extra (team_stats_extra_2025_26.csv)
METRICS_MISC = [
    "PTS_PAINT",
    "PTS_FB",
    "PTS_2ND_CHANCE",
    "PTS_OFF_TOV",
    "OPP_PTS_PAINT",
    "OPP_PTS_FB",
    "OPP_PTS_2ND_CHANCE",
    "OPP_PTS_OFF_TOV",
]
METRICS_SCOR = [
    "PCT_PTS_3PT",
    "PCT_PTS_FT",
    "PCT_PTS_PAINT",
    "PCT_PTS_FB",
]

METRICS_EXTRA_ALL = METRICS_MISC + METRICS_SCOR


def dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated(keep="last")]


def as_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Restituisce una Series numerica anche se la colonna non esiste o è duplicata."""
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    if (df.columns == col).sum() > 1:
        s = df.loc[:, [col]].iloc[:, 0]
    else:
        s = df[col]
    return pd.to_numeric(s, errors="coerce")


def safe_diff(df: pd.DataFrame, a: str, b: str, out: str) -> None:
    df[out] = as_series(df, a) - as_series(df, b)


def ensure_home_away(reg: pd.DataFrame) -> pd.DataFrame:
    """Si assicura che esistano HOME_TEAM e AWAY_TEAM, usando RAW_GH_PATH come fallback."""
    reg = reg.copy()

    if {"HOME_TEAM", "AWAY_TEAM"}.issubset(reg.columns) and reg["HOME_TEAM"].notna().any():
        return reg

    # Se nel dataset regular ci sono gli ID delle squadre
    if {"HOME_TEAM_ID", "VISITOR_TEAM_ID"}.issubset(reg.columns):
        reg["HOME_TEAM"] = reg["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
        reg["AWAY_TEAM"] = reg["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)
        return reg

    # Fallback: usa dataset_raw_2025_26.csv
    if RAW_GH_PATH.exists():
        gh = pd.read_csv(RAW_GH_PATH)
        if {"GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID"}.issubset(gh.columns):
            gh = gh.copy()
            gh["HOME_TEAM"] = gh["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
            gh["AWAY_TEAM"] = gh["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)
            reg = reg.drop(columns=["HOME_TEAM", "AWAY_TEAM"], errors="ignore")
            reg = reg.merge(gh[["GAME_ID", "HOME_TEAM", "AWAY_TEAM"]], on="GAME_ID", how="left")
            return reg

    # Estremo fallback: colonne vuote
    if "HOME_TEAM" not in reg:
        reg["HOME_TEAM"] = pd.NA
    if "AWAY_TEAM" not in reg:
        reg["AWAY_TEAM"] = pd.NA
    return reg


def latest_by_team(stats: pd.DataFrame) -> pd.DataFrame:
    """
    Ritorna lo snapshot più recente per TEAM, usando DATE se presente.
    Normalizza anche nomi alias (TEAM_ABBREVIATION, OFF_RATING, DEF_RATING, NET_RATING).
    """
    if stats.empty:
        return stats

    df = stats.copy()

    rename_map = {
        "TEAM_ABBREVIATION": "TEAM",
        "OFF_RATING": "OFFRTG",
        "DEF_RATING": "DEFRTG",
        "NET_RATING": "NETRTG",
    }
    df.rename(columns=rename_map, inplace=True)

    if "TEAM" not in df.columns and "TEAM_ID" in df.columns:
        df["TEAM"] = df["TEAM_ID"].map(TEAM_ID_TO_ABBR)

    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.sort_values(["TEAM", "DATE"])
        df = df.groupby("TEAM", as_index=False).tail(1)
    else:
        df = df.sort_values("TEAM").groupby("TEAM", as_index=False).tail(1)

    cols = ["TEAM"]
    if "TEAM_ID" in df.columns:
        cols.append("TEAM_ID")
    other_cols = [c for c in df.columns if c not in cols + ["DATE"]]
    return df[cols + other_cols].drop_duplicates(subset=["TEAM"])


def main():
    if not REG_PATH.exists():
        raise SystemExit(f"Dataset non trovato: {REG_PATH}")
    if not TEAMSTATS_PATH.exists():
        raise SystemExit(f"Team stats base non trovate: {TEAMSTATS_PATH}")

    # Carica dataset regular
    reg = pd.read_csv(REG_PATH)
    reg = ensure_home_away(reg)

    # --- PULIZIA COLONNE TEAM STATS PREESISTENTI (idempotenza) ---
    all_metrics = METRICS_BASE + METRICS_EXTRA_ALL
    cols_to_drop = []
    for m in all_metrics:
        for suffix in ("_HOME", "_AWAY", "_DIFF"):
            col = f"{m}{suffix}"
            if col in reg.columns:
                cols_to_drop.append(col)
    if cols_to_drop:
        reg = reg.drop(columns=cols_to_drop, errors="ignore")

    # === BASE STATS ===
    ts_base = pd.read_csv(TEAMSTATS_PATH)
    ts_base = latest_by_team(ts_base)

    # === EXTRA STATS (se presenti) ===
    if TEAMEXTRA_PATH.exists():
        ts_extra = pd.read_csv(TEAMEXTRA_PATH)
        ts_extra = latest_by_team(ts_extra)
    else:
        ts_extra = pd.DataFrame()

    # ---- HOME SIDE ----
    home = ts_base.copy()
    for m in METRICS_BASE:
        if m in home.columns:
            home.rename(columns={m: f"{m}_HOME"}, inplace=True)
    home.rename(columns={"TEAM": "HOME_TEAM"}, inplace=True)

    if not ts_extra.empty:
        extra_home = ts_extra.copy()
        extra_home.rename(columns={"TEAM": "HOME_TEAM"}, inplace=True)
        for m in METRICS_EXTRA_ALL:
            if m in extra_home.columns:
                extra_home.rename(columns={m: f"{m}_HOME"}, inplace=True)
        home = home.merge(extra_home, on="HOME_TEAM", how="left")

    home = dedup_columns(home)
    drop_cols_home = [
        c for c in home.columns
        if c.startswith("TEAM_ID") or c in ["TEAM_NAME", "GP", "TS_PCT", "EFG_PCT", "UPDATED_AT"]
    ]
    home = home.drop(columns=drop_cols_home, errors="ignore")
    keep_home = ["HOME_TEAM"] + [c for c in home.columns if c.endswith("_HOME")]
    home = home[keep_home]

    # ---- AWAY SIDE ----
    away = ts_base.copy()
    for m in METRICS_BASE:
        if m in away.columns:
            away.rename(columns={m: f"{m}_AWAY"}, inplace=True)
    away.rename(columns={"TEAM": "AWAY_TEAM"}, inplace=True)

    if not ts_extra.empty:
        extra_away = ts_extra.copy()
        extra_away.rename(columns={"TEAM": "AWAY_TEAM"}, inplace=True)
        for m in METRICS_EXTRA_ALL:
            if m in extra_away.columns:
                extra_away.rename(columns={m: f"{m}_AWAY"}, inplace=True)
        away = away.merge(extra_away, on="AWAY_TEAM", how="left")

    away = dedup_columns(away)
    drop_cols_away = [
        c for c in away.columns
        if c.startswith("TEAM_ID") or c in ["TEAM_NAME", "GP", "TS_PCT", "EFG_PCT", "UPDATED_AT"]
    ]
    away = away.drop(columns=drop_cols_away, errors="ignore")
    keep_away = ["AWAY_TEAM"] + [c for c in away.columns if c.endswith("_AWAY")]
    away = away[keep_away]

    # ---- MERGE sul dataset regular ----
    reg = reg.merge(home, on="HOME_TEAM", how="left")
    reg = reg.merge(away, on="AWAY_TEAM", how="left")
    reg = dedup_columns(reg)

    # ---- DIFFERENZE metriche base ----
    safe_diff(reg, "PACE_HOME",   "PACE_AWAY",   "PACE_DIFF")
    safe_diff(reg, "OFFRTG_HOME", "OFFRTG_AWAY", "OFFRTG_DIFF")
    safe_diff(reg, "DEFRTG_HOME", "DEFRTG_AWAY", "DEFRTG_DIFF")
    safe_diff(reg, "NETRTG_HOME", "NETRTG_AWAY", "NETRTG_DIFF")
    safe_diff(reg, "TS_HOME",     "TS_AWAY",     "TS_DIFF")
    safe_diff(reg, "EFG_HOME",    "EFG_AWAY",    "EFG_DIFF")

    # ---- DIFFERENZE metriche extra (se presenti) ----
    for m in METRICS_EXTRA_ALL:
        h_col = f"{m}_HOME"
        a_col = f"{m}_AWAY"
        diff_col = f"{m}_DIFF"
        if h_col in reg.columns or a_col in reg.columns:
            safe_diff(reg, h_col, a_col, diff_col)

    reg.to_csv(REG_PATH, index=False)
    print(f"✅ Team stats base+extra aggiunte. Dataset aggiornato: {REG_PATH}")
    print(f"🔢 Numero colonne totali: {reg.shape[1]}")


if __name__ == "__main__":
    main()