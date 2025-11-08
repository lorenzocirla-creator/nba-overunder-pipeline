# features/add_team_stats.py
from pathlib import Path
import pandas as pd

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

# Metriche attese dal CSV team_stats_2025_26.csv
METRICS = ["PACE","OFFRTG","DEFRTG","NETRTG","TS","EFG"]


# ---------------------------- Utilità ---------------------------------

def dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rimuovi colonne duplicate mantenendo l'ultima occorrenza."""
    return df.loc[:, ~df.columns.duplicated(keep="last")]

def as_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Ritorna una Series anche se esistono colonne duplicate con lo stesso nome."""
    if col not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="float64")
    # Se duplicata, prendi la prima colonna con quel nome
    if (df.columns == col).sum() > 1:
        s = df.loc[:, [col]].iloc[:, 0]
    else:
        s = df[col]
    return pd.to_numeric(s, errors="coerce")

def safe_diff(df: pd.DataFrame, a: str, b: str, out: str) -> None:
    """Calcola df[out] = df[a] - df[b] in modo robusto (colonne duplicate/NaN)."""
    s_a = as_series(df, a)
    s_b = as_series(df, b)
    df[out] = s_a - s_b

def ensure_home_away(reg: pd.DataFrame) -> pd.DataFrame:
    """Assicura colonne HOME_TEAM/AWAY_TEAM (abbreviazioni) nel dataset regolare."""
    have_cols = {"HOME_TEAM", "AWAY_TEAM"}.issubset(reg.columns) and reg["HOME_TEAM"].notna().any()
    if have_cols:
        return reg

    # Prova a derivarle direttamente dal dataset regolare se ha gli ID
    if {"HOME_TEAM_ID", "VISITOR_TEAM_ID"}.issubset(reg.columns):
        reg = reg.copy()
        reg["HOME_TEAM"] = reg["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
        reg["AWAY_TEAM"] = reg["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)
        return reg

    # Fallback: prendi da raw game history se disponibile
    if RAW_GH_PATH.exists():
        gh = pd.read_csv(RAW_GH_PATH)
        if {"GAME_ID","HOME_TEAM_ID","VISITOR_TEAM_ID"}.issubset(gh.columns):
            gh = gh.copy()
            gh["HOME_TEAM"] = gh["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
            gh["AWAY_TEAM"] = gh["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)
            reg = reg.drop(columns=["HOME_TEAM","AWAY_TEAM"], errors="ignore")
            reg = reg.merge(gh[["GAME_ID","HOME_TEAM","AWAY_TEAM"]], on="GAME_ID", how="left")
            return reg

    # Ultimo fallback: lascia le colonne vuote per non rompere i merge
    reg = reg.copy()
    if "HOME_TEAM" not in reg: reg["HOME_TEAM"] = pd.NA
    if "AWAY_TEAM" not in reg: reg["AWAY_TEAM"] = pd.NA
    return reg

def latest_team_stats(stats: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza e seleziona la riga più recente per TEAM_ID (preferibile a TEAM).
    Se manca TEAM, lo ricava da TEAM_ID.
    Restituisce: TEAM, TEAM_ID + METRICS disponibili (senza duplicati).
    """
    stats = stats.copy()

    # Normalizza alias di colonne
    rename_map = {
        "OFF_RATING": "OFFRTG",
        "DEF_RATING": "DEFRTG",
        "NET_RATING": "NETRTG",
        "TEAM_ABBREVIATION": "TEAM",
    }
    stats.rename(columns=rename_map, inplace=True)

    # Se manca TEAM ma c'è TEAM_ID, mappa all'abbreviazione
    if "TEAM" not in stats.columns and "TEAM_ID" in stats.columns:
        stats["TEAM"] = stats["TEAM_ID"].map(TEAM_ID_TO_ABBR)

    # Tieni solo righe con TEAM_ID valido
    if "TEAM_ID" in stats.columns:
        stats = stats[stats["TEAM_ID"].notna()].copy()

    # Se c'è DATE, prendi la riga più recente per TEAM_ID
    if "DATE" in stats.columns:
        stats["DATE"] = pd.to_datetime(stats["DATE"], errors="coerce")
        if "TEAM_ID" in stats.columns:
            stats = stats.sort_values(["TEAM_ID","DATE"]).groupby("TEAM_ID", as_index=False).tail(1)
        else:
            stats = stats.sort_values(["TEAM","DATE"]).groupby("TEAM", as_index=False).tail(1)
    else:
        # Altrimenti prendi comunque l’ultima per TEAM_ID/TEAM
        if "TEAM_ID" in stats.columns:
            stats = stats.groupby("TEAM_ID", as_index=False).tail(1)
        else:
            stats = stats.groupby("TEAM", as_index=False).tail(1)

    # Seleziona le colonne utili (TEAM, TEAM_ID, metriche)
    cols = ["TEAM"]
    if "TEAM_ID" in stats.columns:
        cols.append("TEAM_ID")
    cols += [c for c in METRICS if c in stats.columns]

    stats = stats[cols].drop_duplicates(subset=["TEAM_ID"] if "TEAM_ID" in cols else ["TEAM"])
    return stats


# ---------------------------- Main ------------------------------------

def main():
    if not REG_PATH.exists():
        raise SystemExit(f"Dataset non trovato: {REG_PATH}")
    if not TEAMSTATS_PATH.exists():
        raise SystemExit(f"Team stats non trovate: {TEAMSTATS_PATH}")

    # Carica dataset regolare e assicura HOME/AWAY
    reg = pd.read_csv(REG_PATH)
    reg = ensure_home_away(reg)

    # Carica team stats e seleziona la snapshot più recente per team
    ts = pd.read_csv(TEAMSTATS_PATH)
    ts_latest = latest_team_stats(ts)

    # Prepara HOME
    home = ts_latest.copy()
    # Prefissa metriche con _HOME
    for m in METRICS:
        if m in home.columns:
            home.rename(columns={m: f"{m}_HOME"}, inplace=True)
    home.rename(columns={"TEAM": "HOME_TEAM"}, inplace=True)

    # Prepara AWAY
    away = ts_latest.copy()
    for m in METRICS:
        if m in away.columns:
            away.rename(columns={m: f"{m}_AWAY"}, inplace=True)
    away.rename(columns={"TEAM": "AWAY_TEAM"}, inplace=True)

    # Merge (LEFT) e de-dup colonne
    reg = reg.merge(home, on="HOME_TEAM", how="left")
    reg = reg.merge(away, on="AWAY_TEAM", how="left")
    reg = dedup_columns(reg)

    # Differenze (robuste e numeriche)
    safe_diff(reg, "PACE_HOME",   "PACE_AWAY",   "PACE_DIFF")
    safe_diff(reg, "OFFRTG_HOME", "OFFRTG_AWAY", "OFFRTG_DIFF")
    safe_diff(reg, "DEFRTG_HOME", "DEFRTG_AWAY", "DEFRTG_DIFF")
    safe_diff(reg, "NETRTG_HOME", "NETRTG_AWAY", "NETRTG_DIFF")
    safe_diff(reg, "TS_HOME",     "TS_AWAY",     "TS_DIFF")
    safe_diff(reg, "EFG_HOME",    "EFG_AWAY",    "EFG_DIFF")

    # Salva
    reg.to_csv(REG_PATH, index=False)
    print(f"✅ Team stats aggiunte (snapshot più recente). Dataset aggiornato: {REG_PATH}")

if __name__ == "__main__":
    main()