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

METRICS = ["PACE","OFFRTG","DEFRTG","NETRTG","TS","EFG"]

def ensure_home_away(reg: pd.DataFrame) -> pd.DataFrame:
    if {"HOME_TEAM","AWAY_TEAM"}.issubset(reg.columns) and reg["HOME_TEAM"].notna().any():
        return reg
    if RAW_GH_PATH.exists():
        gh = pd.read_csv(RAW_GH_PATH)
        if {"GAME_ID","HOME_TEAM_ID","VISITOR_TEAM_ID"}.issubset(gh.columns):
            gh["HOME_TEAM"] = gh["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
            gh["AWAY_TEAM"] = gh["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)
            reg = reg.drop(columns=["HOME_TEAM","AWAY_TEAM"], errors="ignore")
            reg = reg.merge(gh[["GAME_ID","HOME_TEAM","AWAY_TEAM"]], on="GAME_ID", how="left")
    return reg

def latest_team_stats(stats: pd.DataFrame) -> pd.DataFrame:
    # normalizza nomi colonne (alcuni endpoint usano OFF_RATING ecc.)
    rename_map = {
        "OFF_RATING":"OFFRTG",
        "DEF_RATING":"DEFRTG",
        "NET_RATING":"NETRTG",
        "TEAM_ABBREVIATION":"TEAM"
    }
    stats = stats.rename(columns=rename_map).copy()

    # tieni solo righe con TEAM presente
    stats = stats[stats["TEAM"].notna()].copy()

    # prendi la riga più recente per team
    if "DATE" in stats.columns:
        stats["DATE"] = pd.to_datetime(stats["DATE"], errors="coerce")
        stats = stats.sort_values(["TEAM","DATE"]).groupby("TEAM", as_index=False).tail(1)
    else:
        stats = stats.groupby("TEAM", as_index=False).tail(1)

    # colonne utili
    cols = ["TEAM"] + [c for c in METRICS if c in stats.columns]
    return stats[cols].drop_duplicates("TEAM")

def safe_diff(df, a, b, out):
    if a in df.columns and b in df.columns:
        df[out] = df[a] - df[b]

def main():
    if not REG_PATH.exists():
        raise SystemExit(f"Dataset non trovato: {REG_PATH}")
    if not TEAMSTATS_PATH.exists():
        raise SystemExit(f"Team stats non trovate: {TEAMSTATS_PATH}")

    reg = pd.read_csv(REG_PATH)
    reg = ensure_home_away(reg)

    ts = pd.read_csv(TEAMSTATS_PATH)
    ts_latest = latest_team_stats(ts)

    # merge HOME
    home = ts_latest.rename(columns={m: f"{m}_HOME" for m in METRICS if m in ts_latest.columns})
    home = home.rename(columns={"TEAM":"HOME_TEAM"})
    reg = reg.merge(home, on="HOME_TEAM", how="left")

    # merge AWAY
    away = ts_latest.rename(columns={m: f"{m}_AWAY" for m in METRICS if m in ts_latest.columns})
    away = away.rename(columns={"TEAM":"AWAY_TEAM"})
    reg = reg.merge(away, on="AWAY_TEAM", how="left")

    # differenze
    safe_diff(reg, "PACE_HOME",   "PACE_AWAY",   "PACE_DIFF")
    safe_diff(reg, "OFFRTG_HOME", "OFFRTG_AWAY", "OFFRTG_DIFF")
    safe_diff(reg, "DEFRTG_HOME", "DEFRTG_AWAY", "DEFRTG_DIFF")
    safe_diff(reg, "NETRTG_HOME", "NETRTG_AWAY", "NETRTG_DIFF")
    safe_diff(reg, "TS_HOME",     "TS_AWAY",     "TS_DIFF")
    safe_diff(reg, "EFG_HOME",    "EFG_AWAY",    "EFG_DIFF")

    reg.to_csv(REG_PATH, index=False)
    print(f"✅ Team stats aggiunte. Dataset aggiornato: {REG_PATH}")

if __name__ == "__main__":
    main()