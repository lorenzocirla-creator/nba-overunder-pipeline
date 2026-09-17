#!/usr/bin/env python3
import pandas as pd
from config_season import SEASON_TAG

path = f"dati/dataset_regular_{SEASON_TAG}.csv"

df = pd.read_csv(path, parse_dates=["GAME_DATE"])

# Mapping ESPN → TRICODES NBA
mapping = {
    "GS": "GSW",
    "NO": "NOP",
    "NY": "NYK",
    "WSH": "WAS",
    "UTAH": "UTA",
    "MIN": "MIN",  # già corretto
}

# Fix righe con NaN
def fix_team(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    if x in mapping:
        return mapping[x]
    return x

df["HOME_TEAM"] = df["HOME_TEAM"].apply(fix_team)
df["AWAY_TEAM"] = df["AWAY_TEAM"].apply(fix_team)

# Fix specifico per le tre partite corrotte (ricostruito dallo schedule_full)
corrections = {
    401836753: ("WAS", "CLE"),   # WSH → WAS
    401836754: ("MEM", "UTA"),   # UTAH → UTA
    401836796: ("GSW", "MIN"),   # GS → GSW
}

for gid, (home, away) in corrections.items():
    df.loc[df["GAME_ID"] == gid, ["HOME_TEAM", "AWAY_TEAM"]] = (home, away)

df.to_csv(path, index=False)

print("✅ Correzione completata!")