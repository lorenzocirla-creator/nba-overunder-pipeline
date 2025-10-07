# features/add_context_features.py
"""
Aggiunge le context/calendar features al dataset 2025–26 e lo sovrascrive.

Colonne gestite (create se mancanti):
- STREAK_HOME, STREAK_AWAY, PDIFF_HOME, PDIFF_AWAY
- ROAD_TRIP_HOME, ROAD_TRIP_AWAY, ROAD_TRIP_LEN_HOME, ROAD_TRIP_LEN_AWAY
- 3IN4_HOME, 3IN4_AWAY, 4IN6_HOME, 4IN6_AWAY
- GAMES_LAST4_HOME, GAMES_LAST4_AWAY, GAMES_LAST6_HOME, GAMES_LAST6_AWAY
- REST_DIFF, PACE_DIFF, NETRTG_DIFF, OFFRTG_DIFF, TS_DIFF, EFG_DIFF
- FORMA_SUM_5, FORMA_DIFF_5, DIFESA_SUM_5
- REST_ADV_FLAG, LAST_GAME_ROADTRIP_HOME, LAST_GAME_ROADTRIP_AWAY
- SEASON_PHASE, COAST_TRAVEL
- TREND_HOME, TREND_AWAY
- AVG_PACE, H2H_AVG_TOTAL_LASTN
- THREEPT_DIFF, REB_DIFF, TOV_DIFF
- END_SEASON_FLAG, ALL_STAR_FLAG, PLAYOFF_RACE_FLAG
"""

import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config_season_2526 import path_dataset_regular

# ---- Mapping NOME COMPLETO -> TRICODE (30 squadre NBA) ----
TEAM_TO_TRICODE = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "LA Clippers": "LAC", "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP", "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR", "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

EAST = {"BOS","BKN","NYK","PHI","TOR","MIA","MIL","CHI","ATL","ORL","CLE","IND","DET","WAS","CHA"}
WEST = {"GSW","LAL","LAC","SAC","PHX","POR","UTA","DEN","MIN","OKC","DAL","HOU","MEM","SAS","NOP"}

def to_tricode(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip()
    up = s.upper()
    if up in EAST or up in WEST:
        return up
    alias = {
        "LA LAKERS": "Los Angeles Lakers", "LOS ANGELES LAKERS": "Los Angeles Lakers",
        "LA CLIPPERS": "Los Angeles Clippers", "LOS ANGELES CLIPPERS": "Los Angeles Clippers",
        "GOLDEN STATE": "Golden State Warriors", "OKLAHOMA CITY": "Oklahoma City Thunder",
        "NEW ORLEANS": "New Orleans Pelicans", "SAN ANTONIO": "San Antonio Spurs",
        "PORTLAND": "Portland Trail Blazers",
    }
    if up in alias:
        s = alias[up]
    return TEAM_TO_TRICODE.get(s, up[:3])

def add_context_features():
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)
    if "GAME_DATE" not in df.columns:
        raise ValueError("Colonna GAME_DATE mancante nel dataset.")
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    df = df.sort_values("GAME_DATE")

    # Colonne da garantire
    new_cols = [
        "STREAK_HOME","STREAK_AWAY","PDIFF_HOME","PDIFF_AWAY",
        "ROAD_TRIP_HOME","ROAD_TRIP_AWAY",
        "3IN4_HOME","3IN4_AWAY","4IN6_HOME","4IN6_AWAY",
        "ROAD_TRIP_LEN_HOME","ROAD_TRIP_LEN_AWAY",
        "GAMES_LAST4_HOME","GAMES_LAST4_AWAY",
        "GAMES_LAST6_HOME","GAMES_LAST6_AWAY",
        "REST_DIFF","PACE_DIFF","NETRTG_DIFF","OFFRTG_DIFF","TS_DIFF","EFG_DIFF",
        "FORMA_SUM_5","FORMA_DIFF_5","DIFESA_SUM_5",
        "REST_ADV_FLAG","LAST_GAME_ROADTRIP_HOME","LAST_GAME_ROADTRIP_AWAY",
        "SEASON_PHASE","COAST_TRAVEL",
        # Extra mancanti dal vecchio dataset
        "TREND_HOME","TREND_AWAY",
        "AVG_PACE","H2H_AVG_TOTAL_LASTN",
        "THREEPT_DIFF","REB_DIFF","TOV_DIFF",
        "END_SEASON_FLAG","ALL_STAR_FLAG","PLAYOFF_RACE_FLAG"
    ]
    for col in new_cols:
        if col not in df.columns:
            df[col] = 0

    # Loop per squadra
    teams = pd.unique(pd.concat([df["HOME_TEAM"], df["AWAY_TEAM"]], ignore_index=True))
    for team in teams:
        team_games = df[(df["HOME_TEAM"] == team) | (df["AWAY_TEAM"] == team)].copy()
        team_games = team_games.sort_values("GAME_DATE")

        streak, road_trip_len = 0, 0
        diffs, last_dates = [], []

        for idx, row in team_games.iterrows():
            date = row["GAME_DATE"]
            is_home = (row["HOME_TEAM"] == team)
            pf = row["PTS_HOME"] if is_home else row["PTS_AWAY"]
            pa = row["PTS_AWAY"] if is_home else row["PTS_HOME"]

            # Streak
            if pd.notna(pf) and pd.notna(pa):
                streak = streak + 1 if pf > pa and streak >= 0 else (streak - 1 if pf < pa and streak <= 0 else (1 if pf > pa else -1))

            # Roadtrip
            road_trip_len = road_trip_len + 1 if not is_home else 0

            # Rolling diff ultime 5
            if pd.notna(pf) and pd.notna(pa):
                diffs.append(pf - pa)
            avg_diff = (sum(diffs[-5:]) / min(len(diffs),5)) if diffs else 0
            trend10 = (sum(diffs[-10:]) / min(len(diffs),10)) if diffs else 0

            # Congestione calendario
            last_dates = [d for d in last_dates if (date - d).days <= 6]
            last_dates.append(date)
            games_last4 = sum((date - d).days <= 3 for d in last_dates)
            games_last6 = len(last_dates)
            is_3in4 = (games_last4 >= 3)
            is_4in6 = (games_last6 >= 4)

            if is_home:
                df.at[idx,"STREAK_HOME"] = streak
                df.at[idx,"PDIFF_HOME"] = avg_diff
                df.at[idx,"TREND_HOME"] = trend10
                df.at[idx,"ROAD_TRIP_HOME"] = 0
                df.at[idx,"ROAD_TRIP_LEN_HOME"] = road_trip_len
                df.at[idx,"3IN4_HOME"] = int(is_3in4)
                df.at[idx,"4IN6_HOME"] = int(is_4in6)
                df.at[idx,"GAMES_LAST4_HOME"] = games_last4
                df.at[idx,"GAMES_LAST6_HOME"] = games_last6
            else:
                df.at[idx,"STREAK_AWAY"] = streak
                df.at[idx,"PDIFF_AWAY"] = avg_diff
                df.at[idx,"TREND_AWAY"] = trend10
                df.at[idx,"ROAD_TRIP_AWAY"] = 1
                df.at[idx,"ROAD_TRIP_LEN_AWAY"] = road_trip_len
                df.at[idx,"3IN4_AWAY"] = int(is_3in4)
                df.at[idx,"4IN6_AWAY"] = int(is_4in6)
                df.at[idx,"GAMES_LAST4_AWAY"] = games_last4
                df.at[idx,"GAMES_LAST6_AWAY"] = games_last6

    # Colonne extra globali
    if "PACE_HOME" in df.columns and "PACE_AWAY" in df.columns:
        df["AVG_PACE"] = df[["PACE_HOME","PACE_AWAY"]].mean(axis=1)

    # H2H media ultimi 5 scontri
    df["H2H_AVG_TOTAL_LASTN"] = 0
    for (h,a), g in df.groupby(["HOME_TEAM","AWAY_TEAM"]):
        for i, idx in enumerate(g.index):
            prev = g.loc[g.index < idx].tail(5)
            df.at[idx,"H2H_AVG_TOTAL_LASTN"] = prev["TOTAL_POINTS"].mean() if not prev.empty else 0

    # Flags stagionali
    df["END_SEASON_FLAG"] = df["GAME_DATE"].dt.month.ge(4).astype(int)
    df["PLAYOFF_RACE_FLAG"] = df["GAME_DATE"].dt.month.ge(3).astype(int)
    df["ALL_STAR_FLAG"] = 0  # da calendarizzare meglio

    # Placeholder per differenziali non ancora implementati
    for col in ["THREEPT_DIFF","REB_DIFF","TOV_DIFF"]:
        df[col] = 0

    # Coast travel
    def coast_flag(row):
        home_tri, away_tri = to_tricode(row["HOME_TEAM"]), to_tricode(row["AWAY_TEAM"])
        return 1 if (home_tri in EAST and away_tri in WEST) or (home_tri in WEST and away_tri in EAST) else 0
    df["COAST_TRAVEL"] = df.apply(coast_flag, axis=1)

    # Salvataggio
    df.to_csv(dataset_path, index=False)
    from config_season_2526 import DATA_DIR
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)

    print(f"✅ Context features aggiunte. Dataset aggiornato: {dataset_path}")
    print(f"📌 Master dataset aggiornato in {master_path}")
    return df

if __name__ == "__main__":
    add_context_features()
