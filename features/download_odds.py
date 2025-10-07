import sys
from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# aggiungo la cartella padre (2025_2026) a sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR

API_KEY = "a1e0538e26784a65340eb6fb8ef6ac43"  # la tua key Odds API
SPORT = "basketball_nba"
MARKET = "totals"   # over/under
REGION = "us"       # bookmakers USA
ODDS_FILE = DATA_DIR / "odds_2025_26.csv"


def fetch_odds():
    """Scarica odds correnti NBA dal feed Odds API"""
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    params = {
        "apiKey": API_KEY,
        "regions": REGION,
        "markets": MARKET,
        "oddsFormat": "decimal",
        "dateFormat": "iso"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()


def update_odds():
    """Aggiorna file odds con current line (future) e closing line (ieri)"""
    data = fetch_odds()
    yesterday_et = (datetime.now(ZoneInfo("America/New_York")) - timedelta(days=1)).date()

    rows = []
    for game in data:
        # Parsing commence_time
        utc_time = datetime.fromisoformat(game["commence_time"].replace("Z", "+00:00"))
        et_time = utc_time.astimezone(ZoneInfo("America/New_York"))

        home, away = game["home_team"], game["away_team"]

        # Prendi il primo market "totals"
        total_points = None
        for b in game["bookmakers"]:
            for m in b["markets"]:
                if m["key"] == "totals":
                    total_points = m["outcomes"][0]["point"]
                    break
            if total_points:
                break

        if not total_points:
            continue

        row = {
            "GAME_DATE": et_time.date(),   # Data ufficiale ET
            "HOME_TEAM": home,
            "AWAY_TEAM": away,
            "CURRENT_LINE": total_points,
            "CLOSING_LINE": None
        }

        # Se partita giocata ieri (ET) → assegna closing line
        if et_time.date() == yesterday_et:
            row["CLOSING_LINE"] = total_points

        rows.append(row)

    df_new = pd.DataFrame(rows)

    # Schema finale
    columns_order = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "CURRENT_LINE", "CLOSING_LINE"]

    if ODDS_FILE.exists():
        df_old = pd.read_csv(ODDS_FILE)

        # normalizza vecchi file
        for col in columns_order:
            if col not in df_old.columns:
                df_old[col] = pd.NA
        df_old = df_old[columns_order]

        # concat
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new.copy()

    # parsing tipi
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
    for c in ["CURRENT_LINE", "CLOSING_LINE"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # elimina righe senza GAME_DATE
    df = df.dropna(subset=["GAME_DATE"])

    # deduplica → tieni ultima riga (con closing se disponibile)
    df["has_closing"] = df["CLOSING_LINE"].notna().astype(int)
    df = df.sort_values(["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "has_closing"])
    df = df.drop_duplicates(["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last")
    df = df.drop(columns=["has_closing"])

    # ordina colonne e salva
    df = df[columns_order]
    df.to_csv(ODDS_FILE, index=False)

    print(f"✅ Aggiornato {ODDS_FILE} (totale {len(df)} partite)")
    print(f"   ➡️  Aggiunte/aggiornate: {len(df_new)} righe")


if __name__ == "__main__":
    update_odds()
