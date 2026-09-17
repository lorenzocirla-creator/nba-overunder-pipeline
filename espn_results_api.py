# espn_results_api.py
"""
Risultati NBA da ESPN, usati come fallback quando le line score NBA non hanno il punteggio.

Usa l'endpoint "scoreboard" del site API, che restituisce squadre e punteggi di tutta
la giornata in UNA sola richiesta. (La vecchia implementazione usava sports.core.api,
che richiede ~6 richieste annidate per partita: ~60 chiamate per giornata.)
"""

from __future__ import annotations

import time

import pandas as pd
import requests

SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
)
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 15
RETRIES = 3

COLS = ["GAME_ID", "GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PTS_HOME", "PTS_AWAY", "TOTAL_POINTS"]


def _get_json(url: str) -> dict:
    """GET con retry ed errori espliciti (niente `except:` muto)."""
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(attempt)  # backoff lineare: 1s, 2s
    print(f"⚠️ ESPN: richiesta fallita dopo {RETRIES} tentativi ({type(last_err).__name__}: {last_err})")
    return {}


def fetch_espn_results(day) -> pd.DataFrame:
    """
    Ritorna un DataFrame con le partite CONCLUSE del giorno indicato:
    GAME_ID | GAME_DATE | HOME_TEAM | AWAY_TEAM | PTS_HOME | PTS_AWAY | TOTAL_POINTS

    `day` è una date/datetime/Timestamp. Le partite non ancora finali vengono escluse:
    riportare punteggi parziali falserebbe il dataset di training.
    """
    day = pd.to_datetime(day)
    js = _get_json(SCOREBOARD_URL.format(ymd=day.strftime("%Y%m%d")))

    rows = []
    for ev in js.get("events", []):
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]

        # Solo partite concluse
        status = (comp.get("status") or {}).get("type") or {}
        if not status.get("completed", False):
            continue

        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            continue

        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if home is None or away is None:
            continue

        pts_home = pd.to_numeric(home.get("score"), errors="coerce")
        pts_away = pd.to_numeric(away.get("score"), errors="coerce")
        if pd.isna(pts_home) or pd.isna(pts_away):
            continue

        # GAME_DATE = la giornata richiesta, NON ev["date"]: ESPN espone il tip-off in UTC,
        # quindi una partita serale ET risulterebbe al giorno dopo e non aggancerebbe
        # il dataset NBA (che usa la data ET).
        rows.append({
            "GAME_ID": ev.get("id"),
            "GAME_DATE": day.strftime("%Y-%m-%d"),
            "HOME_TEAM": (home.get("team") or {}).get("abbreviation"),
            "AWAY_TEAM": (away.get("team") or {}).get("abbreviation"),
            "PTS_HOME": pts_home,
            "PTS_AWAY": pts_away,
            "TOTAL_POINTS": pts_home + pts_away,
        })

    return pd.DataFrame(rows, columns=COLS)


if __name__ == "__main__":
    import sys

    target = sys.argv[1] if len(sys.argv) > 1 else "2025-11-24"
    df = fetch_espn_results(pd.to_datetime(target).date())
    print(f"{target}: {len(df)} partite concluse")
    print(df.to_string(index=False))
