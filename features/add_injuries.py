# features/add_injuries.py
import sys
from pathlib import Path
from datetime import timedelta
import unicodedata
import re
import pandas as pd
import numpy as np

# === Import config ===
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR  # noqa: E402

# ---------------- Config ----------------
TOP_N_SCORERS = 5
STATUS_WEIGHTS = {
    "Out": 1.0,
    "Doubtful": 0.5,
    "Questionable": 0.25,
    "Probable": 0.1,
    "Active": 0.0,
    "-": 0.0
}

# --- Team mapping full name -> abbr (tutti i 30 team)
TEAM_NAME_TO_ABBR = {
    "ATLANTA HAWKS": "ATL",
    "BOSTON CELTICS": "BOS",
    "BROOKLYN NETS": "BKN",
    "CHARLOTTE HORNETS": "CHA",
    "CHICAGO BULLS": "CHI",
    "CLEVELAND CAVALIERS": "CLE",
    "DALLAS MAVERICKS": "DAL",
    "DENVER NUGGETS": "DEN",
    "DETROIT PISTONS": "DET",
    "GOLDEN STATE WARRIORS": "GSW",
    "HOUSTON ROCKETS": "HOU",
    "INDIANA PACERS": "IND",
    "LA CLIPPERS": "LAC",
    "LOS ANGELES CLIPPERS": "LAC",
    "LOS ANGELES LAKERS": "LAL",
    "MEMPHIS GRIZZLIES": "MEM",
    "MIAMI HEAT": "MIA",
    "MILWAUKEE BUCKS": "MIL",
    "MINNESOTA TIMBERWOLVES": "MIN",
    "NEW ORLEANS PELICANS": "NOP",
    "NEW YORK KNICKS": "NYK",
    "OKLAHOMA CITY THUNDER": "OKC",
    "ORLANDO MAGIC": "ORL",
    "PHILADELPHIA 76ERS": "PHI",
    "PHOENIX SUNS": "PHX",
    "PORTLAND TRAIL BLAZERS": "POR",
    "SACRAMENTO KINGS": "SAC",
    "SAN ANTONIO SPURS": "SAS",
    "TORONTO RAPTORS": "TOR",
    "UTAH JAZZ": "UTA",
    "WASHINGTON WIZARDS": "WAS",
}

# --- utility: normalizzazione nomi giocatori
_name_keep_apostrophe = re.compile(r"[^a-zA-Z\s']+")

def normalize_name(s) -> str:
    """Rimuove accenti, trasforma 'Cognome, Nome' in 'Nome Cognome',
    riduce spazi, minuscolo. Tiene apostrofi."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    s = str(s)
    # da "Last, First" a "First Last"
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) >= 2:
            s = " ".join(parts[1:] + parts[:1])
    # rimuovi accenti
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    # pulizia caratteri (tieni apostrofi)
    s = _name_keep_apostrophe.sub(" ", s)
    # collapse spazi, lower
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

# --- utility: normalizzazione sigla team
def normalize_team_abbr(x) -> str:
    """Accetta sigle già pronte (LAL) o full name (Los Angeles Lakers) e ritorna sigla."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    if not s:
        return ""
    su = s.upper()
    # se sembra già una sigla tipo LAL, NYK, OKC...
    if len(su) in (2, 3) and su.isalpha():
        return su
    # mappa full name -> sigla
    return TEAM_NAME_TO_ABBR.get(su, "")

# --- utility: selezione colonne player_stats
def _resolve_player_stats_columns(df: pd.DataFrame):
    """Trova colonne per nome giocatore, team, e PPG (o calcola da PTS/GP)."""
    # nome
    name_col = None
    for c in ["PLAYER", "PLAYER_NAME", "Player", "NAME"]:
        if c in df.columns:
            name_col = c
            break
    if name_col is None:
        raise ValueError("player_stats_2025_26.csv: manca colonna nome (PLAYER/PLAYER_NAME).")

    # team (abbr consigliato)
    team_col = None
    for c in ["TEAM", "TEAM_ABBR", "TEAM_ABBREVIATION", "Team"]:
        if c in df.columns:
            team_col = c
            break
    if team_col is None:
        raise ValueError("player_stats_2025_26.csv: manca colonna team (TEAM/TEAM_ABBR).")

    # PPG diretto
    ppg_col = None
    for c in ["PPG", "PTS_PER_GAME"]:
        if c in df.columns:
            ppg_col = c
            break

    # altrimenti prova a calcolarlo da PTS/GP
    if ppg_col is None:
        if "PTS" in df.columns and "GP" in df.columns:
            df["_PPG_CALC"] = pd.to_numeric(df["PTS"], errors="coerce") / pd.to_numeric(df["GP"], errors="coerce").replace(0, np.nan)
            ppg_col = "_PPG_CALC"
        else:
            # fallback: prova "PTS" come PPG (non ideale, ma evita crash)
            if "PTS" in df.columns:
                ppg_col = "PTS"
            else:
                # nessuna metrica di punti trovata
                df["_PPG_CALC"] = 0.0
                ppg_col = "_PPG_CALC"

    return name_col, team_col, ppg_col


# ---------------- Main ----------------
def add_injuries(dataset_path=None, output_path=None):
    if dataset_path is None:
        dataset_path = DATA_DIR / "dataset_regular_2025_26.csv"
    if output_path is None:
        output_path = DATA_DIR / "dataset_injuries.csv"

    # --- Carica dataset partite
    if not Path(dataset_path).exists():
        raise FileNotFoundError(f"Dataset non trovato: {dataset_path}")
    games = pd.read_csv(dataset_path)
    games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"], errors="coerce")

    # Assicurati che le colonne target esistano
    for col in ["KEY_PLAYERS_OUT_HOME", "KEY_PLAYERS_OUT_AWAY", "IMPACT_HOME", "IMPACT_AWAY"]:
        if col not in games.columns:
            games[col] = 0.0 if "IMPACT" in col else 0.0

    # --- Carica Injury Report
    injuries_path = DATA_DIR / "injuries_2025_26.csv"
    if (not injuries_path.exists()) or injuries_path.stat().st_size == 0:
        print("⚠️ File infortuni mancante o vuoto, creo solo colonne.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    injuries = pd.read_csv(injuries_path)
    if injuries.empty:
        print("⚠️ File infortuni vuoto, salto logica.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    # Normalizza data report
    injuries["report_date"] = pd.to_datetime(injuries["report_date"], errors="coerce").dt.tz_localize(None).dt.date

    # Aggiungi TEAM_ABBR dagli injury (full name -> sigla)
    injuries["TEAM_ABBR"] = injuries.get("Team", "").apply(normalize_team_abbr)

    # --- Carica Player Stats (per top scorer)
    player_stats_path = DATA_DIR / "player_stats_2025_26.csv"
    if (not player_stats_path.exists()) or player_stats_path.stat().st_size == 0:
        print("⚠️ File player_stats mancante o vuoto, creo solo colonne.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    ps = pd.read_csv(player_stats_path)
    if ps.empty:
        print("⚠️ player_stats vuoto.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    name_col, team_col, ppg_col = _resolve_player_stats_columns(ps)
    ps["PLAYER_NORM"] = ps[name_col].apply(normalize_name)
    ps["TEAM"] = ps[team_col].astype(str).apply(normalize_team_abbr)
    ps["PPG"] = pd.to_numeric(ps[ppg_col], errors="coerce").fillna(0.0)

    # Top scorer per team
    top_scorers = (
        ps.sort_values(["TEAM", "PPG"], ascending=[True, False])
          .groupby("TEAM")
          .head(TOP_N_SCORERS)
          .reset_index(drop=True)
    )

    # --- Funzione per calcolare l'impatto injuries per (data, team)
    def compute_injury_impact(date_ts, team_abbr):
        # date a livello "date"
        date_only = date_ts.date() if hasattr(date_ts, "date") else date_ts

        day_inj = injuries[(injuries["report_date"] == date_only) & (injuries["TEAM_ABBR"] == team_abbr)]
        if day_inj.empty:
            prev_day = date_only - timedelta(days=1)
            day_inj = injuries[(injuries["report_date"] == prev_day) & (injuries["TEAM_ABBR"] == team_abbr)]

        if day_inj.empty:
            return 0.0, 0.0

        team_top = top_scorers[top_scorers["TEAM"] == team_abbr]
        if team_top.empty:
            return 0.0, 0.0

        # Pre-normalizza i nomi injury solo per le righe del giorno
        day_inj = day_inj.copy()
        day_inj["PLAYER_NORM_INJ"] = day_inj["Player Name"].apply(normalize_name)

        key_out = 0.0
        impact = 0.0

        for _, player in team_top.iterrows():
            pnorm = player["PLAYER_NORM"]
            rows = day_inj[day_inj["PLAYER_NORM_INJ"] == pnorm]
            if rows.empty:
                continue
            # usa la prima occorrenza
            status = str(rows.iloc[0].get("Current Status", "")).strip()
            weight = STATUS_WEIGHTS.get(status, 0.0)
            key_out += weight
            impact += player["PPG"] * weight

        return key_out, impact

    # --- Applica a tutte le partite
    # garantisci che team siano sigle
    games["HOME_TEAM"] = games["HOME_TEAM"].astype(str).apply(normalize_team_abbr)
    games["AWAY_TEAM"] = games["AWAY_TEAM"].astype(str).apply(normalize_team_abbr)

    games["KEY_PLAYERS_OUT_HOME"] = 0.0
    games["KEY_PLAYERS_OUT_AWAY"] = 0.0
    games["IMPACT_HOME"] = 0.0
    games["IMPACT_AWAY"] = 0.0

    for idx, row in games.iterrows():
        dt = row["GAME_DATE"]
        home = row["HOME_TEAM"]
        away = row["AWAY_TEAM"]

        kh, ih = compute_injury_impact(dt, home) if home else (0.0, 0.0)
        ka, ia = compute_injury_impact(dt, away) if away else (0.0, 0.0)

        games.at[idx, "KEY_PLAYERS_OUT_HOME"] = float(kh)
        games.at[idx, "KEY_PLAYERS_OUT_AWAY"] = float(ka)
        games.at[idx, "IMPACT_HOME"] = float(ih)
        games.at[idx, "IMPACT_AWAY"] = float(ia)

    # --- Salva
    games.to_csv(output_path, index=False)
    print(f"✅ Dataset aggiornato con infortuni salvato in {output_path}")
    print("⚖️ Pesi usati:", STATUS_WEIGHTS)
    print(f"🏀 Top scorer considerati per team: {TOP_N_SCORERS}")

    games.to_csv(dataset_path, index=False)
    print(f"📌 Master dataset aggiornato in {dataset_path}")

    # Piccolo riepilogo utilità
    m = (games["KEY_PLAYERS_OUT_HOME"] > 0) | (games["KEY_PLAYERS_OUT_AWAY"] > 0)
    print(f"📊 Partite con almeno un key-out: {int(m.sum())} / {len(games)}")

    return games


if __name__ == "__main__":
    add_injuries()