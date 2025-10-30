# features/add_injuries.py
import pandas as pd
from pathlib import Path
from datetime import timedelta
import unicodedata
import re
import sys

# === Import config ===
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR

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
TEAM_NAME_TO_ABBR = {
    "ATLANTA HAWKS":"ATL", "BOSTON CELTICS":"BOS", "BROOKLYN NETS":"BKN", "CHARLOTTE HORNETS":"CHA",
    "CHICAGO BULLS":"CHI", "CLEVELAND CAVALIERS":"CLE", "DALLAS MAVERICKS":"DAL", "DENVER NUGGETS":"DEN",
    "DETROIT PISTONS":"DET", "GOLDEN STATE WARRIORS":"GSW", "HOUSTON ROCKETS":"HOU", "INDIANA PACERS":"IND",
    "LA CLIPPERS":"LAC", "LOS ANGELES CLIPPERS":"LAC", "LOS ANGELES LAKERS":"LAL", "MEMPHIS GRIZZLIES":"MEM",
    "MIAMI HEAT":"MIA", "MILWAUKEE BUCKS":"MIL", "MINNESOTA TIMBERWOLVES":"MIN", "NEW ORLEANS PELICANS":"NOP",
    "NEW YORK KNICKS":"NYK", "OKLAHOMA CITY THUNDER":"OKC", "ORLANDO MAGIC":"ORL", "PHILADELPHIA 76ERS":"PHI",
    "PHOENIX SUNS":"PHX", "PORTLAND TRAIL BLAZERS":"POR", "SACRAMENTO KINGS":"SAC", "SAN ANTONIO SPURS":"SAS",
    "TORONTO RAPTORS":"TOR", "UTAH JAZZ":"UTA", "WASHINGTON WIZARDS":"WAS", "CLEVELAND CAVS":"CLE"  # qualche alias
}

def map_team_to_abbr(s: str) -> str:
    """
    Restituisce l’abbreviazione a 3 lettere:
    - se già abbr (es. 'DAL'), la restituisce
    - altrimenti prova mapping fullname (es. 'DALLAS MAVERICKS' -> 'DAL')
    - fallback: prova a riconoscere 'LA CLIPPERS' / 'LOS ANGELES CLIPPERS'
    """
    if not s: 
        return ""
    s0 = str(s).strip()
    # se è già tipo DAL/GSW/OKC...
    if len(s0) <= 4 and s0.isalpha() and s0.isupper():
        return s0

    s1 = s0.upper().strip()
    if s1 in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[s1]

    # piccoli fallback per 'LA ' vs 'LOS ANGELES '
    s1 = s1.replace("LOS ANGELES", "LA").strip()
    if s1 in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[s1]

    # ultima spiaggia: prendi ultime parole (es. 'MAVERICKS', 'CLIPPERS') e prova match parziale
    for k,v in TEAM_NAME_TO_ABBR.items():
        if s1.endswith(k) or k.endswith(s1) or s1 in k:
            return v
    return s0[:3].upper()  # fallback brutale, meglio di niente

# ---------------- Normalizzazione nomi ----------------
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _normalize_name(s: str) -> str:
    """
    Regole:
      - rimuove accenti/diacritici (Dončić -> Doncic)
      - gestisce 'Last, First Middle' -> 'First Middle Last'
      - rimuove punteggiatura comune .,'- e compressione spazi
      - lower-case finale
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = _strip_accents(s)

    # se è nel formato "Cognome, Nome"
    if "," in s:
        last, first = s.split(",", 1)
        s = f"{first.strip()} {last.strip()}"

    # togli punteggiatura comune, normalizza spazi
    s = s.replace(".", " ").replace("'", "").replace("’", "").replace("-", " ").replace("`", "")
    s = re.sub(r"\s+", " ", s).strip()

    return s.lower()

def _normalize_team(s: str) -> str:
    # aspettativa: 3-letter abbr (es: DAL). Comunque fai trim/upper.
    return str(s).strip().upper() if s is not None else ""

# ---------------- Main ----------------
def add_injuries(dataset_path=None, output_path=None):
    if dataset_path is None:
        dataset_path = DATA_DIR / "dataset_regular_2025_26.csv"
    if output_path is None:
        output_path = DATA_DIR / "dataset_injuries.csv"

    games = pd.read_csv(dataset_path)
    games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"], errors="coerce")
    games["HOME_TEAM"] = games["HOME_TEAM"].astype(str).map(_normalize_team)
    games["AWAY_TEAM"] = games["AWAY_TEAM"].astype(str).map(_normalize_team)

    # Aggiungo sempre le colonne, anche se non ci sono dati
    for col in ["KEY_PLAYERS_OUT_HOME", "KEY_PLAYERS_OUT_AWAY", "IMPACT_HOME", "IMPACT_AWAY"]:
        if col not in games.columns:
            games[col] = 0.0 if "IMPACT" in col else 0.0

    # --- Injury report
    injuries_path = DATA_DIR / "injuries_2025_26.csv"
    if not injuries_path.exists() or injuries_path.stat().st_size == 0:
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

    # colonne attese (nomi NBA PDF CSV estratti)
    # "report_date", "Team", "Player Name", "Current Status"
    for c in ["report_date", "Team", "Player Name", "Current Status"]:
        if c not in injuries.columns:
            injuries[c] = pd.NA

    # normalizza campi injury
    injuries["report_date"] = pd.to_datetime(injuries["report_date"], errors="coerce").dt.tz_localize(None).dt.date
    injuries["TEAM_ABBR"] = injuries["Team"].astype(str).map(map_team_to_abbr)
    injuries["PLAYER_NORM"] = injuries["Player Name"].astype(str).map(_normalize_name)
    injuries["STATUS_NORM"] = injuries["Current Status"].astype(str).str.strip().str.title()

    # --- Player stats (per PPG e top scorer)
    player_stats_path = DATA_DIR / "player_stats_2025_26.csv"
    if not player_stats_path.exists() or player_stats_path.stat().st_size == 0:
        print("⚠️ File player_stats mancante o vuoto, creo solo colonne.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    ps = pd.read_csv(player_stats_path)
    # colonne attese: TEAM, PLAYER, PPG
    for c in ["TEAM", "PLAYER", "PPG"]:
        if c not in ps.columns:
            ps[c] = pd.NA

    ps["TEAM"] = ps["TEAM"].astype(str).map(_normalize_team)
    ps["PLAYER_NORM"] = ps["PLAYER"].astype(str).map(_normalize_name)
    ps["PPG"] = pd.to_numeric(ps["PPG"], errors="coerce")

    # top N scorer per team
    top_scorers = (
        ps.sort_values(["TEAM", "PPG"], ascending=[True, False])
          .groupby("TEAM")
          .head(TOP_N_SCORERS)
          .reset_index(drop=True)
    )

    # helper
    def compute_injury_impact(date_ts, team_abbr):
        date_only = date_ts.date() if hasattr(date_ts, "date") else date_ts
        # 1) injury del giorno
        day_inj = injuries[(injuries["report_date"] == date_only) & (injuries["TEAM_ABBR"] == team_abbr)]
        # fallback: giorno precedente
        if day_inj.empty:
            day_inj = injuries[(injuries["report_date"] == (date_only - timedelta(days=1))) & (injuries["TEAM_ABBR"] == team_abbr)]

        # se ancora vuoto, ritorna 0
        if day_inj.empty:
            return 0.0, 0.0

        team_top = top_scorers[top_scorers["TEAM"] == team_abbr]
        key_out, impact = 0.0, 0.0

        # match su PLAYER_NORM (es: "doncic luka")
        inj_map = day_inj.set_index("PLAYER_NORM")["STATUS_NORM"].to_dict()

        for _, p in team_top.iterrows():
            pname = p["PLAYER_NORM"]
            if pname in inj_map:
                status = inj_map[pname]
                w = STATUS_WEIGHTS.get(status, 0.0)
                key_out += w
                if pd.notna(p["PPG"]):
                    impact += float(p["PPG"]) * w

        return key_out, impact

    # Applica a tutte le partite
    games["KEY_PLAYERS_OUT_HOME"] = 0.0
    games["KEY_PLAYERS_OUT_AWAY"] = 0.0
    games["IMPACT_HOME"] = 0.0
    games["IMPACT_AWAY"] = 0.0

    for idx, row in games.iterrows():
        home, away, gdate = row["HOME_TEAM"], row["AWAY_TEAM"], row["GAME_DATE"]
        kh, ih = compute_injury_impact(gdate, home)
        ka, ia = compute_injury_impact(gdate, away)
        games.at[idx, "KEY_PLAYERS_OUT_HOME"] = kh
        games.at[idx, "KEY_PLAYERS_OUT_AWAY"] = ka
        games.at[idx, "IMPACT_HOME"] = ih
        games.at[idx, "IMPACT_AWAY"] = ia

    # (opzionale) feature derivate utili al modello
    games["KEY_OUT_DIFF"] = games["KEY_PLAYERS_OUT_HOME"] - games["KEY_PLAYERS_OUT_AWAY"]
    games["IMPACT_DIFF"] = games["IMPACT_HOME"] - games["IMPACT_AWAY"]
    games["ANY_KEY_OUT_HOME"] = (games["KEY_PLAYERS_OUT_HOME"] > 0).astype(int)
    games["ANY_KEY_OUT_AWAY"] = (games["KEY_PLAYERS_OUT_AWAY"] > 0).astype(int)

    # Salva dataset aggiornato
    games.to_csv(output_path, index=False)
    print(f"✅ Dataset aggiornato con infortuni salvato in {output_path}")
    print("⚖️ Pesi usati:", STATUS_WEIGHTS)
    print(f"🏀 Top scorer considerati: {TOP_N_SCORERS}")

    # aggiorna anche il master dataset
    games.to_csv(dataset_path, index=False)
    print(f"📌 Master dataset aggiornato in {dataset_path}")

    return games


if __name__ == "__main__":
    add_injuries()