"""
Mappa unica dei nomi squadra NBA → sigla a 3 lettere.

Serve perché le fonti usano formati diversi: The Odds API restituisce il nome
esteso ("Los Angeles Lakers"), ESPN sigle non standard ("GS", "NY", "UTAH"),
i master NBA le sigle ufficiali. Senza normalizzazione i merge falliscono in
silenzio, lasciando colonne vuote (è quello che succedeva alle quote: scaricate
ma mai agganciate al dataset).
"""

from __future__ import annotations

FULL_TO_ABBR = {
    "ATLANTA HAWKS": "ATL", "BOSTON CELTICS": "BOS", "BROOKLYN NETS": "BKN",
    "CHARLOTTE HORNETS": "CHA", "CHICAGO BULLS": "CHI", "CLEVELAND CAVALIERS": "CLE",
    "DALLAS MAVERICKS": "DAL", "DENVER NUGGETS": "DEN", "DETROIT PISTONS": "DET",
    "GOLDEN STATE WARRIORS": "GSW", "HOUSTON ROCKETS": "HOU", "INDIANA PACERS": "IND",
    "LOS ANGELES CLIPPERS": "LAC", "LA CLIPPERS": "LAC", "LOS ANGELES LAKERS": "LAL",
    "LA LAKERS": "LAL", "MEMPHIS GRIZZLIES": "MEM", "MIAMI HEAT": "MIA",
    "MILWAUKEE BUCKS": "MIL", "MINNESOTA TIMBERWOLVES": "MIN",
    "NEW ORLEANS PELICANS": "NOP", "NEW YORK KNICKS": "NYK",
    "OKLAHOMA CITY THUNDER": "OKC", "ORLANDO MAGIC": "ORL",
    "PHILADELPHIA 76ERS": "PHI", "PHOENIX SUNS": "PHX",
    "PORTLAND TRAIL BLAZERS": "POR", "SACRAMENTO KINGS": "SAC",
    "SAN ANTONIO SPURS": "SAS", "TORONTO RAPTORS": "TOR",
    "UTAH JAZZ": "UTA", "WASHINGTON WIZARDS": "WAS",
}

# Sigle non standard usate da ESPN e altre fonti
ABBR_FIX = {
    "GS": "GSW", "NY": "NYK", "SA": "SAS", "NO": "NOP",
    "UTAH": "UTA", "WSH": "WAS", "PHO": "PHX", "BRK": "BKN", "CHO": "CHA",
}

VALID_ABBRS = set(FULL_TO_ABBR.values())


def to_abbr(x) -> str | None:
    """Normalizza un nome o una sigla squadra. Ritorna None se non riconosciuto."""
    if x is None:
        return None
    s = str(x).strip().upper()
    if not s or s in {"NAN", "NONE", "TBD"}:
        return None
    s = ABBR_FIX.get(s, s)
    if s in VALID_ABBRS:
        return s
    return FULL_TO_ABBR.get(s)


def normalize_columns(df, cols):
    """Applica to_abbr alle colonne indicate di un DataFrame (in place sulla copia)."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(to_abbr)
    return df
