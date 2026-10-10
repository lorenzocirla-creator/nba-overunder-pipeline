"""Controllo serale (2 ore prima della prima partita): confronta le linee appena registrate da
mercato.py con quelle del mattino e avvisa su Telegram solo se qualcuna si è mossa di almeno
SOGLIA punti (infortuni dell'ultima ora, notizie). Niente messaggio se non cambia nulla."""
from __future__ import annotations

import pandas as pd

import telegram
from config_season import DATA_DIR, SEASON_TAG

SOGLIA = 3.0


def main() -> None:
    archivio = DATA_DIR / f"linee_mercato_{SEASON_TAG}.csv"
    if not archivio.exists():
        return
    d = pd.read_csv(archivio)
    d["quando"] = pd.to_datetime(d["RILEVATO_IL"], utc=True)
    oggi_usa = pd.Timestamp.now(tz="US/Eastern").strftime("%Y-%m-%d")
    mezzogiorno = pd.Timestamp.now(tz="Europe/Rome").normalize() + pd.Timedelta(hours=12)
    d = d[(d["GAME_DATE"] == oggi_usa) & (d["BOOKMAKER"] == "mediana")]
    righe = []
    for (casa, fuori), g in d.groupby(["HOME_TEAM", "AWAY_TEAM"]):
        g = g.sort_values("quando")
        mattina = g[g["quando"] < mezzogiorno]
        if mattina.empty:
            continue
        prima, ora = float(mattina["LINEA"].iloc[-1]), float(g["LINEA"].iloc[-1])
        if abs(ora - prima) >= SOGLIA:
            righe.append(f"• {fuori} @ {casa}: {prima:g} → <b>{ora:g}</b> ({ora - prima:+g})")
    if righe:
        telegram.send_message("🔔 <b>Linee mosse da stamattina</b>\n" + "\n".join(righe)
                              + "\n\nDi solito vuol dire notizie su infortuni o formazioni.")
    print(f"sera: {len(righe)} linee mosse di almeno {SOGLIA:g} punti")


if __name__ == "__main__":
    main()
