#!/usr/bin/env python3
"""
Raccolta e archiviazione delle linee di mercato.

Perché serve un archivio proprio
--------------------------------
Per capire se un modello batte il bookmaker non basta sapere quanti punti si
sono segnati: serve sapere **quale linea offriva il mercato** in quel momento.
Le linee storiche non sono disponibili gratuitamente, quindi vanno accumulate
giorno per giorno a partire da adesso.

Per ogni partita si registrano tre momenti:
  - apertura   la prima linea vista (spesso la meno efficiente)
  - corrente   ogni rilevazione intermedia
  - chiusura   l'ultima prima della palla a due: è quella che conta

La differenza fra apertura e chiusura dice dove si è mosso il mercato, e
battere sistematicamente la linea di chiusura (closing line value) è il
segnale più affidabile che un metodo abbia davvero un vantaggio — molto prima
che lo dicano i soldi vinti o persi.

Uso:
    python mercato.py            registra le linee di oggi
    python mercato.py --storico  mostra cosa c'è in archivio
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from config_season import DATA_DIR, SEASON_TAG
from team_names import to_abbr

ARCHIVIO = DATA_DIR / f"linee_mercato_{SEASON_TAG}.csv"
CONFIG = DATA_DIR / "odds_config.json"

API = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
SPORT_EVENTI = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"

COLONNE = [
    "GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "RILEVATO_IL", "ORE_AL_MATCH",
    "LINEA", "QUOTA_OVER", "QUOTA_UNDER", "BOOKMAKER", "N_BOOKMAKER",
]


def _chiave() -> str:
    import os
    chiave = os.environ.get("ODDS_API_KEY")
    if chiave:
        return chiave
    if CONFIG.exists():
        try:
            return json.loads(CONFIG.read_text(encoding="utf-8")).get("api_key", "")
        except Exception:
            return ""
    return ""


def scarica_linee_correnti(verbose: bool = True) -> pd.DataFrame:
    """
    Linee over/under attualmente offerte, mediate fra i bookmaker.

    La mediana fra più operatori è più stabile della quotazione di uno solo:
    un singolo bookmaker può avere una linea fuori mercato per pochi minuti.
    """
    chiave = _chiave()
    if not chiave:
        print("❌ Chiave Odds API non configurata (dati/odds_config.json).")
        return pd.DataFrame(columns=COLONNE)

    try:
        r = requests.get(API, params={
            "apiKey": chiave, "regions": "us,eu", "markets": "totals",
            "oddsFormat": "decimal", "dateFormat": "iso",
        }, timeout=30)
        r.raise_for_status()
        partite = r.json()
    except requests.RequestException as e:
        print(f"❌ Errore nello scarico delle quote: {e}")
        return pd.DataFrame(columns=COLONNE)

    rimaste = r.headers.get("x-requests-remaining")
    if verbose and rimaste:
        print(f"   richieste API rimaste questo mese: {rimaste}")

    adesso = datetime.now(timezone.utc)
    righe = []

    for partita in partite:
        casa = to_abbr(partita.get("home_team"))
        ospite = to_abbr(partita.get("away_team"))
        if not (casa and ospite):
            continue

        inizio = datetime.fromisoformat(partita["commence_time"].replace("Z", "+00:00"))
        ore_al_match = (inizio - adesso).total_seconds() / 3600

        linee, over, under, nomi = [], [], [], []
        for book in partita.get("bookmakers", []):
            for mercato in book.get("markets", []):
                if mercato.get("key") != "totals":
                    continue
                esiti = {e["name"].lower(): e for e in mercato.get("outcomes", [])}
                if "over" not in esiti:
                    continue
                linee.append(esiti["over"].get("point"))
                over.append(esiti["over"].get("price"))
                if "under" in esiti:
                    under.append(esiti["under"].get("price"))
                nomi.append(book.get("title", ""))

        linee = [x for x in linee if x is not None]
        if not linee:
            continue

        righe.append({
            "GAME_DATE": inizio.astimezone().date().isoformat(),
            "HOME_TEAM": casa,
            "AWAY_TEAM": ospite,
            "RILEVATO_IL": adesso.isoformat(timespec="seconds"),
            "ORE_AL_MATCH": round(ore_al_match, 1),
            "LINEA": float(pd.Series(linee).median()),
            "QUOTA_OVER": float(pd.Series(over).median()) if over else None,
            "QUOTA_UNDER": float(pd.Series(under).median()) if under else None,
            "BOOKMAKER": "mediana",
            "N_BOOKMAKER": len(linee),
        })

    df = pd.DataFrame(righe, columns=COLONNE)
    if verbose:
        print(f"   {len(df)} partite quotate")
    return df


def registra(verbose: bool = True) -> int:
    """
    Aggiunge le linee di adesso all'archivio.

    Ogni rilevazione è una riga a sé: serve a ricostruire come si è mossa la
    linea nelle ore precedenti alla partita.
    """
    nuove = scarica_linee_correnti(verbose=verbose)
    if nuove.empty:
        return 0

    if ARCHIVIO.exists():
        try:
            vecchie = pd.read_csv(ARCHIVIO)
            tutte = pd.concat([vecchie, nuove], ignore_index=True)
        except Exception:
            tutte = nuove
    else:
        tutte = nuove

    # Una sola rilevazione per partita ogni ora: evita di gonfiare l'archivio
    tutte["_ora"] = pd.to_datetime(tutte["RILEVATO_IL"], errors="coerce").dt.floor("h")
    tutte = tutte.drop_duplicates(
        subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "_ora"], keep="last")
    tutte = tutte.drop(columns=["_ora"]).sort_values(["GAME_DATE", "RILEVATO_IL"])

    ARCHIVIO.parent.mkdir(parents=True, exist_ok=True)
    tutte.to_csv(ARCHIVIO, index=False)

    if verbose:
        print(f"   archivio: {len(tutte)} rilevazioni su "
              f"{tutte.groupby(['GAME_DATE','HOME_TEAM','AWAY_TEAM']).ngroups} partite")
    return len(nuove)


def linee_per_partita(ore_massime_prima: float = 3.0) -> pd.DataFrame:
    """
    Apertura, chiusura e movimento per ogni partita.

    Come chiusura si prende l'ultima rilevazione entro `ore_massime_prima` ore
    dalla palla a due: più vicina all'inizio, più la linea è quella definitiva.
    """
    if not ARCHIVIO.exists():
        return pd.DataFrame()

    df = pd.read_csv(ARCHIVIO)
    if df.empty:
        return pd.DataFrame()

    df["RILEVATO_IL"] = pd.to_datetime(df["RILEVATO_IL"], errors="coerce")
    df = df.sort_values("RILEVATO_IL")

    gruppi = df.groupby(["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], as_index=False)
    righe = []
    for (data, casa, ospite), g in gruppi:
        apertura = g.iloc[0]
        vicine = g[g["ORE_AL_MATCH"] <= ore_massime_prima]
        chiusura = vicine.iloc[-1] if not vicine.empty else g.iloc[-1]

        righe.append({
            "GAME_DATE": data, "HOME_TEAM": casa, "AWAY_TEAM": ospite,
            "LINEA_APERTURA": apertura["LINEA"],
            "LINEA_CHIUSURA": chiusura["LINEA"],
            "MOVIMENTO": chiusura["LINEA"] - apertura["LINEA"],
            "QUOTA_OVER": chiusura["QUOTA_OVER"],
            "QUOTA_UNDER": chiusura["QUOTA_UNDER"],
            "ORE_DALLA_CHIUSURA": chiusura["ORE_AL_MATCH"],
            "N_RILEVAZIONI": len(g),
        })

    return pd.DataFrame(righe)


def riepilogo_archivio() -> None:
    linee = linee_per_partita()
    if linee.empty:
        print("Archivio vuoto: nessuna linea registrata.")
        print(f"Atteso in {ARCHIVIO}")
        return

    print(f"\n📊 Archivio delle linee — {ARCHIVIO.name}")
    print(f"   partite con linea: {len(linee)}")
    print(f"   periodo: {linee.GAME_DATE.min()} → {linee.GAME_DATE.max()}")
    print(f"   linea media: {linee.LINEA_CHIUSURA.mean():.1f}")
    print(f"   movimento medio apertura→chiusura: "
          f"{linee.MOVIMENTO.abs().mean():.2f} punti")
    vicine = (linee.ORE_DALLA_CHIUSURA <= 3).sum()
    print(f"   chiusure rilevate a meno di 3 ore dal via: {vicine}/{len(linee)}")

    if len(linee) < 200:
        print(f"\n   ⚠️ Servono almeno 300-400 partite per valutare un metodo "
              f"con un minimo di affidabilità.")
        print(f"      Con ~11 partite al giorno, siamo a circa "
              f"{max(0, (300-len(linee))//11)} giorni di raccolta.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--storico", action="store_true",
                    help="mostra cosa c'è in archivio senza scaricare")
    args = ap.parse_args()

    if args.storico:
        riepilogo_archivio()
        return

    print("📥 Registro le linee di mercato…")
    registra()
    riepilogo_archivio()


if __name__ == "__main__":
    main()
