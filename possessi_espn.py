#!/usr/bin/env python3
"""
Recupero dei possessi reali dalle box score ESPN.

Perché i possessi contano più dei punti
---------------------------------------
Il totale di una partita è il prodotto di due cose diverse:

    punti = possessi × efficienza

I possessi (quanto in fretta si gioca) sono molto più stabili e prevedibili
dell'efficienza: una squadra che gioca a ritmo alto lo fa quasi ogni sera,
mentre la percentuale al tiro oscilla parecchio. Stimare separatamente le due
componenti è più accurato che stimare direttamente i punti, perché si applica
la previsione dove il segnale è forte invece di mediare tutto insieme.

I possessi non sono esposti direttamente, ma si ricavano dalla formula standard:

    possessi = tiri tentati − rimbalzi offensivi + palle perse + 0,44 × tiri liberi

Il coefficiente 0,44 stima quanti tiri liberi chiudono davvero un possesso
(i primi di due no, l'ultimo sì).

Uso:
    python possessi_espn.py --da 2025-10-21 --a 2026-04-15
"""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from config_season import DATA_DIR
from team_names import to_abbr

SCOREBOARD = ("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
              "scoreboard?dates={ymd}")
SOMMARIO = ("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
            "summary?event={eid}")
HEADERS = {"User-Agent": "Mozilla/5.0"}
USCITA = DATA_DIR / "possessi_espn.csv"
PAUSA = 0.12

COEFF_TIRI_LIBERI = 0.44


def _get(url: str, tentativi: int = 3):
    for numero in range(tentativi):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 429):
                time.sleep(2 * (numero + 1))
        except requests.RequestException:
            time.sleep(1 + numero)
    return None


def _coppia(valore: str) -> tuple[int | None, int | None]:
    """Converte «42-84» in (42, 84)."""
    try:
        fatti, tentati = valore.split("-")
        return int(fatti), int(tentati)
    except (ValueError, AttributeError):
        return None, None


def statistiche_squadra(voce: dict) -> dict | None:
    """Estrae dalla box score i dati che servono per i possessi."""
    valori = {s["name"]: s.get("displayValue") for s in voce.get("statistics", [])}

    _, tiri_tentati = _coppia(valori.get("fieldGoalsMade-fieldGoalsAttempted", ""))
    _, liberi_tentati = _coppia(valori.get("freeThrowsMade-freeThrowsAttempted", ""))

    def numero(chiave):
        try:
            return int(valori.get(chiave))
        except (TypeError, ValueError):
            return None

    rimbalzi_off = numero("offensiveRebounds")
    perse = numero("totalTurnovers") or numero("turnovers")

    if None in (tiri_tentati, liberi_tentati, rimbalzi_off, perse):
        return None

    possessi = (tiri_tentati - rimbalzi_off + perse
                + COEFF_TIRI_LIBERI * liberi_tentati)

    return {
        "FGA": tiri_tentati, "FTA": liberi_tentati,
        "OREB": rimbalzi_off, "TOV": perse,
        "POSSESSI": round(possessi, 2),
        "TIRI_3_TENTATI": _coppia(valori.get(
            "threePointFieldGoalsMade-threePointFieldGoalsAttempted", ""))[1],
        "FALLI": numero("fouls"),
    }


def recupera(da: date, a: date, verbose: bool = True) -> pd.DataFrame:
    righe = []
    giorno = da
    totale_giorni = (a - da).days + 1
    fatti = 0

    while giorno <= a:
        fatti += 1
        giornata = _get(SCOREBOARD.format(ymd=giorno.strftime("%Y%m%d")))

        for evento in (giornata or {}).get("events", []):
            competizione = (evento.get("competitions") or [{}])[0]
            if not ((competizione.get("status") or {}).get("type") or {}).get("completed"):
                continue

            sommario = _get(SOMMARIO.format(eid=evento["id"]))
            time.sleep(PAUSA)
            squadre = (sommario or {}).get("boxscore", {}).get("teams", [])
            if len(squadre) != 2:
                continue

            dati = {}
            for voce in squadre:
                sigla = to_abbr((voce.get("team") or {}).get("abbreviation"))
                statistiche = statistiche_squadra(voce)
                if sigla and statistiche:
                    dati[sigla] = statistiche

            if len(dati) != 2:
                continue

            competitori = competizione.get("competitors") or []
            casa = next((to_abbr((c.get("team") or {}).get("abbreviation"))
                         for c in competitori if c.get("homeAway") == "home"), None)
            ospite = next((to_abbr((c.get("team") or {}).get("abbreviation"))
                           for c in competitori if c.get("homeAway") == "away"), None)
            if casa not in dati or ospite not in dati:
                continue

            punti = {}
            for c in competitori:
                sigla = to_abbr((c.get("team") or {}).get("abbreviation"))
                try:
                    punti[sigla] = int(c.get("score"))
                except (TypeError, ValueError):
                    pass
            if casa not in punti or ospite not in punti:
                continue

            # I possessi di una partita sono quasi identici per le due squadre:
            # la media è la stima più stabile del ritmo di gioco.
            possessi = (dati[casa]["POSSESSI"] + dati[ospite]["POSSESSI"]) / 2
            totale = punti[casa] + punti[ospite]

            righe.append({
                "GAME_DATE": giorno.isoformat(),
                "HOME_TEAM": casa, "AWAY_TEAM": ospite,
                "PTS_HOME": punti[casa], "PTS_AWAY": punti[ospite],
                "TOTAL_POINTS": totale,
                "POSSESSI": round(possessi, 2),
                # Punti per 100 possessi: l'efficienza, ripulita dal ritmo
                "EFF_HOME": round(punti[casa] / possessi * 100, 2),
                "EFF_AWAY": round(punti[ospite] / possessi * 100, 2),
                "FGA_HOME": dati[casa]["FGA"], "FGA_AWAY": dati[ospite]["FGA"],
                "FTA_HOME": dati[casa]["FTA"], "FTA_AWAY": dati[ospite]["FTA"],
                "TOV_HOME": dati[casa]["TOV"], "TOV_AWAY": dati[ospite]["TOV"],
                "TIRI3_HOME": dati[casa]["TIRI_3_TENTATI"],
                "TIRI3_AWAY": dati[ospite]["TIRI_3_TENTATI"],
                "FALLI_HOME": dati[casa]["FALLI"], "FALLI_AWAY": dati[ospite]["FALLI"],
            })

        if verbose and fatti % 15 == 0:
            print(f"   {giorno} · {fatti}/{totale_giorni} giornate · "
                  f"{len(righe)} partite")

        giorno += timedelta(days=1)
        time.sleep(PAUSA)

    return pd.DataFrame(righe)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--da", default="2025-10-21")
    ap.add_argument("--a", default="2026-04-15")
    ap.add_argument("--uscita", type=Path, default=USCITA)
    argomenti = ap.parse_args()

    da = datetime.strptime(argomenti.da, "%Y-%m-%d").date()
    a = datetime.strptime(argomenti.a, "%Y-%m-%d").date()

    print(f"📥 Recupero le box score da {da} a {a}…")
    df = recupera(da, a)

    if df.empty:
        print("❌ Nessuna box score recuperata.")
        return

    argomenti.uscita.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(argomenti.uscita, index=False)

    print(f"\n✅ {len(df)} partite → {argomenti.uscita}")
    print(f"   possessi per partita: {df.POSSESSI.mean():.1f} "
          f"(deviazione {df.POSSESSI.std():.1f})")
    print(f"   punti totali:         {df.TOTAL_POINTS.mean():.1f} "
          f"(deviazione {df.TOTAL_POINTS.std():.1f})")
    variabilita_possessi = df.POSSESSI.std() / df.POSSESSI.mean()
    variabilita_punti = df.TOTAL_POINTS.std() / df.TOTAL_POINTS.mean()
    print(f"\n   variabilità relativa dei possessi: {variabilita_possessi*100:.1f}%")
    print(f"   variabilità relativa dei punti:    {variabilita_punti*100:.1f}%")
    print(f"   → il ritmo di gioco è {variabilita_punti/variabilita_possessi:.1f} volte "
          f"più stabile del punteggio")


if __name__ == "__main__":
    main()
