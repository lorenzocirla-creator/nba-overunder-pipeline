#!/usr/bin/env python3
"""
Recupero delle linee di mercato storiche da ESPN.

Perché è possibile
------------------
Le linee storiche non si trovano gratuitamente presso i fornitori di quote, ma
l'API interna di ESPN le conserva per ogni partita giocata:

    sports.core.api.espn.com/.../events/{id}/competitions/{id}/odds

Restituisce più operatori. Vanno usati solo quelli **pre-partita**: i provider
con «Live Odds» nel nome riportano la quota mentre la partita è in corso, che
incorpora il punteggio già segnato e non ha alcun valore predittivo.

Con questo si passa da zero a oltre mille partite con linea e risultato: la
differenza fra non poter valutare nulla e poter finalmente misurare se un
metodo batte il mercato.

Uso:
    python linee_storiche_espn.py                  recupera la stagione scorsa
    python linee_storiche_espn.py --da 2025-10-21 --a 2026-04-15
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
ODDS = ("https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/"
        "events/{eid}/competitions/{eid}/odds")
HEADERS = {"User-Agent": "Mozilla/5.0"}

USCITA = DATA_DIR / "linee_storiche_espn.csv"

# Gli operatori che quotano a partita in corso vanno esclusi: la loro linea
# riflette il punteggio già maturato.
DA_ESCLUDERE = ("live",)

PAUSA = 0.12   # cortesia verso l'API


def _get(url: str, tentativi: int = 3):
    for numero in range(tentativi):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 429):
                time.sleep(2 * (numero + 1))
        except requests.RequestException:
            time.sleep(1 + numero)
    return None


def linee_di_una_partita(evento_id: str) -> dict | None:
    """Linea pre-partita di una singola partita, mediata fra gli operatori."""
    dati = _get(ODDS.format(eid=evento_id))
    if not dati:
        return None

    linee, operatori = [], []
    for voce in dati.get("items", []):
        nome = (voce.get("provider", {}) or {}).get("name", "")
        if any(parola in nome.lower() for parola in DA_ESCLUDERE):
            continue
        totale = voce.get("overUnder")
        if totale is None:
            continue
        linee.append(float(totale))
        operatori.append(nome)

    if not linee:
        return None

    return {
        "LINEA": float(pd.Series(linee).median()),
        "LINEA_MIN": min(linee),
        "LINEA_MAX": max(linee),
        "N_OPERATORI": len(linee),
        "OPERATORI": " | ".join(operatori),
    }


def recupera(da: date, a: date, verbose: bool = True) -> pd.DataFrame:
    """Scorre le giornate e raccoglie linea e risultato di ogni partita."""
    righe = []
    giorno = da
    totale_giorni = (a - da).days + 1
    elaborati = 0

    while giorno <= a:
        elaborati += 1
        giornata = _get(SCOREBOARD.format(ymd=giorno.strftime("%Y%m%d")))
        eventi = (giornata or {}).get("events", [])

        for evento in eventi:
            competizione = (evento.get("competitions") or [{}])[0]
            stato = (competizione.get("status") or {}).get("type") or {}
            if not stato.get("completed"):
                continue

            squadre = competizione.get("competitors") or []
            if len(squadre) != 2:
                continue

            casa = next((s for s in squadre if s.get("homeAway") == "home"), None)
            ospite = next((s for s in squadre if s.get("homeAway") == "away"), None)
            if not (casa and ospite):
                continue

            try:
                punti_casa = int(casa.get("score"))
                punti_ospite = int(ospite.get("score"))
            except (TypeError, ValueError):
                continue

            quote = linee_di_una_partita(evento["id"])
            time.sleep(PAUSA)
            if not quote:
                continue

            righe.append({
                "GAME_DATE": giorno.isoformat(),
                "HOME_TEAM": to_abbr((casa.get("team") or {}).get("abbreviation")),
                "AWAY_TEAM": to_abbr((ospite.get("team") or {}).get("abbreviation")),
                "PTS_HOME": punti_casa,
                "PTS_AWAY": punti_ospite,
                "TOTAL_POINTS": punti_casa + punti_ospite,
                **quote,
            })

        if verbose and elaborati % 15 == 0:
            print(f"   {giorno} · {elaborati}/{totale_giorni} giornate · "
                  f"{len(righe)} partite con linea")

        giorno += timedelta(days=1)
        time.sleep(PAUSA)

    df = pd.DataFrame(righe)
    return df.dropna(subset=["HOME_TEAM", "AWAY_TEAM"]) if not df.empty else df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--da", default="2025-10-21")
    ap.add_argument("--a", default="2026-04-15")
    ap.add_argument("--uscita", type=Path, default=USCITA)
    argomenti = ap.parse_args()

    da = datetime.strptime(argomenti.da, "%Y-%m-%d").date()
    a = datetime.strptime(argomenti.a, "%Y-%m-%d").date()

    print(f"📥 Recupero le linee da {da} a {a}…")
    df = recupera(da, a)

    if df.empty:
        print("❌ Nessuna linea recuperata.")
        return

    argomenti.uscita.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(argomenti.uscita, index=False)

    errore = df["TOTAL_POINTS"] - df["LINEA"]
    print(f"\n✅ {len(df)} partite con linea e risultato → {argomenti.uscita}")
    print(f"   errore medio del mercato: {errore.abs().mean():.2f} punti")
    print(f"   distorsione: {errore.mean():+.2f} (positivo = si segna più della linea)")
    print(f"   esiti over: {(errore > 0).mean()*100:.1f}%")
    print(f"   operatori per partita: {df['N_OPERATORI'].mean():.1f} in media")


if __name__ == "__main__":
    main()
