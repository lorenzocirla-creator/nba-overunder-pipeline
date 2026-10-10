#!/usr/bin/env python3
"""
Messaggio di consuntivo: come sono andate le previsioni di ieri.

Arriva dopo che le partite sono state giocate e confronta quanto avevamo
previsto con quanto è successo davvero. Oltre alla giornata mostra il
cumulativo di stagione, perché una singola serata dice poco: con un errore
tipico di quindici punti, una giornata fortunata e una disastrosa sono
entrambe normali.

Dove possibile confronta anche con la linea del bookmaker: sapere di aver
sbagliato di dodici punti conta poco; sapere di aver sbagliato meno o più del
mercato conta molto.

Uso:
    python consuntivo_telegram.py               partite di ieri
    python consuntivo_telegram.py --data 2026-10-21
    python consuntivo_telegram.py --dry-run
"""

from __future__ import annotations

import argparse
import glob
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from config_season import PRED_DIR, historical_datasets, path_dataset_regular
from team_names import to_abbr
import telegram

# Quanto vicino deve essere il pronostico per dirsi "centrato".
# Su un totale che oscilla di venti punti, un punto di tolleranza è severissimo:
# si riportano più soglie per dare un quadro leggibile.
TOLLERANZE = (1, 3, 5, 10)

ARCHIVIO_STAGIONE = PRED_DIR / "consuntivo_stagione.csv"


# =====================================================================
# Dati
# =====================================================================

def carica_previsioni(giorno: date) -> pd.DataFrame:
    percorso = PRED_DIR / f"predictions_today_{giorno.strftime('%Y%m%d')}.csv"
    if not percorso.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(percorso)
    except Exception:
        return pd.DataFrame()
    if df.empty or "PREDICTED_POINTS" not in df.columns:
        return pd.DataFrame()

    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    for colonna in ("HOME_TEAM", "AWAY_TEAM"):
        df[colonna] = df[colonna].map(to_abbr)
    df["PREDICTED_POINTS"] = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
    return df.dropna(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"])


def carica_risultati() -> pd.DataFrame:
    frames = []
    for percorso in [path_dataset_regular(), *historical_datasets()]:
        if not percorso.exists():
            continue
        try:
            df = pd.read_csv(percorso, parse_dates=["GAME_DATE"], low_memory=False)
        except Exception:
            continue
        colonne = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS"]
        if not all(c in df.columns for c in colonne):
            continue
        d = df[colonne].copy()
        d["TOTAL_POINTS"] = pd.to_numeric(d["TOTAL_POINTS"], errors="coerce")
        frames.append(d.dropna(subset=["TOTAL_POINTS"]))

    if not frames:
        return pd.DataFrame()

    risultati = pd.concat(frames, ignore_index=True)
    for colonna in ("HOME_TEAM", "AWAY_TEAM"):
        risultati[colonna] = risultati[colonna].map(to_abbr)
    return risultati.drop_duplicates(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"],
                                     keep="last")


def confronta(giorno: date) -> pd.DataFrame:
    """Previsioni del giorno affiancate al risultato reale."""
    previsioni = carica_previsioni(giorno)
    if previsioni.empty:
        return pd.DataFrame()

    risultati = carica_risultati()
    if risultati.empty:
        return pd.DataFrame()

    unite = previsioni.merge(risultati, on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"],
                             how="inner")
    if unite.empty:
        return unite

    unite["ERRORE"] = unite["TOTAL_POINTS"] - unite["PREDICTED_POINTS"]
    unite["ERRORE_ASS"] = unite["ERRORE"].abs()

    if "MARKET_LINE" in unite.columns:
        unite["MARKET_LINE"] = pd.to_numeric(unite["MARKET_LINE"], errors="coerce")
        unite["ERRORE_MERCATO"] = (unite["TOTAL_POINTS"] - unite["MARKET_LINE"]).abs()
        # Direzione azzeccata: over previsto e over uscito, o viceversa
        previsto_over = unite["PREDICTED_POINTS"] > unite["MARKET_LINE"]
        uscito_over = unite["TOTAL_POINTS"] > unite["MARKET_LINE"]
        unite["DIREZIONE_OK"] = np.where(
            unite["MARKET_LINE"].isna() | (unite["TOTAL_POINTS"] == unite["MARKET_LINE"]),
            np.nan, (previsto_over == uscito_over).astype(float))

    return unite


# =====================================================================
# Archivio di stagione
# =====================================================================

def aggiorna_archivio(giornata: pd.DataFrame) -> pd.DataFrame:
    """Accumula le giornate per poter mostrare il cumulativo stagionale."""
    colonne = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS",
               "TOTAL_POINTS", "ERRORE", "ERRORE_ASS"]
    for extra in ("MARKET_LINE", "ERRORE_MERCATO", "DIREZIONE_OK"):
        if extra in giornata.columns:
            colonne.append(extra)

    nuove = giornata[colonne].copy()

    if ARCHIVIO_STAGIONE.exists():
        try:
            vecchie = pd.read_csv(ARCHIVIO_STAGIONE, parse_dates=["GAME_DATE"])
            tutte = pd.concat([vecchie, nuove], ignore_index=True)
        except Exception:
            tutte = nuove
    else:
        tutte = nuove

    tutte = tutte.drop_duplicates(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"],
                                  keep="last").sort_values("GAME_DATE")
    ARCHIVIO_STAGIONE.parent.mkdir(parents=True, exist_ok=True)
    tutte.to_csv(ARCHIVIO_STAGIONE, index=False)
    return tutte


# =====================================================================
# Messaggio
# =====================================================================

def _riga_tolleranze(errori: pd.Series) -> list[str]:
    righe = []
    for soglia in TOLLERANZE:
        centrate = int((errori <= soglia).sum())
        quota = centrate / len(errori) * 100 if len(errori) else 0
        unita = "punto" if soglia == 1 else "punti"
        righe.append(f"±{soglia} {unita}: <b>{centrate}/{len(errori)}</b> ({quota:.0f}%)")
    return righe


def costruisci_messaggio(giorno: date, giornata: pd.DataFrame,
                         stagione: pd.DataFrame) -> str:
    righe = [f"📋 <b>Come è andata — {giorno.strftime('%d/%m/%Y')}</b>", ""]

    errori = giornata["ERRORE_ASS"]
    righe.append(f"{len(giornata)} partite concluse")
    righe.append(f"Errore medio: <b>{errori.mean():.1f}</b> punti "
                 f"(mediano {errori.median():.1f})")
    righe.append("")

    righe.append("🎯 <b>Quanto ci siamo andati vicino</b>")
    righe += _riga_tolleranze(errori)

    # Miglior colpo e peggiore della serata
    migliore = giornata.loc[errori.idxmin()]
    peggiore = giornata.loc[errori.idxmax()]
    righe.append("")
    righe.append(f"Meglio: {migliore.AWAY_TEAM} @ {migliore.HOME_TEAM} — "
                 f"previsti {migliore.PREDICTED_POINTS:.0f}, "
                 f"fatti {migliore.TOTAL_POINTS:.0f} "
                 f"({migliore.ERRORE:+.0f})")
    righe.append(f"Peggio: {peggiore.AWAY_TEAM} @ {peggiore.HOME_TEAM} — "
                 f"previsti {peggiore.PREDICTED_POINTS:.0f}, "
                 f"fatti {peggiore.TOTAL_POINTS:.0f} "
                 f"({peggiore.ERRORE:+.0f})")

    # Tendenza a sovrastimare o sottostimare
    distorsione = giornata["ERRORE"].mean()
    if abs(distorsione) >= 2:
        verso = "sottostimato" if distorsione > 0 else "sovrastimato"
        righe.append(f"Nel complesso abbiamo {verso} di {abs(distorsione):.1f} punti")

    # Confronto col mercato, se disponibile
    if "ERRORE_MERCATO" in giornata.columns and giornata["ERRORE_MERCATO"].notna().any():
        con_linea = giornata[giornata["ERRORE_MERCATO"].notna()]
        nostro = con_linea["ERRORE_ASS"].mean()
        loro = con_linea["ERRORE_MERCATO"].mean()
        righe.append("")
        righe.append("⚖️ <b>Contro il bookmaker</b>")
        righe.append(f"Noi {nostro:.1f} · mercato {loro:.1f} "
                     f"({'meglio noi' if nostro < loro else 'meglio il mercato'} "
                     f"di {abs(nostro-loro):.1f})")

        if "DIREZIONE_OK" in con_linea.columns:
            direzioni = con_linea["DIREZIONE_OK"].dropna()
            if len(direzioni):
                azzeccate = int(direzioni.sum())
                righe.append(f"Direzione giusta (over/under): "
                             f"<b>{azzeccate}/{len(direzioni)}</b> "
                             f"({azzeccate/len(direzioni)*100:.0f}%)")

    # Cumulativo di stagione
    if len(stagione) > len(giornata):
        righe.append("")
        righe.append(f"📊 <b>Da inizio stagione</b> — {len(stagione)} partite")
        errori_stagione = stagione["ERRORE_ASS"]
        righe.append(f"Errore medio: <b>{errori_stagione.mean():.1f}</b> punti")
        centrate = int((errori_stagione <= 5).sum())
        righe.append(f"Entro ±5 punti: {centrate}/{len(stagione)} "
                     f"({centrate/len(stagione)*100:.0f}%)")

        if "DIREZIONE_OK" in stagione.columns:
            direzioni = stagione["DIREZIONE_OK"].dropna()
            if len(direzioni) >= 20:
                quota = direzioni.mean()
                incertezza = 1.96 * np.sqrt(0.25 / len(direzioni))
                righe.append(f"Direzione giusta: <b>{quota*100:.1f}%</b> "
                             f"su {len(direzioni)} partite (±{incertezza*100:.1f})")
                # Il 52,4% è la soglia oltre la quale una scommessa andrebbe in pari
                if quota - incertezza > 0.524:
                    righe.append("→ sopra la soglia di pareggio, e oltre il margine "
                                 "di incertezza")
                elif quota + incertezza < 0.524:
                    righe.append("→ sotto la soglia di pareggio (52,4%)")
                else:
                    righe.append("→ ancora indistinguibile dal caso")

        # Ultimi sette giorni, per vedere se sta migliorando o peggiorando
        stagione_date = pd.to_datetime(stagione["GAME_DATE"])
        recenti = stagione[stagione_date >= pd.Timestamp(giorno) - pd.Timedelta(days=7)]
        if len(recenti) >= 10 and len(recenti) < len(stagione):
            righe.append(f"Ultimi 7 giorni: {recenti['ERRORE_ASS'].mean():.1f} "
                         f"punti di errore su {len(recenti)} partite")

    righe.append("")
    righe.append("<i>Un errore di 15 punti è la norma su un totale che oscilla "
                 "di 20: una singola giornata dice poco.</i>")
    return "\n".join(righe)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="YYYY-MM-DD (default: ieri)")
    ap.add_argument("--dry-run", action="store_true", help="stampa senza inviare")
    argomenti = ap.parse_args()

    if argomenti.data:
        giorno = datetime.strptime(argomenti.data, "%Y-%m-%d").date()
    else:
        giorno = date.today() - timedelta(days=1)

    giornata = confronta(giorno)
    if giornata.empty:
        print(f"ℹ️ Nessuna partita con previsione e risultato per {giorno}.")
        return

    stagione = aggiorna_archivio(giornata)
    messaggio = costruisci_messaggio(giorno, giornata, stagione)
    telegram.send_message(messaggio, dry_run=argomenti.dry_run)


if __name__ == "__main__":
    main()
