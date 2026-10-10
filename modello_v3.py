#!/usr/bin/env python3
"""
Modello del totale in due stadi: ritmo di gioco ed efficienza.

L'idea
------
Il punteggio di una partita è il prodotto di due grandezze con comportamenti
molto diversi:

    punti totali = possessi × (efficienza casa + efficienza ospiti) / 100

Il ritmo di gioco è stabile: una squadra che corre lo fa quasi ogni sera.
L'efficienza oscilla molto di più, perché dipende da quanto entrano i tiri.
Stimare le due componenti separatamente concentra la previsione dove il segnale
è forte, invece di mediare tutto in un unico numero rumoroso.

È anche il modo in cui ragiona chi fa le quote: parlano di «pace» e «rating»,
non di punti.

Uso:
    python modello_v3.py              confronta i metodi disponibili
    python modello_v3.py --mercato    confronta anche con la linea del bookmaker
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from config_season import DATA_DIR

POSSESSI = DATA_DIR / "possessi_espn.csv"
LINEE = DATA_DIR / "linee_storiche_espn.csv"

FINESTRE = (5, 10)
PRIOR_PARTITE = 6      # quante partite "virtuali" di media di lega pesano all'inizio


# =====================================================================
# Costruzione delle medie mobili
# =====================================================================

def carica_partite() -> pd.DataFrame:
    if not POSSESSI.exists():
        return pd.DataFrame()
    df = pd.read_csv(POSSESSI, parse_dates=["GAME_DATE"])
    return df.sort_values("GAME_DATE").reset_index(drop=True)


def frame_per_squadra(df: pd.DataFrame) -> pd.DataFrame:
    """Una riga per squadra per partita: ritmo, attacco e difesa."""
    casa = pd.DataFrame({
        "GAME_DATE": df.GAME_DATE, "RIGA": df.index, "TEAM": df.HOME_TEAM,
        "IN_CASA": 1, "POSSESSI": df.POSSESSI,
        "ATTACCO": df.EFF_HOME, "DIFESA": df.EFF_AWAY,
    })
    ospiti = pd.DataFrame({
        "GAME_DATE": df.GAME_DATE, "RIGA": df.index, "TEAM": df.AWAY_TEAM,
        "IN_CASA": 0, "POSSESSI": df.POSSESSI,
        "ATTACCO": df.EFF_AWAY, "DIFESA": df.EFF_HOME,
    })
    lungo = pd.concat([casa, ospiti], ignore_index=True)
    return lungo.sort_values(["TEAM", "GAME_DATE", "RIGA"]).reset_index(drop=True)


def medie_precedenti(lungo: pd.DataFrame) -> pd.DataFrame:
    """
    Medie mobili calcolate solo sulle partite già giocate.

    Lo shift di una posizione è essenziale: includere la partita in corso
    significherebbe usare il risultato per prevederlo.
    """
    gruppi = lungo.groupby("TEAM", sort=False)
    for finestra in FINESTRE:
        for colonna in ("POSSESSI", "ATTACCO", "DIFESA"):
            lungo[f"{colonna}_{finestra}"] = gruppi[colonna].transform(
                lambda s: s.shift(1).rolling(finestra, min_periods=2).mean())
    for colonna in ("POSSESSI", "ATTACCO", "DIFESA"):
        lungo[f"{colonna}_STAGIONE"] = gruppi[colonna].transform(
            lambda s: s.shift(1).expanding().mean())
    lungo["PARTITE_GIOCATE"] = gruppi.cumcount()
    return lungo


def stringi_verso_la_lega(valore: pd.Series, partite: pd.Series,
                          media_lega: float, prior: float = PRIOR_PARTITE) -> pd.Series:
    """
    Avvicina la media di una squadra a quella di lega quando le partite sono poche.

    Con tre partite giocate la media di una squadra è quasi solo rumore: pesarla
    come se fossero trenta è il modo più rapido per costruire un modello che
    sembra funzionare sul passato e fallisce sul futuro.
    """
    peso = partite / (partite + prior)
    return peso * valore.fillna(media_lega) + (1 - peso) * media_lega


def costruisci_dati() -> pd.DataFrame:
    """Dataset con le stime di ritmo ed efficienza note prima di ogni partita."""
    df = carica_partite()
    if df.empty:
        return df

    lungo = medie_precedenti(frame_per_squadra(df))

    media_possessi = df.POSSESSI.mean()
    media_efficienza = pd.concat([df.EFF_HOME, df.EFF_AWAY]).mean()

    for finestra in list(FINESTRE) + ["STAGIONE"]:
        lungo[f"POSSESSI_{finestra}_L"] = stringi_verso_la_lega(
            lungo[f"POSSESSI_{finestra}"], lungo.PARTITE_GIOCATE, media_possessi)
        lungo[f"ATTACCO_{finestra}_L"] = stringi_verso_la_lega(
            lungo[f"ATTACCO_{finestra}"], lungo.PARTITE_GIOCATE, media_efficienza)
        lungo[f"DIFESA_{finestra}_L"] = stringi_verso_la_lega(
            lungo[f"DIFESA_{finestra}"], lungo.PARTITE_GIOCATE, media_efficienza)

    casa = lungo[lungo.IN_CASA == 1].set_index("RIGA")
    ospiti = lungo[lungo.IN_CASA == 0].set_index("RIGA")

    dati = df.copy()
    for finestra in list(FINESTRE) + ["STAGIONE"]:
        for colonna in ("POSSESSI", "ATTACCO", "DIFESA"):
            dati[f"{colonna}_{finestra}_CASA"] = casa[f"{colonna}_{finestra}_L"]
            dati[f"{colonna}_{finestra}_OSPITI"] = ospiti[f"{colonna}_{finestra}_L"]

    dati["MEDIA_POSSESSI_LEGA"] = media_possessi
    dati["MEDIA_EFFICIENZA_LEGA"] = media_efficienza
    return dati


# =====================================================================
# Le stime
# =====================================================================

def stima_due_stadi(dati: pd.DataFrame, finestra=10) -> pd.Series:
    """
    Ritmo atteso moltiplicato per efficienza attesa.

    Il ritmo di una partita è ben approssimato dalla media dei ritmi delle due
    squadre. L'efficienza attesa di ciascuna nasce dall'incontro fra il suo
    attacco e la difesa avversaria.
    """
    ritmo = (dati[f"POSSESSI_{finestra}_CASA"] + dati[f"POSSESSI_{finestra}_OSPITI"]) / 2

    eff_casa = (dati[f"ATTACCO_{finestra}_CASA"] + dati[f"DIFESA_{finestra}_OSPITI"]) / 2
    eff_ospiti = (dati[f"ATTACCO_{finestra}_OSPITI"] + dati[f"DIFESA_{finestra}_CASA"]) / 2

    return ritmo * (eff_casa + eff_ospiti) / 100


def stima_diretta_sui_punti(dati: pd.DataFrame, finestra=10) -> pd.Series:
    """Metodo precedente: media dei punti, senza distinguere ritmo ed efficienza."""
    punti_casa = (dati[f"ATTACCO_{finestra}_CASA"] *
                  dati[f"POSSESSI_{finestra}_CASA"] / 100)
    punti_ospiti = (dati[f"ATTACCO_{finestra}_OSPITI"] *
                    dati[f"POSSESSI_{finestra}_OSPITI"] / 100)
    difesa_casa = (dati[f"DIFESA_{finestra}_CASA"] *
                   dati[f"POSSESSI_{finestra}_CASA"] / 100)
    difesa_ospiti = (dati[f"DIFESA_{finestra}_OSPITI"] *
                     dati[f"POSSESSI_{finestra}_OSPITI"] / 100)
    return (punti_casa + difesa_ospiti) / 2 + (punti_ospiti + difesa_casa) / 2


def media_di_lega(dati: pd.DataFrame) -> pd.Series:
    """Termine di paragone minimo: prevedere sempre la media."""
    cumulata = dati.TOTAL_POINTS.expanding().mean().shift(1)
    return cumulata.fillna(dati.TOTAL_POINTS.mean())


# =====================================================================
# Confronto
# =====================================================================

def valuta(dati: pd.DataFrame, con_mercato: bool = False) -> pd.DataFrame:
    """Errore medio di ogni metodo sulle stesse partite."""
    valide = dati.dropna(subset=[f"POSSESSI_{FINESTRE[-1]}_CASA"]).copy()
    if valide.empty:
        return pd.DataFrame()

    reale = valide.TOTAL_POINTS
    metodi = {
        "media di lega": media_di_lega(valide),
        "media dei punti (metodo precedente)": stima_diretta_sui_punti(valide),
        "ritmo × efficienza (5 partite)": stima_due_stadi(valide, 5),
        "ritmo × efficienza (10 partite)": stima_due_stadi(valide, 10),
        "ritmo × efficienza (stagione)": stima_due_stadi(valide, "STAGIONE"),
    }
    metodi["ritmo × efficienza (5 e 10 mediate)"] = (
        metodi["ritmo × efficienza (5 partite)"] +
        metodi["ritmo × efficienza (10 partite)"]) / 2

    if con_mercato and "LINEA" in valide.columns:
        metodi["linea del bookmaker"] = valide.LINEA

    righe = []
    for nome, stima in metodi.items():
        buone = stima.notna() & reale.notna()
        if buone.sum() < 50:
            continue
        errore = (reale[buone] - stima[buone]).abs()
        righe.append({
            "metodo": nome,
            "partite": int(buone.sum()),
            "errore_medio": errore.mean(),
            "errore_mediano": errore.median(),
            "distorsione": (reale[buone] - stima[buone]).mean(),
        })

    return pd.DataFrame(righe).sort_values("errore_medio").reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mercato", action="store_true",
                    help="confronta anche con la linea del bookmaker")
    argomenti = ap.parse_args()

    dati = costruisci_dati()
    if dati.empty:
        print("❌ Mancano i dati sui possessi.")
        print(f"   Attesi in {POSSESSI}")
        print("   Lancia prima: python possessi_espn.py")
        return

    print(f"\n📊 {len(dati)} partite con ritmo ed efficienza")
    print(f"   possessi per partita: {dati.POSSESSI.mean():.1f} "
          f"± {dati.POSSESSI.std():.1f}")
    print(f"   punti per partita:    {dati.TOTAL_POINTS.mean():.1f} "
          f"± {dati.TOTAL_POINTS.std():.1f}")

    if argomenti.mercato and LINEE.exists():
        linee = pd.read_csv(LINEE, parse_dates=["GAME_DATE"])
        dati = dati.merge(linee[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "LINEA"]],
                          on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], how="left")
        print(f"   partite con linea di mercato: {dati.LINEA.notna().sum()}")

    risultati = valuta(dati, con_mercato=argomenti.mercato)
    if risultati.empty:
        print("\n⛔ Partite insufficienti per il confronto.")
        return

    print(f"\n{'metodo':40} {'partite':>8} {'errore':>8} {'mediano':>8}")
    print("-" * 68)
    for _, riga in risultati.iterrows():
        print(f"{riga['metodo']:40} {riga['partite']:>8} "
              f"{riga['errore_medio']:>8.2f} {riga['errore_mediano']:>8.2f}")

    migliore = risultati.iloc[0]
    print(f"\nMigliore: {migliore['metodo']} con {migliore['errore_medio']:.2f} punti "
          f"di errore medio")


if __name__ == "__main__":
    main()
