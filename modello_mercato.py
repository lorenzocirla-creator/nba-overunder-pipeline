#!/usr/bin/env python3
"""
Modello contro il mercato, non contro la realtà.

Il cambio di impostazione
-------------------------
Predire il totale punti da zero e poi confrontarlo con la linea è l'approccio
sbagliato per chi vuole battere il bookmaker. Il mercato sbaglia di circa 10
punti; un modello costruito sui soli dati storici sbaglia di 15. Confrontare
due stime di cui una è peggiore non produce nessun vantaggio.

L'impostazione corretta è opposta: si prende la linea come punto di partenza —
contiene già tutto ciò che il mercato sa — e si cerca di prevedere **l'errore
residuo** del mercato:

    obiettivo = totale realmente segnato − linea di chiusura

Se il residuo fosse impredicibile, il mercato sarebbe perfettamente efficiente e
non ci sarebbe niente da fare. L'ipotesi da verificare è che in alcune
situazioni specifiche il mercato sbagli in modo sistematico.

Come si valuta
--------------
Non con l'errore medio, che qui non dice nulla, ma con:
  - quante giocate finiscono dalla parte giusta della linea
  - il saldo tenendo conto del margine del banco
  - se il risultato è distinguibile dal caso

Quest'ultimo punto è quello che fa fallire quasi tutti: con trecento partite,
una percentuale di vincite del 55% ha un margine di incertezza di ±6 punti.
Non è sufficiente a dimostrare niente.

Uso:
    python modello_mercato.py             valuta con i dati disponibili
    python modello_mercato.py --soglia 6  giocando solo sopra 6 punti di scarto
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

import numpy as np
import pandas as pd

from config_season import historical_datasets, path_dataset_regular
from mercato import linee_per_partita
from team_names import to_abbr

# A quota 1,91 (il prezzo abituale su over/under) serve vincere il 52,4% delle
# giocate solo per andare in pari: è il margine che il banco trattiene.
QUOTA_TIPICA = 1.91
PAREGGIO = 1 / QUOTA_TIPICA

MIN_PARTITE_PER_VALUTARE = 200


# =====================================================================
# Dati
# =====================================================================

def carica_partite() -> pd.DataFrame:
    """Partite giocate, con risultato, dalla stagione corrente e dallo storico."""
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
        extra = [c for c in ["HOME_REST_DAYS", "AWAY_REST_DAYS", "HOME_B2B", "AWAY_B2B",
                             "PTS_HOME", "PTS_AWAY"] if c in df.columns]
        d = df[colonne + extra].copy()
        d["TOTAL_POINTS"] = pd.to_numeric(d["TOTAL_POINTS"], errors="coerce")
        frames.append(d.dropna(subset=["TOTAL_POINTS"]))

    if not frames:
        return pd.DataFrame()

    partite = pd.concat(frames, ignore_index=True)
    for c in ["HOME_TEAM", "AWAY_TEAM"]:
        partite[c] = partite[c].map(to_abbr)
    partite = partite.dropna(subset=["HOME_TEAM", "AWAY_TEAM"])
    return partite.drop_duplicates(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"])


def unisci_linee_e_risultati() -> pd.DataFrame:
    """
    Partite con linea di chiusura e risultato: è il dato di base di tutto.

    Senza questo incrocio non si può né allenare né valutare nulla, ed è anche
    il motivo per cui l'archivio delle linee va alimentato ogni giorno.
    """
    partite = carica_partite()
    linee = linee_per_partita()

    if partite.empty or linee.empty:
        return pd.DataFrame()

    linee["GAME_DATE"] = pd.to_datetime(linee["GAME_DATE"], errors="coerce")
    unite = partite.merge(linee, on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], how="inner")

    if unite.empty:
        return unite

    # L'errore del mercato: positivo se si è segnato più della linea
    unite["ERRORE_MERCATO"] = unite["TOTAL_POINTS"] - unite["LINEA_CHIUSURA"]
    unite["ESITO"] = np.where(unite["ERRORE_MERCATO"] > 0, "over",
                              np.where(unite["ERRORE_MERCATO"] < 0, "under", "pari"))
    return unite.sort_values("GAME_DATE").reset_index(drop=True)


# =====================================================================
# Valutazione delle giocate
# =====================================================================

@dataclass
class EsitoScommesse:
    giocate: int
    vinte: int
    perse: int
    pari: int
    saldo: float
    soglia: float

    @property
    def percentuale_vinte(self) -> float:
        decise = self.vinte + self.perse
        return self.vinte / decise if decise else float("nan")

    @property
    def rendimento(self) -> float:
        return self.saldo / self.giocate if self.giocate else float("nan")

    @property
    def margine_incertezza(self) -> float:
        """
        Quanto può oscillare la percentuale di vincite per puro caso.

        È il numero che smonta la maggior parte dei metodi: con poche giocate,
        qualunque risultato è compatibile con il caso.
        """
        decise = self.vinte + self.perse
        if decise < 2:
            return float("nan")
        return 1.96 * np.sqrt(0.25 / decise)

    @property
    def batte_il_banco(self) -> bool:
        """Vero solo se il vantaggio è oltre il margine di incertezza."""
        if np.isnan(self.percentuale_vinte) or np.isnan(self.margine_incertezza):
            return False
        return (self.percentuale_vinte - self.margine_incertezza) > PAREGGIO

    def descrizione(self) -> str:
        if self.giocate == 0:
            return f"soglia {self.soglia:.1f}: nessuna giocata"
        incertezza = self.margine_incertezza
        riga = (f"soglia {self.soglia:>4.1f} · {self.giocate:>4} giocate · "
                f"vinte {self.percentuale_vinte*100:>5.1f}% "
                f"(±{incertezza*100:.1f}) · saldo {self.saldo:>+7.1f} unità · "
                f"rendimento {self.rendimento*100:>+5.1f}%")
        if self.batte_il_banco:
            riga += "  ← oltre il caso"
        return riga


def valuta_giocate(previsioni: pd.Series, linee: pd.Series, totali: pd.Series,
                   soglia: float = 0.0, quota: float = QUOTA_TIPICA) -> EsitoScommesse:
    """
    Esito delle giocate fatte quando la previsione si discosta dalla linea.

    Si gioca over se si prevede più della linea, under se meno, e solo quando
    lo scarto supera la soglia: è il modo di misurare se la selettività paga.
    """
    scarto = previsioni - linee
    gioca = scarto.abs() >= soglia

    if not gioca.any():
        return EsitoScommesse(0, 0, 0, 0, 0.0, soglia)

    scelta_over = scarto[gioca] > 0
    reale_over = totali[gioca] > linee[gioca]
    pari = totali[gioca] == linee[gioca]

    vinte = int(((scelta_over == reale_over) & ~pari).sum())
    perse = int(((scelta_over != reale_over) & ~pari).sum())
    n_pari = int(pari.sum())

    saldo = vinte * (quota - 1) - perse
    return EsitoScommesse(int(gioca.sum()), vinte, perse, n_pari, saldo, soglia)


# =====================================================================
# Modello sul residuo
# =====================================================================

COLONNE_FEATURE = [
    "MOVIMENTO",          # di quanto si è spostata la linea dall'apertura
    "LINEA_APERTURA",
    "RIPOSO_DIFF",        # asimmetria di riposo fra le due squadre
    "B2B_TOTALI",
    "LINEA_ALTA",         # scostamento della linea dalla media di lega
]


def prepara_feature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Costruisce le poche variabili che hanno una ragione per essere provate.

    Non si aggiungono decine di colonne: con poche centinaia di partite ogni
    variabile in più aumenta la probabilità di trovare un vantaggio che non
    esiste. Queste sono scelte perché il mercato potrebbe prezzarle male:
      - il movimento della linea segnala dove sta andando il denaro informato;
      - il riposo asimmetrico incide sul ritmo di gioco;
      - le linee lontane dalla media tendono a essere meno accurate.
    """
    d = df.copy()

    riposo_casa = pd.to_numeric(d.get("HOME_REST_DAYS"), errors="coerce").fillna(2)
    riposo_ospiti = pd.to_numeric(d.get("AWAY_REST_DAYS"), errors="coerce").fillna(2)
    d["RIPOSO_DIFF"] = riposo_casa - riposo_ospiti

    b2b_casa = pd.to_numeric(d.get("HOME_B2B"), errors="coerce").fillna(0)
    b2b_ospiti = pd.to_numeric(d.get("AWAY_B2B"), errors="coerce").fillna(0)
    d["B2B_TOTALI"] = b2b_casa + b2b_ospiti

    d["LINEA_ALTA"] = d["LINEA_CHIUSURA"] - d["LINEA_CHIUSURA"].mean()
    d["MOVIMENTO"] = d["MOVIMENTO"].fillna(0)
    d["LINEA_APERTURA"] = d["LINEA_APERTURA"].fillna(d["LINEA_CHIUSURA"])

    return d


def valuta_strategie(dati: pd.DataFrame, soglie=(0, 2, 4, 6, 8)) -> pd.DataFrame:
    """
    Confronta alcune strategie semplici sulle partite disponibili.

    Le prime due servono da termine di paragone: se una strategia elaborata non
    batte «gioca sempre over», non sta aggiungendo niente.
    """
    risultati = []
    totali, linee = dati["TOTAL_POINTS"], dati["LINEA_CHIUSURA"]

    strategie = {
        "sempre over": linee + 100,
        "sempre under": linee - 100,
        "segui il movimento della linea": linee + dati["MOVIMENTO"].fillna(0) * 10,
        "contro il movimento della linea": linee - dati["MOVIMENTO"].fillna(0) * 10,
    }

    for nome, previsione in strategie.items():
        for soglia in (0,):
            esito = valuta_giocate(previsione, linee, totali, soglia)
            risultati.append({
                "strategia": nome, "soglia": soglia, "giocate": esito.giocate,
                "vinte_%": esito.percentuale_vinte * 100,
                "incertezza_%": esito.margine_incertezza * 100,
                "saldo": esito.saldo, "rendimento_%": esito.rendimento * 100,
                "oltre_il_caso": esito.batte_il_banco,
            })

    return pd.DataFrame(risultati)


# =====================================================================
# Diagnosi
# =====================================================================

def diagnosi() -> None:
    """Fotografia di cosa si può e non si può ancora valutare."""
    print("\n" + "=" * 68)
    print("  MODELLO CONTRO IL MERCATO — stato dei dati")
    print("=" * 68)

    partite = carica_partite()
    print(f"\nPartite con risultato disponibili: {len(partite)}")
    if not partite.empty:
        print(f"   periodo: {partite.GAME_DATE.min().date()} → "
              f"{partite.GAME_DATE.max().date()}")

    linee = linee_per_partita()
    print(f"Partite con linea di mercato registrata: {len(linee)}")

    dati = unisci_linee_e_risultati()
    print(f"Partite utilizzabili (linea + risultato): {len(dati)}")

    if dati.empty:
        print("\n⛔ Non è ancora possibile valutare nulla.")
        print("   Servono partite che abbiano insieme la linea di chiusura e il")
        print("   risultato finale. Le linee si stanno raccogliendo da oggi; i")
        print("   risultati arriveranno con l'inizio della stagione.")
        print(f"\n   Con circa 11 partite al giorno servono {MIN_PARTITE_PER_VALUTARE//11} "
              f"giorni di stagione per una prima valutazione,")
        print("   e non meno di una stagione intera per un giudizio attendibile.")
        return

    print(f"\n📏 Quanto sbaglia il mercato su queste partite")
    errore = dati["ERRORE_MERCATO"]
    print(f"   errore medio assoluto: {errore.abs().mean():.2f} punti")
    print(f"   distorsione (media con segno): {errore.mean():+.2f} punti")
    print(f"   esiti over: {(errore > 0).mean()*100:.1f}% · "
          f"under: {(errore < 0).mean()*100:.1f}%")

    if abs(errore.mean()) > 1.5 and len(dati) > 100:
        verso = "alto" if errore.mean() > 0 else "basso"
        print(f"   ⚠️ il mercato sembra sistematicamente {verso}: da verificare "
              f"su più partite")

    print(f"\n🎲 Strategie di riferimento")
    for _, riga in valuta_strategie(dati).iterrows():
        segnale = "  ← oltre il caso" if riga["oltre_il_caso"] else ""
        print(f"   {riga['strategia']:32} {riga['giocate']:>4} giocate · "
              f"{riga['vinte_%']:>5.1f}% (±{riga['incertezza_%']:.1f}) · "
              f"rendimento {riga['rendimento_%']:>+5.1f}%{segnale}")

    if len(dati) < MIN_PARTITE_PER_VALUTARE:
        print(f"\n   ⚠️ Con {len(dati)} partite nessuno di questi numeri è "
              f"significativo.")
        print(f"      Il margine di incertezza è ancora più grande di qualunque "
              f"vantaggio realistico.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--soglia", type=float, default=None,
                    help="valuta le giocate sopra questo scarto dalla linea")
    args = ap.parse_args()

    diagnosi()

    if args.soglia is not None:
        dati = unisci_linee_e_risultati()
        if dati.empty:
            return
        print(f"\nGiocate con scarto minimo di {args.soglia} punti:")
        esito = valuta_giocate(dati["LINEA_CHIUSURA"] + dati["MOVIMENTO"] * 5,
                               dati["LINEA_CHIUSURA"], dati["TOTAL_POINTS"],
                               soglia=args.soglia)
        print("   " + esito.descrizione())


if __name__ == "__main__":
    main()
