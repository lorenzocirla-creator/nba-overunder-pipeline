#!/usr/bin/env python3
"""
Report mensile: il modello batte il mercato?

Confronta, sulle partite già giocate del periodo, tre cose:
  - quanto sbaglia il modello        (|previsione - totale reale|)
  - quanto sbaglia il mercato        (|quota - totale reale|)
  - come sarebbero andate le giocate sopra una soglia di scostamento

La domanda che conta non è "il modello è preciso", ma "è più preciso della
quota del bookmaker": si scommette contro quella, non contro la realtà.
Senza quote reali il confronto non è possibile e il report lo dice.

Uso:
    python monthly_report.py                  # mese scorso
    python monthly_report.py --month 2026-11
    python monthly_report.py --dry-run
"""

from __future__ import annotations

import argparse
import glob
import re
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from config_season import PRED_DIR, historical_datasets, path_dataset_regular
from team_names import to_abbr
import telegram

ROOT = Path(__file__).resolve().parent

# Scommessa "di prova": si punta solo quando il modello si scosta dalla quota
# di almeno questa soglia. Serve a capire se un eventuale vantaggio esiste solo
# sulle differenze marcate.
EDGE_THRESHOLD = 5.0

# Quota tipica su over/under: a 1.91 servono il 52,4% di giocate vinte per pareggiare
TYPICAL_ODDS = 1.91
BREAK_EVEN = 1 / TYPICAL_ODDS  # ≈ 0.524


def load_predictions(start: date, end: date) -> pd.DataFrame:
    """Raccoglie i file di predizione giornalieri del periodo."""
    rows = []
    for f in sorted(glob.glob(str(PRED_DIR / "predictions_today_*.csv"))):
        m = re.search(r"(\d{8})", Path(f).name)
        if not m:
            continue
        day = datetime.strptime(m.group(1), "%Y%m%d").date()
        if not (start <= day <= end):
            continue
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        if df.empty or "PREDICTED_POINTS" not in df.columns:
            continue
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
        rows.append(df)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True)
    for c in ["HOME_TEAM", "AWAY_TEAM"]:
        out[c] = out[c].map(to_abbr)
    return out.dropna(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"])


def load_results() -> pd.DataFrame:
    """Risultati reali, stagione corrente più storico."""
    frames = []
    for p in [path_dataset_regular(), *historical_datasets()]:
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, parse_dates=["GAME_DATE"], low_memory=False)
        except Exception:
            continue
        cols = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS"]
        if not all(c in df.columns for c in cols):
            continue
        d = df[cols].copy()
        d["TOTAL_POINTS"] = pd.to_numeric(d["TOTAL_POINTS"], errors="coerce")
        frames.append(d.dropna(subset=["TOTAL_POINTS"]))

    if not frames:
        return pd.DataFrame(columns=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS"])

    out = pd.concat(frames, ignore_index=True)
    for c in ["HOME_TEAM", "AWAY_TEAM"]:
        out[c] = out[c].map(to_abbr)
    return out.drop_duplicates(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last")


MESI_IT = ["gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
           "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]


def month_bounds(month: str | None) -> tuple[date, date, str]:
    if month:
        period = pd.Period(month, freq="M")
    else:
        period = pd.Period(pd.Timestamp.today(), freq="M") - 1  # mese scorso
    label = f"{MESI_IT[period.month - 1]} {period.year}"
    return period.start_time.date(), period.end_time.date(), label


def build_report(start: date, end: date, label: str) -> str:
    preds = load_predictions(start, end)
    if preds.empty:
        return (f"📊 <b>Report {label}</b>\n\n"
                f"Nessuna predizione registrata nel periodo.")

    results = load_results()
    df = preds.merge(results, on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], how="left")
    df["PREDICTED_POINTS"] = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
    df["MARKET_LINE"] = pd.to_numeric(df.get("MARKET_LINE"), errors="coerce")

    played = df[df["TOTAL_POINTS"].notna()].copy()

    lines = [f"📊 <b>Report {label}</b>", ""]
    lines.append(f"Partite previste: {len(df)} — con risultato noto: {len(played)}")

    if played.empty:
        lines.append("\nNessuna partita conclusa: confronto rimandato.")
        return "\n".join(lines)

    model_err = (played["TOTAL_POINTS"] - played["PREDICTED_POINTS"]).abs()
    lines.append(f"\n🎯 <b>Precisione modello</b>")
    lines.append(f"Errore medio: <b>{model_err.mean():.1f}</b> punti")

    with_market = played[played["MARKET_LINE"].notna()].copy()

    if with_market.empty:
        lines.append("\n⚠️ <b>Confronto col mercato impossibile</b>")
        lines.append("Nessuna quota reale registrata nel periodo. Senza quote non si può "
                     "sapere se il modello ha un vantaggio: serve che lo scarico quote "
                     "giri ogni giorno.")
        return "\n".join(lines)

    market_err = (with_market["TOTAL_POINTS"] - with_market["MARKET_LINE"]).abs()
    m_err = (with_market["TOTAL_POINTS"] - with_market["PREDICTED_POINTS"]).abs()
    delta = market_err.mean() - m_err.mean()

    lines.append(f"\n⚖️ <b>Modello contro mercato</b> ({len(with_market)} partite quotate)")
    lines.append(f"Errore modello:  <b>{m_err.mean():.1f}</b>")
    lines.append(f"Errore mercato:  <b>{market_err.mean():.1f}</b>")
    if delta > 0:
        lines.append(f"→ il modello è più preciso di {delta:.1f} punti")
    else:
        lines.append(f"→ il mercato è più preciso di {abs(delta):.1f} punti")

    # Esito delle giocate che avremmo fatto sopra la soglia
    bets = with_market[(with_market["PREDICTED_POINTS"] - with_market["MARKET_LINE"]).abs()
                       >= EDGE_THRESHOLD].copy()
    lines.append(f"\n🎲 <b>Giocate simulate</b> (scarto ≥ {EDGE_THRESHOLD:.0f} punti)")

    if bets.empty:
        lines.append("Nessuna giocata avrebbe superato la soglia.")
    else:
        bets["SCELTA"] = np.where(
            bets["PREDICTED_POINTS"] > bets["MARKET_LINE"], "OVER", "UNDER")
        vinte = np.where(
            bets["SCELTA"] == "OVER",
            bets["TOTAL_POINTS"] > bets["MARKET_LINE"],
            bets["TOTAL_POINTS"] < bets["MARKET_LINE"])
        push = bets["TOTAL_POINTS"] == bets["MARKET_LINE"]
        n = int((~push).sum())
        n_win = int((vinte & ~push).sum())

        if n == 0:
            lines.append("Tutte le giocate sarebbero finite in pareggio sulla linea.")
        else:
            pct = n_win / n
            profitto = n_win * (TYPICAL_ODDS - 1) - (n - n_win)
            lines.append(f"Giocate: {n} — indovinate: {n_win} ({pct:.0%})")
            lines.append(f"Soglia di pareggio a quota {TYPICAL_ODDS}: {BREAK_EVEN:.0%}")
            lines.append(f"Saldo teorico: <b>{profitto:+.1f}</b> unità")
            if pct > BREAK_EVEN:
                lines.append("→ periodo in utile, ma su pochi numeri: serve continuità "
                             "su più mesi prima di crederci.")
            else:
                lines.append("→ periodo in perdita: sotto la soglia di pareggio.")

    lines.append(f"\n<i>Generato {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM (default: mese scorso)")
    ap.add_argument("--dry-run", action="store_true", help="stampa senza inviare")
    args = ap.parse_args()

    start, end, label = month_bounds(args.month)
    print(f"📆 Periodo: {start} → {end}")
    text = build_report(start, end, label)
    telegram.send_message(text, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
