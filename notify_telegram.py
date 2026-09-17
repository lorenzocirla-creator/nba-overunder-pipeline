#!/usr/bin/env python3
"""
Invia su Telegram le partite del giorno con il totale punti stimato.

Credenziali (una delle due vie):
  1) variabili d'ambiente TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID
  2) file dati/telegram_config.json:  {"bot_token": "...", "chat_id": "..."}

Uso:
    python notify_telegram.py              # invia le predizioni di oggi
    python notify_telegram.py --date 2025-12-01
    python notify_telegram.py --dry-run    # stampa il messaggio senza inviarlo

Se le credenziali mancano lo script NON fallisce: stampa le istruzioni ed esce 0,
così la pipeline giornaliera non si interrompe.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from config_season import PRED_DIR, season_progress
import telegram

ROOT = Path(__file__).resolve().parent




def build_message(target: date) -> str | None:
    """Testo del messaggio, o None se non ci sono partite da segnalare."""
    pred_file = PRED_DIR / f"predictions_today_{target.strftime('%Y%m%d')}.csv"
    if not pred_file.exists():
        print(f"ℹ️ Nessun file predizioni per {target} ({pred_file.name}).")
        return None

    df = pd.read_csv(pred_file)
    if df.empty or "PREDICTED_POINTS" not in df.columns:
        print(f"ℹ️ Nessuna partita in {pred_file.name}.")
        return None

    df["PREDICTED_POINTS"] = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
    df = df.dropna(subset=["PREDICTED_POINTS"])
    if df.empty:
        print("ℹ️ Nessuna predizione valida.")
        return None

    # Linea di mercato, se disponibile
    line_col = next((c for c in ["MARKET_LINE", "CLOSING_LINE", "CURRENT_LINE", "FINAL_LINE", "BASE_LINE"]
                     if c in df.columns and pd.to_numeric(df[c], errors="coerce").notna().any()), None)

    giorno = target.strftime("%d/%m/%Y")
    righe = [f"🏀 <b>NBA — {giorno}</b>", f"{len(df)} partite\n"]

    for _, r in df.iterrows():
        match = f"{r['AWAY_TEAM']} @ {r['HOME_TEAM']}"
        pred = r["PREDICTED_POINTS"]
        riga = f"• <b>{match}</b> — stima <b>{pred:.1f}</b>"

        if line_col:
            line = pd.to_numeric(r.get(line_col), errors="coerce")
            if pd.notna(line):
                delta = pred - line
                segno = "OVER" if delta > 0 else "UNDER"
                etichetta = "quota" if line_col == "MARKET_LINE" else "stima interna"
                riga += f" ({etichetta} {line:.1f} → {segno} {abs(delta):.1f})"
        righe.append(riga)

    # Raccomandazioni, se lo step le ha prodotte
    bets_file = PRED_DIR / f"recommended_bets_today_{target.strftime('%Y%m%d')}.csv"
    if bets_file.exists():
        try:
            bets = pd.read_csv(bets_file)
            if not bets.empty and "RECOMMENDATION" in bets.columns:
                rec = bets["RECOMMENDATION"].astype(str).str.upper()
                consigli = bets[bets["RECOMMENDATION"].notna() & ~rec.str.contains("NO BET")]
                if not consigli.empty:
                    righe.append("\n🎯 <b>Segnalazioni</b>")
                    for _, b in consigli.iterrows():
                        margine = pd.to_numeric(b.get("MARGIN"), errors="coerce")
                        extra = f" ({margine:.1f} pt)" if pd.notna(margine) else ""
                        righe.append(f"• {b['AWAY_TEAM']} @ {b['HOME_TEAM']}: "
                                     f"{b['RECOMMENDATION']}{extra}")
        except Exception as e:
            print(f"⚠️ Raccomandazioni non leggibili: {e}")

    righe.append(f"\n<i>Generato {datetime.now().strftime('%d/%m %H:%M')}</i>")
    return "\n".join(righe)






def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (default: oggi)")
    ap.add_argument("--dry-run", action="store_true", help="stampa il messaggio senza inviarlo")
    args = ap.parse_args()

    target = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()

    text = build_message(target)
    if text is None:
        print("⏭️  Niente da inviare.")
        return

    telegram.send_message(text, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
