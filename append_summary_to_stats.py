# append_summary_to_stats.py
from pathlib import Path
import re
import math

ROOT = Path(__file__).resolve().parent
MD_PATH = ROOT / "predictions" / "stats_predictions_vs_results.md"

def _extract_table_block(text: str) -> tuple[str, int, int]:
    """
    Ritorna (table_block, start_idx, end_idx) del primo blocco tabella markdown.
    Il blocco include header, separatore e tutte le righe che iniziano con '|'.
    """
    lines = text.splitlines()
    start = end = -1
    for i, ln in enumerate(lines):
        if ln.strip().startswith("|") and "DATE" in ln and "DIFF" in ln:
            start = i
            break
    if start == -1:
        return ("", -1, -1)
    end = start
    for j in range(start + 1, len(lines)):
        if lines[j].strip().startswith("|"):
            end = j
        else:
            break
    block = "\n".join(lines[start:end + 1])
    return block, start, end

def _parse_diff_count(table_block: str) -> tuple[int, int]:
    """
    Legge la tabella markdown e calcola:
      - total rows (escludendo header e riga separatrice)
      - quante hanno |DIFF| < 5
    Non modifica l’ordine/colonne: usa gli indici della tabella presente.
    """
    rows = [ln for ln in table_block.splitlines() if ln.strip().startswith("|")]
    if len(rows) < 3:
        return 0, 0  # no data rows
    header = [c.strip() for c in rows[0].strip().strip("|").split("|")]
    # trova indice colonna DIFF (case insensitive)
    try:
        diff_idx = [h.strip().upper() for h in header].index("DIFF")
    except ValueError:
        return 0, 0

    # salta la riga separatrice (seconda riga)
    data_rows = rows[2:]
    total = 0
    close = 0
    for r in data_rows:
        parts = [c.strip() for c in r.strip().strip("|").split("|")]
        if len(parts) <= diff_idx:
            continue
        val = parts[diff_idx].replace(",", ".")  # nel dubbio
        try:
            d = float(val)
            if not math.isnan(d):
                total += 1
                if abs(d) < 5.0:
                    close += 1
        except ValueError:
            # cella vuota o non numerica → ignora la riga
            continue
    return close, total

def _strip_old_summary(text: str) -> str:
    """
    Rimuove eventuale vecchio riepilogo alla fine (con o senza **).
    Cerca righe tipo: 'Partite con |diff| < 5 pt: X / N (Y%)'
    """
    lines = text.rstrip().splitlines()
    # rimuovi trailing righe vuote
    while lines and not lines[-1].strip():
        lines.pop()

    pattern = re.compile(r"^\**\s*Partite\s+con\s+\|diff\|\s*<\s*5\s*pt:\s*\d+\s*/\s*\d+\s*\(\s*\d+(\.\d+)?%\s*\)\s*\**\s*$")
    if lines and pattern.match(lines[-1].strip()):
        lines.pop()
        # rimuovi eventuali due righe vuote sopra
        while lines and not lines[-1].strip():
            lines.pop()
        while lines and not lines[-1].strip():
            lines.pop()
    return "\n".join(lines) + "\n"

def main():
    if not MD_PATH.exists():
        print(f"⚠️ File non trovato: {MD_PATH}")
        return
    text = MD_PATH.read_text(encoding="utf-8")

    table_block, s, e = _extract_table_block(text)
    if s == -1:
        print("⚠️ Nessuna tabella trovata nel file; niente da aggiornare.")
        return

    close, total = _parse_diff_count(table_block)
    pct = (close / total * 100.0) if total > 0 else 0.0
    summary_line = f"Partite con |diff| < 5 pt: {close} / {total} ({pct:.1f}%)"

    # rimuovi eventuale riepilogo precedente e aggiungi quello nuovo
    base = _strip_old_summary(text)
    updated = base.rstrip() + "\n\n\n" + summary_line + "\n"

    MD_PATH.write_text(updated, encoding="utf-8")
    print("✅ Riepilogo aggiornato:")
    print(summary_line)

if __name__ == "__main__":
    main()