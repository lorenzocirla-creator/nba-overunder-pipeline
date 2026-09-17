#!/usr/bin/env python3
"""
Migrazione di stagione: archivia i dati della stagione conclusa e slega i
sorgenti dal nome della stagione.

Cosa fa:
1. Sposta i CSV della stagione conclusa in dati/history/
2. Sostituisce nei .py i nomi di file scritti a mano ("dataset_regular_2025_26.csv")
   con f-string basate su SEASON_TAG, aggiungendo l'import dove serve.
   I nomi di SCRIPT (.py) non vengono toccati.

Uso:
    python migrate_season.py --check          # mostra cosa cambierebbe
    python migrate_season.py --apply          # applica
    python migrate_season.py --archive 2025_26 --apply
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "dati"
HISTORY = DATA / "history"

# File dati che seguono la convenzione <nome>_<stagione>.csv
DATA_STEMS = [
    "dataset_regular", "dataset_raw", "schedule_raw", "schedule_full",
    "schedule_future", "odds", "injuries", "team_stats", "team_stats_extra",
    "player_stats", "manual_totals",
]

# Questi file non vanno riscritti: definiscono loro stessi la configurazione
SKIP_FILES = {"config_season.py", "config_season_2526.py", "migrate_season.py"}

IMPORT_LINE = "from config_season import SEASON_TAG"


def data_pattern(old_tag: str) -> re.Pattern:
    stems = "|".join(re.escape(s) for s in sorted(DATA_STEMS, key=len, reverse=True))
    # Solo dentro stringhe che finiscono in .csv: i nomi degli script restano intatti
    return re.compile(rf'(["\'])((?:[^"\']*?/)?)({stems})_{re.escape(old_tag)}\.csv\1')


def migrate_sources(old_tag: str, apply: bool) -> list[tuple[Path, int]]:
    pat = data_pattern(old_tag)
    changed = []

    for py in sorted(ROOT.rglob("*.py")):
        if py.name in SKIP_FILES or "history" in py.parts or ".git" in py.parts:
            continue
        if "Archivio" in py.parts or "copie" in py.parts:
            continue

        text = py.read_text(encoding="utf-8")
        if not pat.search(text):
            continue

        new_text, n = pat.subn(
            lambda m: f'f"{m.group(2)}{m.group(3)}_{{SEASON_TAG}}.csv"', text
        )

        # L'f-string ha bisogno di SEASON_TAG nello scope del modulo
        if "SEASON_TAG" not in text.split("f\"")[0] and IMPORT_LINE not in new_text:
            lines = new_text.split("\n")
            insert_at = 0
            for i, line in enumerate(lines[:60]):
                if line.startswith(("import ", "from ")) and "__future__" not in line:
                    insert_at = i + 1
            # Gli script in features/ e utils/ aggiungono il parent a sys.path:
            # l'import va dopo quella riga, altrimenti il modulo non si trova.
            for i, line in enumerate(lines[:60]):
                if "sys.path" in line:
                    insert_at = max(insert_at, i + 1)
            lines.insert(insert_at, IMPORT_LINE)
            new_text = "\n".join(lines)

        changed.append((py.relative_to(ROOT), n))
        if apply:
            py.write_text(new_text, encoding="utf-8")

    return changed


def archive_data(old_tag: str, apply: bool) -> list[str]:
    HISTORY.mkdir(parents=True, exist_ok=True)
    moved = []
    for f in sorted(DATA.glob(f"*_{old_tag}.csv")):
        dest = HISTORY / f.name
        moved.append(f.name)
        if apply:
            shutil.move(str(f), str(dest))
    return moved


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", default="2025_26", help="tag della stagione da archiviare")
    ap.add_argument("--apply", action="store_true", help="applica le modifiche")
    ap.add_argument("--check", action="store_true", help="mostra soltanto cosa cambierebbe")
    args = ap.parse_args()

    apply = args.apply and not args.check

    print(f"{'APPLICO' if apply else 'ANTEPRIMA'} — stagione da archiviare: {args.archive}\n")

    changed = migrate_sources(args.archive, apply)
    print(f"📝 Sorgenti con nomi file da slegare dalla stagione: {len(changed)}")
    for path, n in changed:
        print(f"   {path}  ({n} riferimenti)")

    moved = archive_data(args.archive, apply)
    print(f"\n📦 File dati da archiviare in dati/history/: {len(moved)}")
    for name in moved:
        print(f"   {name}")

    if not apply:
        print("\nNessuna modifica scritta. Rilancia con --apply per applicare.")


if __name__ == "__main__":
    main()
