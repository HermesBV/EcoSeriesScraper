"""Borra pestañas de ``BD.xlsx`` para que la próxima corrida las reconstruya desde la fuente.

``BD.xlsx`` conserva el historial: cada corrida suma y pisa fechas, nunca borra. Si un
scraper fechaba mal una serie, el arreglo agrega las fechas correctas pero las viejas
quedan. Para esas pestañas hay que partir de cero::

    python -m tools.rehacer_pestanas "EMP-LI - Hog-S-f2587" "EMP-CBA y CBT-T-318e0"
    python -m scrapers.scraper_IED        # o el scraper que las escribe

Sólo sirve para pestañas cuya fuente publica la serie completa. Deja una copia en
``BD.backup-rehacer.xlsx``.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from openpyxl import load_workbook

RAIZ = Path(__file__).resolve().parents[1]


def rehacer(pestanas: list[str], ruta: Path = RAIZ / "BD.xlsx") -> list[str]:
    libro = load_workbook(ruta)
    faltantes = [nombre for nombre in pestanas if nombre not in libro.sheetnames]
    if faltantes:
        raise SystemExit(f"No existen en {ruta.name}: {', '.join(faltantes)}")
    shutil.copy2(ruta, ruta.with_name("BD.backup-rehacer.xlsx"))
    for nombre in pestanas:
        del libro[nombre]
    libro.save(ruta)
    return pestanas


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    print("Pestañas borradas:", ", ".join(rehacer(sys.argv[1:])))
