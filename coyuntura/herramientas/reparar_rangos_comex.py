"""Amplía los rangos fijos de la columna AC del cuadro anual de COMEX.

La columna AC promedia por año el tipo de cambio nominal de «TCR Mensual»
(``AVERAGEIF('TCR Mensual'!$A$2:$A$902, B<fila>, 'TCR Mensual'!$S$2:$S$902)``).
El rango fijo terminaba en la fila 902 (enero de 2025), así que 2025 promediaba
un solo mes y los años siguientes darían ``#DIV/0!``. Esta herramienta deja la
misma fórmula sobre columnas enteras (``$A:$A`` y ``$S:$S``), que ya no hay que
ampliar. Se corre una sola vez; las filas de años nuevos copian la fórmula.

Uso: ``python -m coyuntura.herramientas.reparar_rangos_comex [--origen ruta]``
"""

from __future__ import annotations

import argparse
import re
import sys

from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string

from coyuntura.actualizador import config
from coyuntura.actualizador.excel_com import Excel

HOJA = "COMEX"
COLUMNA = "AC"
PATRON = re.compile(r"'TCR Mensual'!\$([A-Z]+)\$\d+:\$([A-Z]+)\$\d+")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--origen", default=config.ARCHIVO)
    args = parser.parse_args(argv)
    ws = load_workbook(args.origen)[HOJA]
    columna = column_index_from_string(COLUMNA)
    cambios = []
    for fila in range(1, ws.max_row + 1):
        valor = ws.cell(fila, columna).value
        if isinstance(valor, str) and PATRON.search(valor):
            nueva = PATRON.sub(lambda m: f"'TCR Mensual'!${m[1]}:${m[1]}" if m[1] == m[2] else m[0], valor)
            if nueva != valor:
                cambios.append((fila, valor, nueva))
    if not cambios:
        print("No hay rangos fijos que ampliar")
        return 0
    with Excel() as excel:
        excel.abrir(args.origen)
        hoja = excel.hoja(HOJA)
        for fila, _, nueva in cambios:
            hoja.Cells(fila, columna).Formula = nueva
        excel.recalcular()
        excel.guardar()
    for fila, antes, despues in cambios:
        print(f"{COLUMNA}{fila}: {antes}  ->  {despues}")
    print(f"{len(cambios)} fórmulas ampliadas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
