"""Compara cada columna mapeada del Excel de coyuntura con su fuente en ``BD.xlsx``.

Uso (desde la raíz del repositorio)::

    python -m coyuntura.herramientas.verificar_mapeo [--ultimos 48]

Para cada columna informa cuántas fechas ya cargadas coinciden con la fuente.
Un porcentaje bajo señala un mapeo equivocado o una serie que la fuente revisó
entera (EMAE, IPI y ventas desestacionalizadas rondan el 85-95 %).
"""

from __future__ import annotations

import argparse
import math
import sys

from coyuntura.actualizador import config, mapeo
from coyuntura.actualizador.estructura import Libro
from coyuntura.actualizador.fuentes import Fuentes
from coyuntura.actualizador.plan import clave

UMBRAL = 0.8


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ultimos", type=int, default=48, help="fechas recientes a comparar por columna")
    parser.add_argument("--origen", default=config.ARCHIVO)
    args = parser.parse_args(argv)
    libro = Libro(args.origen)
    fuentes = Fuentes()
    faltantes = fuentes.cargar(mapeo.fuentes_necesarias())
    for clave_ in sorted(faltantes):
        print(f"FALTA en el inventario: {clave_}")
    sospechosas = 0
    for hoja in mapeo.HOJAS:
        if not hoja.columnas:
            continue
        estructura = libro.analizar(hoja)
        filas = sorted(estructura.fechas)[-args.ultimos:]
        print(f"\n[{hoja.nombre}]")
        for letra, columna in hoja.columnas.items():
            if any(c in faltantes for c in mapeo.claves_de(columna.fuente)):
                print(f"  {letra:3} sin fuente disponible ({fuentes.describir(columna.fuente)})")
                continue
            serie = {clave(f, hoja.frecuencia): float(v) for f, v in fuentes.evaluar(columna.fuente).items()}
            total = iguales = 0
            for fila in filas:
                valor = estructura.celdas.get((fila, letra))
                k = clave(estructura.fechas[fila], hoja.frecuencia)
                if not isinstance(valor, (int, float)) or isinstance(valor, bool) or k not in serie:
                    continue
                total += 1
                iguales += math.isclose(valor, serie[k], rel_tol=1e-6, abs_tol=1e-6)
            proporcion = iguales / total if total else 0.0
            marca = "" if proporcion >= UMBRAL or total == 0 else "  <-- revisar"
            print(f"  {letra:3} {iguales:3}/{total:<3} iguales ({proporcion:4.0%}) {fuentes.describir(columna.fuente)}{marca}")
            sospechosas += proporcion < UMBRAL and total > 0
    print(f"\nColumnas por revisar: {sospechosas}")
    return 1 if sospechosas else 0


if __name__ == "__main__":
    sys.exit(main())
