"""Filas que faltan o sobran en el medio de las hojas diarias.

Las series tienen que ser continuas (los gráficos del economista se cortan con
un hueco). Antes de planificar:

* se insertan los días hábiles que publica la fuente del calendario de la hoja
  y el Excel no tiene (el A3500 del 20/08/2025 en «Tipo de cambio diario»);
  la fila nueva copia las fórmulas de la de arriba y el plan completa sus datos
  desde la fuente o, si una columna no lo tiene, con el promedio del dato
  anterior y el posterior;
* se quita la segunda fila de una fecha repetida (Riesgo país, 23/09/2025).

Las inserciones y bajas se hacen con Excel para que las referencias, los
rangos y los gráficos se ajusten solos.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

import pandas as pd

from . import mapeo
from .estructura import Libro
from .plan import dia_habil, dias_habiles


@dataclass
class Arreglo:
    hoja: str
    insertar: list[tuple[int, pd.Timestamp]] = field(default_factory=list)  # (fila de la fecha anterior, fecha)
    borrar: list[tuple[int, pd.Timestamp]] = field(default_factory=list)    # (fila duplicada, fecha)


def detectar(libro: Libro, fuentes, faltantes: set) -> list[Arreglo]:
    habiles = dias_habiles(fuentes)
    arreglos = []
    for hoja in mapeo.HOJAS:
        if hoja.frecuencia != "D" or hoja.nombre not in libro.hojas():
            continue
        e = libro.analizar(hoja)
        arreglo = Arreglo(hoja.nombre)
        vistas: set[pd.Timestamp] = set()
        for fila, fecha in sorted(e.fechas.items()):
            if fecha in vistas:
                arreglo.borrar.append((fila, fecha))
            vistas.add(fecha)
        letras = hoja.calendario if isinstance(hoja.calendario, list) else list(hoja.columnas)
        publicadas: set[pd.Timestamp] = set()
        for letra in letras:
            fuente = hoja.columnas[letra].fuente
            if any(clave in faltantes for clave in mapeo.claves_de(fuente)):
                continue
            serie = fuentes.evaluar(fuente)
            publicadas |= {fecha.normalize() for fecha, valor in serie.items() if valor != 0}
        if publicadas:
            fechas = sorted(e.fechas.items(), key=lambda x: x[0])
            desde = max(min(e.fechas.values()), min(publicadas))
            hasta = e.ultima_fecha
            for fecha in sorted(publicadas):
                if not (desde < fecha < hasta) or fecha in vistas                         or not (hoja.dias_corridos or dia_habil(fecha, habiles)):
                    continue
                previa = max(fila for fila, f in fechas if f < fecha)
                arreglo.insertar.append((previa, fecha))
        if arreglo.insertar or arreglo.borrar:
            arreglos.append(arreglo)
    return arreglos


def aplicar(excel, arreglos: list[Arreglo], col_fecha: dict[str, str], log) -> None:
    """Bajas e inserciones de abajo hacia arriba, para que los números de fila
    pendientes no cambien."""
    from openpyxl.utils import column_index_from_string

    for arreglo in arreglos:
        ws = excel.hoja(arreglo.hoja)
        ultima_col = ws.UsedRange.Column + ws.UsedRange.Columns.Count - 1
        operaciones = [("borrar", fila, fecha) for fila, fecha in arreglo.borrar]
        # Varias fechas después de la misma fila: se insertan de la última a la
        # primera en la misma posición, así quedan en orden.
        operaciones += [("insertar", fila + 1, fecha) for fila, fecha in arreglo.insertar]
        for accion, fila, fecha in sorted(operaciones, key=lambda x: (x[1], x[2]), reverse=True):
            if accion == "borrar":
                ws.Rows(fila).Delete()
                _reparar_referencias(ws, fila, ultima_col)
                log(f"    Excel: {arreglo.hoja}: fila {fila} borrada (fecha {fecha.date()} repetida)")
                continue
            excel.insertar_filas(ws, fila, 1)
            excel.escribir_fecha(ws, fila, col_fecha[arreglo.hoja], fecha)
            fecha_col = column_index_from_string(col_fecha[arreglo.hoja])
            for col in range(1, ultima_col + 1):
                if col == fecha_col:
                    continue
                arriba, abajo = ws.Cells(fila - 1, col), ws.Cells(fila + 1, col)
                if arriba.HasFormula and abajo.HasFormula:
                    ws.Cells(fila, col).FormulaR1C1 = arriba.FormulaR1C1
                elif _numero(arriba.Value2) and _numero(abajo.Value2) and not arriba.HasFormula and not abajo.HasFormula:
                    # Columnas de valores: promedio de la fila anterior y la siguiente
                    # (las del mapeo las reemplaza después el plan con la fuente).
                    ws.Cells(fila, col).Value2 = (arriba.Value2 + abajo.Value2) / 2
            log(f"    Excel: {arreglo.hoja}: fila {fila} insertada para el {fecha.date()}")


def _numero(valor) -> bool:
    return isinstance(valor, (int, float)) and not isinstance(valor, bool)


def _reparar_referencias(ws, fila: int, ultima_col: int, alcance: int = 400) -> None:
    """Al borrar una fila, las fórmulas que la usaban (``=C704/C703-1``) quedan
    con #REF!; se rehacen con la fórmula de la fila de arriba."""
    for f in range(fila, fila + alcance):
        for col in range(1, ultima_col + 1):
            celda = ws.Cells(f, col)
            if celda.HasFormula and "#REF!" in celda.Formula and ws.Cells(f - 1, col).HasFormula:
                celda.FormulaR1C1 = ws.Cells(f - 1, col).FormulaR1C1
