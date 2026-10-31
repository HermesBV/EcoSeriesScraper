"""Lectura de la estructura de cada hoja del Excel de coyuntura (sin modificarlo).

Se usa openpyxl porque es rápido para leer; las modificaciones van por COM de
Excel para que las fórmulas, gráficos y referencias se desplacen como cuando el
economista inserta filas a mano.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string, get_column_letter

from .mapeo import Hoja

TEMAS_AZULES = {4, 7}          # accent1 y accent4 del tema: los dos azules que usa el libro
RGB_AZULES = {"FF156082", "FF0F9ED5"}
BUSQUEDA_BARRA = 6             # filas que se revisan debajo del último dato para hallar la barra
BUSQUEDA_FORMULA = 600         # filas hacia arriba para encontrar la fórmula vigente de una columna


@dataclass
class Estructura:
    hoja: str
    fila_azul: int
    ultima_fila: int
    fechas: dict[int, pd.Timestamp]
    celdas: dict[tuple[int, str], object]
    formulas: dict[str, tuple[int, str]]
    max_col: int
    graficos: int
    bloques_anuales: list[tuple[int, int, list[int]]] = field(default_factory=list)

    @property
    def ultima_fecha(self) -> pd.Timestamp | None:
        return self.fechas[self.ultima_fila] if self.ultima_fila in self.fechas else None


def _es_azul(fill) -> bool:
    if fill is None or fill.fill_type != "solid" or fill.fgColor is None:
        return False
    color = fill.fgColor
    try:
        if color.type == "theme":
            return color.theme in TEMAS_AZULES
        if color.type == "rgb":
            return str(color.rgb).upper() in RGB_AZULES
    except (AttributeError, TypeError, ValueError):
        return False
    return False


def _fila_azul(ws, desde: int, max_col: int) -> int | None:
    for fila in range(desde, desde + BUSQUEDA_BARRA + 1):
        dimension = ws.row_dimensions.get(fila)
        if dimension is not None and _es_azul(dimension.fill):
            return fila
        if any(_es_azul(ws.cell(fila, columna).fill) for columna in range(1, max_col + 1)):
            return fila
    return None


def _fecha(valor: object) -> pd.Timestamp | None:
    if isinstance(valor, (datetime, date)) and not isinstance(valor, bool):
        return pd.Timestamp(valor).normalize()
    return None


def _es_formula(valor: object) -> bool:
    return isinstance(valor, str) and valor.startswith("=")


def _anio(valor: object) -> int | None:
    if isinstance(valor, bool):
        return None
    if isinstance(valor, (int, float)) and float(valor).is_integer() and 1900 <= valor <= 2100:
        return int(valor)
    return None


def _bloques_anuales(ws_valores, col_anio: str, desde: int, hasta: int) -> list[tuple[int, int, list[int]]]:
    """Corridas contiguas de años enteros en ``col_anio`` debajo de la barra azul."""
    columna = column_index_from_string(col_anio)
    bloques: list[tuple[int, int, list[int]]] = []
    fila = desde
    while fila <= hasta:
        anio = _anio(ws_valores.cell(fila, columna).value)
        if anio is None:
            fila += 1
            continue
        inicio, anios = fila, []
        while fila <= hasta and (anio := _anio(ws_valores.cell(fila, columna).value)) is not None:
            anios.append(anio)
            fila += 1
        if len(anios) >= 2 and anios == sorted(anios):
            bloques.append((inicio, fila - 1, anios))
    return bloques


class Libro:
    """Las dos lecturas (fórmulas y valores cacheados) del libro de coyuntura."""

    def __init__(self, ruta: Path) -> None:
        self.ruta = ruta
        self.formulas = load_workbook(ruta)
        self.valores = load_workbook(ruta, data_only=True)

    def hojas(self) -> list[str]:
        return list(self.formulas.sheetnames)

    def analizar(self, hoja: Hoja, col_anio: str | None = None) -> Estructura:
        ws = self.formulas[hoja.nombre]
        wv = self.valores[hoja.nombre]
        col_fecha = column_index_from_string(hoja.col_fecha)
        max_col = ws.max_column
        fechas: dict[int, pd.Timestamp] = {}
        fila = hoja.fila_datos
        while True:
            fecha = _fecha(wv.cell(fila, col_fecha).value)
            if fecha is None:
                break
            fechas[fila] = fecha
            fila += 1
        if not fechas:
            raise ValueError(f"{hoja.nombre}: no hay fechas desde la fila {hoja.fila_datos}")
        ultima = fila - 1
        fila_azul = _fila_azul(ws, ultima + 1, max_col)
        if fila_azul is None:
            raise ValueError(f"{hoja.nombre}: no se encontró la barra azul debajo de la fila {ultima}")
        celdas: dict[tuple[int, str], object] = {}
        formulas: dict[str, tuple[int, str]] = {}
        for columna in range(1, max_col + 1):
            letra = get_column_letter(columna)
            # La fórmula vigente de una columna es la más cercana al último dato;
            # el economista a veces pega valores en las filas recientes.
            for fila in range(ultima, max(hoja.fila_datos, ultima - BUSQUEDA_FORMULA) - 1, -1):
                valor = ws.cell(fila, columna).value
                if _es_formula(valor):
                    formulas[letra] = (fila, valor)
                    break
        for letra in list(hoja.columnas) + [hoja.col_fecha]:
            columna = column_index_from_string(letra)
            for fila in fechas:
                celdas[(fila, letra)] = ws.cell(fila, columna).value
        bloques = _bloques_anuales(wv, col_anio, fila_azul + 1, wv.max_row) if col_anio else []
        return Estructura(hoja.nombre, fila_azul, ultima, fechas, celdas, formulas, max_col,
                          len(ws._charts), bloques)

    def fila_bloque(self, nombre_hoja: str, fila: int, max_col: int) -> dict[str, object]:
        ws = self.formulas[nombre_hoja]
        return {get_column_letter(c): ws.cell(fila, c).value for c in range(1, max_col + 1)}
