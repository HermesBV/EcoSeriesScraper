"""Edición del libro con Excel (COM), para que la inserción de filas se comporte
exactamente como en la aplicación: fórmulas, rangos de gráficos y referencias
cruzadas se desplazan solas y la barra azul baja con todo lo que tiene debajo.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl.utils import column_index_from_string

XL_SHIFT_DOWN = -4121
XL_FORMAT_FROM_LEFT_OR_ABOVE = 0
XL_CALCULATION_MANUAL = -4135
XL_CALCULATION_AUTOMATIC = -4105
ORIGEN_SERIAL = datetime(1899, 12, 30)


def serial_excel(fecha: pd.Timestamp) -> float:
    return float((fecha.to_pydatetime() - ORIGEN_SERIAL).days)


class Excel:
    """Instancia propia e invisible de Excel; se cierra siempre al salir."""

    def __init__(self) -> None:
        self.app = None
        self.libro = None

    def __enter__(self) -> "Excel":
        import pythoncom
        import win32com.client
        pythoncom.CoInitialize()
        self.app = win32com.client.DispatchEx("Excel.Application")
        self.app.Visible = False
        self.app.DisplayAlerts = False
        self.app.ScreenUpdating = False
        self.app.EnableEvents = False
        self.app.AskToUpdateLinks = False
        return self

    def __exit__(self, *exc) -> None:
        import pythoncom
        try:
            if self.libro is not None:
                self.libro.Close(SaveChanges=False)
        finally:
            if self.app is not None:
                self.app.Quit()
            self.app = self.libro = None
            pythoncom.CoUninitialize()

    def abrir(self, ruta: Path):
        # Excel resuelve rutas relativas contra su propio directorio, no contra el nuestro.
        self.libro = self.app.Workbooks.Open(str(Path(ruta).resolve()), UpdateLinks=0, ReadOnly=False)
        self.app.Calculation = XL_CALCULATION_MANUAL
        return self.libro

    def hoja(self, nombre: str):
        return self.libro.Worksheets(nombre)

    @staticmethod
    def insertar_filas(ws, fila: int, cantidad: int) -> None:
        if cantidad > 0:
            ws.Range(f"{fila}:{fila + cantidad - 1}").Insert(XL_SHIFT_DOWN, XL_FORMAT_FROM_LEFT_OR_ABOVE)

    @staticmethod
    def escribir(ws, fila: int, col: str, valor: float) -> None:
        ws.Cells(fila, column_index_from_string(col)).Value2 = float(valor)

    @staticmethod
    def escribir_fecha(ws, fila: int, col: str, fecha: pd.Timestamp) -> None:
        ws.Cells(fila, column_index_from_string(col)).Value2 = serial_excel(fecha)

    @staticmethod
    def copiar_formula(ws, col: str, fila_origen: int, desde: int, hasta: int) -> None:
        columna = column_index_from_string(col)
        formula = ws.Cells(fila_origen, columna).FormulaR1C1
        ws.Range(f"{col}{desde}:{col}{hasta}").FormulaR1C1 = formula

    def recalcular(self) -> None:
        self.app.Calculation = XL_CALCULATION_AUTOMATIC
        self.app.CalculateFull()

    def guardar(self) -> None:
        self.libro.Save()
