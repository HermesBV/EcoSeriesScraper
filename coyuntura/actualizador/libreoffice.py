"""Edición del libro con LibreOffice sin pantalla, para servidores sin Excel (Debian).

Misma interfaz que ``excel_com.Excel``. LibreOffice desplaza solo las referencias al
insertar o borrar filas, recalcula y guarda en ``.xlsx`` con los valores calculados.
Conserva las fórmulas; los gráficos pueden perderse o cambiar (ya no se usan).

Requiere ``libreoffice-calc`` y ``python3-uno`` (``apt install``). El módulo ``uno``
vive en los paquetes del sistema: si el entorno virtual no lo ve, se lo busca ahí.
"""

from __future__ import annotations

from pathlib import Path
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile

import pandas as pd
from openpyxl.utils import column_index_from_string, get_column_letter

from .excel_com import serial_excel

FILTRO_XLSX = "Calc MS Excel 2007 XML"
SINTAXIS_EXCEL = 3      # com.sun.star.sheet.AddressConvention.XL_OOX: «Hoja!A1» y coma entre argumentos
CONTENIDO = 1 | 2 | 4 | 16   # CellFlags VALUE, DATETIME, STRING y FORMULA
_COLUMNA_HASTA_EL_FINAL = re.compile(r'<col [^>]*max="16384"[^>]*/>')


def _uno():
    try:
        import uno
    except ImportError:
        # python3-uno se instala para el Python del sistema; al final de la ruta, para
        # que nada de ahí tape a los paquetes del entorno virtual.
        sys.path.append("/usr/lib/python3/dist-packages")
        try:
            import uno
        except ImportError as error:
            raise RuntimeError("Falta LibreOffice para Python: apt install libreoffice-calc python3-uno") from error
    return uno


def disponible() -> bool:
    if not (shutil.which("soffice") or shutil.which("libreoffice")):
        return False
    try:
        _uno()
    except RuntimeError:
        return False
    return True


def quitar_formato_hasta_el_final(ruta: Path) -> int:
    """Saca de cada hoja el formato de columna que llega hasta la última (XFD).

    Con ese formato LibreOffice escribe una celda vacía por cada columna de cada
    fila y el libro pasa de 7 a 40 MB. Devuelve cuántas hojas se tocaron.
    """
    tocadas = 0
    temporal = ruta.with_name(f".{ruta.name}.sin-formato")
    with zipfile.ZipFile(ruta) as origen, zipfile.ZipFile(temporal, "w", zipfile.ZIP_DEFLATED) as destino:
        for parte in origen.infolist():
            datos = origen.read(parte.filename)
            if parte.filename.startswith("xl/worksheets/sheet"):
                texto = datos.decode("utf-8")
                corte = texto.find("<sheetData")
                cabecera, cantidad = _COLUMNA_HASTA_EL_FINAL.subn("", texto[:corte])
                if cantidad:
                    datos = (cabecera.replace("<cols></cols>", "") + texto[corte:]).encode("utf-8")
                    tocadas += 1
            destino.writestr(parte, datos)
    if tocadas:
        os.replace(temporal, ruta)
    else:
        temporal.unlink()
    return tocadas


class LibreOffice:
    """Instancia propia de LibreOffice, con su perfil descartable; se cierra siempre al salir."""

    def __init__(self) -> None:
        self.proceso = None
        self.libro = None
        self._perfil = None
        self._escritorio = None
        self._analizador = None
        self._url = ""

    # --- Ciclo de vida ---------------------------------------------------------------

    def __enter__(self) -> "LibreOffice":
        uno = _uno()
        binario = shutil.which("soffice") or shutil.which("libreoffice")
        if not binario:
            raise RuntimeError("No se encontró LibreOffice (soffice): apt install libreoffice-calc")
        nombre = f"coyuntura-{uuid.uuid4().hex[:10]}"
        self._perfil = tempfile.mkdtemp(prefix="coyuntura-lo-")
        self.proceso = subprocess.Popen(
            [binario, "--headless", "--invisible", "--norestore", "--nologo", "--nodefault",
             f"-env:UserInstallation={Path(self._perfil).as_uri()}", f"--accept=pipe,name={nombre};urp;"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        local = uno.getComponentContext()
        puente = local.ServiceManager.createInstanceWithContext("com.sun.star.bridge.UnoUrlResolver", local)
        contexto = None
        for _ in range(240):
            try:
                contexto = puente.resolve(f"uno:pipe,name={nombre};urp;StarOffice.ComponentContext")
                break
            except Exception:
                if self.proceso.poll() is not None:
                    break
                time.sleep(0.5)
        if contexto is None:
            self.__exit__()
            raise RuntimeError("LibreOffice no respondió al iniciarse")
        self._escritorio = contexto.ServiceManager.createInstanceWithContext("com.sun.star.frame.Desktop", contexto)
        return self

    def __exit__(self, *exc) -> None:
        try:
            if self.libro is not None:
                self.libro.close(True)
        except Exception:
            pass
        try:
            if self._escritorio is not None:
                self._escritorio.terminate()
        except Exception:
            pass
        if self.proceso is not None:
            try:
                self.proceso.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proceso.kill()
        if self._perfil:
            shutil.rmtree(self._perfil, ignore_errors=True)
        self.proceso = self.libro = self._escritorio = self._analizador = self._perfil = None

    def _propiedad(self, nombre: str, valor):
        from com.sun.star.beans import PropertyValue
        propiedad = PropertyValue()
        propiedad.Name, propiedad.Value = nombre, valor
        return propiedad

    def abrir(self, ruta: Path):
        ruta = Path(ruta).resolve()
        quitar_formato_hasta_el_final(ruta)
        self._url = ruta.as_uri()
        self.libro = self._escritorio.loadComponentFromURL(self._url, "_blank", 0, (self._propiedad("Hidden", True),))
        if self.libro is None:
            raise RuntimeError(f"LibreOffice no pudo abrir {ruta}")
        self._analizador = self.libro.createInstance("com.sun.star.sheet.FormulaParser")
        self._analizador.FormulaConvention = SINTAXIS_EXCEL
        self._analizador.CompileEnglish = True
        return self.libro

    def recalcular(self) -> None:
        self.libro.calculateAll()

    def guardar(self) -> None:
        self.libro.storeAsURL(self._url, (self._propiedad("FilterName", FILTRO_XLSX),))

    # --- Celdas ----------------------------------------------------------------------

    def hoja(self, nombre: str):
        return self.libro.Sheets.getByName(nombre)

    @staticmethod
    def _celda(ws, fila: int, col: int):
        return ws.getCellByPosition(col - 1, fila - 1)

    def _celda_a1(self, ws, celda: str):
        return ws.getCellRangeByName(celda.replace("$", "")).getCellByPosition(0, 0)

    @staticmethod
    def ultima_columna(ws) -> int:
        cursor = ws.createCursor()
        cursor.gotoEndOfUsedArea(False)
        return cursor.RangeAddress.EndColumn + 1

    def insertar_filas(self, ws, fila: int, cantidad: int) -> None:
        """Filas nuevas con el formato de la de arriba, como «insertar» en Excel."""
        if cantidad <= 0:
            return
        ws.Rows.insertByIndex(fila - 1, cantidad)
        if fila > 1:
            ultima = self.ultima_columna(ws) - 1
            origen = ws.getCellRangeByPosition(0, fila - 2, ultima, fila - 2).RangeAddress
            for nueva in range(fila - 1, fila - 1 + cantidad):
                ws.copyRange(ws.getCellByPosition(0, nueva).CellAddress, origen)
            ws.getCellRangeByPosition(0, fila - 1, ultima, fila - 2 + cantidad).clearContents(CONTENIDO)

    @staticmethod
    def borrar_fila(ws, fila: int) -> None:
        ws.Rows.removeByIndex(fila - 1, 1)

    def escribir(self, ws, fila: int, col: str, valor: float) -> None:
        self._celda(ws, fila, column_index_from_string(col)).setValue(float(valor))

    def escribir_fecha(self, ws, fila: int, col: str, fecha: pd.Timestamp) -> None:
        self._celda(ws, fila, column_index_from_string(col)).setValue(serial_excel(fecha))

    def valor(self, ws, fila: int, col: int):
        """Número, texto o ``None``; de una fórmula, su resultado."""
        celda = self._celda(ws, fila, col)
        tipo = celda.Type.value
        if tipo == "EMPTY":
            return None
        if tipo == "TEXT":
            return celda.String
        if tipo == "FORMULA" and (celda.getError() or celda.FormulaResultType2 != 1):
            return celda.String
        return celda.getValue()

    def tiene_formula(self, ws, fila: int, col: int) -> bool:
        return self._celda(ws, fila, col).Type.value == "FORMULA"

    def formula(self, ws, fila: int, col: int) -> str:
        """La fórmula escrita como en Excel (``=Hoja!A1``), o cadena vacía."""
        celda = self._celda(ws, fila, col)
        if celda.Type.value != "FORMULA":
            return ""
        return "=" + self._analizador.printFormula(celda.getTokens(), celda.CellAddress)

    def _poner_formula(self, celda, formula: str) -> None:
        celda.setTokens(self._analizador.parseFormula(formula.lstrip("="), celda.CellAddress))

    def escribir_formula(self, ws, celda: str, formula: str) -> None:
        self._poner_formula(self._celda_a1(ws, celda), formula)

    def limpiar(self, ws, celda: str) -> None:
        ws.getCellRangeByName(celda.replace("$", "")).clearContents(CONTENIDO)

    def copiar_formula(self, ws, col: str, fila_origen: int, desde: int, hasta: int) -> None:
        """Copia relativa (lo que hace FormulaR1C1 en Excel): sin tocar formatos."""
        from com.sun.star.table import CellAddress
        columna = column_index_from_string(col)
        origen = self._celda(ws, fila_origen, columna)
        if origen.Type.value != "FORMULA":
            return
        self._analizador.FormulaConvention = 2   # XL_R1C1: la misma fórmula sirve en cualquier fila
        try:
            relativa = self._analizador.printFormula(origen.getTokens(), origen.CellAddress)
            for fila in range(desde, hasta + 1):
                destino = self._celda(ws, fila, columna)
                destino.setTokens(self._analizador.parseFormula(relativa, destino.CellAddress))
        finally:
            self._analizador.FormulaConvention = SINTAXIS_EXCEL

    def ampliar_rangos(self, hoja: str, inicio: int, fin: int, nuevas: int) -> int:
        """Lleva a ``fin + nuevas`` los rangos de ``hoja`` que terminan en ``fin``, en las
        fórmulas de todo el libro. (Los gráficos no se tocan.)"""
        from .formulas import ampliar_rangos

        cambios = 0
        marca = str(fin)
        for indice in range(self.libro.Sheets.Count):
            ws = self.libro.Sheets.getByIndex(indice)
            cursor = ws.createCursor()
            cursor.gotoEndOfUsedArea(False)
            area = cursor.RangeAddress
            crudas = ws.getCellRangeByPosition(0, 0, area.EndColumn, area.EndRow).getFormulaArray()
            for i, fila in enumerate(crudas):
                for j, cruda in enumerate(fila):
                    # Filtro barato sobre el texto nativo; la fórmula precisa sólo para las candidatas.
                    if not (isinstance(cruda, str) and cruda.startswith("=") and ":" in cruda and marca in cruda):
                        continue
                    formula = self.formula(ws, i + 1, j + 1)
                    nueva = ampliar_rangos(formula, ws.Name, hoja, inicio, fin, nuevas)
                    if nueva != formula:
                        self._poner_formula(ws.getCellByPosition(j, i), nueva)
                        cambios += 1
        return cambios
