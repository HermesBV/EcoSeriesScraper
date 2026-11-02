"""Indice de Produccion Industrial Manufacturero del INDEC."""

from pathlib import Path
import pandas as pd

from scrapers.indec_workbooks import descargar_anual, extract_tables, fuente_anual
from scrapers.scraper_IED import guardar_datos_preservando_formato

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "INDEC" / "Industria"
ARCHIVO = CARPETA / "ipi_manufacturero.xls"
URL = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipi_manufacturero_{anio}.xls"  # el año cambia cada enero
ROUTE = "INDEC / Economía / Industria Manufacturera / Índice de Producción Industrial Manufacturero"
CODE = "indec-ipi-manufacturero"


def _validar(ruta: Path) -> None:
    with pd.ExcelFile(ruta, engine="xlrd"):
        pass


def descargar() -> Path:
    descargar_anual(URL, ARCHIVO, _validar)
    return ARCHIVO


def procesar(fetch: bool = True) -> dict[str, int]:
    path = descargar() if fetch else ARCHIVO
    sheets, index = extract_tables(path, ROUTE, CODE, "INDEC IPI", ids_estables=True)
    index["Fuente"] = fuente_anual(URL, ARCHIVO)
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice(); obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, {name: "M" for name in sheets}, obsolete)
    from tools.generar_codificacion import generar
    generar(index)
    return {"series": len(index), "hojas": len(sheets)}


def ejecutar() -> None: procesar(True)
