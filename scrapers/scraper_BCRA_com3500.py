"""Tipo de Cambio de Referencia A3500 y promedio nominal mensual."""

from pathlib import Path
import pandas as pd
import requests

from scrapers.bcra_workbooks import extract_bcra
from scrapers.scraper_IED import guardar_datos_preservando_formato

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "BCRA" / "TipoCambio3500"
ARCHIVO = CARPETA / "com3500.xls"
URL = "https://www.bcra.gob.ar/pdfs/publicacionesestadisticas/com3500.xls"
CODE = "bcra-com3500"
ROUTE = "BCRA / Estadísticas monetarias y financieras / Tipo de Cambio de Referencia Comunicación A 3500 y Tipo de Cambio Nominal Promedio Mensual (TCNPM)"


def descargar() -> Path:
    CARPETA.mkdir(parents=True, exist_ok=True); temp = CARPETA / "com3500.descarga.xls"
    try:
        response = requests.get(URL, timeout=(20, 240)); response.raise_for_status(); temp.write_bytes(response.content)
        with pd.ExcelFile(temp, engine="xlrd"): pass
        temp.replace(ARCHIVO)
    except Exception:
        if not ARCHIVO.is_file(): raise
    finally: temp.unlink(missing_ok=True)
    return ARCHIVO


def procesar(fetch: bool = True) -> dict[str, int]:
    path = descargar() if fetch else ARCHIVO
    sheets, index = extract_bcra(path, CODE, ROUTE, "BCRA 3500", URL)
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice(); obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    frequency = {}
    for sheet in sheets:
        sheet_series = index.loc[index["Pestaña BD"].eq(sheet), "Frecuencia"].astype(str)
        frequency[sheet] = "D" if sheet_series.eq("D").any() else "M"
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, frequency, obsolete)
    from tools.generar_codificacion import generar
    generar(index)
    return {"series": len(index), "hojas": len(sheets)}


def ejecutar() -> None: procesar(True)
