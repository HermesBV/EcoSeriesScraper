"""Intercambio Comercial Argentino (ICA) del INDEC."""

from pathlib import Path
import pandas as pd
import requests

from scrapers.indec_workbooks import extract_tables
from scrapers.scraper_IED import guardar_datos_preservando_formato

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "INDEC" / "ComercioExterior"
ARCHIVO = CARPETA / "balanmensual.xls"
URL = "https://www.indec.gob.ar/ftp/cuadros/economia/balanmensual.xls"
ROUTE = "INDEC / Economía / Comercio Exterior / Intercambio Comercial Argentino"
CODE = "indec-ica"


def descargar() -> Path:
    CARPETA.mkdir(parents=True, exist_ok=True); temp = CARPETA / "balanmensual.descarga.xls"
    try:
        r = requests.get(URL, timeout=(20, 180)); r.raise_for_status(); temp.write_bytes(r.content)
        with pd.ExcelFile(temp, engine="xlrd"): pass
        temp.replace(ARCHIVO)
    except Exception:
        if not ARCHIVO.is_file(): raise
    finally: temp.unlink(missing_ok=True)
    return ARCHIVO


def procesar(fetch: bool = True) -> dict[str, int]:
    path = descargar() if fetch else ARCHIVO
    sheets, index = extract_tables(path, ROUTE, CODE, "INDEC ICA")
    index["Fuente"] = URL
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice(); obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, {name: "M" for name in sheets}, obsolete)
    from tools.generar_codificacion import generar
    generar(index)
    return {"series": len(index), "hojas": len(sheets), "fecha_fin": str(index["Fecha fin"].max())}


def ejecutar() -> None: procesar(True)
