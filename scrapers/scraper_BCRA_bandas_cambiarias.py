"""Serie completa de las bandas cambiarias del BCRA."""

from pathlib import Path
import pandas as pd
import requests

from scrapers.bcra_workbooks import extract_bcra
from scrapers.scraper_IED import guardar_datos_preservando_formato

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "BCRA" / "BandasCambiarias"
ARCHIVO = CARPETA / "bandas.xlsx"
URL = "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/serie-completa-bandas-cambiarias.xlsx"
CODE = "bcra-bandas"
ROUTE = "BCRA / Estadísticas monetarias y financieras / Régimen de Bandas Cambiarias"


def descargar() -> Path:
    CARPETA.mkdir(parents=True, exist_ok=True); temp = ARCHIVO.with_name("bandas.descarga.xlsx")
    try:
        r = requests.get(URL, timeout=(20, 180)); r.raise_for_status(); temp.write_bytes(r.content)
        with pd.ExcelFile(temp): pass
        temp.replace(ARCHIVO)
    except Exception:
        if not ARCHIVO.is_file(): raise
    finally: temp.unlink(missing_ok=True)
    return ARCHIVO


def procesar(fetch: bool = True) -> dict[str, int]:
    path = descargar() if fetch else ARCHIVO
    sheets, index = extract_bcra(path, CODE, ROUTE, "BCRA Bandas", URL)
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice(); obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, {name: "D" for name in sheets}, obsolete)
    from tools.generar_codificacion import generar
    generar(index)
    return {"series": len(index), "hojas": len(sheets)}


def ejecutar() -> None: procesar(True)
