"""Series del anexo de Evolucion del Mercado de Cambios y Balance Cambiario."""

from pathlib import Path
import pandas as pd
import requests

from scrapers.bcra_workbooks import extract_bcra
from scrapers.scraper_IED import guardar_datos_preservando_formato

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "BCRA" / "MercadoCambios"
ARCHIVO = CARPETA / "anexo-estadistico-mercado-cambios-balance-cambiario.xlsx"
URL = "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/informes/anexo-estadistico-mercado-cambios-balance-cambiario.xlsx"
CODE = "bcra-mc-bc"
ROUTE = "BCRA / Estadísticas monetarias y financieras / Evolución del Mercado de Cambios y Balance Cambiario"


def descargar() -> Path:
    CARPETA.mkdir(parents=True, exist_ok=True); temp = ARCHIVO.with_name("anexo.descarga.xlsx")
    try:
        with requests.get(URL, stream=True, timeout=(20, 240)) as response:
            response.raise_for_status()
            with temp.open("wb") as handle:
                for block in response.iter_content(65536):
                    if block: handle.write(block)
        with pd.ExcelFile(temp): pass
        temp.replace(ARCHIVO)
    except Exception:
        if not ARCHIVO.is_file(): raise
    finally: temp.unlink(missing_ok=True)
    return ARCHIVO


def procesar(fetch: bool = True) -> dict[str, int]:
    path = descargar() if fetch else ARCHIVO
    sheets, index = extract_bcra(path, CODE, ROUTE, "BCRA MC", URL)
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice(); obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, {name: "M" for name in sheets}, obsolete)
    from tools.generar_codificacion import generar
    generar(index)
    return {"series": len(index), "hojas": len(sheets)}


def ejecutar() -> None: procesar(True)
