"""Descarga el EMAE general y sus aperturas por sector de actividad."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from scrapers.scraper_IED import guardar_datos_preservando_formato
from scrapers.indec_workbooks import extract_tables


ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "INDEC" / "EMAE"
URLS = {
    "emae_mensual.xls": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls",
    "emae_actividad.xls": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_actividad_base2004.xls",
}
CODIGO_FUENTE = "indec-emae"


def descargar() -> dict[str, Path]:
    CARPETA.mkdir(parents=True, exist_ok=True)
    result = {}
    for name, url in URLS.items():
        path, temp = CARPETA / name, CARPETA / f"{name}.descarga"
        try:
            response = requests.get(url, timeout=(20, 240), headers={"User-Agent": "EcoSeriesScraper/1.0"})
            response.raise_for_status(); temp.write_bytes(response.content)
            with pd.ExcelFile(temp, engine="xlrd"):
                pass
            temp.replace(path)
        except Exception:
            if not path.is_file(): raise
        finally:
            temp.unlink(missing_ok=True)
        result[name] = path
    return result


def procesar(descargar_archivos: bool = True) -> dict[str, int]:
    files = descargar() if descargar_archivos else {name: CARPETA / name for name in URLS}
    sheets: dict[str, pd.DataFrame] = {}
    inventories: list[pd.DataFrame] = []
    route = "INDEC / Economía / Cuentas Nacionales / Estimador Mensual de Actividad (EMAE)"
    for filename, path in files.items():
        output, inventory = extract_tables(path, route, CODIGO_FUENTE, "INDEC EMAE", ids_estables=True)
        inventory["Fuente"] = URLS[filename]
        sheets.update(output)
        inventories.append(inventory)
    inventory = pd.concat(inventories, ignore_index=True)
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice()
    obsolete = set(current.loc[current["Código fuente"].eq(CODIGO_FUENTE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, {name: "M" for name in sheets}, obsolete)
    from tools.generar_codificacion import generar
    generar(inventory)
    result = {"series": len(inventory), "hojas": len(sheets)}
    print(f"INDEC EMAE terminado: {result}", flush=True)
    return result


def ejecutar() -> None:
    procesar(True)
