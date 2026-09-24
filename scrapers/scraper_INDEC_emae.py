"""Descarga el EMAE general y sus aperturas por sector de actividad."""

from __future__ import annotations

from pathlib import Path
import hashlib
import re
import unicodedata

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
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
         "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}


def _texto(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


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


def _sheet_name(filename: str, sheet: str) -> str:
    digest = hashlib.sha1(f"{filename}|{sheet}".encode()).hexdigest()[:4]
    return f"INDEC EMAE {sheet[:13]} {digest}"[:31]


def construir_salida(files: dict[str, Path]) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    outputs, inventory = {}, []
    for filename, path in files.items():
        with pd.ExcelFile(path) as book:
            for sheet in book.sheet_names:
                raw = pd.read_excel(book, sheet_name=sheet, header=None)
                headers = [_texto(x) for x in raw.iloc[2, 2:]]
                year = pd.to_numeric(raw.iloc[:, 0], errors="coerce").ffill()
                month = raw.iloc[:, 1].map(lambda x: MESES.get(_texto(x).casefold()))
                rows = [i for i in range(len(raw)) if pd.notna(year.iloc[i]) and pd.notna(month.iloc[i])]
                data = pd.DataFrame({"fecha": [pd.Timestamp(int(year.iloc[i]), int(month.iloc[i]), 1) for i in rows]})
                sheet_bd = _sheet_name(filename, sheet)
                title = _texto(raw.iloc[0, 0])
                header_counts = pd.Series([header for header in headers if header]).value_counts()
                for offset, header in enumerate(headers, 2):
                    if not header: continue
                    column_name = header if header_counts.get(header, 0) == 1 else f"{header} ({offset + 1})"
                    values = pd.to_numeric(raw.iloc[rows, offset], errors="coerce").reset_index(drop=True)
                    if values.notna().sum() == 0: continue
                    data[column_name] = values
                    dates = data.loc[data[column_name].notna(), "fecha"]
                    inventory.append({
                        "ID": f"{filename}|{sheet}|{_slug(header)}|col-{offset + 1}", "Código fuente": CODIGO_FUENTE,
                        "Nombre serie": column_name, "Variable": _slug(header),
                        "Unidades": "Porcentaje" if "var" in sheet.casefold() or "var %" in header.casefold() else "Índice 2004=100",
                        "Valoración": "No aplica / no informado", "Descripción": title,
                        "Frecuencia": "M", "Pestaña BD": sheet_bd, "Columna BD": column_name,
                        "Archivo origen": filename, "Hoja origen": sheet,
                        "Origen": "Instituto Nacional de Estadística y Censos (INDEC)", "Fuente": URLS[filename],
                        "Catálogo ID": "indec-emae", "Dataset ID": "emae",
                        "Título dataset": title,
                        "Tema dataset": "INDEC / Economía / Cuentas Nacionales / Estimador Mensual de Actividad (EMAE)",
                        "Responsable dataset": "Instituto Nacional de Estadística y Censos (INDEC)",
                        "Fuente de valores": "Excel INDEC", "Fecha inicio": dates.min(), "Fecha fin": dates.max(),
                        "Estado": "VIGENTE",
                    })
                outputs[sheet_bd] = data.sort_values("fecha").drop_duplicates("fecha", keep="last")
    return outputs, pd.DataFrame(inventory)


def procesar(descargar_archivos: bool = True) -> dict[str, int]:
    files = descargar() if descargar_archivos else {name: CARPETA / name for name in URLS}
    sheets: dict[str, pd.DataFrame] = {}
    inventories: list[pd.DataFrame] = []
    route = "INDEC / Economía / Cuentas Nacionales / Estimador Mensual de Actividad (EMAE)"
    for filename, path in files.items():
        output, inventory = extract_tables(path, route, CODIGO_FUENTE, "INDEC EMAE")
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
