"""Descarga todas las series del Sistema de Indices de Precios Mayoristas."""

from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd
import requests

from scrapers.scraper_IED import guardar_datos_preservando_formato


ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "INDEC" / "SIPM"
ARCHIVO = CARPETA / "series_sipm_dic2015.xls"
URL = "https://www.indec.gob.ar/ftp/cuadros/economia/series_sipm_dic2015.xls"
CODIGO_FUENTE = "indec-sipm"
HOJAS = {"IPIM": "INDEC SIPM IPIM M", "IPIB": "INDEC SIPM IPIB M", "IPP": "INDEC SIPM IPP M"}
MESES = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
         "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12}


def _texto(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def descargar() -> Path:
    CARPETA.mkdir(parents=True, exist_ok=True)
    temp = ARCHIVO.with_name("series_sipm.descarga.xls")
    try:
        response = requests.get(URL, timeout=(20, 240), headers={"User-Agent": "EcoSeriesScraper/1.0"})
        response.raise_for_status()
        temp.write_bytes(response.content)
        with pd.ExcelFile(temp, engine="xlrd"):
            pass
        temp.replace(ARCHIVO)
    except Exception:
        if not ARCHIVO.is_file():
            raise
    finally:
        temp.unlink(missing_ok=True)
    return ARCHIVO


def construir_salida(ruta: Path) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    salidas, inventario = {}, []
    for hoja_origen, hoja_bd in HOJAS.items():
        raw = pd.read_excel(ruta, sheet_name=hoja_origen, header=None)
        years = pd.to_numeric(raw.iloc[3, 2:], errors="coerce").ffill()
        months = raw.iloc[4, 2:].map(lambda x: _texto(x).casefold()[:3])
        dates = [pd.Timestamp(int(y), MESES[m], 1) if pd.notna(y) and m in MESES else pd.NaT
                 for y, m in zip(years, months)]
        valid = [i for i, date in enumerate(dates) if pd.notna(date)]
        data = pd.DataFrame({"fecha": [dates[i] for i in valid]})
        title = _texto(raw.iloc[1, 0])
        for row in range(7, len(raw)):
            code, description = _texto(raw.iloc[row, 0]), _texto(raw.iloc[row, 1])
            values = pd.to_numeric(raw.iloc[row, 2:], errors="coerce")
            if not code or not description or values.notna().sum() == 0:
                continue
            column = f"{code} - {description}"
            data[column] = [values.iloc[i] for i in valid]
            present = data.loc[data[column].notna(), "fecha"]
            inventario.append({
                "ID": f"{hoja_origen}|{code}", "Código fuente": CODIGO_FUENTE,
                "Nombre serie": f"{hoja_origen}: {description}", "Variable": code,
                "Unidades": "Índice diciembre 2015=100", "Valoración": "No aplica / no informado",
                "Descripción": title, "Frecuencia": "M", "Pestaña BD": hoja_bd,
                "Columna BD": column, "Archivo origen": ruta.name, "Hoja origen": hoja_origen,
                "Origen": "Instituto Nacional de Estadística y Censos (INDEC)", "Fuente": URL,
                "Catálogo ID": "indec-sipm", "Dataset ID": hoja_origen.casefold(),
                "Título dataset": title, "Tema dataset": "INDEC / Economía / Precios / Precios Mayoristas (SIPM)",
                "Responsable dataset": "Instituto Nacional de Estadística y Censos (INDEC)",
                "Fuente de valores": "Excel INDEC", "Fecha inicio": present.min(),
                "Fecha fin": present.max(), "Estado": "VIGENTE",
            })
        salidas[hoja_bd] = data.sort_values("fecha").reset_index(drop=True)
    return salidas, pd.DataFrame(inventario)


def procesar(descargar_archivo: bool = True) -> dict[str, int]:
    ruta = descargar() if descargar_archivo else ARCHIVO
    hojas, inventario = construir_salida(ruta)
    guardar_datos_preservando_formato(ARCHIVO_BD, hojas, {name: "M" for name in hojas}, set(HOJAS.values()))
    from tools.generar_codificacion import generar
    generar(inventario)
    resumen = {"series": len(inventario), "hojas": len(hojas)}
    print(f"INDEC SIPM terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar(True)
