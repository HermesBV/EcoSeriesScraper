"""CPIAUCSL: IPC de EE.UU. desestacionalizado, publicado por FRED."""

from __future__ import annotations

from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import cargar_indice, generar

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "fuentes_BD" / "BLS" / "CPIAUCSL"
FILE = DIRECTORY / "CPIAUCSL.csv"
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
PAGE = "https://fred.stlouisfed.org/series/CPIAUCSL"
SHEET = "BLS CPIAUCSL mensual"
CODE = "fred-cpiaucsl"
TITLE = "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average (CPIAUCSL)"


def parse_csv(content: bytes) -> pd.DataFrame:
    data = pd.read_csv(BytesIO(content))
    if list(data.columns) != ["observation_date", "CPIAUCSL"]:
        raise ValueError("El CSV de FRED no contiene las columnas CPIAUCSL esperadas")
    frame = pd.DataFrame({"fecha": pd.to_datetime(data["observation_date"], errors="raise"),
                          "CPIAUCSL": pd.to_numeric(data["CPIAUCSL"], errors="coerce")})
    frame = frame.dropna(subset=["CPIAUCSL"]).sort_values("fecha").drop_duplicates("fecha", keep="last")
    if len(frame) < 500 or frame["fecha"].min().date() != date(1947, 1, 1):
        raise ValueError("La serie CPIAUCSL descargada está incompleta")
    return frame.reset_index(drop=True)


def download() -> Path:
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    try:
        response = requests.get(URL, params={"id": "CPIAUCSL", "cosd": "1947-01-01"}, timeout=(15, 120))
        response.raise_for_status()
        parse_csv(response.content)
        temporary = DIRECTORY / ".CPIAUCSL.download.csv"
        try:
            temporary.write_bytes(response.content)
            temporary.replace(FILE)
        finally:
            temporary.unlink(missing_ok=True)
    except (requests.RequestException, ValueError):
        if not FILE.is_file():
            raise
    return FILE


def procesar(fetch: bool = True) -> dict[str, int]:
    path = download() if fetch else FILE
    frame = parse_csv(path.read_bytes())
    index = pd.DataFrame([{
        "ID": "CPIAUCSL", "Código fuente": CODE, "Nombre serie": "CPIAUCSL",
        "Variable": "consumer-price-index-all-urban-consumers-all-items",
        "Unidad": "Índice 1982-1984=100", "Valoración": "No aplica",
        "Descripción": TITLE + ". Seasonally Adjusted.", "Frecuencia": "M",
        "Pestaña BD": SHEET, "Columna BD": "CPIAUCSL", "Archivo origen": path.name,
        "Hoja origen": "CPIAUCSL", "Origen": "U.S. Bureau of Labor Statistics",
        "Fuente": PAGE, "Catálogo ID": "FRED", "Dataset ID": "CPIAUCSL",
        "Título dataset": TITLE, "Tema dataset": "Estados Unidos / Economía / Precios / IPC",
        "Responsable dataset": "U.S. Bureau of Labor Statistics",
        "Fuente de valores": "CSV FRED", "Fecha inicio": frame["fecha"].min(),
        "Fecha fin": frame["fecha"].max(), "Estado": "VIGENTE",
        "Institución": "U.S. Bureau of Labor Statistics", "Área": "Economía",
        "Subárea 1": "Precios", "Subárea 2": "IPC", "Tema": "Precios",
    }])
    current = cargar_indice()
    obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ROOT / "BD.xlsx", {SHEET: frame}, {SHEET: "M"}, obsolete)
    generar(index)
    return {"serie": 1, "meses": len(frame)}


def ejecutar() -> None:
    procesar()
