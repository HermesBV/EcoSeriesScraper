"""Descubrimiento y metadatos de todas las series contenidas en los Excel IED."""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from pathlib import Path
from typing import Callable

import pandas as pd
import requests


API_URL = "https://apis.datos.gob.ar/series/api/series/"
METADATA_FILE = Path(__file__).resolve().parents[1] / "series-tiempo-metadatos.csv"


def _text(value: object, fallback: str = "") -> str:
    if pd.isna(value):
        return fallback
    result = str(value).strip()
    return result if result and result.lower() != "nan" else fallback


def load_api_catalog(path: Path = METADATA_FILE) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Falta el catálogo de series de la API: {path}")
    catalog = pd.read_csv(path, dtype=str, low_memory=False)
    required = {
        "serie_id", "distribucion_titulo", "serie_unidades", "serie_descripcion",
        "serie_titulo", "indice_tiempo_frecuencia",
    }
    missing = required - set(catalog.columns)
    if missing:
        raise ValueError(f"Faltan columnas en {path.name}: {', '.join(sorted(missing))}")
    catalog["serie_id"] = catalog["serie_id"].str.strip()
    return catalog.dropna(subset=["serie_id"]).drop_duplicates("serie_id", keep="last").set_index("serie_id")


def load_existing_catalog(path: Path) -> pd.DataFrame:
    """Recupera asignaciones previas, incluidos IDs que ya no están en la API."""
    if not path.is_file():
        return pd.DataFrame()
    with pd.ExcelFile(path) as book:
        if "Codificacion" not in book.sheet_names:
            return pd.DataFrame()
        raw = pd.read_excel(book, sheet_name="Codificacion", header=None)
    header = raw.index[raw.iloc[:, 0].isin(["ID", "ID provisorio"])]
    if header.empty:
        return pd.DataFrame()
    result = raw.iloc[int(header[0]) + 1 :].copy()
    result.columns = raw.iloc[int(header[0])]
    result = result.dropna(how="all")
    origin_column = "ID origen" if "ID origen" in result else "ID fuente"
    if origin_column not in result:
        return pd.DataFrame()
    result[origin_column] = result[origin_column].astype(str).str.strip()
    return result.drop_duplicates(origin_column, keep="last").set_index(origin_column)


def frequency_code(api_frequency: object, series_id: str) -> str:
    mapping = {"R/P1Y": "A", "R/P6M": "S", "R/P3M": "T", "R/P1M": "M", "R/P1D": "D"}
    frequency = mapping.get(_text(api_frequency))
    if frequency:
        return frequency
    matches = re.findall(r"_([ASTMD])(?:_|$)", series_id.upper())
    return matches[-1] if matches else "I"


def output_sheet_name(filename: str, source_sheet: str, frequency: str) -> str:
    prefixes = {
        "actividad.xlsx": "ACT", "empleo_ingresos.xlsx": "EMP",
        "precios.xlsx": "PRE", "sector_externo.xlsx": "EXT",
        "dinero_bancos.xlsx": "DIN", "finanzas_publicas.xlsx": "FPU",
        "finanzas.xlsx": "FIN", "internacional.xlsx": "INT",
    }
    clean = re.sub(r"[\\/*?:\[\]]", "-", source_sheet).strip()
    digest = hashlib.sha1(f"{filename}|{source_sheet}|{frequency}".encode()).hexdigest()[:5]
    return f"{prefixes.get(filename, 'IED')}-{clean[:19]}-{frequency}-{digest}"[:31]


def download_api_series(session: requests.Session, series_id: str) -> pd.DataFrame:
    rows: list[list[object]] = []
    start = 0
    while True:
        response = session.get(
            API_URL,
            params={"ids": series_id, "metadata": "none", "limit": 1000, "start": start, "sort": "asc"},
            timeout=(15, 90),
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("data") or []
        rows.extend(batch)
        start += len(batch)
        if start >= int(payload.get("count", 0)) or not batch:
            break
    result = pd.DataFrame(rows, columns=["fecha", "valor"])
    result["fecha"] = pd.to_datetime(result["fecha"], errors="coerce")
    result["valor"] = pd.to_numeric(result["valor"], errors="coerce")
    return result.dropna(subset=["fecha", "valor"])


def discover_and_extract(
    books: dict[str, dict[str, pd.DataFrame]],
    indexes: dict[str, dict[str, tuple[str, int, int]]],
    existing: pd.DataFrame,
    extract: Callable[[dict[str, pd.DataFrame], tuple[str, int, int]], pd.DataFrame],
    normalize_dates: Callable[[pd.Series, str], pd.Series],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, list[tuple[str, str]]]:
    """Extrae cada ID catalogado presente en los libros; usa la API sólo como respaldo."""
    api = load_api_catalog()
    api_ids = set(api.index)
    existing_ids = set(existing.index) if not existing.empty else set()
    groups: dict[str, list[pd.DataFrame]] = defaultdict(list)
    inventory: list[dict[str, object]] = []
    failures: list[tuple[str, str]] = []
    seen: set[str] = set()
    session = requests.Session()
    session.trust_env = False

    for filename, index in indexes.items():
        candidates = sorted((set(index) & api_ids) | (set(index) & existing_ids))
        for series_id in candidates:
            if series_id in seen:
                continue
            seen.add(series_id)
            source_sheet = index[series_id][0]
            api_row = api.loc[series_id] if series_id in api.index else pd.Series(dtype=object)
            old_row = existing.loc[series_id] if series_id in existing.index else pd.Series(dtype=object)
            frequency = frequency_code(api_row.get("indice_tiempo_frecuencia"), series_id)
            source_used = "Excel IED"
            try:
                series = extract(books[filename], index[series_id])
                series["valor"] = pd.to_numeric(series["valor"], errors="coerce")
                series = series.dropna(subset=["fecha", "valor"])
                if series.empty:
                    raise ValueError("sin valores numéricos")
            except Exception as excel_error:
                if series_id not in api.index:
                    failures.append((series_id, f"{filename}/{source_sheet}: {excel_error}"))
                    continue
                try:
                    series = download_api_series(session, series_id)
                    if series.empty:
                        raise ValueError("la API no devolvió valores")
                    source_used = "API (respaldo por fallo de extracción Excel)"
                except Exception as api_error:
                    failures.append((series_id, f"Excel: {excel_error}; API: {api_error}"))
                    continue

            series["fecha"] = normalize_dates(series["fecha"], frequency)
            series = series.dropna(subset=["fecha"]).drop_duplicates("fecha", keep="last").set_index("fecha")
            first_date = series.index.min()
            last_date = series.index.max()
            output_sheet = output_sheet_name(filename, source_sheet, frequency)
            series = series[["valor"]].rename(columns={"valor": series_id})
            groups[output_sheet].append(series)

            def meta(column: str, fallback: str = "") -> str:
                return _text(api_row.get(column), _text(old_row.get(column), fallback))

            inventory.append({
                "Código fuente": "datos.gob.ar",
                "ID origen": series_id,
                "Nombre serie": meta("distribucion_titulo", meta("Nombre serie", meta("Variable", series_id))),
                "Variable": meta("serie_titulo", meta("Variable", series_id)),
                "Unidades": meta("serie_unidades", meta("Unidades")),
                "Descripción": meta("serie_descripcion", meta("Descripción")),
                "Frecuencia": frequency,
                "Pestaña BD": output_sheet,
                "Columna BD": series_id,
                "Archivo origen": filename,
                "Hoja origen": source_sheet,
                "Origen": meta("dataset_fuente", meta("Origen", "IED")),
                "Fuente": meta("distribucion_url_descarga", meta("Fuente")),
                "Catálogo ID": meta("catalogo_id"),
                "Dataset ID": meta("dataset_id"),
                "Distribución ID": meta("distribucion_id"),
                "Título dataset": meta("dataset_titulo"),
                "Tema dataset": meta("dataset_tema"),
                "Responsable dataset": meta("dataset_responsable"),
                "Fuente de valores": source_used,
                "Fecha inicio": first_date,
                "Fecha fin": last_date,
                "Estado": "VIGENTE" if last_date >= pd.Timestamp.now().normalize() - pd.DateOffset(years=3) else "CERRADA",
            })

    output = {}
    for sheet, frames in groups.items():
        data = pd.concat(frames, axis=1, join="outer").sort_index().copy()
        data = data.loc[~data.index.duplicated(keep="last")]
        data.index.name = "fecha"
        output[sheet] = data.reset_index().dropna(how="all", subset=data.columns)
    return output, pd.DataFrame(inventory), failures
