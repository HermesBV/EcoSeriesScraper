"""Utilidades compartidas para tablas temporales de los libros estadisticos INDEC."""

from __future__ import annotations

from pathlib import Path
import hashlib
import re
import unicodedata

import pandas as pd


MONTHS = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
          "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9,
          "octubre": 10, "noviembre": 11, "diciembre": 12}


def text(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def slug(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def sheet_name(prefix: str, source: str, sheet: str) -> str:
    digest = hashlib.sha1(f"{source}|{sheet}".encode()).hexdigest()[:5]
    clean = re.sub(r"[\\/*?:\[\]]", "-", sheet).strip()
    return f"{prefix} {clean[:23]} {digest}"[:31]


def _month(value: object) -> int | None:
    normalized = unicodedata.normalize("NFKD", text(value)).encode("ascii", "ignore").decode().casefold()
    normalized = re.sub(r"[^a-z].*$", "", normalized)
    if normalized.endswith("e") and normalized[:-1] in MONTHS:
        normalized = normalized[:-1]
    return next((number for name, number in MONTHS.items() if name.startswith(normalized) and len(normalized) >= 3), None)


def _find_year_month(raw: pd.DataFrame) -> tuple[list[pd.Timestamp], int, int] | None:
    best = None
    for month_col in range(min(4, raw.shape[1])):
        months = raw.iloc[:, month_col].map(_month)
        count = int(months.notna().sum())
        if count < 10:
            continue
        for year_col in range(month_col):
            # No tomar años del título/base (p. ej. "base 2004=100") como
            # fechas: puntuar los años explícitos sólo en filas con mes y
            # rellenar hacia abajo después de elegir la columna correcta.
            raw_years = pd.to_numeric(
                raw.iloc[:, year_col].astype(str).str.extract(r"((?:19|20)\d{2})")[0],
                errors="coerce",
            )
            observed = int((raw_years.notna() & months.notna()).sum())
            if observed == 0:
                continue
            years = raw_years.ffill()
            dates = [pd.Timestamp(int(year), int(month), 1) if pd.notna(year) and pd.notna(month) else pd.NaT
                     for year, month in zip(years, months)]
            valid = sum(pd.notna(date) for date in dates)
            score = (valid, observed)
            if best is None or score > best[0]:
                best = (score, dates, year_col, month_col)
    if best is None:
        return None
    return best[1], best[2], best[3]


def extract_tables(
    path: Path,
    route: str,
    source_code: str,
    sheet_prefix: str,
    *,
    skip_sheets: set[str] | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    inventory: list[dict[str, object]] = []
    skip = {name.casefold() for name in (skip_sheets or {"índice", "indice", "observaciones"})}
    with pd.ExcelFile(path) as workbook:
        for source_sheet in workbook.sheet_names:
            if source_sheet.casefold() in skip:
                continue
            raw = pd.read_excel(workbook, sheet_name=source_sheet, header=None)
            if raw.empty or raw.shape[1] < 3:
                continue

            # Algunos libros INDEC ya tienen una fecha real en la primera columna.
            date_candidates = raw.iloc[:, 0].map(
                lambda value: pd.NaT if isinstance(value, (int, float)) and not isinstance(value, bool) else value
            )
            direct_dates = pd.to_datetime(date_candidates, errors="coerce")
            direct_dates = direct_dates.where(direct_dates.dt.year.between(1900, 2100))
            direct_count = int(direct_dates.notna().sum())
            year_month = _find_year_month(raw)
            ym_count = sum(pd.notna(value) for value in year_month[0]) if year_month else 0
            if year_month and ym_count > direct_count:
                date_values, year_col, month_col = year_month
                date_cols = {year_col, month_col}
                first_data = next(i for i, d in enumerate(date_values) if pd.notna(d))
            elif direct_count >= 10:
                date_values = list(direct_dates)
                date_cols = {0}
                first_data = next(i for i, d in enumerate(date_values) if pd.notna(d))
            else:
                continue

            valid_rows = [i for i, value in enumerate(date_values) if pd.notna(value)]
            if len(valid_rows) < 10:
                continue
            header_end = first_data
            header_matrix = raw.iloc[:header_end].ffill(axis=1)
            series: dict[str, pd.Series] = {}
            for col in range(raw.shape[1]):
                if col in date_cols:
                    continue
                values = pd.to_numeric(raw.iloc[valid_rows, col], errors="coerce").reset_index(drop=True)
                if values.notna().sum() < 2:
                    continue
                parts: list[str] = []
                for cell in header_matrix.iloc[:, col].tail(6):
                    part = text(cell)
                    if part and part.casefold() not in {p.casefold() for p in parts}:
                        parts.append(part)
                label = " - ".join(parts) or f"Columna {col + 1}"
                if label in series:
                    label = f"{label} ({col + 1})"
                series[label] = values
                ident = f"{path.name}|{source_sheet}|{col + 1}"
                dates = pd.Series([date_values[i] for i in valid_rows])
                present = dates[values.notna()]
                title = text(raw.iloc[0, 0]) or text(raw.iloc[0, min(col, raw.shape[1] - 1)]) or source_sheet
                inventory.append({
                    "ID": ident, "Código fuente": source_code, "Nombre serie": label,
                    "Variable": slug(label), "Unidades": "Ver descripción de la serie",
                    "Valoración": "No aplica / no informado", "Descripción": title,
                    "Frecuencia": "M", "Pestaña BD": sheet_name(sheet_prefix, path.name, source_sheet),
                    "Columna BD": label, "Archivo origen": path.name, "Hoja origen": source_sheet,
                    "Origen": "Instituto Nacional de Estadística y Censos (INDEC)",
                    "Fuente": "", "Catálogo ID": source_code,
                    "Dataset ID": slug(route.split("/")[-1]), "Título dataset": title,
                    "Tema dataset": route, "Responsable dataset": "Instituto Nacional de Estadística y Censos (INDEC)",
                    "Fuente de valores": "Excel INDEC", "Fecha inicio": present.min(),
                    "Fecha fin": present.max(), "Estado": "VIGENTE",
                })
            if series:
                frame = pd.DataFrame({"fecha": [date_values[i] for i in valid_rows], **series})
                outputs[sheet_name(sheet_prefix, path.name, source_sheet)] = (
                    frame.sort_values("fecha").drop_duplicates("fecha", keep="last").reset_index(drop=True)
                )
    inventory_df = pd.DataFrame(inventory)
    return outputs, inventory_df
