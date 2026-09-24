"""Extraccion de tablas historicas con fechas en los libros BCRA."""

from __future__ import annotations

from pathlib import Path
import hashlib
import re
import unicodedata

import pandas as pd


def _text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def _sheet(prefix: str, source: str, original: str) -> str:
    digest = hashlib.sha1(f"{source}|{original}".encode()).hexdigest()[:5]
    safe = re.sub(r"[\\/*?:\[\]]", "-", original)
    return f"{prefix} {safe[:19]} {digest}"[:31]


def extract_bcra(path: Path, code: str, route: str, prefix: str, url: str,
                 allowed_sheets: set[str] | None = None) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    outputs, inventory = {}, []
    with pd.ExcelFile(path) as workbook:
        for sheet in workbook.sheet_names:
            if sheet.casefold() in {"índice", "indice", "observaciones"}:
                continue
            if allowed_sheets and sheet not in allowed_sheets:
                continue
            raw = pd.read_excel(workbook, sheet_name=sheet, header=None)
            date_col, date_series, date_count = None, None, 0
            for col in range(min(5, raw.shape[1])):
                candidates = raw.iloc[:, col].map(
                    lambda value: pd.NaT if isinstance(value, (int, float)) and not isinstance(value, bool) else value
                )
                parsed = pd.to_datetime(candidates, errors="coerce")
                parsed = parsed.where(parsed.dt.year.between(1900, 2100))
                count = int(parsed.notna().sum())
                if count > date_count:
                    date_col, date_series, date_count = col, parsed, count
            if date_col is None or date_count < 5:
                continue
            date_rows = [idx for idx, date in enumerate(date_series) if pd.notna(date)]
            first = date_rows[0]
            # Prefer BCRA variable codes when the table publishes a code row.
            code_row = None
            for row in range(max(0, first - 8), first):
                count = sum(bool(re.fullmatch(r"[A-Za-z]{2,8}\d{0,4}", _text(value)))
                            for value in raw.iloc[row, :].tolist())
                if count >= 3:
                    code_row = row
            headers = raw.iloc[:first].ffill(axis=1)
            data: dict[str, object] = {"fecha": [date_series.iloc[idx] for idx in date_rows]}
            labels: list[tuple[str, str, str, pd.Series]] = []
            for col in range(date_col + 1, raw.shape[1]):
                values = pd.to_numeric(raw.iloc[date_rows, col], errors="coerce").reset_index(drop=True)
                if values.notna().sum() < 2:
                    continue
                native = _text(raw.iloc[code_row, col]) if code_row is not None else ""
                parts = []
                for value in headers.iloc[:, col].tail(8):
                    part = _text(value)
                    if part and part.casefold() not in {p.casefold() for p in parts}:
                        parts.append(part)
                name = " - ".join(parts) or f"{sheet} {native or col + 1}"
                if name in data:
                    name = f"{name} ({col + 1})"
                data[name] = values
                ident = f"{native}" if native else f"{_slug(sheet)}-{col + 1}"
                labels.append((ident, name, native or str(col + 1), values))
            if not labels:
                continue
            output_sheet = _sheet(prefix, path.name, sheet)
            frame = pd.DataFrame(data).sort_values("fecha").drop_duplicates("fecha", keep="last")
            outputs[output_sheet] = frame.reset_index(drop=True)
            for ident, name, variable, values in labels:
                dates = frame.loc[values.notna(), "fecha"] if len(values) == len(frame) else frame["fecha"]
                differences = pd.to_datetime(dates).sort_values().drop_duplicates().diff().dropna().dt.days
                frequency = "D" if not differences.empty and differences.median() < 8 else "M"
                inventory.append({
                    "ID": f"{sheet}|{ident}", "Código fuente": code, "Nombre serie": name,
                    "Variable": variable, "Unidades": "Según serie BCRA",
                    "Valoración": "No aplica / no informado", "Descripción": name,
                    "Frecuencia": frequency,
                    "Pestaña BD": output_sheet, "Columna BD": name,
                    "Archivo origen": path.name, "Hoja origen": sheet,
                    "Origen": "Banco Central de la República Argentina (BCRA)", "Fuente": url,
                    "Catálogo ID": code, "Dataset ID": _slug(route.split("/")[-1]),
                    "Título dataset": route.split("/")[-1], "Tema dataset": route,
                    "Responsable dataset": "Banco Central de la República Argentina (BCRA)",
                    "Fuente de valores": "Excel BCRA", "Fecha inicio": dates.min(),
                    "Fecha fin": dates.max(), "Estado": "VIGENTE",
                })
    result = pd.DataFrame(inventory)
    # Una pestaña puede mezclar observaciones diarias y mensuales (por ejemplo,
    # TCR diario y TCNPM). Separarlas conserva fechas completas y formatos.
    for original_sheet in list(outputs):
        rows = result.loc[result["Pestaña BD"].eq(original_sheet)]
        series_frequencies = rows["Frecuencia"].dropna().astype(str).unique()
        if len(series_frequencies) < 2:
            continue
        original_data = outputs.pop(original_sheet)
        for frequency in series_frequencies:
            selected = rows.loc[rows["Frecuencia"].eq(frequency)]
            new_sheet = f"{original_sheet[:26]} {frequency}"
            columns = selected["Columna BD"].astype(str).tolist()
            split_data = original_data[["fecha", *columns]].copy()
            outputs[new_sheet] = split_data.dropna(subset=columns, how="all").reset_index(drop=True)
            result.loc[selected.index, "Pestaña BD"] = new_sheet
    if not result.empty and result[["Código fuente", "ID"]].duplicated().any():
        raise ValueError(f"IDs duplicados al leer {path.name}")
    return outputs, result
