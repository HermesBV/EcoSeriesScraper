"""Rutas de navegación derivadas de la estructura publicada por cada fuente."""

from __future__ import annotations

from collections import Counter, defaultdict
import re

import pandas as pd


GROUP_COLUMNS = ["Grupo de hojas", "Grupo de series 1", "Grupo de series 2"]
FREQUENCIES = {"mensual", "trimestral", "semestral", "anual", "diaria", "diario", "irregular"}


def _text(value: object) -> str:
    return " ".join(str(value).split()).strip() if pd.notna(value) else ""


def _sheet_family(source: str, sheet: str) -> str:
    if source == "indec-ipc":
        if "gba" in sheet.casefold():
            return "IPC Gran Buenos Aires"
        if "nacional" in sheet.casefold():
            return "IPC cobertura nacional"
    section = re.match(r"^(\d+\.\d+)\.\d+(?:\.|\s|$)", sheet)
    if section:
        return f"Sección {section.group(1)}"
    numbered = re.match(r"^(\d+)\.\d+(?:\.|\s|$)", sheet)
    return f"Capítulo {numbered.group(1)}" if numbered else ""


def _title_parts(source: str, sheet: str, title: str, variable: str) -> tuple[str, ...]:
    parts = [_text(part) for part in title.split("|") if _text(part)]
    if len(parts) > 1 and parts[-1].casefold() in FREQUENCIES:
        parts.pop()
    if len(parts) > 1 and variable and parts[-1].casefold() == variable.casefold():
        parts.pop()
    if source == "indec-ipc" and "nacional" in sheet.casefold() and parts:
        match = re.match(r"^(Región\s+[^-]+?)\s+-\s+(.+)$", parts[-1], re.IGNORECASE)
        if match:
            return (match.group(1).strip(),)
    return tuple(parts[1:-1]) if len(parts) > 2 else ()


def completar_agrupaciones(inventory: pd.DataFrame) -> pd.DataFrame:
    """Añade grupos compartidos sin inventar categorías para hojas aisladas."""
    result = inventory.copy()
    for column in GROUP_COLUMNS:
        result[column] = None
    if result.empty:
        return result

    records = []
    families: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    series_counts: Counter[tuple[str, str, str, tuple[str, ...]]] = Counter()
    for row in result.to_dict("records"):
        source = _text(row.get("Código fuente"))
        workbook = _text(row.get("Archivo origen"))
        sheet = _text(row.get("Hoja origen"))
        title = _text(row.get("Nombre serie"))
        family = _sheet_family(source, sheet) if sheet else ""
        parts = _title_parts(source, sheet, title, _text(row.get("Variable")))
        if family:
            families[(source, workbook, family)].add(sheet)
        for depth in range(1, min(len(parts), 2) + 1):
            series_counts[(source, workbook, sheet, parts[:depth])] += 1
        records.append((source, workbook, sheet, family, parts))

    for position, (source, workbook, sheet, family, parts) in enumerate(records):
        index = result.index[position]
        if family and len(families[(source, workbook, family)]) > 1:
            result.at[index, "Grupo de hojas"] = family
        for depth in range(1, min(len(parts), 2) + 1):
            if series_counts[(source, workbook, sheet, parts[:depth])] < 2:
                break
            result.at[index, f"Grupo de series {depth}"] = parts[depth - 1]
    return result
