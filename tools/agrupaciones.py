"""Rutas de navegación derivadas de la estructura publicada por cada fuente."""

from __future__ import annotations

from collections import Counter, defaultdict
import re

import pandas as pd


GROUP_COLUMNS = ["Grupo de hojas", "Grupo de series 1", "Grupo de series 2"]
FREQUENCIES = {"mensual", "trimestral", "semestral", "anual", "diaria", "diario", "irregular"}


def _text(value: object) -> str:
    return " ".join(str(value).split()).strip() if pd.notna(value) else ""


def _sheet_family(source: str, sheet: str, dataset: str) -> str:
    if source == "indec-ipc":
        if "gba" in sheet.casefold():
            return "IPC Gran Buenos Aires"
        if "nacional" in sheet.casefold():
            return "IPC cobertura nacional"
    description = re.sub(r"^Cuadro\s+\d+\s*:\s*", "", dataset, flags=re.IGNORECASE).casefold()
    if source == "indec-cin":
        if "deuda externa" in description:
            return "Deuda externa"
        if "inversión internacional" in description or "inversion internacional" in description:
            return "Posición de inversión internacional"
        if any(part in description for part in ("balanza de pagos", "cuenta corriente", "cuenta financiera")):
            return "Balanza de pagos"
    if source == "indec-pib":
        if description.startswith("oferta y demanda globales"):
            return "Oferta y demanda globales"
        if description.startswith("producto interno bruto"):
            return "Producto interno bruto"
        if description.startswith("formación bruta de capital fijo"):
            return "Formación bruta de capital fijo"
    if source == "bcra-pas":
        if sheet.startswith("Estra_dia"):
            return "Depósitos por estrato de monto"
        if sheet.startswith("Series_diarias"):
            return "Depósitos por plazo y tipo de entidad"
    return ""


def _title_parts(source: str, workbook: str, sheet: str, title: str, variable: str) -> tuple[str, ...]:
    concept = title.split(" | ", 1)[0].casefold().strip()
    if source == "datos.gob.ar" and workbook == "finanzas_publicas.xlsx":
        if concept.startswith(("erogaciones", "total de erogaciones")):
            for detail in ("corrientes", "de capital", "figurativas"):
                if concept.startswith(f"erogaciones {detail}"):
                    return "Erogaciones", detail.capitalize()
            return ("Erogaciones",)
        if concept.startswith(("financiamiento", "necesidad de financiamiento",
                               "aumentos pasivos")):
            return ("Financiamiento",)
        if concept.startswith("total de recursos"):
            return ("Recursos",)
        for initial, group in (("ingresos", "Ingresos"), ("recursos", "Recursos"),
                               ("gastos", "Gastos"), ("gasto ", "Gastos"),
                               ("aplicaciones financieras", "Aplicaciones financieras"),
                               ("fuentes financieras", "Fuentes financieras"),
                               ("contribuciones figurativas", "Contribuciones figurativas"),
                               ("resultado", "Resultados"), ("ahorro", "Resultados"),
                               ("superavit", "Resultados"),
                               ("superávit", "Resultados")):
            if concept.startswith(initial):
                if group in {"Ingresos", "Gastos"}:
                    for detail in ("corrientes", "de capital", "figurativos", "primarios"):
                        if concept.startswith(f"{initial.rstrip()} {detail}"):
                            return group, detail.capitalize()
                return (group,)
        return ()
    if source == "datos.gob.ar" and sheet == "8.14 Situacion Patrimonial":
        entity = _text(title.split(".", 1)[0])
        if entity in {"Sistema Financiero", "Bancos Privados Extranjeros",
                      "Bancos Privados Nacionales", "Bancos Privados", "Bancos Públicos",
                      "Entidades Financieras No Bancarias"}:
            return (entity,)
    if source == "datos.gob.ar" and sheet in {
        "8.12 Indicadores financieros", "8.13 Rentabilidad financiera",
    }:
        for prefix, group in (
            ("Bancos Privados extranjeros", "Bancos privados extranjeros"),
            ("Bancos Privados nacionales", "Bancos privados nacionales"),
            ("Bancos ext priv", "Bancos privados extranjeros"),
            ("Bancos priv nac", "Bancos privados nacionales"),
            ("Entidades Financieras no", "Entidades financieras no bancarias"),
            ("Datos enti finan no banc", "Entidades financieras no bancarias"),
            ("Bancos Privados", "Bancos privados"),
            ("Bancos priv", "Bancos privados"),
            ("Bancos Públicos", "Bancos públicos"),
            ("Bancos pub", "Bancos públicos"),
            ("Sistema Financiero", "Sistema financiero"),
            ("Sist financ", "Sistema financiero"),
        ):
            if concept.startswith(prefix.casefold()):
                return (group,)
    if source == "datos.gob.ar" and sheet == "8.9 Balance BCRA":
        if concept.startswith(("apertura activo", "apertura de otros activos", "total del activo")):
            return ("Activo",)
        if concept.startswith(("apertura pasivo", "total del pasivo", "total pasivo")):
            return ("Pasivo",)
        if concept.startswith("total patrimonio neto"):
            return ("Patrimonio neto",)
    if source == "datos.gob.ar" and sheet == "1.17 Productos ind.":
        return (title.split(" | ", 1)[0].strip(),)
    if source == "datos.gob.ar" and sheet == "4.1.2 IPC Capitulos":
        region = re.search(r"\.\s*(Región\s+[^.]+|GBA|Nacional|Patagonia|Cuyo)\.\s*Base", title,
                           re.IGNORECASE)
        if region:
            return (region.group(1).strip(),)
    if source == "indec-supermercados" and sheet in {"Cuadro 5.", "Cuadro 6."}:
        pieces = title.rsplit(" - ", 3)
        if len(pieces) == 4 and pieces[1].strip():
            return (pieces[1].strip(),)
    if source == "indec-cin" and sheet == "Cuadro 14":
        code = re.match(r"^([123])(?:\.|\s)", concept)
        if code:
            return ({"1": "Cuenta corriente", "2": "Cuenta de capital",
                     "3": "Cuenta financiera"}[code.group(1)],)
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
        family = _sheet_family(source, sheet, _text(row.get("Título dataset"))) if sheet else ""
        parts = _title_parts(source, workbook, sheet, title, _text(row.get("Variable")))
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
