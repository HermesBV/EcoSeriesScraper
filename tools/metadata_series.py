"""Títulos y valoración legibles para el inventario de series."""

from __future__ import annotations

import re
import unicodedata

import pandas as pd


def _text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value)).strip() if pd.notna(value) else ""


def _plain(value: object) -> str:
    return unicodedata.normalize("NFKD", _text(value)).encode("ascii", "ignore").decode().casefold()


def _descriptor(row: pd.Series) -> str:
    description = _text(row.get("Descripción"))
    description = re.sub(r"^Datos (?:de )?(?=exportaciones|importaciones)", "", description, flags=re.I).strip()
    description = re.split(r"\.\s*(?:En |Metodolog[ií]a |Base \d|Fuente:)", description, maxsplit=1, flags=re.I)[0]
    description = description.strip(" .;:-")
    title = _text(row.get("Nombre serie"))
    if not description or _plain(description) == _plain(title):
        description = _text(row.get("Variable")).replace("_", " ")
    if len(description) > 170:
        shorter = description[:170].rsplit(" ", 1)[0]
        description = shorter if len(shorter) >= 80 else description
    return description.strip(" .;:-")


def mejorar_titulos(inventory: pd.DataFrame) -> pd.DataFrame:
    """Pone la variable concreta en cada título compartido por varias series."""
    result = inventory.copy()
    titles = result["Nombre serie"].fillna("").astype(str).str.strip()
    repeated = titles.ne("") & titles.duplicated(keep=False)
    for index in result.index[repeated]:
        descriptor = _descriptor(result.loc[index])
        if descriptor and _plain(descriptor) not in _plain(titles.loc[index]):
            result.at[index, "Nombre serie"] = f"{descriptor} | {titles.loc[index]}"
    # Si la descripción publicada se repite, la variable suele identificar la
    # desagregación (provincia, rubro, país, etc.).
    updated = result["Nombre serie"].fillna("").astype(str)
    repeated = updated.ne("") & updated.duplicated(keep=False)
    for index in result.index[repeated]:
        variable = _text(result.at[index, "Variable"]).replace("_", " ")
        if variable and _plain(variable) not in _plain(updated.loc[index]):
            result.at[index, "Nombre serie"] = f"{updated.loc[index]} | {variable}"
    updated = result["Nombre serie"].fillna("").astype(str)
    repeated = updated.ne("") & updated.duplicated(keep=False)
    frequency_names = {"A": "anual", "S": "semestral", "T": "trimestral", "M": "mensual", "D": "diaria", "I": "irregular"}
    for index in result.index[repeated]:
        frequency = frequency_names.get(_text(result.at[index, "Frecuencia"]))
        if frequency and frequency not in _plain(updated.loc[index]):
            result.at[index, "Nombre serie"] = f"{updated.loc[index]} | {frequency}"
    updated = result["Nombre serie"].fillna("").astype(str)
    repeated = updated.ne("") & updated.duplicated(keep=False)
    for index in result.index[repeated]:
        start, end = _text(result.at[index, "Desde"]), _text(result.at[index, "Hasta"])
        if re.fullmatch(r"(?:19|20)\d{2}", start) and re.fullmatch(r"(?:19|20)\d{2}", end):
            period = f"{start}-{end}"
            title = updated.loc[index]
            revised = re.sub(r"\((?:19|20)\d{2}[-–](?:19|20)\d{2}\)", f"({period})", title, count=1)
            result.at[index, "Nombre serie"] = revised if revised != title else f"{title} | {period}"
    return result


def mejorar_valoracion(inventory: pd.DataFrame) -> pd.DataFrame:
    """Completa precios corrientes/constantes sólo con evidencia del metadato."""
    result = inventory.copy()
    for index, row in result.loc[result["Valoración"].eq("No informado")].iterrows():
        source = _text(row.get("Código fuente"))
        title = _plain(row.get("Nombre serie"))
        unit = _plain(row.get("Unidad"))
        if source in {"indec-ipi-manufacturero", "indec-isac", "bcra-com3500", "bcra-bandas"}:
            result.at[index, "Valoración"] = "No aplica"
            continue
        leaf = title.rsplit(" - ", 1)[-1]
        if source == "indec-ica":
            result.at[index, "Valoración"] = "No aplica" if "var.%" in leaf else "Precios corrientes"
            continue
        if source == "indec-supermercados" and re.search(
            r"\bindice\b|variacion|porcentual|porcentaje|%", leaf
        ):
            result.at[index, "Valoración"] = "No aplica"
            continue
        if source == "indec-supermercados" and "indice de ventas" in title:
            result.at[index, "Valoración"] = "No aplica"
            continue
        monetary = any(mark in unit for mark in ("peso", "dolar", "usd", "ars", "moneda", "$", "u$s"))
        context = _plain(" ".join(_text(row.get(column)) for column in (
            "Nombre serie", "Variable", "Descripción", "Título dataset", "Hoja origen",
        )))
        constant = bool(re.search(
            r"precios? constantes?|pesos? constantes?|moneda constante|valores? constantes?|"
            r"a precios de (?:19|20)\d{2}|base (?:19|20)\d{2}.*(?:pesos|millones)|"
            r"volumen encadenado|\bpib real\b|\bvab real\b|\bvbp real\b",
            context,
        ))
        current = bool(re.search(
            r"precios? corrientes?|pesos? corrientes?|moneda corriente|valores? corrientes?|"
            r"\bnominal(?:es)?\b|\ba precios de mercado\b",
            context,
        ))
        if constant and not current:
            result.at[index, "Valoración"] = "Precios constantes"
            continue
        if current and not constant:
            result.at[index, "Valoración"] = "Precios corrientes"
            continue
        if not monetary:
            continue
        if constant or re.search(r"\breal(?:es)?\b|deflactad", context):
            continue
        # Son importes observados en la contabilidad o en transacciones; su
        # significado nominal se desprende del concepto, sin suponer una base.
        nominal_concepts = (
            r"exportacion|importacion|saldo comercial|balanza comercial|"
            r"balance de pagos|inversion extranjera directa|posicion de inversion|"
            r"reservas internacionales|deuda publica|sector publico|tesoro nacional|"
            r"ingresos y gastos|recurso[s]? tributario|recaudacion|impuesto|"
            r"situacion patrimonial|balance cambiario|mercado de cambios|"
            r"depositos|prestamos|activos y pasivos|remuneracion|salario|haber|"
            r"canasta basica|tipo de cambio|precio[s]? al consumidor"
        )
        if re.search(nominal_concepts, context):
            result.at[index, "Valoración"] = "Precios corrientes"
        elif _text(row.get("Código fuente")) in {"bcra-pas", "bcra-dmd"}:
            result.at[index, "Valoración"] = "Precios corrientes"
    return result


def mejorar_metadatos(inventory: pd.DataFrame) -> pd.DataFrame:
    return mejorar_valoracion(mejorar_titulos(inventory))
