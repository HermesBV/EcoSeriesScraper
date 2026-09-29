"""Títulos y valoración legibles para el inventario de series."""

from __future__ import annotations

import re
import unicodedata

import pandas as pd


def _text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value)).strip() if pd.notna(value) else ""


def _plain(value: object) -> str:
    return unicodedata.normalize("NFKD", _text(value)).encode("ascii", "ignore").decode().casefold()


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
    # El título es descriptivo y puede repetirse; la identidad está en
    # (Código fuente, ID). Conservar el nombre provisto por cada fuente.
    return mejorar_valoracion(inventory)
