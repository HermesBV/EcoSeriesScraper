"""Taxonomía controlada de temas del inventario público."""

import pandas as pd

TEMAS = (
    "Actividad",
    "Economía internacional",
    "Finanzas públicas",
    "Mercados financieros",
    "Moneda y sistema financiero",
    "Precios",
    "Sector externo",
    "Trabajo e ingresos",
)

_CANONICOS = {tema.casefold(): tema for tema in TEMAS}
_ALIAS = {
    "riesgo país": "Mercados financieros",
    "salarios": "Trabajo e ingresos",
    "tipo de cambio": "Sector externo",
    "trabajo": "Trabajo e ingresos",
}


def normalizar_tema(value: object) -> str:
    """Usa un tema existente o exige revisar una etiqueta nueva."""
    if pd.isna(value):
        return "Sin clasificar"
    text = " ".join(str(value).split()).strip()
    if not text or text.casefold() in {"nan", "none", "sin clasificar"}:
        return "Sin clasificar"
    key = text.casefold()
    if key in _ALIAS:
        return _ALIAS[key]
    if key in _CANONICOS:
        return _CANONICOS[key]
    raise ValueError(
        f"Tema no reconocido: {text!r}. Revisar si corresponde a uno de los ocho temas existentes "
        "antes de ampliar la taxonomía."
    )
