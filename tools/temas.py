"""Taxonomía controlada de temas del inventario público."""

import re

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


# Activos de reserva (BPM6, cap. 6): se registran en la cuenta financiera y en la
# posición de inversión internacional, igual que las series de INDEC. Los pasivos y
# las contrapartidas del balance del BCRA siguen en «Moneda y sistema financiero».
_RESERVAS_BCRA = re.compile(
    r"^(?:apertura activo\. (?:apertura de )?reservas internacionales"
    r"|reservas internacionales (?:del bcra|bcra)"
    r"|saldo de (?:reservas internacionales|oro, divisas|divisas-pase pasivo)"
    r"|total de variación diaria de las reservas"
    r"|variación diaria de reservas internacionales)"
    r"|variación (?:contable )?de reservas internacionales",
    re.IGNORECASE,
)


def unificar_tema_reservas(inventario: pd.DataFrame) -> pd.DataFrame:
    """Lleva a «Sector externo» las series BCRA que miden saldo, composición o variación de reservas."""
    es_reserva = inventario["Institución"].astype(str).eq("BCRA") & inventario["Nombre serie"].astype(str).str.contains(
        _RESERVAS_BCRA, regex=True
    )
    inventario.loc[es_reserva, "Tema"] = "Sector externo"
    return inventario
