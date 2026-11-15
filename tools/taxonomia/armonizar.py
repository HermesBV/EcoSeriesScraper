"""Acuerdos entre módulos de reglas.

Cada módulo clasifica sus fuentes por separado. Acá se resuelve lo que cruza más de
un módulo, para que un mismo indicador quede con un solo árbol y una sola grafía sin
repetir la decisión en cada archivo. Se aplica después de la regla del módulo.
"""

from __future__ import annotations

import re
from dataclasses import replace

from . import Clasificacion, Serie
from .texto import SEPARADOR

# Sectores de actividad: una sola denominación para EMAE, PIB, precios implícitos y
# cuentas de ingreso (la de los cuadros de cuentas nacionales del INDEC, con la sección
# L abreviada para que los nombres no pasen de 120 caracteres).
SECTORES = {
    "Comercio mayorista y minorista y reparaciones": "Comercio mayorista, minorista y reparaciones",
    "Suministro de electricidad, gas y agua": "Electricidad, gas y agua",
    "Transporte y comunicaciones": "Transporte, almacenamiento y comunicaciones",
    "Administración pública y defensa; planes de seguridad social de afiliación obligatoria":
        "Administración pública y defensa",
    "Otras actividades de servicios comunitarios, sociales y personales":
        "Otras actividades de servicios comunitarias, sociales y personales",
}
TIPO_DE_CAMBIO = "Tipo de cambio nominal"
OFICIAL = "Oficial"
PARALELOS = "Paralelos y financieros"
_COBERTURA_TC = {"Oficial mayorista": OFICIAL, "Dólar blue": PARALELOS, "Dólar MEP": PARALELOS}


def _segmentos(nombre: str) -> tuple[list[str], str]:
    """Partes del nombre y la medida final entre paréntesis (si la hay)."""
    medida = ""
    coincidencia = re.search(r"\s(\([^()]*\))$", nombre)
    if coincidencia:
        medida, nombre = " " + coincidencia.group(1), nombre[:coincidencia.start()]
    return nombre.split(SEPARADOR), medida


def _tipo_de_cambio(serie: Serie, c: Clasificacion) -> Clasificacion:
    # Todas las cotizaciones se llaman «Tipo de cambio · …» y se agrupan por mercado:
    # oficial, paralelos y financieros, bandas, otras monedas y regímenes históricos.
    nombre = re.sub(r"^Tipo de cambio nominal\b", "Tipo de cambio", c.nombre)
    nombre = nombre.replace("(A 3500) (promedio)", "(A 3500, promedio)")
    cobertura = _COBERTURA_TC.get(c.cobertura, c.cobertura)
    if not cobertura:
        cobertura = PARALELOS if "Implícito en ADR" in nombre else OFICIAL
    unidad = "Pesos por dólar" if c.unidad == "Pesos por US$" else c.unidad
    return replace(c, nombre=nombre, cobertura=cobertura, unidad=unidad)


def armonizar(serie: Serie, c: Clasificacion) -> Clasificacion:
    if c.indicador == TIPO_DE_CAMBIO:
        return _tipo_de_cambio(serie, c)
    if c.indicador == "PIB" and c.nombre == "PIB · Tipo de cambio nominal":
        # La hoja del PIB en dólares publica el tipo de cambio que usa para convertir.
        return replace(c, tema="Sector externo", indicador=TIPO_DE_CAMBIO, cobertura=OFICIAL, apertura="",
                       medida="", nombre="Tipo de cambio · Promedio de las cuentas nacionales")
    if c.indicador == "PIB" and c.nombre == "PIB · Población":
        return replace(c, nombre="Población")
    if c.indicador == "EMAE" and c.cobertura == "Sectores":
        c = replace(c, cobertura="Por sector de actividad")
    partes, medida = _segmentos(c.nombre)
    if any(parte in SECTORES for parte in partes):
        c = replace(c, nombre=SEPARADOR.join(SECTORES.get(parte, parte) for parte in partes) + medida)
    return c
