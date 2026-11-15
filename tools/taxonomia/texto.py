"""Utilidades de texto para armar nombres de series parejos entre fuentes."""

from __future__ import annotations

import re
import unicodedata

SEPARADOR = " · "

# Siglas y nombres propios que conservan su grafía dentro de una oración.
SIGLAS = (
    "IPC", "IPIM", "IPIB", "IPP", "ICC", "PIB", "VAB", "VBP", "GBA", "CABA", "NOA", "NEA",
    "INDEC", "BCRA", "REM", "CBA", "CBT", "FOB", "CIF", "EMAE", "IVA", "FMI", "BLS",
    "US$", "USD", "DEG", "SIPM", "IPI", "ISAC", "EE. UU.",
)
_SIGLAS_POR_CLAVE = {sigla.casefold(): sigla for sigla in SIGLAS}
_NOMBRES_PROPIOS = (
    "Buenos Aires", "Gran Buenos Aires", "Ciudad de Buenos Aires", "Córdoba", "Mendoza", "Neuquén",
    "San Luis", "Santa Fe", "Tucumán", "Chaco", "Gran Resistencia", "Argentina", "Estados Unidos",
    "Brasil", "China", "Cuyo", "Patagonia", "Pampeana", "Noreste", "Noroeste", "Capital Federal",
)


def plano(valor: str) -> str:
    """Minúsculas sin tildes ni espacios repetidos, para comparar."""
    sin_tildes = unicodedata.normalize("NFKD", valor).encode("ascii", "ignore").decode()
    return " ".join(sin_tildes.casefold().split())


def limpiar(valor: str) -> str:
    """Espacios parejos y sin puntuación sobrante en los extremos."""
    texto = " ".join(str(valor).split())
    texto = re.sub(r"\s+([,.;:])", r"\1", texto)
    return texto.strip(" .,;:-–—·")


def oracion(valor: str) -> str:
    """Mayúscula inicial y el resto en minúsculas, respetando siglas y nombres propios.

    Sirve para textos que la fuente publica EN MAYÚSCULAS o Con Mayúsculas En Cada Palabra.
    """
    texto = limpiar(valor)
    if not texto:
        return ""
    palabras = []
    for palabra in texto.split(" "):
        nucleo = palabra.strip("()[],.;:")
        sigla = _SIGLAS_POR_CLAVE.get(nucleo.casefold())
        palabras.append(palabra.replace(nucleo, sigla) if sigla else palabra.lower())
    resultado = " ".join(palabras)
    for propio in sorted(_NOMBRES_PROPIOS, key=len, reverse=True):
        resultado = re.sub(re.escape(propio), propio, resultado, flags=re.IGNORECASE)
    return resultado[:1].upper() + resultado[1:]


def unir(*partes: str) -> str:
    """«Indicador · apertura · cobertura», salteando las partes vacías."""
    return SEPARADOR.join(parte for parte in (limpiar(p) for p in partes) if parte)


def con_medida(nombre: str, medida: str) -> str:
    """Agrega la medida entre paréntesis cuando la serie no es el nivel del indicador."""
    return f"{nombre} ({medida})" if medida else nombre
