"""Calendario anticipado de publicaciones del BCRA (compromisos NEDD/SDDS del FMI).

La página lista las publicaciones diarias (sin fechas: salen dentro de 24 o 48
horas hábiles), una tabla semanal y dos mensuales. Cada tabla trae pares
«fecha a la que se refiere la información / fecha de publicación» y va
precedida por el título de la publicación, en el mismo orden.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
import html
import re

import pandas as pd

from tools.calendario.modelo import CABECERAS, MESES, Lectura, Publicacion, sesion

INSTITUCION = "BCRA"
PAGINA = "https://www.bcra.gob.ar/normas-especiales-para-la-divulgacion-de-datos-fmi/"
MESES_CORTOS = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7, "ago": 8,
                "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dic": 12}

# Título publicado -> códigos fuente que dependen de esa publicación.
CODIGOS = {
    "planilla de reservas internacionales": ("bcra-irfcl", "iiep-rin"),
}


def _texto(fragmento: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", fragmento))).strip()


def _fecha_referencia(texto: str) -> pd.Timestamp | None:
    """Fecha de referencia, exacta («31/12/2025») o mensual («nov-25»)."""
    if coincidencia := re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", texto):
        return pd.Timestamp(int(coincidencia[3]), int(coincidencia[2]), int(coincidencia[1]))
    if (coincidencia := re.fullmatch(r"([a-z]+)\.?-(\d{2})", texto.casefold())) and coincidencia[1] in MESES_CORTOS:
        return pd.Timestamp(2000 + int(coincidencia[2]), MESES_CORTOS[coincidencia[1]], 1)
    return None


def _tabla(fragmento: str) -> list[tuple[pd.Timestamp, date]]:
    pares = []
    for fila in re.findall(r"<tr\b.*?</tr>", fragmento, flags=re.S | re.I):
        celdas = [_texto(celda) for celda in re.findall(r"<t[dh]\b.*?</t[dh]>", fila, flags=re.S | re.I)]
        for referencia, publicacion in zip(celdas[::2], celdas[1::2]):
            leida = _fecha_referencia(referencia)
            if leida and re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", publicacion):
                pares.append((leida, datetime.strptime(publicacion, "%d/%m/%Y").date()))
    return pares


def leer_pagina(contenido: str) -> list[Publicacion]:
    inicio = contenido.find("Calendario Anticipado")
    if inicio < 0:
        raise ValueError("La página del BCRA ya no tiene el «Calendario Anticipado de Publicaciones»")
    seccion = contenido[inicio:]
    fin = re.search(r"<h3[^>]*>\s*Referencias", seccion)
    seccion = seccion[:fin.start()] if fin else seccion
    titulos: list[str] = []
    publicaciones: list[Publicacion] = []
    periodicas = False
    for elemento in re.finditer(r"<h3\b.*?</h3>|<table\b.*?</table>|<p\b.*?</p>", seccion, flags=re.S | re.I):
        bloque = elemento.group(0)
        texto = _texto(bloque)
        if bloque[:3].lower() == "<h3":
            periodicas = periodicas or texto.casefold().startswith(("publicaciones semanales", "publicaciones mensuales"))
        elif bloque[:6].lower() == "<table":
            if not titulos:
                raise ValueError("Tabla del calendario del BCRA sin título")
            nombre = titulos.pop(0)
            codigos = next((c for clave, c in CODIGOS.items() if nombre.casefold().startswith(clave)), ())
            # La planilla de reservas se refiere a fin de mes; el balance semanal, a un día.
            semanal = nombre.casefold().startswith("estado resumido")
            for referencia, publicado in _tabla(bloque):
                rotulo = f"Datos al {referencia:%d/%m/%Y}" if semanal else f"{MESES[referencia.month - 1]} de {referencia.year}"
                publicaciones.append(Publicacion(
                    INSTITUCION, f"{nombre}. {rotulo}", publicado, None,
                    referencia if semanal else referencia.replace(day=1), codigos, PAGINA))
        elif periodicas and texto and not texto.startswith("*"):
            titulos.append(texto.rstrip("* ").strip())
    return publicaciones


def descargar(hoy: date) -> Lectura:
    with sesion() as cliente:
        respuesta = cliente.get(PAGINA, timeout=(15, 60), headers=CABECERAS)
        respuesta.raise_for_status()
        respuesta.encoding = "utf-8"
    publicaciones = leer_pagina(respuesta.text)
    if not publicaciones:
        raise ValueError("El calendario del BCRA no trajo fechas")
    fechas = [p.fecha for p in publicaciones]
    return Lectura(INSTITUCION, PAGINA, publicaciones, min(fechas), max(fechas) + timedelta(days=1))
