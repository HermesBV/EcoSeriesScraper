"""Calendario de difusión del U.S. Bureau of Labor Statistics (``bls.ics``).

Se conserva sólo el CPI, que la base toma de FRED (CPIAUCSL) el mismo día. El
BLS publica a las 8:30 de Washington el CPI del mes anterior.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import re

import pandas as pd

from tools.calendario.modelo import CABECERAS, MESES, Lectura, Publicacion, sesion

INSTITUCION = "U.S. Bureau of Labor Statistics"
URL = "https://www.bls.gov/schedule/news_release/bls.ics"
WASHINGTON = ZoneInfo("America/New_York")
ARGENTINA = ZoneInfo("America/Argentina/Buenos_Aires")
CODIGOS = {"Consumer Price Index": ("fred-cpiaucsl",)}


def leer_ics(contenido: str) -> list[Publicacion]:
    contenido = re.sub(r"\r?\n[ \t]", "", contenido)  # Despliega las líneas continuadas.
    publicaciones = []
    for evento in re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", contenido, flags=re.S):
        campos = dict(re.findall(r"^([A-Z-]+)(?:;[^:\r\n]*)?:(.*?)\r?$", evento, flags=re.M))
        resumen = campos.get("SUMMARY", "").replace("\\,", ",").strip()
        if resumen not in CODIGOS or not re.fullmatch(r"\d{8}T\d{6}", campos.get("DTSTART", "")):
            continue
        local = datetime.strptime(campos["DTSTART"], "%Y%m%dT%H%M%S").replace(tzinfo=WASHINGTON).astimezone(ARGENTINA)
        anterior = pd.Timestamp(local.year, local.month, 1) - pd.DateOffset(months=1)
        titulo = f"{resumen}. {MESES[anterior.month - 1]} de {anterior.year}"
        publicaciones.append(Publicacion(INSTITUCION, titulo, local.date(), local.time().replace(tzinfo=None),
                                         anterior, CODIGOS[resumen], URL))
    return publicaciones


def descargar(hoy: date) -> Lectura:
    with sesion() as cliente:
        respuesta = cliente.get(URL, timeout=(15, 60), headers=CABECERAS)
        respuesta.raise_for_status()
    publicaciones = leer_ics(respuesta.text)
    if not publicaciones:
        raise ValueError("El calendario del BLS no trajo publicaciones del CPI")
    fechas = [p.fecha for p in publicaciones]
    return Lectura(INSTITUCION, URL, publicaciones, min(fechas), max(fechas) + timedelta(days=1))
