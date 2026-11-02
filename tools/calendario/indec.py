"""Calendario de difusión del INDEC.

La web arma el calendario por JavaScript desde ``/Calendario/FiltrosCalendario``,
que devuelve por mes un fragmento HTML con día, día de la semana, título y hora.
El período de referencia está al final del título («Índice de precios al
consumidor (IPC). Cobertura nacional. Septiembre de 2026»).
"""

from __future__ import annotations

from datetime import date, time
import html
import re

from tools.calendario.modelo import CABECERAS, MESES, Lectura, Publicacion, periodo_del_titulo, sesion

INSTITUCION = "INDEC"
URL_MES = "https://www.indec.gob.ar/Calendario/FiltrosCalendario/mes/{mes}-{anio}/1"
PAGINA = "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"
DIAS = {"LUNES", "MARTES", "MIÉRCOLES", "JUEVES", "VIERNES", "SÁBADO", "DOMINGO"}

# Código fuente -> comienzo del título en el calendario. Al sumar una fuente
# INDEC, agregar su título acá para que el estado la controle.
PUBLICACIONES = {
    "indec-ipc": r"Índice de precios al consumidor \(IPC\)",
    "indec-emae": r"Estimador mensual de actividad económica",
    "indec-isac": r"Indicadores de coyuntura de la actividad de la construcción",
    "indec-ipi-manufacturero": r"Índice de producción industrial manufacturero",
    "indec-sipm": r"Sistema de índices de precios mayoristas",
    "indec-supermercados": r"Encuesta de supermercados",
    "indec-ica": r"Intercambio comercial argentino \(ICA\)",
    "indec-salarios": r"Índice de salarios",
    "indec-pib": r"Informe de avance del nivel de actividad",
    "indec-cin": r"Balanza de pagos, posición de inversión internacional",
}


def codigos_del_titulo(titulo: str) -> tuple[str, ...]:
    return tuple(codigo for codigo, patron in PUBLICACIONES.items() if re.match(patron, titulo, re.I))


def leer_mes(contenido: str, anio: int, mes: int) -> list[Publicacion]:
    """Todas las difusiones de un mes; las de fuentes propias llevan sus códigos."""
    contenido = re.sub(r"<(script|style)\b.*?</\1>", "", contenido, flags=re.S | re.I)
    textos = [t.strip() for t in html.unescape(re.sub(r"<[^>]+>", "|", contenido)).split("|")]
    resultado: list[Publicacion] = []
    dia = None
    for texto in filter(None, textos):
        if re.fullmatch(r"\d{1,2}", texto):
            dia = int(texto)
        elif hora := re.fullmatch(r"(\d{1,2}):(\d{2})\s*hs?", texto, re.I):
            if resultado and resultado[-1].hora is None and resultado[-1].fecha.day == dia:
                resultado[-1] = resultado[-1].con_hora(time(int(hora[1]), int(hora[2])))
        elif dia is not None and texto.upper() not in DIAS:
            resultado.append(Publicacion(INSTITUCION, texto, date(anio, mes, dia), None,
                                         periodo_del_titulo(texto), codigos_del_titulo(texto), PAGINA))
    return resultado


def descargar(hoy: date, antes: int = 3, despues: int = 3) -> Lectura:
    """Meses del calendario entre ``antes`` meses atrás y ``despues`` adelante."""
    publicaciones: list[Publicacion] = []
    base = hoy.year * 12 + hoy.month - 1
    with sesion() as cliente:
        for desplazamiento in range(-antes, despues + 1):
            anio, mes0 = divmod(base + desplazamiento, 12)
            respuesta = cliente.get(URL_MES.format(mes=MESES[mes0], anio=anio), timeout=(15, 60), headers=CABECERAS)
            respuesta.raise_for_status()
            respuesta.encoding = "utf-8"
            publicaciones.extend(leer_mes(respuesta.text, anio, mes0 + 1))
    inicio_anio, inicio_mes0 = divmod(base - antes, 12)
    fin_anio, fin_mes0 = divmod(base + despues + 1, 12)
    return Lectura(INSTITUCION, PAGINA, publicaciones, date(inicio_anio, inicio_mes0 + 1, 1),
                   date(fin_anio, fin_mes0 + 1, 1))
