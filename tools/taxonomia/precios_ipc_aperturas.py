"""IPC: principales aperturas por región, del libro ``sh_ipc_aperturas.xls`` del INDEC.

Van al mismo indicador y a los mismos grupos que el resto del IPC (``precios_ipc``):
Precios > IPC > <región> > Principales aperturas, con el nombre «IPC · <apertura> · <región>»
y la medida entre paréntesis cuando no es el índice.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .precios_ipc import INDICE_2016, PRINCIPALES_APERTURAS, VAR_INTERANUAL, VAR_MENSUAL
from .texto import con_medida, unir

FUENTE = "indec-ipc-aperturas"
REGIONES = {"Región GBA": "GBA", "Región Pampeana": "Pampeana", "Región Noroeste": "Noroeste",
            "Región Noreste": "Noreste", "Región Cuyo": "Cuyo", "Región Patagonia": "Patagonia"}
# Hoja del libro -> (medida del nombre, unidad).
MEDIDAS = {
    "Índices aperturas": ("", INDICE_2016),
    "Variación mensual aperturas": (VAR_MENSUAL, "%"),
    "Var. interanual aperturas": (VAR_INTERANUAL, "%"),
}
_ROTULO = re.compile(r"^(Región \w+) - (.+)$")


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.hoja not in MEDIDAS:
        return None
    partes = _ROTULO.match(serie.columna or serie.nombre)
    if not partes or partes.group(1) not in REGIONES:
        return None
    region = REGIONES[partes.group(1)]
    medida, unidad = MEDIDAS[serie.hoja]
    return Clasificacion(tema="Precios", indicador="IPC", cobertura=region, apertura=PRINCIPALES_APERTURAS,
                         nombre=con_medida(unir("IPC", partes.group(2), region), medida), unidad=unidad)
