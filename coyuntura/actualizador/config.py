"""Rutas y nombres fijos del Excel de coyuntura."""

from __future__ import annotations

import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
CARPETA = RAIZ / "coyuntura"
NOMBRE_BASE = "coyuntura iiep reemplazar"
ARCHIVO = CARPETA / f"{NOMBRE_BASE}.xlsx"
CARPETA_BKP = CARPETA / "bkp"
CARPETA_LOGS = CARPETA / "logs"
CARPETA_TRABAJO = CARPETA / ".trabajo"
BD_XLSX = RAIZ / "BD.xlsx"
INDICE_XLSX = RAIZ / "IndiceSeries.xlsx"
# Copia que publica SeriesMacro (descarga con «abrete sesamo»). Se omite si el
# repositorio hermano no está en la misma carpeta que éste.
# ``SERIESMACRO_DIR`` indica la carpeta de SeriesMacro cuando no es la hermana.
DESTINO_WEB = Path(os.environ.get("SERIESMACRO_DIR") or RAIZ.parent / "SeriesMacro") / "coyuntura" / f"{NOMBRE_BASE}.xlsx"
# Respaldos: los Excel del mes en curso quedan sueltos; al cerrar un mes se
# comprimen en «<nombre> AAMM.zip» y se conservan sólo los dos zip más nuevos.
ZIPS_A_CONSERVAR = 2
