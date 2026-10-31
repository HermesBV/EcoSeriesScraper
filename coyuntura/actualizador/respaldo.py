"""Respaldos fechados del Excel de coyuntura y su rotación mensual.

La carpeta ``bkp`` guarda una copia por actualización, con sufijo AAMMDD.
Cuando un mes queda atrás, sus copias se comprimen en ``<nombre> AAMM.zip`` y
se conservan únicamente los dos zip más recientes: nunca hay más de tres meses
respaldados (dos comprimidos y el que está en curso).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import re
import shutil
import zipfile

from . import config


def nombre_respaldo(dia: date) -> str:
    return f"{config.NOMBRE_BASE} {dia:%y%m%d}.xlsx"


def guardar_copia(origen: Path, dia: date, carpeta: Path = config.CARPETA_BKP) -> Path:
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / nombre_respaldo(dia)
    shutil.copy2(origen, destino)
    return destino


def _mes_de(path: Path) -> str | None:
    coincidencia = re.fullmatch(rf"{re.escape(config.NOMBRE_BASE)} (\d{{4}})\d{{2}}\.xlsx", path.name, re.I)
    return coincidencia[1] if coincidencia else None


def rotar(hoy: date, carpeta: Path = config.CARPETA_BKP, conservar: int = config.ZIPS_A_CONSERVAR) -> dict[str, list[str]]:
    """Comprime los meses cerrados y borra los zip más viejos que ``conservar``."""
    resultado = {"comprimidos": [], "borrados": []}
    if not carpeta.is_dir():
        return resultado
    mes_actual = f"{hoy:%y%m}"
    por_mes: dict[str, list[Path]] = {}
    for path in carpeta.glob("*.xlsx"):
        mes = _mes_de(path)
        if mes and mes < mes_actual:
            por_mes.setdefault(mes, []).append(path)
    for mes, archivos in sorted(por_mes.items()):
        destino = carpeta / f"{config.NOMBRE_BASE} {mes}.zip"
        with zipfile.ZipFile(destino, "a", compression=zipfile.ZIP_DEFLATED) as archivo:
            existentes = set(archivo.namelist())
            for path in sorted(archivos):
                if path.name not in existentes:
                    archivo.write(path, arcname=path.name)
        for path in archivos:
            path.unlink()
        resultado["comprimidos"].append(destino.name)
    zips = sorted(carpeta.glob(f"{config.NOMBRE_BASE} ????.zip"), key=lambda p: p.name)
    for path in zips[:-conservar] if conservar else zips:
        path.unlink()
        resultado["borrados"].append(path.name)
    return resultado
