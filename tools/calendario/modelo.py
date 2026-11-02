"""Tipos y utilidades comunes a los lectores de calendarios."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, time
import re

import pandas as pd
import requests

CABECERAS = {"User-Agent": "SeriesScraper/1.0 (calendario de publicaciones)"}
MESES = ("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
         "Septiembre", "Octubre", "Noviembre", "Diciembre")
TRIMESTRES = ("primer", "segundo", "tercer", "cuarto")


@dataclass(frozen=True)
class Publicacion:
    """Una difusión anunciada. ``hora`` es hora argentina; ``periodo`` el primer día del período."""

    institucion: str
    titulo: str
    fecha: date
    hora: time | None
    periodo: pd.Timestamp | None
    codigos: tuple[str, ...]
    calendario: str

    @property
    def clave(self) -> tuple[str, str, str]:
        return self.institucion, self.titulo, self.fecha.isoformat()

    def con_hora(self, hora: time) -> Publicacion:
        return replace(self, hora=hora)

    def a_dict(self) -> dict:
        return {
            "institucion": self.institucion, "titulo": self.titulo, "fecha": self.fecha.isoformat(),
            "hora": self.hora.strftime("%H:%M") if self.hora else None,
            "periodo": self.periodo.strftime("%Y-%m-%d") if self.periodo is not None else None,
            "codigos": list(self.codigos), "calendario": self.calendario,
        }

    @classmethod
    def de_dict(cls, datos: dict) -> Publicacion:
        return cls(datos["institucion"], datos["titulo"], date.fromisoformat(datos["fecha"]),
                   time.fromisoformat(datos["hora"]) if datos.get("hora") else None,
                   pd.Timestamp(datos["periodo"]) if datos.get("periodo") else None,
                   tuple(datos.get("codigos") or ()), datos["calendario"])


@dataclass(frozen=True)
class Lectura:
    """Lo leído de un calendario: reemplaza lo guardado de ``institucion`` entre ``desde`` y ``hasta`` (excluido)."""

    institucion: str
    calendario: str
    publicaciones: list[Publicacion]
    desde: date
    hasta: date


def sesion() -> requests.Session:
    cliente = requests.Session()
    cliente.headers.update(CABECERAS)
    return cliente


def periodo_del_titulo(titulo: str) -> pd.Timestamp | None:
    """Primer mes del período de referencia con que cierra el título, si lo hay."""
    meses = "|".join(MESES)
    coincidencia = re.search(rf"\b({meses}) de (\d{{4}})\s*$", titulo, re.I)
    if coincidencia:
        mes = next(i for i, nombre in enumerate(MESES, 1) if nombre.casefold() == coincidencia[1].casefold())
        return pd.Timestamp(int(coincidencia[2]), mes, 1)
    coincidencia = re.search(rf"\b({'|'.join(TRIMESTRES)}) trimestre de (\d{{4}})\s*$", titulo, re.I)
    if coincidencia:
        return pd.Timestamp(int(coincidencia[2]), TRIMESTRES.index(coincidencia[1].casefold()) * 3 + 1, 1)
    return None
