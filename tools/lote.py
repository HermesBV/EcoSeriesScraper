"""Lote de guardado: una sola escritura de ``BD.xlsx`` e ``IndiceSeries.xlsx`` por corrida.

Cada scraper guarda con ``guardar_datos_preservando_formato`` y ``generar``;
sueltos, escriben en el acto. Dentro de ``abrir()`` (lo usa ``main.py``) las
hojas y el inventario se acumulan, fusionados contra lo ya publicado
exactamente como lo haría cada guardado individual, y se vuelcan una vez al
salir del bloque, también si algún scraper falló. Los scrapers que leen hojas
de otros (IIEP) pasan por ``leer_hoja`` y ven lo pendiente.

Las hojas pendientes se guardan en disco (``.lote/``, un pickle por hoja) y no
en memoria: la base completa en pandas más el libro openpyxl del volcado no
entran juntos en 16 GB.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
import os
import re
import shutil
import time
from typing import Callable, Iterator

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
CARPETA_LOTE = RAIZ / ".lote"


def _nombre_archivo(hoja: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", hoja) + ".pkl"


class HojasEnDisco(Mapping):
    """Diccionario hoja -> DataFrame que carga cada pickle al pedirlo."""

    def __init__(self, carpeta: Path) -> None:
        self.carpeta = carpeta
        self._rutas: dict[str, Path] = {}

    def guardar(self, hoja: str, datos: pd.DataFrame) -> None:
        ruta = self.carpeta / _nombre_archivo(hoja)
        datos.to_pickle(ruta)
        self._rutas[hoja] = ruta

    def __getitem__(self, hoja: str) -> pd.DataFrame:
        return pd.read_pickle(self._rutas[hoja])

    def __iter__(self) -> Iterator[str]:
        return iter(self._rutas)

    def __len__(self) -> int:
        return len(self._rutas)


class Lote:
    def __init__(self, carpeta: Path | None = None) -> None:
        # Carpeta propia del proceso: una corrida y las pruebas (u otra corrida)
        # no se borran las hojas pendientes entre sí.
        self.carpeta = carpeta or CARPETA_LOTE / str(os.getpid())
        shutil.rmtree(self.carpeta, ignore_errors=True)
        self.carpeta.mkdir(parents=True, exist_ok=True)
        self.ruta_bd: Path | None = None
        self.hojas = HojasEnDisco(self.carpeta)
        self.frecuencias: dict[str, str] = {}
        self.obsoletas: set[str] = set()
        self.inventario: pd.DataFrame | None = None
        self._libro: pd.ExcelFile | None = None

    def _abrir_bd(self, ruta: Path) -> pd.ExcelFile | None:
        if self._libro is None and ruta.is_file():
            self.ruta_bd = ruta
            self._libro = pd.ExcelFile(ruta)
        return self._libro

    def leer_hoja(self, ruta: Path, nombre: str) -> pd.DataFrame:
        nombre = nombre.strip()[:31]
        if nombre in self.hojas:
            return self.hojas[nombre]
        libro = self._abrir_bd(ruta)
        if libro is None or nombre not in libro.sheet_names:
            raise ValueError(f"La hoja {nombre!r} no existe en {ruta}")
        return pd.read_excel(libro, sheet_name=nombre)

    def encolar(self, ruta: Path, hojas: Mapping[str, pd.DataFrame], frecuencias: dict[str, str],
                obsoletas: set[str], fusionar: Callable[[pd.DataFrame, pd.DataFrame, str], pd.DataFrame]) -> None:
        self.ruta_bd = ruta
        libro = self._abrir_bd(ruta)
        for nombre, datos in hojas.items():
            nombre_hoja = str(nombre).strip()[:31]
            if nombre_hoja in self.hojas:
                anterior = self.hojas[nombre_hoja]
            elif libro is not None and nombre_hoja in libro.sheet_names:
                anterior = pd.read_excel(libro, sheet_name=nombre_hoja)
            else:
                anterior = None
            if anterior is not None and len(anterior.columns):
                if "fecha" not in anterior.columns:
                    raise ValueError(f"La hoja {nombre_hoja} existente no tiene encabezado fecha")
                datos = fusionar(anterior, datos, frecuencias.get(nombre, "D"))
            self.hojas.guardar(nombre_hoja, datos)
            self.frecuencias[nombre_hoja] = frecuencias.get(nombre, "D")
            self.obsoletas.discard(nombre_hoja)
        # Las hojas propias que la entrega no trajo se conservan (ver
        # scraper_IED.avisar_hojas_obsoletas); sólo se anotan para el aviso.
        self.obsoletas |= {str(n).strip()[:31] for n in obsoletas} - set(self.hojas)

    def cerrar_lectura(self) -> None:
        if self._libro is not None:
            self._libro.close()
            self._libro = None

    def volcar(self) -> dict[str, int]:
        """Escribe lo acumulado; devuelve cuántas hojas y series publicó."""
        from scrapers.scraper_IED import guardar_datos_preservando_formato
        from tools.generar_codificacion import escribir_indice
        self.cerrar_lectura()
        resumen = {"hojas": 0, "series": 0}
        inicio = time.time()
        if len(self.hojas) and self.ruta_bd is not None:
            guardar_datos_preservando_formato(self.ruta_bd, self.hojas, self.frecuencias, self.obsoletas)
            resumen["hojas"] = len(self.hojas)
        if self.inventario is not None:
            escribir_indice(self.inventario)
            resumen["series"] = len(self.inventario)
        resumen["segundos"] = round(time.time() - inicio)
        return resumen

    def limpiar(self) -> None:
        self.cerrar_lectura()
        shutil.rmtree(self.carpeta, ignore_errors=True)


_actual: Lote | None = None


def activo() -> bool:
    return _actual is not None


def actual() -> Lote:
    if _actual is None:
        raise RuntimeError("No hay un lote abierto")
    return _actual


@contextmanager
def abrir(carpeta: Path | None = None) -> Iterator[Lote]:
    """Acumula los guardados del bloque y los vuelca al salir, incluso con errores."""
    global _actual
    if _actual is not None:
        raise RuntimeError("Ya hay un lote abierto")
    _actual = Lote(carpeta)
    try:
        yield _actual
    finally:
        lote, _actual = _actual, None
        try:
            resumen = lote.volcar()
            print(f"Lote volcado: {resumen['hojas']} hojas, {resumen['series']} series en {resumen['segundos']} s", flush=True)
        finally:
            lote.limpiar()


def leer_hoja(ruta: Path, nombre: str) -> pd.DataFrame:
    """Hoja de BD.xlsx como la vería el próximo guardado (pendiente o en disco)."""
    if _actual is not None:
        return _actual.leer_hoja(ruta, nombre)
    return pd.read_excel(ruta, sheet_name=nombre)
