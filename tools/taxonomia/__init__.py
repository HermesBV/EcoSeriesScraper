"""Taxonomía del inventario: dónde se anida cada serie y cómo se llama.

Cada nivel significa siempre lo mismo (ver ``docs/taxonomia.md``):

    Tema  >  Indicador  >  Cobertura o versión  >  Apertura  >  Medida

y se guarda en las columnas ``Tema``, ``Área``, ``Subárea 1``, ``Subárea 2`` y
``Subárea 3``. El nombre de la serie dice qué mide; la frecuencia, la unidad, la
base, el estado y la institución tienen su propia columna y no van en el nombre.
Dos series que miden lo mismo llevan el mismo nombre.

Las reglas leen sólo campos que la fuente publica y que no cambian al reclasificar
(``Nombre fuente``, ``ID``, ``Variable``, ``Descripción``, ``Hoja origen``…), nunca
``Nombre serie`` ni la clasificación previa: aplicar la taxonomía dos veces da el
mismo resultado.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..temas import TEMAS

NIVELES = ("Área", "Subárea 1", "Subárea 2", "Subárea 3")
GRUPOS_HEREDADOS = ("Grupo de hojas", "Grupo de series 1", "Grupo de series 2")


def _texto(valor: object) -> str:
    return " ".join(str(valor).split()) if pd.notna(valor) else ""


@dataclass(frozen=True)
class Serie:
    """Lo que la fuente dice de una serie. Es la única entrada de las reglas."""

    fuente: str          # Código fuente
    id: str
    nombre: str          # Nombre fuente: el título tal como lo entrega la fuente
    variable: str
    descripcion: str
    unidad: str
    frecuencia: str      # A, S, T, M, D o I
    archivo: str         # Archivo origen
    hoja: str            # Hoja origen
    columna: str         # Columna BD
    dataset: str         # Título dataset
    estado: str          # VIGENTE, CERRADA o DISCONTINUADA


@dataclass(frozen=True)
class Clasificacion:
    """Lugar de una serie en el árbol y su nombre."""

    tema: str
    indicador: str
    nombre: str
    cobertura: str = ""
    apertura: str = ""
    medida: str = ""
    # Unidad legible cuando la fuente no la informa (p. ej. «Índice dic-2016=100»).
    unidad: str = ""

    def niveles(self) -> tuple[str, str, str, str]:
        return self.indicador, self.cobertura, self.apertura, self.medida


MODULOS = (
    "precios_ipc", "precios_ipc_aperturas", "precios_otros", "actividad_supermercados",
    "actividad_cuentas_nacionales",
    "actividad_industria",
    "actividad_otros",
    "trabajo_mercado",
    "trabajo_salarios",
    "finanzas_publicas_caja",
    "finanzas_publicas_otros",
    "externo_comercio_tablas",
    "externo_comercio",
    "externo_balance_pagos",
    "externo_cambiario",
    "moneda_bcra",
    "moneda_sistema_financiero",
    "moneda_depositos_tasas",
    "mercados_internacional",
)
_REGLAS: tuple | None = None


def _reglas():
    """Reglas de cada módulo, en orden. Se importan una vez (los módulos importan Serie de acá)."""
    global _REGLAS
    if _REGLAS is None:
        from importlib import import_module
        _REGLAS = tuple(import_module(f"{__name__}.{nombre}").clasificar for nombre in MODULOS)
    return _REGLAS


EXCLUIDA = "EXCLUIDA"
_CORRECCIONES: tuple[dict, dict] | None = None
_HOJAS_EXCLUIDAS: dict | None = None


def hojas_excluidas() -> dict:
    """Hojas que no se publican enteras: (fuente, archivo, hoja) -> (motivo, ID que sí se publican).

    Para hojas donde varias series tienen contenido erróneo comprobado y las demás no se
    pueden verificar. Se conservan sólo las series confirmadas contra otra fuente.
    """
    global _HOJAS_EXCLUIDAS
    if _HOJAS_EXCLUIDAS is None:
        from importlib import import_module
        hojas: dict[tuple[str, str, str], tuple[str, frozenset[str]]] = {}
        for nombre in MODULOS:
            hojas.update(getattr(import_module(f"{__name__}.{nombre}"), "HOJAS_EXCLUIDAS", {}))
        _HOJAS_EXCLUIDAS = hojas
    return _HOJAS_EXCLUIDAS


def correcciones() -> tuple[dict, dict]:
    """Correcciones explícitas que declaran los módulos, por (Código fuente, ID).

    ``EXCLUIDAS``: clave -> motivo. La serie queda en el índice con Estado EXCLUIDA y
    no se publica: no es una serie (una columna auxiliar del libro), es copia exacta
    de otra, o su contenido no corresponde a lo que la fuente declara y no se puede
    reparar. Los datos de ``BD.xlsx`` no se tocan.

    ``UNIDADES``: clave -> unidad. Reemplaza una unidad que la fuente informa mal.
    """
    global _CORRECCIONES
    if _CORRECCIONES is None:
        from importlib import import_module
        excluidas: dict[tuple[str, str], str] = {}
        unidades: dict[tuple[str, str], str] = {}
        for nombre in MODULOS:
            modulo = import_module(f"{__name__}.{nombre}")
            excluidas.update(getattr(modulo, "EXCLUIDAS", {}))
            unidades.update(getattr(modulo, "UNIDADES", {}))
        _CORRECCIONES = excluidas, unidades
    return _CORRECCIONES


def clasificar(serie: Serie) -> Clasificacion | None:
    """Primera regla que reconoce la serie; None si todavía no tiene regla."""
    for regla in _reglas():
        resultado = regla(serie)
        if resultado is not None:
            from .armonizar import armonizar
            resultado = armonizar(serie, resultado)
            validar(resultado, serie)
            return resultado
    return None


def validar(clasificacion: Clasificacion, serie: Serie) -> None:
    donde = f"{serie.fuente}::{serie.id}"
    if clasificacion.tema not in TEMAS:
        raise ValueError(f"{donde}: tema fuera de la taxonomía: {clasificacion.tema!r}")
    if not clasificacion.indicador.strip() or not clasificacion.nombre.strip():
        raise ValueError(f"{donde}: la clasificación necesita indicador y nombre")
    niveles = clasificacion.niveles()
    # Un nivel vacío corta la rama: no puede haber niveles informados por debajo.
    for posicion in range(1, len(niveles)):
        if niveles[posicion] and not niveles[posicion - 1]:
            raise ValueError(f"{donde}: hay un nivel vacío en medio de la rama: {niveles}")


def serie_de(fila: dict) -> Serie:
    return Serie(
        fuente=_texto(fila.get("Código fuente")), id=_texto(fila.get("ID")),
        nombre=_texto(fila.get("Nombre fuente")) or _texto(fila.get("Nombre serie")),
        variable=_texto(fila.get("Variable")), descripcion=_texto(fila.get("Descripción")),
        unidad=_texto(fila.get("Unidad")), frecuencia=_texto(fila.get("Frecuencia")),
        archivo=_texto(fila.get("Archivo origen")), hoja=_texto(fila.get("Hoja origen")),
        columna=_texto(fila.get("Columna BD")), dataset=_texto(fila.get("Título dataset")),
        estado=_texto(fila.get("Estado")),
    )


def aplicar(inventario: pd.DataFrame) -> pd.DataFrame:
    """Reclasifica y renombra las series que tienen regla; el resto queda como está."""
    resultado = inventario.copy()
    if resultado.empty:
        return resultado
    if "Nombre fuente" not in resultado:
        resultado["Nombre fuente"] = None
    # El nombre de la fuente se registra una sola vez, antes de renombrar.
    sin_registro = resultado["Nombre fuente"].isna() | resultado["Nombre fuente"].astype(str).str.strip().eq("")
    resultado.loc[sin_registro, "Nombre fuente"] = resultado.loc[sin_registro, "Nombre serie"]
    for columna in ("Tema", "Unidad", "Estado", *NIVELES, *GRUPOS_HEREDADOS):
        if columna not in resultado:
            resultado[columna] = None
        resultado[columna] = resultado[columna].astype(object)
    # «taxonomía»: niveles y nombre fijados por una regla; «fuente»: heredados de la fuente.
    resultado["Clasificación"] = "fuente"

    excluidas, unidades = correcciones()
    hojas = hojas_excluidas()
    for indice, fila in zip(resultado.index, resultado.to_dict("records")):
        serie = serie_de(fila)
        clave = (serie.fuente, serie.id)
        if clave in unidades:
            resultado.at[indice, "Unidad"] = unidades[clave]
        hoja = hojas.get((serie.fuente, serie.archivo, serie.hoja))
        if clave in excluidas or (hoja is not None and serie.id not in hoja[1]):
            resultado.at[indice, "Estado"] = EXCLUIDA
        elif _texto(fila.get("Estado")) == EXCLUIDA:
            # Dejó de estar excluida (se corrigió el problema): recupera el estado que le daría su scraper.
            fin = pd.to_datetime(fila.get("Fecha fin"), errors="coerce")
            reciente = pd.notna(fin) and fin >= pd.Timestamp.now().normalize() - pd.DateOffset(years=3)
            resultado.at[indice, "Estado"] = "VIGENTE" if reciente else "CERRADA"
        clasificacion = clasificar(serie)
        if clasificacion is None:
            continue
        resultado.at[indice, "Tema"] = clasificacion.tema
        resultado.at[indice, "Nombre serie"] = clasificacion.nombre
        for columna, valor in zip(NIVELES, clasificacion.niveles()):
            resultado.at[indice, columna] = valor or None
        if clasificacion.unidad and not _texto(fila.get("Unidad")) and clave not in unidades:
            resultado.at[indice, "Unidad"] = clasificacion.unidad
        # Las unidades empiezan con mayúscula en todo el inventario («Millones de dólares», «Índice»).
        unidad = _texto(resultado.at[indice, "Unidad"])
        if unidad and unidad[0].islower():
            resultado.at[indice, "Unidad"] = unidad[0].upper() + unidad[1:]
        resultado.at[indice, "Clasificación"] = "taxonomía"
        # Los grupos derivados de títulos y hojas ya no hacen falta: el árbol sale de los niveles.
        for columna in GRUPOS_HEREDADOS:
            resultado.at[indice, columna] = None
    return resultado
