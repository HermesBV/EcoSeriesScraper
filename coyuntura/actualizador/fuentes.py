"""Lectura de las series de ``BD.xlsx`` por (Código fuente, ID) y su evaluación."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import config
from .mapeo import Anual, Fuente, Promedio, Serie, Suma, claves_de


class Fuentes:
    """Carga una sola vez las pestañas de ``BD.xlsx`` que usa el mapeo."""

    def __init__(self, bd: Path = config.BD_XLSX, indice: Path = config.INDICE_XLSX) -> None:
        self.bd = bd
        inventario = pd.read_excel(indice, sheet_name="Codificacion",
                                   usecols=["Código fuente", "ID", "Pestaña BD", "Columna BD"])
        self._ubicacion = {
            (str(fila["Código fuente"]), str(fila["ID"])): (str(fila["Pestaña BD"]), str(fila["Columna BD"]))
            for _, fila in inventario.iterrows()
        }
        self._hojas: dict[str, pd.DataFrame] = {}
        self._series: dict[tuple[str, str], pd.Series] = {}

    def cargar(self, claves: set[tuple[str, str]]) -> list[tuple[str, str]]:
        """Lee las pestañas necesarias y devuelve las claves que no existen."""
        faltantes = [clave for clave in claves if clave not in self._ubicacion]
        pestanas = sorted({self._ubicacion[clave][0] for clave in claves if clave in self._ubicacion})
        pendientes = [pestana for pestana in pestanas if pestana not in self._hojas]
        if pendientes:
            leidas = pd.read_excel(self.bd, sheet_name=pendientes)
            for nombre, datos in leidas.items():
                datos = datos.copy()
                datos["fecha"] = pd.to_datetime(datos["fecha"])
                self._hojas[nombre] = datos.set_index("fecha")
        return faltantes

    def serie(self, serie: Serie) -> pd.Series:
        clave = (serie.codigo, serie.id)
        if clave not in self._series:
            pestana, columna = self._ubicacion[clave]
            if pestana not in self._hojas:
                self.cargar({clave})
            datos = self._hojas[pestana]
            if columna not in datos.columns:
                raise KeyError(f"La pestaña {pestana!r} no tiene la columna {columna!r} ({clave})")
            valores = pd.to_numeric(datos[columna], errors="coerce").dropna().astype(float)
            valores.index = pd.to_datetime(valores.index).normalize()
            self._series[clave] = valores[~valores.index.duplicated(keep="last")].sort_index()
        resultado = self._series[clave]
        return resultado * serie.factor if serie.factor != 1.0 else resultado

    def evaluar(self, fuente: Fuente) -> pd.Series:
        if isinstance(fuente, Serie):
            return self.serie(fuente)
        if isinstance(fuente, Suma):
            partes = [self.serie(parte) for parte in fuente.partes]
            return pd.concat(partes, axis=1).dropna().sum(axis=1) * fuente.factor
        if isinstance(fuente, Promedio):
            partes = [self.serie(parte) for parte in fuente.partes]
            return pd.concat(partes, axis=1).dropna().mean(axis=1)
        if isinstance(fuente, Anual):
            return anualizar(self.serie(fuente.serie), fuente.metodo)
        raise TypeError(f"Fuente desconocida: {fuente!r}")

    def describir(self, fuente: Fuente) -> str:
        return " + ".join(f"{codigo}:{identificador}" for codigo, identificador in claves_de(fuente))


def anualizar(trimestral: pd.Series, metodo: str) -> pd.Series:
    """Años con exactamente cuatro trimestres; la fecha resultante es el 1 de enero."""
    por_anio = trimestral.groupby(trimestral.index.year)
    completos = por_anio.count() == 4
    agregado = por_anio.sum() if metodo == "suma" else por_anio.mean()
    agregado = agregado[completos]
    agregado.index = pd.to_datetime([f"{anio}-01-01" for anio in agregado.index])
    return agregado
