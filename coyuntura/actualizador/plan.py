"""Cálculo de qué filas agregar y qué celdas escribir en cada hoja.

El plan se calcula completo antes de tocar Excel. Las filas nuevas van entre el
último dato y la barra azul, de modo que ninguna fila existente cambia de
número; las escrituras sobre filas existentes sólo ocurren en modo
``reemplazar`` y quedan registradas con el valor anterior.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
import math
import re

import pandas as pd
from openpyxl.utils import column_index_from_string

from .estructura import Estructura, Libro
from .fuentes import Fuentes
from .mapeo import BloqueAnual, Columna, Hoja, Serie

# El economista suele pegar los valores redondeados a una décima; esas
# diferencias no son revisiones y no se reescriben.
TOLERANCIA = 1e-6


@dataclass
class Escritura:
    fila: int
    col: str
    valor: float
    anterior: object
    motivo: str  # «nuevo», «reemplazo», «completa», «imputada»


@dataclass
class PlanHoja:
    hoja: Hoja
    estructura: Estructura
    filas_nuevas: list[tuple[int, pd.Timestamp]] = field(default_factory=list)
    escrituras: list[Escritura] = field(default_factory=list)
    # columna -> (fila con la fórmula vigente, filas donde se copia); lo completa formulas.py
    formulas: dict[str, tuple[int, list[int]]] = field(default_factory=dict)
    conservadas: list[tuple[int, str, object]] = field(default_factory=list)
    imputadas: list[tuple[int, str, object, float]] = field(default_factory=list)
    limpiar: list[str] = field(default_factory=list)  # celdas con fórmula que esperan un dato
    formulas_respetadas: int = 0
    avisos: list[str] = field(default_factory=list)
    fechas_finales: list[pd.Timestamp] = field(default_factory=list)

    @property
    def insertadas(self) -> int:
        return len(self.filas_nuevas)

    @property
    def cambia(self) -> bool:
        return bool(self.filas_nuevas or self.escrituras or self.formulas or self.limpiar)


@dataclass
class PlanBloque:
    bloque: BloqueAnual
    fila_inicio: int
    fila_fin: int
    anios: list[int]
    nuevos: list[int] = field(default_factory=list)
    escrituras: list[Escritura] = field(default_factory=list)
    formulas: list[str] = field(default_factory=list)
    columnas_anio: list[str] = field(default_factory=list)
    avisos: list[str] = field(default_factory=list)


def clave(fecha: pd.Timestamp, frecuencia: str):
    if frecuencia == "D":
        return fecha.normalize()
    if frecuencia == "M":
        return (fecha.year, fecha.month)
    return fecha.year


def fecha_desde_clave(valor, frecuencia: str, referencia: pd.Timestamp) -> pd.Timestamp:
    """Fecha a escribir para una clave nueva, con la convención de la última fecha."""
    if frecuencia == "D":
        return valor
    anio, mes = valor
    if referencia.day == 1:
        return pd.Timestamp(anio, mes, 1)
    return pd.Timestamp(anio, mes, monthrange(anio, mes)[1])


# El BCRA publica a veces el saldo del 1° de mes aunque caiga en feriado o fin
# de semana; esas fechas no abren filas diarias. Los días hábiles salen del
# calendario del A3500 (el BCRA sólo lo publica en días hábiles); fuera de su
# cobertura, lunes a viernes salvo los feriados de fecha fija.
FERIADOS_FIJOS = {(1, 1), (5, 1), (12, 25)}
A3500 = Serie("bcra-com3500", "TCR diario y TCNPM|Tipo de Cambio de Referencia - en Pesos - por Dólar")


def dia_habil(k, habiles: set | None = None) -> bool:
    if habiles and min(habiles) <= k <= max(habiles):
        return k in habiles
    return k.weekday() < 5 and (k.month, k.day) not in FERIADOS_FIJOS


def dias_habiles(fuentes) -> set | None:
    try:
        return set(fuentes.evaluar(A3500).index.normalize())
    except Exception:  # sin la serie (pruebas, inventario incompleto): regla de calendario
        return None


NUMERO_TEXTO = re.compile(r"^-?\d{1,3}(?:\.\d{3})*(?:,\d+)?$|^-?\d+(?:,\d+)?$")


def numero_de_texto(valor: object) -> float | None:
    """«  1.000,00 » (pegado como texto con formato argentino) -> 1000.0."""
    if not isinstance(valor, str) or valor.startswith("="):
        return None
    limpio = valor.replace("\xa0", " ").strip()
    if not NUMERO_TEXTO.match(limpio):
        return None
    return float(limpio.replace(".", "").replace(",", "."))


def _iguales(a: object, b: float) -> bool:
    return isinstance(a, (int, float)) and not isinstance(a, bool) and math.isclose(float(a), b, rel_tol=TOLERANCIA, abs_tol=TOLERANCIA)


def _serie_por_clave(serie: pd.Series, frecuencia: str) -> dict:
    resultado = {}
    for fecha, valor in serie.items():
        resultado[clave(fecha, frecuencia)] = float(valor)  # la última observación del período manda
    return resultado


def planificar_hoja(hoja: Hoja, estructura: Estructura, fuentes: Fuentes,
                    planes: dict[str, "PlanHoja"]) -> PlanHoja:
    plan = PlanHoja(hoja, estructura)
    frecuencia = hoja.frecuencia
    series = {letra: _serie_por_clave(fuentes.evaluar(columna.fuente), frecuencia)
              for letra, columna in hoja.columnas.items()}
    existentes = {clave(fecha, frecuencia): fila for fila, fecha in estructura.fechas.items()}
    ultima_clave = clave(estructura.ultima_fecha, frecuencia)

    # --- calendario
    if isinstance(hoja.calendario, tuple):
        _, nombre_base, _ = hoja.calendario
        base = planes[nombre_base]
        candidatas = {clave(fecha, frecuencia) for fecha in base.fechas_finales}
    elif isinstance(hoja.calendario, list):
        candidatas = {k for letra in hoja.calendario for k in series[letra]}
    else:
        candidatas = {k for valores in series.values() for k in valores}
    nuevas = sorted(k for k in candidatas if k > ultima_clave and k not in existentes)
    if frecuencia == "D" and not hoja.dias_corridos:
        habiles = dias_habiles(fuentes)
        nuevas = [k for k in nuevas if dia_habil(k, habiles)]
    if hoja.especial == "comex" and nuevas:
        anio_max = max(k[0] for k in nuevas)
        anio_ult, mes_ult = ultima_clave
        nuevas = []
        anio, mes = anio_ult, mes_ult
        while (anio, mes) < (anio_max, 12):
            mes += 1
            if mes > 12:
                anio, mes = anio + 1, 1
            nuevas.append((anio, mes))

    fila = estructura.fila_azul
    for k in nuevas:
        plan.filas_nuevas.append((fila, fecha_desde_clave(k, frecuencia, estructura.ultima_fecha)))
        fila += 1
    filas_por_clave = dict(existentes)
    filas_por_clave.update({clave(fecha, frecuencia): fila for fila, fecha in plan.filas_nuevas})

    # --- valores
    for letra, columna in hoja.columnas.items():
        # Un 0 de la fuente es «sin dato» (el BCRA lo publica en feriados).
        valores = {k: v for k, v in series[letra].items() if v != 0}
        if not valores:
            plan.avisos.append(f"{hoja.nombre}!{letra}: la fuente no tiene datos")
            continue
        primera_fuente = min(valores)
        dato: dict[int, float] = {}   # fila -> valor final de la celda
        huecos: list[int] = []        # filas sin dato de la fuente, a imputar
        for k, fila in sorted(filas_por_clave.items(), key=lambda x: x[1]):
            anterior = estructura.celdas.get((fila, letra))
            existe = fila in estructura.fechas
            es_formula = isinstance(anterior, str) and anterior.startswith("=")
            numero = anterior if isinstance(anterior, (int, float)) and not isinstance(anterior, bool) else None
            texto = numero_de_texto(anterior)
            if texto is not None:
                # Un número pegado como texto no entra en los gráficos: se convierte.
                numero = texto
                if k not in valores and (columna.modo == "agregar" or k < primera_fuente):
                    plan.escrituras.append(Escritura(fila, letra, texto, anterior, "texto a número"))
            if k not in valores:
                if es_formula:
                    plan.formulas_respetadas += k >= primera_fuente
                elif numero is not None and (columna.modo == "agregar" or k < primera_fuente):
                    dato[fila] = float(numero)   # valor del economista: se respeta
                elif numero is None or k >= primera_fuente:
                    huecos.append(fila)          # vacía, o sin dato en la fuente
                continue
            valor = valores[k]
            dato[fila] = valor
            if not existe:
                plan.escrituras.append(Escritura(fila, letra, valor, None, "nuevo"))
            elif anterior is None or anterior == "":
                plan.escrituras.append(Escritura(fila, letra, valor, None, "completa"))
            elif es_formula:
                if columna.pisar_formulas:
                    plan.escrituras.append(Escritura(fila, letra, valor, anterior, "reemplazo"))
                else:
                    plan.formulas_respetadas += 1
            elif columna.modo == "reemplazar" and not _iguales(anterior, valor):
                plan.escrituras.append(Escritura(fila, letra, valor, anterior, "reemplazo"))
            elif columna.modo == "agregar" and numero is not None:
                dato[fila] = float(numero)
        # Ninguna serie queda con un agujero en el medio (los gráficos se cortan):
        # una fecha sin dato toma el promedio del dato anterior y el posterior.
        filas_con_dato = sorted(dato)
        for fila in huecos:
            previas = [f for f in filas_con_dato if f < fila]
            siguientes = [f for f in filas_con_dato if f > fila]
            anterior = estructura.celdas.get((fila, letra))
            if not previas or not siguientes:
                if anterior is not None and anterior != "":
                    plan.conservadas.append((fila, letra, anterior))
                continue
            valor = (dato[previas[-1]] + dato[siguientes[0]]) / 2
            if not _iguales(anterior, valor):
                plan.escrituras.append(Escritura(fila, letra, valor, anterior, "imputada"))
                plan.imputadas.append((fila, letra, anterior, valor))

    plan.fechas_finales = sorted(estructura.fechas.values()) + [fecha for _, fecha in plan.filas_nuevas]
    return plan


def _diciembre_con_dato(plan: PlanHoja, col_control: str) -> set[int]:
    """Años cuyo diciembre ya tiene (o tendrá) dato en la columna de control."""
    escritas = {(e.fila, e.col) for e in plan.escrituras}
    anios = set()
    for fila, fecha in list(plan.estructura.fechas.items()) + plan.filas_nuevas:
        if fecha.month != 12:
            continue
        valor = plan.estructura.celdas.get((fila, col_control))
        if (fila, col_control) in escritas or (valor is not None and valor != ""):
            anios.add(fecha.year)
    return anios


def planificar_bloque(bloque: BloqueAnual, libro: Libro, estructura: Estructura,
                      plan_mensual: PlanHoja, fuentes: Fuentes) -> PlanBloque | None:
    if len(estructura.bloques_anuales) < bloque.orden:
        return None
    inicio, fin, anios = estructura.bloques_anuales[bloque.orden - 1]
    plan = PlanBloque(bloque, inicio, fin, anios)
    ultima = libro.fila_bloque(bloque.hoja, fin, estructura.max_col)
    for letra, valor in ultima.items():
        if isinstance(valor, str) and valor.startswith("="):
            plan.formulas.append(letra)
        elif isinstance(valor, (int, float)) and not isinstance(valor, bool) and int(valor) == anios[-1]:
            plan.columnas_anio.append(letra)
    plan.nuevos = sorted(anio for anio in _diciembre_con_dato(plan_mensual, bloque.col_control) if anio > anios[-1])
    for letra, columna in bloque.columnas.items():
        serie = fuentes.evaluar(columna.fuente)
        anuales = {fecha.year: float(valor) for fecha, valor in serie.items()}
        filas = {anio: inicio + i for i, anio in enumerate(anios)}
        filas.update({anio: fin + 1 + i for i, anio in enumerate(plan.nuevos)})
        for anio, fila in filas.items():
            if anio not in anuales:
                continue
            anterior = libro.formulas[bloque.hoja].cell(fila, column_index_from_string(letra)).value if anio in anios else None
            if anio not in anios:
                plan.escrituras.append(Escritura(fila, letra, anuales[anio], None, "nuevo"))
            elif anterior is None or anterior == "":
                plan.escrituras.append(Escritura(fila, letra, anuales[anio], None, "completa"))
            elif isinstance(anterior, str) and anterior.startswith("="):
                if columna.pisar_formulas:
                    plan.escrituras.append(Escritura(fila, letra, anuales[anio], anterior, "reemplazo"))
            elif columna.modo == "reemplazar" and not _iguales(anterior, anuales[anio]):
                plan.escrituras.append(Escritura(fila, letra, anuales[anio], anterior, "reemplazo"))
    return plan
