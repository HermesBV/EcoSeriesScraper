"""Reglas de la Encuesta de supermercados del INDEC (``supermercados.xlsx``).

Árbol: Actividad > Ventas en supermercados > <qué se abre> > <apertura>.

    Total                      Cuadros 1, 2, 3 (totales nacionales) y el total nacional de los cuadros 4 a 6
    Por canal de venta         Cuadro 4: salón de ventas y canales online
    Por medio de pago          Cuadro 4: efectivo, tarjetas y otros medios
    Por grupo de artículos     Cuadro 5: un grupo por rubro (Almacén, Bebidas…), con las 26 jurisdicciones
    Por jurisdicción           Cuadros 5 y 6: un grupo por medida (ventas totales, bocas, superficie…)

Cómo se deriva cada dimensión (las hojas no traen el encabezado jerárquico, sólo el rótulo
de la columna, que es lo único estable: el título del cuadro lleva el período, que cambia
todos los meses, y por eso nunca se lee):

* Cuadros 1 a 3: el rótulo se busca en una tabla cerrada. Que sea a precios corrientes o
  constantes sale de la posición y de los valores (los rótulos de INDEC arrastran textos
  de celdas combinadas, p. ej. «acumulada del año…» en columnas que no lo son).
* Cuadro 4: «Canal de venta - X», «Medio de pago - X» o «Ventas totales».
* Cuadros 5 y 6: «<jurisdicción> - <grupo o medida> - <unidad>».

Convenciones de nombre: sin sufijo es el valor a precios corrientes; «(precios constantes)»
es el valor deflactado. Un índice y el valor en pesos del mismo concepto comparten nombre
(son la misma serie con otra escala); la unidad los distingue en su columna.
"""

from __future__ import annotations

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

FUENTE = "indec-supermercados"
TEMA = "Actividad"
INDICADOR = "Ventas en supermercados"
ESTRUCTURA = "Supermercados"

INDICE = "Índice 2017=100"
PORCENTAJE = "%"

# Nivel 3
TOTAL = "Total"
POR_CANAL = "Por canal de venta"
POR_MEDIO_DE_PAGO = "Por medio de pago"
POR_GRUPO = "Por grupo de artículos"
POR_JURISDICCION = "Por jurisdicción"

CONSTANTES = "precios constantes"
INTERANUAL = "var. % interanual"
MENSUAL = "var. % mensual"
ACUMULADA = "var. % acumulada"

# Cuadro 1: serie de ventas a precios constantes, base 2017=100.
_CUADRO_1 = {
    "Serie original - Índice de ventas totales a precios constantes - Números índice":
        (INDICE, (CONSTANTES,)),
    "Serie original - Variación porcentual - respecto al mismo mes del año anterior - %":
        (PORCENTAJE, (CONSTANTES, INTERANUAL)),
    "Serie original - Variación porcentual - acumulada del año respecto a igual acumulado del año anterior - %":
        (PORCENTAJE, (CONSTANTES, ACUMULADA)),
    "Serie desestacionalizada (1) - Índice de ventas totales a precios constantes - acumulada del año respecto a igual acumulado del año anterior - Números índice":
        (INDICE, (CONSTANTES, "desestacionalizado")),
    "Serie desestacionalizada (1) - Variación porcentual respecto al mes anterior - acumulada del año respecto a igual acumulado del año anterior - %":
        (PORCENTAJE, (CONSTANTES, "desestacionalizado", MENSUAL)),
    "Serie tendencia-ciclo - Índice de ventas totales a precios constantes - acumulada del año respecto a igual acumulado del año anterior - Números índice":
        (INDICE, (CONSTANTES, "tendencia-ciclo")),
    "Serie tendencia-ciclo - Variación porcentual respecto al mes anterior - acumulada del año respecto a igual acumulado del año anterior - %":
        (PORCENTAJE, (CONSTANTES, "tendencia-ciclo", MENSUAL)),
}

# Cuadro 2: índices de ventas corrientes, constantes y de precios implícitos (base 2017=100).
# Los rótulos de las variaciones no dicen de qué índice son; se asignaron por orden de columna
# y se verificaron contra los valores (p. ej. la variación interanual de los constantes
# coincide con la del Cuadro 3, y 1,2421 / 0,9626 = 1,2906 es la de los implícitos).
_PRECIOS_IMPLICITOS = "Precios implícitos"
_CUADRO_2 = {
    "Índice ventas totales a precios corrientes - Números índice":
        (TOTAL, INDICE, ()),
    "Variación porcentual - respecto al mismo mes del año anterior - %":
        (TOTAL, PORCENTAJE, (INTERANUAL,)),
    "Variación porcentual - acumulada del año respecto a igual acumulado del año anterior - %":
        (TOTAL, PORCENTAJE, (ACUMULADA,)),
    "Índice ventas totales a precios constantes - acumulada del año respecto a igual acumulado del año anterior - Números índice":
        (TOTAL, INDICE, (CONSTANTES,)),
    "Variación porcentual respecto al mismo mes del año anterior - acumulada del año respecto a igual acumulado del año anterior - %":
        (TOTAL, PORCENTAJE, (CONSTANTES, INTERANUAL)),
    "Índice de precios implícitos - acumulada del año respecto a igual acumulado del año anterior - Números índice":
        (_PRECIOS_IMPLICITOS, INDICE, ()),
    "Variación porcentual - respecto al mismo mes del año anterior - % (2)":
        (_PRECIOS_IMPLICITOS, PORCENTAJE, (INTERANUAL,)),
    "Variación porcentual - respecto al mes anterior - %":
        (_PRECIOS_IMPLICITOS, PORCENTAJE, (MENSUAL,)),
}

# Cuadro 3: ventas totales en millones de pesos y su variación interanual.
_CUADRO_3 = {
    "Ventas totales - a precios corrientes - Millones de pesos": ("Millones de pesos", ()),
    "Ventas totales - a precios constantes - Millones de pesos": ("Millones de pesos", (CONSTANTES,)),
    "Variación porcentual respecto al mismo período del año anterior - a precios corrientes - %":
        (PORCENTAJE, (INTERANUAL,)),
    "Variación porcentual respecto al mismo período del año anterior - a precios constantes - %":
        (PORCENTAJE, (CONSTANTES, INTERANUAL)),
}

# Cuadro 4: ventas corrientes en miles de pesos, por canal y por medio de pago.
_MILES = "Miles de pesos"
_CUADRO_4 = {
    "Ventas totales - Miles de pesos": (TOTAL, ""),
    "Canal de venta - Salón de ventas - Miles de pesos": (POR_CANAL, "Salón de ventas"),
    "Canal de venta - Canales online - Miles de pesos": (POR_CANAL, "Canales online"),
    "Medio de pago - Efectivo - Miles de pesos": (POR_MEDIO_DE_PAGO, "Efectivo"),
    "Medio de pago - Tarjetas de débito - Miles de pesos": (POR_MEDIO_DE_PAGO, "Tarjetas de débito"),
    "Medio de pago - Tarjetas de crédito - Miles de pesos": (POR_MEDIO_DE_PAGO, "Tarjetas de crédito"),
    "Medio de pago - Otros medios - Miles de pesos": (POR_MEDIO_DE_PAGO, "Otros medios"),
}

# Jurisdicciones de los cuadros 5 y 6, con la grafía de INDEC. «Total del país» es la
# cobertura natural de la encuesta y no se escribe en el nombre.
_PAIS = "Total del país"
_JURISDICCIONES = (
    "24 partidos del Gran Buenos Aires", "Catamarca", "Chaco", "Chubut",
    "Ciudad Autónoma de Buenos Aires", "Córdoba", "Corrientes", "Entre Ríos", "Formosa",
    "Jujuy", "La Pampa", "La Rioja", "Mendoza", "Misiones", "Neuquén",
    "Resto de Buenos Aires", "Río Negro", "Salta", "San Juan", "San Luis", "Santa Cruz",
    "Santa Fe", "Santiago del Estero", "Tierra del Fuego", "Tucumán",
)
_JURISDICCION_POR_CLAVE = {plano(j): j for j in (*_JURISDICCIONES, _PAIS)}

# Cuadro 5: rubros de venta (grupos de artículos) tal como los nombra INDEC.
_GRUPOS = (
    "Almacén", "Alimentos preparados y rotisería", "Artículos de limpieza y perfumería",
    "Bebidas", "Carnes", "Electrónicos y artículos para el hogar",
    "Indumentaria, calzado y textiles para el hogar", "Lácteos", "Otros", "Panadería",
    "Verdulería y frutería",
)
_GRUPO_POR_CLAVE = {plano(g): g for g in _GRUPOS}

# Cuadro 6: (grupo del árbol, nombre del indicador, apertura, unidad del rótulo, unidad legible).
_CUADRO_6 = {
    "ventas totales": ("Ventas totales", INDICADOR, TOTAL, "Miles de pesos", "Miles de pesos"),
    "cantidad de bocas": ("Bocas de expendio", ESTRUCTURA, "Bocas de expendio", "Unidades", "Unidades"),
    "cantidad de operaciones": ("Operaciones", ESTRUCTURA, "Operaciones", "Unidades", "Unidades"),
    "superficie del area de ventas": ("Superficie del área de ventas", ESTRUCTURA,
                                      "Superficie del área de ventas", "m2", "Metros cuadrados"),
    "ventas por boca": ("Ventas por boca de expendio", ESTRUCTURA,
                        "Ventas por boca de expendio", "Miles de pesos", "Miles de pesos"),
    "ventas por m2": ("Ventas por metro cuadrado", ESTRUCTURA,
                      "Ventas por metro cuadrado", "Pesos", "Pesos"),
    "ventas por operacion": ("Ventas por operación", ESTRUCTURA,
                             "Ventas por operación", "Pesos", "Pesos"),
}


def _hoja(serie: Serie) -> str:
    return plano(serie.hoja).rstrip(". ")


def _buscar(tabla: dict, rotulo: str):
    """Valor de la tabla para el rótulo de la columna, o None.

    Acepta que el título del cuadro (con su período) venga antepuesto al rótulo.
    """
    clave = plano(rotulo)
    for texto, valor in tabla.items():
        if plano(texto) == clave:
            return valor
    for texto, valor in sorted(tabla.items(), key=lambda par: -len(par[0])):
        if clave.endswith(" - " + plano(texto)):
            return valor
    return None


def _clasificacion(indicador: str, grupo: str, apertura: str, nombre_apertura: str,
                   cobertura: str, medida: tuple[str, ...], unidad: str) -> Clasificacion:
    nombre = con_medida(unir(indicador, nombre_apertura, cobertura), ", ".join(medida))
    return Clasificacion(tema=TEMA, indicador=INDICADOR, cobertura=grupo, apertura=apertura,
                         nombre=nombre, unidad=unidad)


def _cuadro_1(serie: Serie) -> Clasificacion | None:
    encontrado = _buscar(_CUADRO_1, serie.columna)
    if encontrado is None:
        return None
    unidad, medida = encontrado
    return _clasificacion(INDICADOR, TOTAL, "", TOTAL, "", medida, unidad)


def _cuadro_2(serie: Serie) -> Clasificacion | None:
    encontrado = _buscar(_CUADRO_2, serie.columna)
    if encontrado is None:
        return None
    apertura, unidad, medida = encontrado
    return _clasificacion(INDICADOR, TOTAL, "", apertura, "", medida, unidad)


def _cuadro_3(serie: Serie) -> Clasificacion | None:
    encontrado = _buscar(_CUADRO_3, serie.columna)
    if encontrado is None:
        return None
    unidad, medida = encontrado
    return _clasificacion(INDICADOR, TOTAL, "", TOTAL, "", medida, unidad)


def _cuadro_4(serie: Serie) -> Clasificacion | None:
    encontrado = _buscar(_CUADRO_4, serie.columna)
    if encontrado is None:
        return None
    grupo, apertura = encontrado
    return _clasificacion(INDICADOR, grupo, "", apertura or TOTAL, "", (), _MILES)


def _partes_jurisdiccion(serie: Serie) -> tuple[str, str, str] | None:
    """(jurisdicción, grupo o medida, unidad) del rótulo «<jurisdicción> - <x> - <unidad>»."""
    partes = [parte.strip() for parte in serie.columna.split(" - ")]
    if len(partes) < 3:
        return None
    jurisdiccion = _JURISDICCION_POR_CLAVE.get(plano(partes[-3]))
    if jurisdiccion is None:
        return None
    return jurisdiccion, partes[-2], partes[-1]


def _cuadro_5(serie: Serie) -> Clasificacion | None:
    partes = _partes_jurisdiccion(serie)
    if partes is None:
        return None
    jurisdiccion, rubro, unidad = partes
    if plano(unidad) != plano(_MILES):
        return None
    cobertura = "" if jurisdiccion == _PAIS else jurisdiccion
    if plano(rubro) == "total":
        # El total de cada jurisdicción es el mismo dato que «Ventas totales» del Cuadro 6.
        if jurisdiccion == _PAIS:
            return _clasificacion(INDICADOR, TOTAL, "", TOTAL, "", (), _MILES)
        return _clasificacion(INDICADOR, POR_JURISDICCION, "Ventas totales", TOTAL, cobertura, (), _MILES)
    grupo = _GRUPO_POR_CLAVE.get(plano(rubro))
    if grupo is None:
        return None
    return _clasificacion(INDICADOR, POR_GRUPO, grupo, grupo, cobertura, (), _MILES)


def _cuadro_6(serie: Serie) -> Clasificacion | None:
    partes = _partes_jurisdiccion(serie)
    if partes is None:
        return None
    jurisdiccion, medida, unidad = partes
    definicion = _CUADRO_6.get(plano(medida))
    if definicion is None:
        return None
    grupo, indicador, apertura, unidad_rotulo, unidad_legible = definicion
    if plano(unidad) != plano(unidad_rotulo):
        return None
    if jurisdiccion == _PAIS and grupo == "Ventas totales":
        return _clasificacion(INDICADOR, TOTAL, "", TOTAL, "", (), unidad_legible)
    cobertura = "" if jurisdiccion == _PAIS else jurisdiccion
    return _clasificacion(indicador, POR_JURISDICCION, grupo, apertura, cobertura, (), unidad_legible)


_POR_HOJA = {
    "cuadro 1": _cuadro_1,
    "cuadro 2": _cuadro_2,
    "cuadro 3": _cuadro_3,
    "cuadro 4": _cuadro_4,
    "cuadro 5": _cuadro_5,
    "cuadro 6": _cuadro_6,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE:
        return None
    regla = _POR_HOJA.get(_hoja(serie))
    return regla(serie) if regla else None
