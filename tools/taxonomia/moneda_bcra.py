"""Reglas del dinero, las tasas y el balance del BCRA.

Dos orígenes:

* ``bcra-dmd``: los datos monetarios diarios del BCRA (``series.xlsm``, 154 series).
* ``datos.gob.ar``, archivo ``dinero_bancos.xlsx``, hojas 8.1, 8.2, 8.3, 8.5, 8.6, 8.7, 8.9,
  8.10 y 8.11 (las hojas 8.4 y 8.8 y las 8.12 a 8.14 son de otros módulos).

Árbol (tema «Moneda y sistema financiero» salvo indicación)::

    Base monetaria
      ├── Total                       saldo, con cuasimonedas, variación diaria
      ├── Por componente              circulación monetaria, cheques cancelatorios, cuentas corrientes
      ├── Factores de explicación     > Total | Compras de divisas | Sector público |
      │                                 Pases y redescuentos | Títulos y letras | Otros
      └── Series históricas           hoja 8.7
    Agregados monetarios              M2, M3, M3*, M2 privado y M2 transaccional privado
      └── Series históricas           M1, M2 y M3 en pesos y en dólares (hoja 8.7)
    Depósitos                         > Total | Sector privado | Sector público > En pesos | En dólares |
      └── Series históricas             En pesos y dólares
    Préstamos                         > En pesos | En dólares | En pesos y dólares | Series históricas
    Tasas de interés                  > Política monetaria | Depósitos | Préstamos | Series históricas
    Mercado de call y pases           call, BAIBAR y pases entre terceros (tasas y montos operados)
    Instrumentos del BCRA             > Lebac y Nobac | Leliq y Notaliq | Pases | Otros instrumentos
                                        (los dos primeros se abren en saldos, tasas, mercado secundario)
    Balance del BCRA                  > Activo | Pasivo | Patrimonio neto > rubro
    Encajes                           > Categoría I | Categorías II a VI | Series históricas
    Coeficientes de actualización     CER, UVA, UVI y CVS

Sector externo (series del BCRA que son de reservas y tipo de cambio)::

    Reservas internacionales          saldos y, en «Variación diaria», los factores
      └── Series históricas           hoja 8.7
    Tipo de cambio nominal            valuación contable (bcra-dmd y balance), y en
      └── Series históricas           «Series históricas» las dos series de la hoja 8.7

Cómo se deriva cada dimensión:

* ``bcra-dmd``: por ``ID`` (único y estable en la fuente). El nombre del BCRA trae la unidad
  entre paréntesis («en millones de $», «en %»): se pasa a ``unidad`` y se quita del nombre; la
  tasa lleva «(TNA)» o «(TEA)» según lo que dice el título.
* ``datos.gob.ar``: por ``Variable`` (hojas 8.1, 8.2, 8.5, 8.6, 8.7, 8.9, 8.11) o por
  ``Descripción`` (8.3, cuyas variables son irregulares). La hoja 8.10 trae la fecha en la
  ``Variable``: la ``Descripción`` tiene tres fechas con error de tipeo y una serie (la del
  21 de octubre de 2018) tiene el mes mal en las dos; se verificó contra el último dato de cada
  serie, que cae poco antes del vencimiento.
* Nunca se lee el título del cuadro (es el mismo para toda la hoja).

Convenciones de nombre: sin sufijo es el valor en pesos; «(en dólares)» es el saldo en dólares
(unidad «Millones de dólares»); «(en dólares, expresados en pesos)» es el equivalente en
pesos de lo que está en dólares; «(en pesos y dólares, expresados en pesos)» es la suma de
ambos. Las tasas llevan «(TNA)» o «(TEA)»; las tasas en dólares, «(en dólares, TNA)».
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

FUENTE_DMD = "bcra-dmd"
FUENTE_DATOS = "datos.gob.ar"
ARCHIVO = "dinero_bancos.xlsx"

TEMA = "Moneda y sistema financiero"
TEMA_EXTERNO = "Sector externo"

M_PESOS = "Millones de pesos"
M_USD = "Millones de dólares"
MIL_PESOS = "Miles de pesos"
PCT = "%"
PESOS_POR_USD = "Pesos por dólar"

HISTORICAS = "Series históricas"

# Indicadores
BASE = "Base monetaria"
AGREGADOS = "Agregados monetarios"
DEPOSITOS = "Depósitos"
PRESTAMOS = "Préstamos"
TASAS = "Tasas de interés"
CALL = "Mercado de call y pases"
INSTRUMENTOS = "Instrumentos del BCRA"
BALANCE = "Balance del BCRA"
ENCAJES = "Encajes"
COEFICIENTES = "Coeficientes de actualización"
RESERVAS = "Reservas internacionales"
TIPO_DE_CAMBIO = "Tipo de cambio nominal"

TNA = "TNA"
TEA = "TEA"
USD = "en dólares"
USD_EN_PESOS = "en dólares, expresados en pesos"
MIXTO_EN_PESOS = "en pesos y dólares, expresados en pesos"
VAR_DIARIA = "variación diaria"
PROMEDIO = "promedio mensual"

HOJA_TASAS = "8.1 Tasas de interes"
HOJA_AGREGADOS = "8.2 Agregados monetarios"
HOJA_PRESTAMOS = "8.3 Prest. de Entidades finac."
HOJA_ENCAJES = "8.5 Exigencia encajes"
HOJA_COEFICIENTES = "8.6 CER, UVA y UVI"
HOJA_HISTORICAS = "8.7 Series historicas"
HOJA_BALANCE = "8.9 Balance BCRA"
HOJA_LEBAC = "8.10 Lebac mercado secundario"
HOJA_FACTORES = "8.11 Factores Base Monetaria"


def _clasificacion(indicador: str, partes: tuple[str, ...] | list[str], *, n3: str = "", n4: str = "",
                   medida: str = "", unidad: str = "", tema: str = TEMA) -> Clasificacion:
    """Nombre «Indicador · parte · parte (medida)» y lugar en el árbol."""
    return Clasificacion(tema=tema, indicador=indicador, cobertura=n3, apertura=n4,
                         nombre=con_medida(unir(indicador, *partes), medida), unidad=unidad)


def _medida(*partes: str) -> str:
    return ", ".join(parte for parte in partes if parte)


# ======================================================================================
# Datos monetarios diarios del BCRA (bcra-dmd)
# ======================================================================================

# Factores de explicación de la base monetaria (nivel 4)
F_TOTAL = "Total"
F_COMPRAS = "Compras de divisas"
F_SECTOR_PUBLICO = "Sector público"
F_PASES = "Pases y redescuentos"
F_TITULOS = "Títulos y letras"
F_OTROS = "Otros"
FACTORES = "Factores de explicación"

# Nivel 3 de la base monetaria
BM_TOTAL = "Total"
BM_COMPONENTE = "Por componente"
BM_FACTORES = "Factores de explicación"

LEFI = "Letras Fiscales de Liquidez (LEFI)"
LEBAC_NOBAC = "Lebac y Nobac"
LELIQ_NOTALIQ = "Leliq y Notaliq"
INTERESES = "Intereses, primas y remuneración de cuentas corrientes"


def _base(n3: str, n4: str, partes: tuple[str, ...], medida: str = "") -> Clasificacion:
    return _clasificacion(BASE, partes, n3=n3, n4=n4, medida=medida, unidad=M_PESOS)


def _base_ampliada(medida: str = "") -> Clasificacion:
    """Base monetaria más cuasimonedas: el nombre no repite el indicador."""
    return Clasificacion(tema=TEMA, indicador=BASE, cobertura=BM_TOTAL, unidad=M_PESOS,
                         nombre=con_medida("Base monetaria más cuasimonedas", medida))


def _factor(n4: str, *partes: str) -> Clasificacion:
    return _base(BM_FACTORES, n4, (FACTORES, *partes))


def _componente(*partes: str, medida: str = "") -> Clasificacion:
    return _base(BM_COMPONENTE, "", partes, medida)


_CIRC_PUBLICO = ("Circulación monetaria", "En poder del público")
_CIRC_ENTIDADES = ("Circulación monetaria", "En entidades financieras")
_CUENTAS_BCRA = ("Cuentas corrientes en pesos en el BCRA",)

_DMD_BASE = {
    "46": _factor(F_TOTAL, "Total"),
    "47": _factor(F_COMPRAS, F_COMPRAS, "Al sector privado y otros"),
    "48": _factor(F_COMPRAS, F_COMPRAS, "Al Tesoro Nacional"),
    "49": _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Adelantos transitorios al Tesoro Nacional"),
    "50": _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Transferencia de utilidades al Tesoro Nacional"),
    "51": _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Resto de operaciones con el Tesoro Nacional"),
    "52": _factor(F_PASES, F_PASES, "Pases"),
    "53": _factor(F_PASES, F_PASES, "Leliq y Notaliq"),
    "54": _factor(F_PASES, F_PASES, "Redescuentos y adelantos"),
    "55": _factor(F_PASES, F_PASES, INTERESES),
    "56": _factor(F_TITULOS, F_TITULOS, LEBAC_NOBAC),
    "57": _factor(F_OTROS, F_OTROS, "Rescate de cuasimonedas"),
    "58": _factor(F_TITULOS, F_TITULOS, LEFI),
    "59": _factor(F_OTROS, F_OTROS),
    "60": _componente(*_CIRC_PUBLICO, medida=VAR_DIARIA),
    "61": _componente(*_CIRC_ENTIDADES, medida=VAR_DIARIA),
    "62": _componente("Cheques cancelatorios", medida=VAR_DIARIA),
    "63": _componente(*_CUENTAS_BCRA, medida=VAR_DIARIA),
    "64": _base(BM_TOTAL, "", (), VAR_DIARIA),
    "65": _base(BM_TOTAL, "", ("Cuasimonedas",), VAR_DIARIA),
    "66": _base_ampliada(VAR_DIARIA),
    "67": _componente(*_CIRC_PUBLICO),
    "68": _componente(*_CIRC_ENTIDADES),
    "69": _componente("Cheques cancelatorios"),
    "70": _componente(*_CUENTAS_BCRA),
    "71": _base(BM_TOTAL, "", ()),
    "72": _base(BM_TOTAL, "", ("Cuasimonedas",)),
    "73": _base_ampliada(),
}

# --- Depósitos -----------------------------------------------------------------------
D_TOTAL = "Total"
D_PRIVADO = "Sector privado"
D_PUBLICO = "Sector público"
EN_PESOS = "En pesos"
EN_DOLARES = "En dólares"
EN_AMBAS = "En pesos y dólares"


def _deposito(sector: str, moneda: str, concepto: str, medida: str = "", unidad: str = M_PESOS) -> Clasificacion:
    """Nombre «Depósitos · concepto · sector (medida)»; el total de los sectores no lleva cobertura."""
    cobertura = "" if sector == D_TOTAL else sector
    return _clasificacion(DEPOSITOS, (concepto, cobertura), n3=sector, n4=moneda, medida=medida, unidad=unidad)


_DMD_DEPOSITOS = {}
for _id, _concepto in zip(
        ("85", "86", "87", "88", "89", "90", "91", "92", "93"),
        ("Cuenta corriente", "Caja de ahorro", "Plazo fijo no ajustable por CER/UVA",
         "Plazo fijo ajustable por CER/UVA", "Otros depósitos", "CEDROS con CER", "Total más CEDROS",
         "BODEN", "Total más CEDROS y BODEN")):
    _DMD_DEPOSITOS[_id] = _deposito(D_TOTAL, EN_PESOS, _concepto)
for _id, _concepto in zip(
        ("94", "95", "96", "97", "98", "99", "100", "101", "102"),
        ("Cuenta corriente", "Caja de ahorro", "Plazo fijo no ajustable por CER/UVA",
         "Plazo fijo ajustable por CER/UVA", "Otros depósitos", "CEDROS con CER", "Total más CEDROS",
         "BODEN", "Total más CEDROS y BODEN")):
    _DMD_DEPOSITOS[_id] = _deposito(D_PRIVADO, EN_PESOS, _concepto)
_DMD_DEPOSITOS.update({
    "103": _deposito(D_TOTAL, EN_DOLARES, "Total", USD_EN_PESOS),
    "104": _deposito(D_PRIVADO, EN_DOLARES, "Total", USD_EN_PESOS),
    "105": _deposito(D_TOTAL, EN_AMBAS, "Total", MIXTO_EN_PESOS),
    "106": _deposito(D_PRIVADO, EN_AMBAS, "Total", MIXTO_EN_PESOS),
    "107": _deposito(D_TOTAL, EN_DOLARES, "Total", USD, M_USD),
    "108": _deposito(D_PRIVADO, EN_DOLARES, "Total", USD, M_USD),
})

_DMD_AGREGADOS = {
    "109": _clasificacion(AGREGADOS, ("M2",), unidad=M_PESOS),
    "197": _clasificacion(AGREGADOS, ("M2 transaccional privado",), unidad=M_PESOS),
}


# --- Préstamos -----------------------------------------------------------------------
P_PESOS = "En pesos"
P_DOLARES = "En dólares"
P_AMBAS = "En pesos y dólares"
SECTOR_PRIVADO = "Sector privado"
SECTOR_PUBLICO = "Sector público"


def _prestamo(moneda: str, concepto: str, medida: str = "", unidad: str = M_PESOS,
              sector: str = SECTOR_PRIVADO) -> Clasificacion:
    return _clasificacion(PRESTAMOS, (concepto, sector), n3=moneda, medida=medida, unidad=unidad)


_PRESTAMOS_TIPOS = ("Adelantos en cuenta corriente", "Documentos", "Hipotecarios", "Prendarios",
                    "Personales", "Tarjetas de crédito", "Otros", "Total")
_DMD_PRESTAMOS = {}
for _id, _concepto in zip(("110", "111", "112", "113", "114", "115", "116", "117"), _PRESTAMOS_TIPOS):
    _DMD_PRESTAMOS[_id] = _prestamo(P_PESOS, _concepto)
for _id, _concepto in zip(("118", "119", "120", "121", "122", "123", "124", "125"), _PRESTAMOS_TIPOS):
    _DMD_PRESTAMOS[_id] = _prestamo(P_DOLARES, _concepto, USD, M_USD)
_DMD_PRESTAMOS["126"] = _prestamo(P_DOLARES, "Total", USD_EN_PESOS)
_DMD_PRESTAMOS["127"] = _prestamo(P_AMBAS, "Total", MIXTO_EN_PESOS)


# --- Tasas de interés -----------------------------------------------------------------
T_POLITICA = "Política monetaria"
T_DEPOSITOS = "Depósitos"
T_PRESTAMOS = "Préstamos"

PRIVADOS = "Bancos privados"
TOTAL_BANCOS = "Total de bancos"
ADELANTOS_EMPRESAS = "Adelantos en cuenta corriente a empresas"
ADELANTOS_ACUERDO = "Acuerdo de 1 a 7 días y de 10 millones o más"
PRIME = "Prime"
PRIMERA_LINEA = "Préstamos a empresas de primera línea a 30 días"


def _tasa(n3: str, partes: tuple[str, ...], medida: str = TNA, indicador: str = TASAS) -> Clasificacion:
    return _clasificacion(indicador, partes, n3=n3, medida=medida, unidad=PCT)


_DMD_TASAS = {
    "160": _tasa(T_POLITICA, ("Tasa de política monetaria",), TNA),
    "161": _tasa(T_POLITICA, ("Tasa de política monetaria",), TEA),
    "135": _tasa(T_DEPOSITOS, ("TAMAR", TOTAL_BANCOS), TNA),
    "136": _tasa(T_DEPOSITOS, ("TAMAR", PRIVADOS), TNA),
    "137": _tasa(T_DEPOSITOS, ("TAMAR", PRIVADOS), TEA),
    "138": _tasa(T_DEPOSITOS, ("BADLAR", TOTAL_BANCOS), TNA),
    "139": _tasa(T_DEPOSITOS, ("BADLAR", PRIVADOS), TNA),
    "140": _tasa(T_DEPOSITOS, ("BADLAR", PRIVADOS), TEA),
    "141": _tasa(T_DEPOSITOS, ("TM20", TOTAL_BANCOS), TNA),
    "142": _tasa(T_DEPOSITOS, ("TM20", PRIVADOS), TNA),
    "143": _tasa(T_DEPOSITOS, ("TM20", PRIVADOS), TEA),
    "1189": _tasa(T_DEPOSITOS, ("Plazo fijo", "Total de operaciones"), TNA),
    "1190": _tasa(T_DEPOSITOS, ("Plazo fijo", "Personas humanas"), TNA),
    "1191": _tasa(T_DEPOSITOS, ("Plazo fijo", "Prestadoras de servicios financieros"), TNA),
    "1192": _tasa(T_DEPOSITOS, ("Plazo fijo", "Otras personas jurídicas"), TNA),
    "1193": _tasa(T_DEPOSITOS, ("Plazo fijo", "Total de operaciones"), _medida(USD, TNA)),
    "1194": _tasa(T_DEPOSITOS, ("Plazo fijo", "Personas humanas"), _medida(USD, TNA)),
    "1195": _tasa(T_DEPOSITOS, ("Plazo fijo", "Personas jurídicas"), _medida(USD, TNA)),
    "1196": _tasa(T_DEPOSITOS, ("Depósitos a la vista remunerados", "Prestadoras de servicios financieros"), TNA),
    "144": _tasa(T_PRESTAMOS, ("Préstamos personales",), TNA),
    "145": _tasa(T_PRESTAMOS, (ADELANTOS_EMPRESAS, ADELANTOS_ACUERDO), TNA),
}

MONTO = "Monto operado"
_DMD_CALL = {
    "146": _tasa("", ("BAIBAR",), TNA, CALL),
    "147": _clasificacion(CALL, ("BAIBAR", MONTO), unidad=M_PESOS),
    "148": _tasa("", ("Call",), TNA, CALL),
    "149": _clasificacion(CALL, ("Call", MONTO), unidad=M_PESOS),
    "150": _tasa("", ("Pases entre terceros a 1 día",), TNA, CALL),
    "151": _clasificacion(CALL, ("Pases entre terceros", MONTO), unidad=M_PESOS),
}


# --- Instrumentos del BCRA ------------------------------------------------------------
I_LEBAC = "Lebac y Nobac"
I_LELIQ = "Leliq y Notaliq"
I_PASES = "Pases"
I_OTROS = "Otros instrumentos"
I_SALDOS = "Saldos"
I_TASAS = "Tasas de interés"
I_SECUNDARIO = "Tasas en el mercado secundario"
I_MARGENES = "Márgenes de Nobac"


def _instrumento(n3: str, n4: str, partes: tuple[str, ...], medida: str = "", unidad: str = M_PESOS) -> Clasificacion:
    return _clasificacion(INSTRUMENTOS, partes, n3=n3, n4=n4, medida=medida, unidad=unidad)


def _tasa_instrumento(n3: str, partes: tuple[str, ...], medida: str = TNA, n4: str = I_TASAS) -> Clasificacion:
    return _instrumento(n3, n4, partes, medida, PCT)


_DMD_INSTRUMENTOS = {
    "152": _instrumento(I_PASES, I_SALDOS, ("Pases pasivos",)),
    "153": _instrumento(I_PASES, I_SALDOS, ("Pases pasivos", "Fondos comunes de inversión")),
    "154": _instrumento(I_PASES, I_SALDOS, ("Pases activos",)),
    "155": _instrumento(I_LELIQ, "", ("Leliq y Notaliq",)),
    "156": _instrumento(I_LEBAC, I_SALDOS, ("Lebac y Nobac en pesos, Legar y Lemin",)),
    "157": _instrumento(I_LEBAC, I_SALDOS, ("Lebac y Nobac en pesos de entidades financieras",)),
    "158": _instrumento(I_LEBAC, I_SALDOS, ("Lebac en dólares, Lediv y Bopreal",), USD, M_USD),
    "159": _instrumento(I_OTROS, "", ("Nocom",)),
    "162": _tasa_instrumento(I_PASES, ("Pases pasivos", "A 1 día")),
    "163": _tasa_instrumento(I_PASES, ("Pases pasivos", "A 7 días")),
    "164": _tasa_instrumento(I_PASES, ("Pases activos", "A 1 día")),
    "165": _tasa_instrumento(I_PASES, ("Pases activos", "A 7 días")),
    "166": _tasa_instrumento(I_LEBAC, ("Lebac y Leliq en pesos", "A 1 mes")),
    "171": _tasa_instrumento(I_LEBAC, ("Lebac y Leliq en pesos", "A 6 meses")),
    "180": _tasa_instrumento(I_LEBAC, ("Lebac ajustables por CER", "A 6 meses")),
    "181": _tasa_instrumento(I_LEBAC, ("Lebac ajustables por CER", "A 12 meses")),
    "182": _tasa_instrumento(I_LEBAC, ("Lebac ajustables por CER", "A 18 meses")),
    "183": _tasa_instrumento(I_LEBAC, ("Lebac ajustables por CER", "A 24 meses")),
    "184": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en pesos", "A 1 mes"), _medida(USD, TNA)),
    "185": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en pesos", "A 6 meses"), _medida(USD, TNA)),
    "186": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en pesos", "A 12 meses"), _medida(USD, TNA)),
    "187": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en dólares", "A 1 mes"), _medida(USD, TNA)),
    "188": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en dólares", "A 3 meses"), _medida(USD, TNA)),
    "189": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en dólares", "A 6 meses"), _medida(USD, TNA)),
    "190": _tasa_instrumento(I_LEBAC, ("Lebac en dólares liquidables en dólares", "A 12 meses"), _medida(USD, TNA)),
    "191": _tasa_instrumento(I_LEBAC, ("Nobac a 9 meses", "Margen sobre BADLAR de bancos privados"), "", I_MARGENES),
    "192": _tasa_instrumento(I_LEBAC, ("Nobac a 12 meses", "Margen sobre BADLAR de bancos privados"), "", I_MARGENES),
    "193": _tasa_instrumento(I_LEBAC, ("Nobac a 2 años", "Margen sobre BADLAR total de bancos"), "", I_MARGENES),
    "194": _tasa_instrumento(I_LEBAC, ("Nobac a 2 años", "Margen sobre BADLAR de bancos privados"), "", I_MARGENES),
    "195": _tasa_instrumento(I_LELIQ, ("Notaliq a 190 días", "Margen sobre la tasa de política monetaria"), "", ""),
    "196": _instrumento(I_OTROS, "", (LEFI, "En cartera de entidades financieras (valor técnico)")),
    "198": _instrumento(I_OTROS, "", ("Otros instrumentos",)),
}
# Lebac en pesos de 2 a 24 meses (el 1 y el 6 son series combinadas con Leliq).
for _id, _plazo in zip(
        ("167", "168", "169", "170", "172", "173", "174", "175", "176", "177", "178", "179"),
        (2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 18, 24)):
    _DMD_INSTRUMENTOS[_id] = _tasa_instrumento(I_LEBAC, ("Lebac en pesos", f"A {_plazo} meses"))


# --- Reservas y tipo de cambio (Sector externo) -------------------------------------------------
VARIACION_DIARIA = "Variación diaria"


def _reservas(*partes: str, n3: str = "") -> Clasificacion:
    return _clasificacion(RESERVAS, partes, n3=n3, unidad=M_USD, tema=TEMA_EXTERNO)


_DMD_RESERVAS = {
    "74": _reservas("Brutas"),
    "75": _reservas("Oro, divisas, colocaciones a plazo y otros activos de reserva"),
    "76": _reservas("Divisas por pase pasivo en dólares con el exterior"),
    "77": _reservas(VARIACION_DIARIA, "Total", n3=VARIACION_DIARIA),
    "78": _reservas(VARIACION_DIARIA, "Compra de divisas", n3=VARIACION_DIARIA),
    "79": _reservas(VARIACION_DIARIA, "Operaciones con organismos internacionales", n3=VARIACION_DIARIA),
    "80": _reservas(VARIACION_DIARIA, "Otras operaciones del sector público", n3=VARIACION_DIARIA),
    "81": _reservas(VARIACION_DIARIA, "Efectivo mínimo", n3=VARIACION_DIARIA),
    "82": _reservas(VARIACION_DIARIA, "Otras operaciones", n3=VARIACION_DIARIA),
    "83": _reservas("Asignaciones de DEG de 2009"),
}

_TIPO_DE_CAMBIO_CONTABLE = Clasificacion(
    tema=TEMA_EXTERNO, indicador=TIPO_DE_CAMBIO, nombre="Tipo de cambio nominal · Valuación contable",
    unidad=PESOS_POR_USD)
_DMD_CAMBIO = {"84": _TIPO_DE_CAMBIO_CONTABLE}

_DMD = {**_DMD_BASE, **_DMD_DEPOSITOS, **_DMD_AGREGADOS, **_DMD_PRESTAMOS, **_DMD_TASAS, **_DMD_CALL,
        **_DMD_INSTRUMENTOS, **_DMD_RESERVAS, **_DMD_CAMBIO}


# ======================================================================================
# 8.1 Tasas de interés
# ======================================================================================

_TASAS_81 = {
    "tasas_interes_badlar": _tasa(T_DEPOSITOS, ("BADLAR", PRIVADOS)),
    "tasas_interes_cajas_ahorro": _tasa(T_DEPOSITOS, ("Caja de ahorro",)),
    "tasas_interes_plazo_fijo_30_59_dias": _tasa(T_DEPOSITOS, ("Plazo fijo de 30 a 59 días",)),
    "tasas_interes_plazo_fijo_mas_60_dias": _tasa(T_DEPOSITOS, ("Plazo fijo de más de 60 días",)),
    "tasas_interes_pm": _tasa(T_POLITICA, ("Tasa de política monetaria",)),
    "instr_bcra_tasa_politica_monetaria": _tasa(T_POLITICA, ("Tasa de política monetaria",)),
    "tasas_interes_adelantos_cc": _tasa(T_PRESTAMOS, (ADELANTOS_EMPRESAS, ADELANTOS_ACUERDO)),
    "tasas_interes_activas_prendarios": _tasa(T_PRESTAMOS, ("Préstamos prendarios",)),
    "tasas_interes_activas_hipotecarios": _tasa(T_PRESTAMOS, ("Préstamos hipotecarios",)),
    "tasas_interes_activas_personales": _tasa(T_PRESTAMOS, ("Préstamos personales",)),
    "tasas_interes_activas_tarjetas": _tasa(T_PRESTAMOS, ("Préstamos con tarjeta de crédito",)),
    "tasas_interes_activas_adelantos": _tasa(T_PRESTAMOS, ("Adelantos en cuenta corriente",)),
    "tasas_interes_activas_documentos": _tasa(T_PRESTAMOS, ("Documentos",)),
    "tasas_interes_prime": _tasa(T_PRESTAMOS, (PRIME, PRIMERA_LINEA)),
    "tasas_interes_call": _tasa("", ("Call",), TNA, CALL),
    "instr_bcra_pases_pesos_pasivos_1_dia": _tasa_instrumento(I_PASES, ("Pases pasivos", "A 1 día")),
    "instr_bcra_pases_pesos_pasivos_7_dias": _tasa_instrumento(I_PASES, ("Pases pasivos", "A 7 días")),
    "instr_bcra_pases_pesos_activos_1_dias": _tasa_instrumento(I_PASES, ("Pases activos", "A 1 día")),
    "instr_bcra_pases_pesos_activos_7_dias": _tasa_instrumento(I_PASES, ("Pases activos", "A 7 días")),
}
_LEBAC_81 = re.compile(r"^instr_bcra_lebac_pesos_(\d+)_m$")


def _hoja_tasas(serie: Serie) -> Clasificacion | None:
    encontrada = _TASAS_81.get(serie.variable)
    if encontrada is not None:
        return encontrada
    lebac = _LEBAC_81.match(serie.variable)
    if lebac:
        plazo = int(lebac.group(1))
        return _tasa_instrumento(I_LEBAC, ("Lebac en pesos", f"A {plazo} {'mes' if plazo == 1 else 'meses'}"))
    return None


# ======================================================================================
# 8.2 Agregados monetarios
# ======================================================================================

_AGREGADOS_82 = {
    "agregados_monetarios_totales_m2": _clasificacion(AGREGADOS, ("M2",), unidad=M_PESOS),
    "agregados_monetarios_totales_m3": _clasificacion(AGREGADOS, ("M3",), unidad=M_PESOS),
    "agregados_monetarios_totales_m2_privado": _clasificacion(AGREGADOS, ("M2 privado",), unidad=M_PESOS),
    "agregados_monetarios_totales_m3_alternativa": _clasificacion(AGREGADOS, ("M3*",), unidad=M_PESOS),
    "base_monetaria_total": _base(BM_TOTAL, "", (), PROMEDIO),
    "base_monetaria_circulacion_monetaria_total": _componente("Circulación monetaria", medida=PROMEDIO),
    "base_monetaria_circulacion_monetaria_publico": _componente(*_CIRC_PUBLICO, medida=PROMEDIO),
    "base_monetaria_circulacion_monetaria_ent_financieras": _componente(*_CIRC_ENTIDADES, medida=PROMEDIO),
    "base_monetaria_cuenta_corriente_bcra": _componente(*_CUENTAS_BCRA, medida=PROMEDIO),
}

_SECTORES_82 = {"total": D_TOTAL, "privado": D_PRIVADO, "publico": D_PUBLICO}
_DEPOSITOS_82 = re.compile(
    r"^depositos_(caja_ahorro|cuenta_corriente|cedros_cer|otros_depositos|pf_ajustable_cer_uva|"
    r"pf_no_ajustable_cer_uva|totales_dolares|totales_pesos)_(total|privado|publico)$")
_CONCEPTOS_82 = {
    "caja_ahorro": "Caja de ahorro",
    "cuenta_corriente": "Cuenta corriente",
    "cedros_cer": "CEDROS con CER",
    "otros_depositos": "Otros depósitos",
    "pf_ajustable_cer_uva": "Plazo fijo ajustable por CER/UVA",
    "pf_no_ajustable_cer_uva": "Plazo fijo no ajustable por CER/UVA",
    "totales_pesos": "Total",
    "totales_dolares": "Total",
}

# Detalle desagregado del plazo fijo del sector privado, por moneda y por plazo o tipo de depositante.
_PLAZO_FIJO_82 = re.compile(
    r"^det_desagreg_plazo_fijo_sec_priv_(?:(pesos_usd|pesos|usd)_)?"
    r"(hasta_59_dias|60_89_dias|90_179_dias|180_365_dias|366_mas|total|mayoristas|minoristas)$")
_PLAZOS_82 = {
    "hasta_59_dias": "Plazo fijo hasta 59 días",
    "60_89_dias": "Plazo fijo de 60 a 89 días",
    "90_179_dias": "Plazo fijo de 90 a 179 días",
    "180_365_dias": "Plazo fijo de 180 a 365 días",
    "366_mas": "Plazo fijo de más de 365 días",
    "total": "Plazo fijo",
    "mayoristas": "Plazo fijo mayorista",
    "minoristas": "Plazo fijo minorista",
}
# Depósitos en dólares por tipo (millones de dólares) y sector.
_DOLARES_82 = re.compile(
    r"^det_desagreg_(cta_corr|otros_depo|caja_ahorro|plazo_fijo)_usd_(sec_publico|sec_priv_|total)$")
_TIPOS_DOLARES_82 = {
    "cta_corr": "Cuenta corriente", "otros_depo": "Otros depósitos",
    "caja_ahorro": "Caja de ahorro", "plazo_fijo": "Plazo fijo",
}
_SECTORES_DOLARES_82 = {"sec_publico": D_PUBLICO, "sec_priv_": D_PRIVADO, "total": D_TOTAL}


def _hoja_agregados(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    fija = _AGREGADOS_82.get(variable)
    if fija is not None:
        return fija
    deposito = _DEPOSITOS_82.match(variable)
    if deposito:
        tipo, sector = deposito.groups()
        sector = _SECTORES_82[sector]
        if tipo == "totales_dolares":
            return _deposito(sector, EN_DOLARES, "Total", USD_EN_PESOS)
        return _deposito(sector, EN_PESOS, _CONCEPTOS_82[tipo])
    plazo = _PLAZO_FIJO_82.match(variable)
    if plazo:
        moneda, tramo = plazo.groups()
        concepto = _PLAZOS_82[tramo]
        if moneda == "pesos":
            return _deposito(D_PRIVADO, EN_PESOS, concepto)
        if moneda == "usd":
            return _deposito(D_PRIVADO, EN_DOLARES, concepto, USD, M_USD)
        return _deposito(D_PRIVADO, EN_AMBAS, concepto, MIXTO_EN_PESOS)
    dolares = _DOLARES_82.match(variable)
    if dolares:
        tipo, sector = dolares.groups()
        return _deposito(_SECTORES_DOLARES_82[sector], EN_DOLARES, _TIPOS_DOLARES_82[tipo], USD, M_USD)
    return None


# ======================================================================================
# 8.3 Préstamos de entidades financieras
# ======================================================================================

_CONCEPTOS_PRESTAMOS = {
    "": "Total",
    "consumo": "Consumo",
    "comerciales": "Comerciales",
    "garantia real": "Garantía real",
    "garantia real prendarios": "Prendarios",
    "garantia real hipotecarios": "Hipotecarios",
    "consumo personales": "Personales",
    "consumo tarjetas": "Tarjetas de crédito",
    "comerciales adelantos": "Adelantos en cuenta corriente",
    "comerciales documentos": "Documentos",
    "comerciales otros": "Otros comerciales",
}
_PRESTAMOS_83 = re.compile(r"^(?:detalle )?prestamos al sector privado (?:en|es) (pesos y dolares|pesos|dolares)(?::\s*(.*))?$")


def _hoja_prestamos(serie: Serie) -> Clasificacion | None:
    # La «Descripción» de esta hoja es regular («Detalle préstamos al sector privado en dolares: consumo
    # tarjetas»); las variables, no.
    coincide = _PRESTAMOS_83.match(plano(serie.descripcion))
    if not coincide:
        return None
    moneda, resto = coincide.groups()
    concepto = _CONCEPTOS_PRESTAMOS.get(plano(resto or ""))
    if concepto is None:
        return None
    if moneda == "pesos":
        return _prestamo(P_PESOS, concepto)
    if moneda == "dolares":
        return _prestamo(P_DOLARES, concepto, USD, M_USD)
    return _prestamo(P_AMBAS, concepto, MIXTO_EN_PESOS)


# ======================================================================================
# 8.5 Exigencia de encajes
# ======================================================================================

E_CAT_I = "Categoría I"
E_CAT_II_VI = "Categorías II a VI"
EXIGENCIA = "Exigencia"

_ENCAJES_CONCEPTOS = {
    "caja_ahorros_cuenta_basica_cuenta_gratuita": "Caja de ahorro, cuenta básica y cuenta gratuita universal",
    "cuentas_corrientes_cuentas_vista": "Cuentas corrientes y cuentas a la vista de cajas de crédito cooperativas",
    "pf_14_29_dias": "Plazo fijo de 14 a 29 días",
    "pf_30_59_dias": "Plazo fijo de 30 a 59 días",
    "pf_60_89_dias": "Plazo fijo de 60 a 89 días",
    "pf_90_179_dias": "Plazo fijo de 90 a 179 días",
    "pf_hasta_29_dias": "Plazo fijo hasta 29 días",
    "pf_mas_180_dias": "Plazo fijo de más de 180 días",
    "uva_uvis_30_59_dias": "Depósitos en UVA y UVI de 30 a 59 días",
    "uva_uvis_60_89_dias": "Depósitos en UVA y UVI de 60 a 89 días",
    "uva_uvis_mas_90_dias": "Depósitos en UVA y UVI de más de 90 días",
}
# Antes de octubre de 2012 las exigencias no distinguían categorías de entidades.
_ENCAJES_HISTORICOS = {
    "caja_ahorro": "Caja de ahorro",
    "cuenta_corriente_pesos": "Cuenta corriente",
    "pf_7_13_dias": "Plazo fijo de 7 a 13 días",
    "pf_14_29_dias": "Plazo fijo de 14 a 29 días",
    "pf_hasta_29": "Plazo fijo hasta 29 días",
    "pf_30_59_dias": "Plazo fijo de 30 a 59 días",
    "pf_60_89_dias": "Plazo fijo de 60 a 89 días",
    "pf_90_179_dias": "Plazo fijo de 90 a 179 días",
    "pf_180_365_dias": "Plazo fijo de 180 a 365 días",
    "pf_mas_360_dias": "Plazo fijo de más de 365 días",
}
_ENCAJES_CATEGORIA = re.compile(r"^(.*)_cat_(i|ii_vi)$")


def _hoja_encajes(serie: Serie) -> Clasificacion | None:
    historica = _ENCAJES_HISTORICOS.get(serie.variable)
    if historica is not None:
        return _clasificacion(ENCAJES, (EXIGENCIA, historica), n3=HISTORICAS, unidad=PCT)
    coincide = _ENCAJES_CATEGORIA.match(serie.variable)
    if not coincide:
        return None
    concepto = _ENCAJES_CONCEPTOS.get(coincide.group(1))
    if concepto is None:
        return None
    categoria = E_CAT_I if coincide.group(2) == "i" else E_CAT_II_VI
    return _clasificacion(ENCAJES, (EXIGENCIA, concepto, categoria), n3=categoria, unidad=PCT)


# ======================================================================================
# 8.6 CER, UVA y UVI (y CVS)
# ======================================================================================

_COEFICIENTES = {
    "cer_diario": _clasificacion(COEFICIENTES, ("CER",), unidad="Coeficiente (2-feb-2002=1)"),
    "uva_diario": _clasificacion(COEFICIENTES, ("UVA",), unidad="Pesos"),
    "uvi_diario": _clasificacion(COEFICIENTES, ("UVI",), unidad="Pesos"),
    "total": _clasificacion(COEFICIENTES, ("CVS", "Salarios totales"), unidad="Índice oct-2016=100"),
    "total_registrado": _clasificacion(COEFICIENTES, ("CVS", "Salarios registrados"), unidad="Índice oct-2016=100"),
}


def _hoja_coeficientes(serie: Serie) -> Clasificacion | None:
    return _COEFICIENTES.get(serie.variable)


# ======================================================================================
# 8.7 Series históricas de estadísticas monetarias
# ======================================================================================


def _hist(indicador: str, partes: tuple[str, ...], medida: str = "", unidad: str = M_PESOS,
          tema: str = TEMA, n4: str = "") -> Clasificacion:
    return _clasificacion(indicador, partes, n3=HISTORICAS, n4=n4, medida=medida, unidad=unidad, tema=tema)


def _historica_tasa(partes: tuple[str, ...], dolares: bool = False, indicador: str = TASAS) -> Clasificacion:
    return _hist(indicador, partes, _medida(USD if dolares else "", TNA), PCT)


_HISTORICAS = {
    "agregados_monetarios_en_pesos_bm": _hist(BASE, ()),
    "agregados_monetarios_en_pesos_bym_en_entidades": _hist(BASE, _CIRC_ENTIDADES),
    "agregados_monetarios_en_pesos_bym_en_circulacion": _hist(BASE, _CIRC_PUBLICO),
    "agregados_monetarios_en_pesos_bym_totales": _hist(BASE, ("Circulación monetaria",)),
    "agregados_monetarios_en_pesos_m1": _hist(AGREGADOS, ("M1",)),
    "agregados_monetarios_en_pesos_m2": _hist(AGREGADOS, ("M2",)),
    "agregados_monetarios_en_pesos_m3": _hist(AGREGADOS, ("M3",)),
    "agregados_usd_m1": _hist(AGREGADOS, ("M1",), USD_EN_PESOS),
    "agregados_usd_m2": _hist(AGREGADOS, ("M2",), USD_EN_PESOS),
    "agregados_usd_m3": _hist(AGREGADOS, ("M3",), USD_EN_PESOS),
    "depositos_totales_pesos": _hist(DEPOSITOS, ("Total",)),
    "depositos_al_sector_privado_pesos": _hist(DEPOSITOS, ("Total", D_PRIVADO)),
    "depositos_al_sector_publico_pesos": _hist(DEPOSITOS, ("Total", D_PUBLICO)),
    "depositos_totales_usd": _hist(DEPOSITOS, ("Total",), USD_EN_PESOS),
    "depositos_al_sector_privado_usd": _hist(DEPOSITOS, ("Total", D_PRIVADO), USD_EN_PESOS),
    "depositos_al_sector_publico_usd": _hist(DEPOSITOS, ("Total", D_PUBLICO), USD_EN_PESOS),
    "depositos_totales_total": _hist(DEPOSITOS, ("Total",), MIXTO_EN_PESOS),
    "depositos_al_sector_privado_total": _hist(DEPOSITOS, ("Total", D_PRIVADO), MIXTO_EN_PESOS),
    "depositos_al_sector_publico_total": _hist(DEPOSITOS, ("Total", D_PUBLICO), MIXTO_EN_PESOS),
    "prestamos_totales_pesos": _hist(PRESTAMOS, ("Total",)),
    "prestamos_al_sector_privado_pesos": _hist(PRESTAMOS, ("Total", SECTOR_PRIVADO)),
    "prestamos_al_sector_publico_pesos": _hist(PRESTAMOS, ("Total", SECTOR_PUBLICO)),
    "prestamos_totales_usd": _hist(PRESTAMOS, ("Total",), USD_EN_PESOS),
    "prestamos_al_sector_privado_usd": _hist(PRESTAMOS, ("Total", SECTOR_PRIVADO), USD_EN_PESOS),
    "prestamos_al_sector_publico_usd": _hist(PRESTAMOS, ("Total", SECTOR_PUBLICO), USD_EN_PESOS),
    "prestamos_totales_total": _hist(PRESTAMOS, ("Total",), MIXTO_EN_PESOS),
    "prestamos_al_sector_privado_total": _hist(PRESTAMOS, ("Total", SECTOR_PRIVADO), MIXTO_EN_PESOS),
    "prestamos_al_sector_publico_total": _hist(PRESTAMOS, ("Total", SECTOR_PUBLICO), MIXTO_EN_PESOS),
    "reservas_internacionales_bcra_saldos": _hist(RESERVAS, ("Brutas",), "", M_USD, TEMA_EXTERNO),
    "reservas_internacionales_bcra_prom_mensual_de_saldos_diarios":
        _hist(RESERVAS, ("Brutas",), PROMEDIO, M_USD, TEMA_EXTERNO),
    "tipo_de_cambio_moneda_de_cada_momento":
        _hist(TIPO_DE_CAMBIO, (), "en moneda de cada momento", "Moneda nacional de cada momento por dólar", TEMA_EXTERNO),
    "tipo_de_cambio_en_pesos_equivalentes":
        _hist(TIPO_DE_CAMBIO, (), "en pesos equivalentes", PESOS_POR_USD, TEMA_EXTERNO),
    "tasa_interes_deposito_plazo_fijo_30_59d_pesos": _historica_tasa(("Plazo fijo de 30 a 59 días",)),
    "tasa_interes_deposito_plazo_fijo_30_59d_usd": _historica_tasa(("Plazo fijo de 30 a 59 días",), True),
    "tasa_interes_deposito_caja_de_ahorro_pesos": _historica_tasa(("Caja de ahorro",)),
    "tasa_interes_deposito_caja_de_ahorro_usd": _historica_tasa(("Caja de ahorro",), True),
    "tasa_interes_obligaciones_entidades_financieras_exterior":
        _historica_tasa(("Obligaciones con entidades financieras del exterior",)),
    "tasa_interes_adelantos_en_cc_sect_priv_no_financiero_pesos":
        _historica_tasa(("Adelantos en cuenta corriente",)),
    "tasa_interes_adelantos_en_cc_al_sect_priv_no_financiero_usd":
        _historica_tasa(("Adelantos en cuenta corriente",), True),
    "tasa_interes_prestamos_empresas_primera_linea_30d_pesos": _historica_tasa((PRIME, PRIMERA_LINEA)),
    "tasa_interes_prestamos_empresas_primera_linea_30d_plazo_usd": _historica_tasa((PRIME, PRIMERA_LINEA), True),
    "tasa_interes_prestamos_hipot_sect_priv_5_10_anios_pesos":
        _historica_tasa(("Préstamos hipotecarios de 5 a 10 años",)),
    "tasa_interes_prestamos_hipotecarios_sect_priv_5_10_anios_usd":
        _historica_tasa(("Préstamos hipotecarios de 5 a 10 años",), True),
    "tasa_interes_prestamos_interfinancieros_hasta_15d_pesos":
        _historica_tasa(("Préstamos interfinancieros hasta 15 días",), indicador=CALL),
    "tasa_interes_prestamos_interfinancieros_hasta_15d_plazo_usd":
        _historica_tasa(("Préstamos interfinancieros hasta 15 días",), True, CALL),
}


def _hoja_historicas(serie: Serie) -> Clasificacion | None:
    return _HISTORICAS.get(serie.variable)


# ======================================================================================
# 8.9 Balance del BCRA
# ======================================================================================

ACTIVO = "Activo"
PASIVO = "Pasivo"
PATRIMONIO = "Patrimonio neto"

# Rubros (nivel 4)
R_RESERVAS = "Reservas internacionales"
R_TITULOS = "Títulos públicos"
R_ADELANTOS = "Adelantos transitorios al Gobierno Nacional"
R_CREDITOS = "Créditos al sistema financiero del país"
R_DERIVADOS = "Pases, operaciones a término y derivados"
R_ORGANISMOS_ACTIVO = "Organismos internacionales"
R_OTROS_ACTIVOS = "Otros activos"
R_BASE = "Base monetaria"
R_EMITIDOS = "Títulos y letras emitidos"
R_DEPOSITOS = "Depósitos"
R_OTRAS_MONEDAS = "Otras monedas"
R_ORGANISMOS_PASIVO = "Organismos internacionales"
R_OTROS_PASIVOS = "Otros pasivos"

_LEY_NACIONAL = "Bajo ley nacional"
_LEY_EXTRANJERA = "Bajo ley extranjera"
_LETRAS = "Letras intransferibles"
_CARTERA_PASES = "Cartera de pases del sistema financiero"

# variable -> (nivel 3, nivel 4, partes del nombre después de «Balance del BCRA»)
_BALANCE = {
    # --- Activo: reservas internacionales
    "ap_act_reservas_inter": (ACTIVO, R_RESERVAS, (R_RESERVAS,)),
    "ap_act_reservas_inter_oro_neto_previsiones": (ACTIVO, R_RESERVAS, (R_RESERVAS, "Oro neto de previsiones")),
    "ap_act_reservas_inter_ap_oro_oro": (ACTIVO, R_RESERVAS, (R_RESERVAS, "Oro")),
    "ap_act_reservas_interap_oro_prevision_fineza_oro": (ACTIVO, R_RESERVAS, (R_RESERVAS, "Previsión por fineza de oro")),
    "ap_act_reservas_inter_divisas": (ACTIVO, R_RESERVAS, (R_RESERVAS, "Divisas")),
    "ap_act_reservas_inter_colocaciones_realizables_divisas":
        (ACTIVO, R_RESERVAS, (R_RESERVAS, "Colocaciones realizables en divisas")),
    "ap_act_reservas_inter_tit_pub": (ACTIVO, R_RESERVAS, (R_RESERVAS, "Títulos públicos")),
    "ap_act_reservas_inter_aladi_neto": (ACTIVO, R_RESERVAS, (R_RESERVAS, "ALADI (neto)")),
    "ap_act_reservas_inter_convenios_multilaterales_credito":
        (ACTIVO, R_RESERVAS, (R_RESERVAS, "Convenios multilaterales de crédito")),
    "ap_act_reservas_inter_divisas_pase_pas_uss_exterior":
        (ACTIVO, R_RESERVAS, (R_RESERVAS, "Divisas por pase pasivo en dólares con el exterior")),
    "ap_act_reservas_inter_instrumentos_derivados_reservas_inter":
        (ACTIVO, R_RESERVAS, (R_RESERVAS, "Instrumentos derivados sobre reservas internacionales")),
    # --- Activo: títulos públicos
    "ap_act_tit_pub": (ACTIVO, R_TITULOS, (R_TITULOS,)),
    "ap_act_tit_pub_prevision_desvalor_titulos_pub":
        (ACTIVO, R_TITULOS, (R_TITULOS, "Neto de previsión por desvalorización")),
    "ap_act_tit_pub_ley_nac": (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL)),
    "ap_act_tit_pub_ley_nac_otros": (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, "Otros")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tesoro_nac": (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS)),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2016":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2016")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2020_dec_29710":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2020 (Dec. 297/10)")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2020_dec_29810":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2020 (Dec. 298/10)")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2021_dec_205410":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2021 (Dec. 2054/10)")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2021_dec_27611":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2021 (Dec. 276/11)")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2022_resol_13112":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2022 (Res. 131/12)")),
    "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2022_resol_17112":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, _LETRAS, "Vencimiento 2022 (Res. 171/12)")),
    "ap_act_tit_pub_ley_nac_bono_consolidado_tesoro_nac_1990":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_NACIONAL, "Bono Consolidado del Tesoro Nacional 1990")),
    "ap_act_tit_pub_ley_nac_reg_deven_bono_consolidado_1990":
        (ACTIVO, R_TITULOS, (R_TITULOS, "Bajo ley nacional", "Neto de regularización del Bono Consolidado 1990")),
    "ap_act_tit_pub_bajo_ley_extranjera": (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_EXTRANJERA)),
    "ap_act_tit_pub_ley_extranjera_otros": (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_EXTRANJERA, "Otros")),
    "ap_act_tit_pub_ley_extranjera_letras_sobre_obligaciones_fmi":
        (ACTIVO, R_TITULOS, (R_TITULOS, _LEY_EXTRANJERA, "Letras sobre obligaciones con el FMI")),
    "ap_act_tit_pub_cartera_pases_sist_financiero": (ACTIVO, R_TITULOS, (R_TITULOS, _CARTERA_PASES)),
    "ap_act_tit_pub_pases_sist_finan_tit_recibidos_op_pases_acts":
        (ACTIVO, R_TITULOS, (R_TITULOS, _CARTERA_PASES, "Títulos recibidos por pases activos")),
    "ap_act_tit_pub_pases_sist_finan_tit_recibidos_op_pases_cruz":
        (ACTIVO, R_TITULOS, (R_TITULOS, _CARTERA_PASES, "Títulos recibidos por pases cruzados")),
    # --- Activo: adelantos transitorios
    "ap_act_adelantos_transitorios_gob_nac": (ACTIVO, R_ADELANTOS, (R_ADELANTOS,)),
    "ap_act_adelantos_transitorios_gob_nac_pago_oblig_org_inter":
        (ACTIVO, R_ADELANTOS, (R_ADELANTOS, "Pago de obligaciones con organismos internacionales")),
    "ap_act_adelantos_transitorios_gob_nac_otras_aplicaciones":
        (ACTIVO, R_ADELANTOS, (R_ADELANTOS, "Otras aplicaciones")),
    # --- Activo: créditos al sistema financiero
    "ap_act_creditos_sist_financiero_pais": (ACTIVO, R_CREDITOS, (R_CREDITOS,)),
    "ap_act_creditos_sist_financiero_pais_entidades_financieras":
        (ACTIVO, R_CREDITOS, (R_CREDITOS, "Entidades financieras")),
    "ap_act_creditos_sist_finan_pais_prevision_incobrabilidad":
        (ACTIVO, R_CREDITOS, (R_CREDITOS, "Previsión por incobrabilidad")),
    "ap_act_cred_sist_finan_pais_bienes_dchos_recibir_art35_bis":
        (ACTIVO, R_CREDITOS, (R_CREDITOS, "Bienes y derechos a recibir (art. 35 bis)")),
    "ap_act_cred_sist_fin_pais_b_dchos_recib_art35bis_ley_ent_fin":
        (ACTIVO, R_CREDITOS, (R_CREDITOS, "Bienes y derechos a recibir (art. 35 bis, Ley 21.526)")),
    # --- Activo: derechos por pases, a término y derivados
    "ap_act_derechos_operaciones_pases": (ACTIVO, R_DERIVADOS, ("Derechos por operaciones de pases",)),
    "ap_act_derechos_operaciones_termino": (ACTIVO, R_DERIVADOS, ("Derechos por operaciones a término",)),
    "ap_act_dchos_provenientes_otros_instru_finan_derivados":
        (ACTIVO, R_DERIVADOS, ("Derechos provenientes de otros instrumentos financieros derivados",)),
    # --- Activo: organismos internacionales
    "ap_act_aportes_organismos_inter": (ACTIVO, R_ORGANISMOS_ACTIVO, ("Aportes a organismos internacionales",)),
    "ap_act_fondos_transf_gob_nac_colocaciones_fmi":
        (ACTIVO, R_ORGANISMOS_ACTIVO, ("Fondos transferidos al Gobierno Nacional por colocaciones al FMI",)),
    "ap_act_contra_uso_tramo_rsrvas_fdos_transf_gob_nac_coloc_fmi":
        (ACTIVO, R_ORGANISMOS_ACTIVO,
         ("Contrapartida del uso del tramo de reservas (fondos transferidos por colocaciones al FMI)",)),
    # --- Activo: otros activos
    "ap_act_otros_acts": (ACTIVO, R_OTROS_ACTIVOS, (R_OTROS_ACTIVOS,)),
    "ap_act_otros_acts_diversos": (ACTIVO, R_OTROS_ACTIVOS, (R_OTROS_ACTIVOS, "Activos diversos")),
    "ap_act_otros_acts_cuentas_transitorias_activas":
        (ACTIVO, R_OTROS_ACTIVOS, (R_OTROS_ACTIVOS, "Cuentas transitorias activas")),
    "ap_act_prevision_desvalorizacion_otros_acts":
        (ACTIVO, R_OTROS_ACTIVOS, (R_OTROS_ACTIVOS, "Previsión por desvalorización")),
    "total_act": (ACTIVO, "", ("Total del activo",)),
    # --- Pasivo: base monetaria
    "ap_pas_base_monetaria": (PASIVO, R_BASE, (R_BASE,)),
    "ap_pas_base_monetaria_circulacion_monetaria": (PASIVO, R_BASE, (R_BASE, "Circulación monetaria")),
    "ap_pas_base_monet_circ_monet_billetes_monedas_circ":
        (PASIVO, R_BASE, (R_BASE, "Circulación monetaria", "Billetes y monedas en circulación")),
    "ap_pas_base_monet_circ_monet_cheques_cancelat_pesos_circ":
        (PASIVO, R_BASE, (R_BASE, "Circulación monetaria", "Cheques cancelatorios en pesos")),
    "ap_pas_base_monetaria_cuentas_corrientes_pesos": (PASIVO, R_BASE, (R_BASE, "Cuentas corrientes en pesos")),
    # --- Pasivo: títulos y letras emitidos
    "ap_pas_titulos_emitidos_bcra": (PASIVO, R_EMITIDOS, ("Títulos emitidos por el BCRA",)),
    "ap_pas_letras_emitidas_moneda_nac": (PASIVO, R_EMITIDOS, ("Letras emitidas", "En moneda nacional")),
    "ap_pas_letras_emitidas_moneda_extranjera": (PASIVO, R_EMITIDOS, ("Letras emitidas", "En moneda extranjera")),
    "lebaq_nobac": (PASIVO, R_EMITIDOS, ("Letras emitidas", "Lebac y Nobac")),
    # --- Pasivo: depósitos
    "ap_pas_depositos_gob_nac_otros": (PASIVO, R_DEPOSITOS, ("Depósitos del Gobierno Nacional y otros",)),
    "ap_pas_depositos_gob_nac_otros_depositos":
        (PASIVO, R_DEPOSITOS, ("Depósitos del Gobierno Nacional y otros", "Otros depósitos")),
    "ap_pas_dep_gob_nac_otros_dep_fortalec_reservas_inter_bcra":
        (PASIVO, R_DEPOSITOS,
         ("Depósitos del Gobierno Nacional y otros", "Depósitos para el fortalecimiento de las reservas")),
    "ap_pas_otros_depositos_varios": (PASIVO, R_DEPOSITOS, ("Otros depósitos varios",)),
    # --- Pasivo: otras monedas
    "ap_pas_cuentas_corrientes_otras_monedas": (PASIVO, R_OTRAS_MONEDAS, ("Cuentas corrientes en otras monedas",)),
    "ap_pas_cc_otras_monedas":
        (PASIVO, R_OTRAS_MONEDAS, ("Cuentas corrientes en otras monedas", "Cuentas corrientes")),
    "ap_pas_ctas_corr_otras_monedas_fondo_liquidez_bancaria":
        (PASIVO, R_OTRAS_MONEDAS, ("Cuentas corrientes en otras monedas", "Fondo de liquidez bancaria")),
    "ap_pas_medios_pago_otras_monedas": (PASIVO, R_OTRAS_MONEDAS, ("Medios de pago en otras monedas",)),
    "ap_pas_medios_pago_otras_monedas_certificados_dep_inv":
        (PASIVO, R_OTRAS_MONEDAS, ("Medios de pago en otras monedas", "Certificados de depósito para la inversión")),
    "ap_pas_medios_pago_cheques_cancelat_otras_monedas_circ":
        (PASIVO, R_OTRAS_MONEDAS, ("Medios de pago en otras monedas", "Cheques cancelatorios en circulación")),
    # --- Pasivo: organismos internacionales
    "ap_pas_obligaciones_organismos_inter":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Obligaciones con organismos internacionales",)),
    "ap_pas_obligaciones_organismos_inter_fmi":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Obligaciones con organismos internacionales", "Fondo Monetario Internacional")),
    "ap_pas_obligaciones_organismos_inter_obligaciones":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Obligaciones con organismos internacionales", "Obligaciones")),
    "ap_pas_obligaciones_organismos_inter_otros":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Obligaciones con organismos internacionales", "Otros")),
    "ap_pas_oblig_org_inter_contrap_uso_tramo_reservas":
        (PASIVO, R_ORGANISMOS_PASIVO,
         ("Obligaciones con organismos internacionales", "Contrapartida del uso del tramo de reservas")),
    "ap_pas_deudas_convenios_multilaterales_credito":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Deudas por convenios multilaterales de crédito",)),
    "ap_pas_contrapartida_aportes_gob_nac_organismos_inter":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Contrapartida de aportes del Gobierno Nacional a organismos internacionales",)),
    "ap_pas_asignaciones_deg": (PASIVO, R_ORGANISMOS_PASIVO, ("Asignaciones de DEG",)),
    "ap_pas_apertura_asignaciones_deg":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Asignaciones de DEG", "Valor de las asignaciones")),
    "ap_pas_asignaciones_deg_contrapartida_asignaciones_deg":
        (PASIVO, R_ORGANISMOS_PASIVO, ("Asignaciones de DEG", "Contrapartida de asignaciones de DEG")),
    # --- Pasivo: pases, a término y derivados
    "ap_pas_obligaciones_operaciones_pase": (PASIVO, R_DERIVADOS, ("Obligaciones por operaciones de pase",)),
    "ap_pas_obligaciones_operaciones_termino": (PASIVO, R_DERIVADOS, ("Obligaciones por operaciones a término",)),
    "ap_pas_oblig_provenientes_otros_instru_finan_derivados":
        (PASIVO, R_DERIVADOS, ("Obligaciones provenientes de otros instrumentos financieros derivados",)),
    # --- Pasivo: otros pasivos
    "ap_pas_otros_pas": (PASIVO, R_OTROS_PASIVOS, (R_OTROS_PASIVOS,)),
    "ap_pas_otros_pas_cuentas_transitorias_pasivas":
        (PASIVO, R_OTROS_PASIVOS, (R_OTROS_PASIVOS, "Cuentas transitorias pasivas")),
    "ap_pas_otros": (PASIVO, R_OTROS_PASIVOS, (R_OTROS_PASIVOS, "Otros")),
    "ap_pas_previsiones": (PASIVO, R_OTROS_PASIVOS, ("Previsiones",)),
    "total_pas": (PASIVO, "", ("Total del pasivo",)),
    "total_pas_mas_patrimonio_neto": (PASIVO, "", ("Total pasivo más patrimonio neto",)),
    "total_patrimonio_neto": (PATRIMONIO, "", (PATRIMONIO,)),
}


def _hoja_balance(serie: Serie) -> Clasificacion | None:
    if serie.variable == "tipo_cambio":
        # Es el tipo de cambio de valuación contable que usa el balance, igual al de bcra-dmd.
        return _TIPO_DE_CAMBIO_CONTABLE
    encontrada = _BALANCE.get(serie.variable)
    if encontrada is None:
        return None
    n3, n4, partes = encontrada
    return _clasificacion(BALANCE, partes, n3=n3, n4=n4, unidad=MIL_PESOS)


# ======================================================================================
# 8.10 Lebac en el mercado secundario
# ======================================================================================

_LEBAC_SECUNDARIO = re.compile(r"^lebac_interna_pesos_(\d{4})_(\d{1,2})_(\d{1,2})$")
FRACCION = "Proporción (1 = 100%)"
# Variable y descripción dicen 2018-10-21, pero la serie llega hasta el 2018-11-20: vence el 21 de noviembre.
_LEBAC_FECHA_CORREGIDA = {"lebac_interna_pesos_2018_10_21": (2018, 11, 21)}


def _hoja_lebac(serie: Serie) -> Clasificacion | None:
    # La fecha sale de la variable: en tres series la descripción tiene el mes mal tipeado.
    coincide = _LEBAC_SECUNDARIO.match(serie.variable)
    if not coincide:
        return None
    año, mes, día = (int(parte) for parte in coincide.groups())
    if serie.variable in _LEBAC_FECHA_CORREGIDA:
        año, mes, día = _LEBAC_FECHA_CORREGIDA[serie.variable]
    vencimiento = f"Vencimiento {año:04d}-{mes:02d}-{día:02d}"
    return _clasificacion(INSTRUMENTOS, ("Lebac en pesos", vencimiento), n3=I_LEBAC, n4=I_SECUNDARIO,
                          medida=TNA, unidad=FRACCION)


# ======================================================================================
# 8.11 Factores de explicación de la base monetaria
# ======================================================================================

_FACTORES_811 = {
    "total_variacion": _factor(F_TOTAL, "Total"),
    "compras_netas_divisas_total": _factor(F_COMPRAS, F_COMPRAS, "Total"),
    "compras_netas_divisas_al_tesoro_nacional": _factor(F_COMPRAS, F_COMPRAS, "Al Tesoro Nacional"),
    "compras_netas_divisas_al_sector_privado_otros": _factor(F_COMPRAS, F_COMPRAS, "Al sector privado y otros"),
    "otras_operaciones_tesoro_nacional_total": _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Total"),
    "otras_operaciones_tesoro_nacional_adelantos_transitorios":
        _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Adelantos transitorios al Tesoro Nacional"),
    "otras_operaciones_tesoro_nacional_transferencia_utilidades":
        _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Transferencia de utilidades al Tesoro Nacional"),
    "otras_operaciones_tesoro_nacional_resto":
        _factor(F_SECTOR_PUBLICO, F_SECTOR_PUBLICO, "Resto de operaciones con el Tesoro Nacional"),
    "pases_redescuentos_total": _factor(F_PASES, F_PASES, "Total"),
    "pases_redescuentos_pases": _factor(F_PASES, F_PASES, "Pases"),
    "pases_redescuentos_leliq": _factor(F_PASES, F_PASES, "Leliq y Notaliq"),
    "pases_redescuentos_redescuentos_adelantos": _factor(F_PASES, F_PASES, "Redescuentos y adelantos"),
    "pases_redescuentos_intereses_primas_remun_ctas_ctes": _factor(F_PASES, F_PASES, INTERESES),
    "titulos_del_bcra": _factor(F_TITULOS, F_TITULOS, LEBAC_NOBAC),
    "operaciones_con_lefi": _factor(F_TITULOS, F_TITULOS, LEFI),
    "rescate_cuasimonedas": _factor(F_OTROS, F_OTROS, "Rescate de cuasimonedas"),
    "otros": _factor(F_OTROS, F_OTROS),
    "saldo_base_monetaria": _base(BM_TOTAL, "", ()),
}


def _hoja_factores(serie: Serie) -> Clasificacion | None:
    return _FACTORES_811.get(serie.variable)


# ======================================================================================
# Despacho
# ======================================================================================

_POR_HOJA = {
    HOJA_TASAS: _hoja_tasas,
    HOJA_AGREGADOS: _hoja_agregados,
    HOJA_PRESTAMOS: _hoja_prestamos,
    HOJA_ENCAJES: _hoja_encajes,
    HOJA_COEFICIENTES: _hoja_coeficientes,
    HOJA_HISTORICAS: _hoja_historicas,
    HOJA_BALANCE: _hoja_balance,
    HOJA_LEBAC: _hoja_lebac,
    HOJA_FACTORES: _hoja_factores,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == FUENTE_DMD:
        return _DMD.get(serie.id)
    if serie.fuente == FUENTE_DATOS and serie.archivo == ARCHIVO:
        regla = _POR_HOJA.get(serie.hoja)
        return regla(serie) if regla else None
    return None
