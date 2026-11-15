"""Reglas del libro «Series diarias de tasas de interés y montos colocados» del BCRA (``pas2026.xls``).

Fuente ``bcra-pas``: 19 hojas, todas diarias. Tema «Moneda y sistema financiero». Hay dos indicadores:

* ``Tasas de interés`` > ``Tasas de referencia por depósitos`` > BADLAR | TM20 | TAMAR: las tasas
  de referencia de la hoja ``Totales_diarios`` (las mismas que publican los datos monetarios
  del BCRA) y los montos operados con los que se calculan.
* ``Tasas y montos de depósitos`` > <quién deposita o dónde> > <producto>: el resto del libro.

Árbol del segundo indicador (nivel 3 = universo de depósitos que se mide, nivel 4 = producto):

    Total de entidades financieras     Estra_dia, Series_diarias, UVA_UVI y las tasas de caja y plazo fijo de Totales_diarios
    Bancos / Bancos privados /         Estra_dia_bancos, _bcos.priv, _bcos.pub, _no_banc. y sus Series_diarias_*
    Bancos públicos / Entidades
    financieras no bancarias
    Sector privado, Sector público,    Estra_dia_pri, _pub, _fis, _jur, _fin, _res y _ext: depósitos según quién los hace
    Personas físicas, Personas
    jurídicas, Prestadoras de
    servicios financieros, Otras
    personas jurídicas, Residentes
    en el exterior

Los productos (nivel 4) son los que arman el nombre: «Plazo fijo en pesos», «Plazo fijo en dólares»,
«Plazo fijo en UVA y UVI», «Caja de ahorros en pesos / dólares» y «Cuenta corriente remunerada en
pesos / dólares».

Cómo se deriva cada dimensión. Los encabezados del libro son jerárquicos y de celdas combinadas, y
en el inventario sólo la primera columna de cada bloque trae el encabezado completo; el resto sólo
la hoja del último nivel («Tasa de interés», «De 100.000 a 499.999 Monto»). Por eso la identidad sale
de la posición del código de variable, que es regular, y el texto de la fuente sólo se usa para
verificar (si el último rótulo o el estrato no coinciden con lo esperado, la serie queda sin regla
en vez de quedar mal rotulada):

* Hojas ``Estra_dia*``: los códigos ``pasdes<n>`` (con sufijo según la hoja) van de a 8 por bloque
  (n = 2 es el primero): para cada uno de los 4 estratos de monto, una columna de monto o saldo y
  una de tasa. Bloques: cuenta corriente remunerada en pesos (saldo), caja de ahorros en pesos
  (saldo), plazo fijo en pesos (total, hasta 59 días, 30 a 44, 45 a 59, 60 o más; monto colocado),
  cuenta corriente remunerada y caja de ahorros en dólares (saldo) y plazo fijo en dólares con los
  mismos plazos. ``Estra_dia`` agrega al final el plazo fijo total en pesos hasta $49.999.999
  (códigos 114 y 115) y ``Estra_dia_pub`` abre además los plazos de hasta 13 y de 14 a 29 días en
  dólares. El estrato está en la moneda del depósito (en dólares, «Hasta US$99.999»): los montos
  de los estratos en dólares son del orden de cientos de miles de miles de dólares por día, y
  serían imposibles si el corte fuera en pesos.
* ``Series_diarias*`` y las tasas ``tan…`` de ``Totales_diarios``: caja de ahorros común (1), plazo
  fijo de 30 a 59 días (2), de 60 o más días (3), a 30 días (4) y plazo fijo total, en pesos y en
  dólares. La caja de ahorros común coincide con la caja de ahorros sin incentivos de las hojas de
  estratos (el promedio ponderado por saldo de sus cuatro estratos da el mismo valor), por eso van
  al mismo concepto. El plazo fijo total de ``Series_diarias`` coincide con el promedio ponderado
  de las hojas de estratos.
* ``Totales_diarios``: BADLAR (depósitos de más de $1 millón o US$1 millón, 30 a 35 días), TM20
  (depósitos de 20 o más millones) y TAMAR (mayorista, en pesos), por tipo de banco, y sus montos
  operados.
* ``UVA_UVI``: plazo fijo con capital ajustable por CER o UVI, por 4 estratos de monto más el tramo
  hasta $49.999.999, cada uno con monto colocado, tasa y plazo (promedio).

Convenciones de nombre: «<Producto> · <plazo> · <estrato> · <universo> (<medida>)». Sin sufijo es la
tasa de interés; «(monto colocado)» es el monto colocado en plazo fijo, «(saldo)» el saldo de
cuentas a la vista y «(plazo promedio)» el plazo del plazo fijo en UVA y UVI. El universo se omite
para el total de entidades financieras, que es el natural del libro.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

FUENTE = "bcra-pas"
ARCHIVO = "pas2026.xls"
TEMA = "Moneda y sistema financiero"

# Tasas de referencia (indicador compartido con las tasas de los datos monetarios del BCRA)
INDICADOR_REFERENCIA = "Tasas de interés"
COBERTURA_REFERENCIA = "Tasas de referencia por depósitos"
# Resto del libro
INDICADOR = "Tasas y montos de depósitos"

TASA = "% nominal anual"
MILES_PESOS = "Miles de pesos"
MILES_DOLARES = "Miles de dólares"
DIAS = "Días"

MONTO_OPERADO = "monto operado"
MONTO_COLOCADO = "monto"
SALDO = "saldo"
PLAZO_PROMEDIO = "plazo promedio"

# --- Universos (nivel 3) -------------------------------------------------------------------
TOTAL = "Total de entidades financieras"
# hoja -> (universo del nivel 3, cómo se escribe en el nombre; vacío para el total)
_UNIVERSOS = {
    "Estra_dia": (TOTAL, ""),
    "Series_diarias": (TOTAL, ""),
    "Totales_diarios": (TOTAL, ""),
    "UVA_UVI": (TOTAL, ""),
    "Estra_dia_bancos": ("Bancos", "Bancos"),
    "Series_diarias_Bancos": ("Bancos", "Bancos"),
    "Estra_dia_bcos.priv": ("Bancos privados", "Bancos privados"),
    "Series_diarias_bcos.privados": ("Bancos privados", "Bancos privados"),
    "Estra_dia_bcos.pub": ("Bancos públicos", "Bancos públicos"),
    "Series_diarias_bcos.publicos": ("Bancos públicos", "Bancos públicos"),
    "Estra_dia_no_banc.": ("Entidades financieras no bancarias", "Entidades financieras no bancarias"),
    "Series_diarias_no_bancarias": ("Entidades financieras no bancarias", "Entidades financieras no bancarias"),
    "Estra_dia_pri": ("Sector privado", "Sector privado"),
    "Estra_dia_pub": ("Sector público", "Sector público"),
    "Estra_dia_fis": ("Personas físicas", "Personas físicas"),
    "Estra_dia_jur": ("Personas jurídicas", "Personas jurídicas"),
    "Estra_dia_fin": ("Prestadoras de servicios financieros", "Prestadoras de servicios financieros"),
    "Estra_dia_res": ("Otras personas jurídicas", "Otras personas jurídicas"),
    "Estra_dia_ext": ("Residentes en el exterior", "Residentes en el exterior"),
}
_HOJAS_ESTRATOS = tuple(hoja for hoja in _UNIVERSOS if hoja.startswith("Estra_dia"))
_HOJAS_SERIES = tuple(hoja for hoja in _UNIVERSOS if hoja.startswith("Series_diarias"))

# --- Productos (nivel 4) --------------------------------------------------------------------
PESOS = "pesos"
DOLARES = "dólares"
_SIMBOLO = {PESOS: "$", DOLARES: "US$"}
_UNIDAD_MONTO = {PESOS: MILES_PESOS, DOLARES: MILES_DOLARES}

CUENTA_CORRIENTE = "Cuenta corriente remunerada"
CAJA_DE_AHORROS = "Caja de ahorros"
PLAZO_FIJO = "Plazo fijo"
PLAZO_FIJO_UVA = "Plazo fijo en UVA y UVI"


def _producto(producto: str, moneda: str) -> str:
    return f"{producto} en {moneda}"


# --- Estratos de monto ----------------------------------------------------------------------
# (rótulo con %s para el símbolo de la moneda, fragmento que debe aparecer en el rótulo de la fuente)
_ESTRATOS = (
    ("Hasta %s99.999", "hasta 99.999"),
    ("De %s100.000 a %s499.999", "499.999"),
    ("De %s500.000 a %s999.999", "999.999"),
    ("De %s1.000.000 y más", "1.000.000 y mas"),
)
_ESTRATO_HASTA_49_999_999 = ("Hasta %s49.999.999", "hasta 49.999.999")


def _estrato(plantilla: str, moneda: str) -> str:
    return plantilla.replace("%s", _SIMBOLO[moneda])


# --- Hojas de estratos (Estra_dia*) ----------------------------------------------------------
# Bloque -> (producto, moneda, plazo del nombre, medida del monto). Cada bloque son 8 columnas
# consecutivas desde el código 2: por estrato, una de monto o saldo y una de tasa.
_PLAZO_TOTAL = ""
_BLOQUES = (
    (CUENTA_CORRIENTE, PESOS, _PLAZO_TOTAL, SALDO),
    (CAJA_DE_AHORROS, PESOS, _PLAZO_TOTAL, SALDO),
    (PLAZO_FIJO, PESOS, _PLAZO_TOTAL, MONTO_COLOCADO),
    (PLAZO_FIJO, PESOS, "Hasta 59 días", MONTO_COLOCADO),
    (PLAZO_FIJO, PESOS, "30 a 44 días", MONTO_COLOCADO),
    (PLAZO_FIJO, PESOS, "45 a 59 días", MONTO_COLOCADO),
    (PLAZO_FIJO, PESOS, "60 días o más", MONTO_COLOCADO),
    (CUENTA_CORRIENTE, DOLARES, _PLAZO_TOTAL, SALDO),
    (CAJA_DE_AHORROS, DOLARES, _PLAZO_TOTAL, SALDO),
    (PLAZO_FIJO, DOLARES, _PLAZO_TOTAL, MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "Hasta 59 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "30 a 44 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "45 a 59 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "60 días o más", MONTO_COLOCADO),
)
# El sector público desagrega además los plazos cortos en dólares: tras «Hasta 59 días» (bloque 10)
# siguen «Hasta 13 días» y «14 a 29 días», y después los plazos de siempre.
_BLOQUES_SECTOR_PUBLICO = _BLOQUES[:11] + (
    (PLAZO_FIJO, DOLARES, "Hasta 13 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "14 a 29 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "30 a 44 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "45 a 59 días", MONTO_COLOCADO),
    (PLAZO_FIJO, DOLARES, "60 días o más", MONTO_COLOCADO),
)

_CODIGO_ESTRATOS = re.compile(r"pasdes(?:bancos|priv|pub|fis|jur|fin|res|ext|pri)?(\d+)")


def _rotulo(serie: Serie) -> str:
    """Texto de la fuente sin el guion que parte las palabras de las celdas combinadas."""
    return plano(serie.nombre).replace(" - ", " ")


def _termina_en(serie: Serie, *finales: str) -> bool:
    """El último rótulo de la fuente («Tasa de interés», «Monto», «Saldo», «Plazo»)."""
    return plano(serie.nombre).endswith(finales)


def _estratos(serie: Serie) -> Clasificacion | None:
    coincidencia = _CODIGO_ESTRATOS.fullmatch(serie.variable)
    if coincidencia is None:
        return None
    posicion = int(coincidencia.group(1)) - 2
    bloques = _BLOQUES_SECTOR_PUBLICO if serie.hoja == "Estra_dia_pub" else _BLOQUES
    universo, sufijo = _UNIVERSOS[serie.hoja]

    if serie.hoja == "Estra_dia" and posicion in (112, 113):
        # Plazo fijo total en pesos de hasta $49.999.999: columnas 114 (monto) y 115 (tasa).
        producto, moneda, plazo, medida = _BLOQUES[2]
        estrato, fragmento = _ESTRATO_HASTA_49_999_999
        es_tasa = posicion == 113
    else:
        bloque, dentro = divmod(posicion, 8)
        if posicion < 0 or bloque >= len(bloques):
            return None
        producto, moneda, plazo, medida = bloques[bloque]
        estrato, fragmento = _ESTRATOS[dentro // 2]
        es_tasa = dentro % 2 == 1

    # Verificación contra el rótulo de la fuente: si el libro cambia de disposición, no se rotula.
    if es_tasa:
        if not _termina_en(serie, "interes"):
            return None
    else:
        if not _termina_en(serie, "monto", "saldo"):
            return None
        rotulo = _rotulo(serie)
        if fragmento not in rotulo:
            return None
        if "u$s" in rotulo and moneda != DOLARES:
            return None

    concepto = _producto(producto, moneda)
    nombre = unir(concepto, plazo, _estrato(estrato, moneda), sufijo)
    if es_tasa:
        return Clasificacion(TEMA, INDICADOR, nombre, cobertura=universo, apertura=concepto, unidad=TASA)
    return Clasificacion(
        TEMA, INDICADOR, con_medida(nombre, medida), cobertura=universo, apertura=concepto,
        unidad=_UNIDAD_MONTO[moneda],
    )


# --- Series_diarias* y tasas tan… de Totales_diarios ---------------------------------------------
_CODIGO_DIARIAS = re.compile(r"(?:ban|priv|pub|nob)?tan(pes|dol|plapes|pladol)(\d?)")
_MONEDA_DIARIAS = {"pes": PESOS, "plapes": PESOS, "dol": DOLARES, "pladol": DOLARES}
# número de columna -> (producto, plazo del nombre, texto que debe tener el rótulo)
_COLUMNAS_DIARIAS = {
    "1": (CAJA_DE_AHORROS, "", ("en caja", "tasas de interes", "depositos de")),
    "2": (PLAZO_FIJO, "30 a 59 días", ("de 30 a 59",)),
    "3": (PLAZO_FIJO, "60 días o más", ("de 60 o",)),
    "4": (PLAZO_FIJO, "A 30 días", ("a 30 dias",)),
    "": (PLAZO_FIJO, "", ("a plazo fijo",)),
}


def _diarias(serie: Serie) -> Clasificacion | None:
    coincidencia = _CODIGO_DIARIAS.fullmatch(serie.variable)
    if coincidencia is None:
        return None
    clave, numero = coincidencia.groups()
    # tanplapes y tanpladol no llevan número; tanpes y tandol siempre.
    if (clave in ("pes", "dol")) == (numero == ""):
        return None
    producto, plazo, esperado = _COLUMNAS_DIARIAS[numero]
    rotulo = plano(serie.nombre)
    if not any(texto in rotulo for texto in esperado):
        return None
    moneda = _MONEDA_DIARIAS[clave]
    universo, sufijo = _UNIVERSOS[serie.hoja]
    concepto = _producto(producto, moneda)
    return Clasificacion(
        TEMA, INDICADOR, unir(concepto, plazo, sufijo), cobertura=universo, apertura=concepto, unidad=TASA,
    )


# --- Tasas de referencia (Totales_diarios) -----------------------------------------------------
# código -> (tasa de referencia, moneda, tipo de bancos, es monto). Los bancos «Total de bancos»
# son el total de bancos informantes.
_TOTAL_BANCOS = "Total de bancos"
_PUBLICOS = "Bancos públicos"
_PRIVADOS = "Bancos privados"
_REFERENCIA = {
    "tan327": ("BADLAR", PESOS, _TOTAL_BANCOS, False),
    "tan328": ("BADLAR", PESOS, _PUBLICOS, False),
    "tan329": ("BADLAR", PESOS, _PRIVADOS, False),
    "tan339": ("BADLAR", DOLARES, _TOTAL_BANCOS, False),
    "tan340": ("BADLAR", DOLARES, _PUBLICOS, False),
    "tan341": ("BADLAR", DOLARES, _PRIVADOS, False),
    "monpesbadpub": ("BADLAR", PESOS, _PUBLICOS, True),
    "monpesbadpri": ("BADLAR", PESOS, _PRIVADOS, True),
    "mondolbadpub": ("BADLAR", DOLARES, _PUBLICOS, True),
    "mondolbadpri": ("BADLAR", DOLARES, _PRIVADOS, True),
    "TM2001": ("TM20", PESOS, _TOTAL_BANCOS, False),
    "TM2002": ("TM20", PESOS, _PUBLICOS, False),
    "TM2003": ("TM20", PESOS, _PRIVADOS, False),
    "TM2004": ("TM20", DOLARES, _TOTAL_BANCOS, False),
    "TM2005": ("TM20", DOLARES, _PUBLICOS, False),
    "TM2006": ("TM20", DOLARES, _PRIVADOS, False),
    "TM2008": ("TM20", PESOS, _PUBLICOS, True),
    "TM2009": ("TM20", PESOS, _PRIVADOS, True),
    "TM2011": ("TM20", DOLARES, _PUBLICOS, True),
    "TM2012": ("TM20", DOLARES, _PRIVADOS, True),
    "tamar01": ("TAMAR", PESOS, _TOTAL_BANCOS, False),
    "tamar02": ("TAMAR", PESOS, _PUBLICOS, False),
    "tamar03": ("TAMAR", PESOS, _PRIVADOS, False),
    "tamar04": ("TAMAR", PESOS, _PUBLICOS, True),
    "tamar05": ("TAMAR", PESOS, _PRIVADOS, True),
}


def _referencia(serie: Serie) -> Clasificacion | None:
    datos = _REFERENCIA.get(serie.variable)
    if datos is None:
        return None
    tasa, moneda, bancos, es_monto = datos
    rotulo = plano(serie.nombre)
    # Los montos dicen «Montos operados»; las tasas, no.
    if es_monto != ("montos" in rotulo and "operados" in rotulo):
        return None
    concepto = tasa if moneda == PESOS else f"{tasa} en dólares"
    nombre = unir(concepto, bancos)
    if es_monto:
        return Clasificacion(
            TEMA, INDICADOR_REFERENCIA, con_medida(nombre, MONTO_OPERADO),
            cobertura=COBERTURA_REFERENCIA, apertura=tasa, unidad=_UNIDAD_MONTO[moneda],
        )
    return Clasificacion(
        TEMA, INDICADOR_REFERENCIA, nombre, cobertura=COBERTURA_REFERENCIA, apertura=tasa, unidad=TASA,
    )


# --- UVA_UVI ---------------------------------------------------------------------------------------
_CODIGO_UVA = re.compile(r"pasvar(\d+)")


def _uva(serie: Serie) -> Clasificacion | None:
    coincidencia = _CODIGO_UVA.fullmatch(serie.variable)
    if coincidencia is None:
        return None
    posicion = int(coincidencia.group(1)) - 1
    grupo, tipo = divmod(posicion, 3)
    if posicion < 0 or grupo > 4:
        return None
    estrato, fragmento = _ESTRATOS[grupo] if grupo < 4 else _ESTRATO_HASTA_49_999_999
    esperado = (("monto", "saldo"), ("interes",), ("plazo",))[tipo]
    if not _termina_en(serie, *esperado):
        return None
    if tipo == 0 and fragmento not in _rotulo(serie):
        return None
    nombre = unir(PLAZO_FIJO_UVA, _estrato(estrato, PESOS))
    if tipo == 0:
        return Clasificacion(TEMA, INDICADOR, con_medida(nombre, MONTO_COLOCADO), cobertura=TOTAL,
                             apertura=PLAZO_FIJO_UVA, unidad=MILES_PESOS)
    if tipo == 1:
        return Clasificacion(TEMA, INDICADOR, nombre, cobertura=TOTAL, apertura=PLAZO_FIJO_UVA, unidad=TASA)
    return Clasificacion(TEMA, INDICADOR, con_medida(nombre, PLAZO_PROMEDIO), cobertura=TOTAL,
                         apertura=PLAZO_FIJO_UVA, unidad=DIAS)


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.archivo != ARCHIVO or serie.hoja not in _UNIVERSOS:
        return None
    if serie.hoja in _HOJAS_ESTRATOS:
        return _estratos(serie)
    if serie.hoja == "UVA_UVI":
        return _uva(serie)
    if serie.hoja == "Totales_diarios":
        return _referencia(serie) or _diarias(serie)
    if serie.hoja in _HOJAS_SERIES:
        return _diarias(serie)
    return None
