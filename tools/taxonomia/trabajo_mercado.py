"""Mercado de trabajo según la EPH, empleo (EIL), pobreza, distribución del ingreso y política de ingresos
(``empleo_ingresos.xlsx`` de datos.gob.ar).

Cada serie se reconoce por ``fuente``, ``archivo`` y ``hoja``. La identidad sale de ``Variable`` o
de ``Descripción`` (nunca de los IDs: en ``TE 03-`` y ``TSD 03-`` las siglas de las regiones Noreste y
Noroeste están cruzadas respecto de la descripción, y las descripciones se verificaron contra los datos:
tasa de empleo = tasa de actividad × (1 − tasa de desocupación) por región).

Árbol (Tema > indicador > cobertura o versión > apertura):

    Trabajo e ingresos > Tasas del mercado de trabajo (EPH)
        > Total aglomerados | Regiones | Aglomerados      > tasa (Actividad, Empleo, Desocupación, …)
        > Series históricas (EPH puntual 1974-2003)       > tasa
    Trabajo e ingresos > Población según condición de actividad (EPH)
        > Total aglomerados | Total urbano | Series históricas (expansión a total urbano 1991-2010)
    Trabajo e ingresos > Empleo (EIL)
        > Total | Sectores de actividad | Aglomerados
    Trabajo e ingresos > Pobreza e indigencia
        > Total aglomerados | Regiones | Aglomerados      > Pobreza | Indigencia
        > Series históricas (EPH puntual, Gran Buenos Aires 1988-2003)     > Pobreza | Indigencia
    (los cuadros 63.1 y 64.1, rotulados «EPH puntual 2001-2003», no se publican: ver ``EXCLUIDAS``)
    Trabajo e ingresos > Distribución del ingreso > Ingreso per cápita familiar | Ingreso de la ocupación principal
    Trabajo e ingresos > Salario mínimo y haberes
    Trabajo e ingresos > Asignación Universal por Hijo
    Precios > Canastas básicas > <región> | Ciudad Autónoma de Buenos Aires | Series históricas

Convenciones de nombre: «Tasa de actividad · Gran Córdoba»; el total de aglomerados (la cobertura natural
de la EPH) no se escribe. Los aglomerados y las regiones llevan siempre el mismo nombre en todas las hojas
(los nombres compuestos con la grafía del INDEC, con raya « – »: «Bahía Blanca – Cerri»; la región GBA de
las canastas se llama «GBA», igual que en el IPC, y el aglomerado de la EPH «Gran Buenos Aires»).
Las regiones se escriben sin la palabra «Región» (igual que el IPC): «Pampeana», «Noreste».
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import plano, unir

FUENTE = "datos.gob.ar"
ARCHIVO = "empleo_ingresos.xlsx"

TEMA = "Trabajo e ingresos"
PRECIOS = "Precios"

PROPORCION = "Proporción (1 = 100%)"
MILES_DE_PERSONAS = "Miles de personas"
PESOS_CORRIENTES = "Pesos corrientes"

HISTORICAS = "Series históricas"
TOTAL_AGLOMERADOS = "Total aglomerados"
REGIONES = "Regiones"
AGLOMERADOS = "Aglomerados"

# ===================================================================================================
# Coberturas geográficas de la EPH
# ===================================================================================================
# (nombre oficial, expresiones sobre el texto sin tildes ni mayúsculas). El orden importa: los
# partidos van antes que el Gran Buenos Aires, cuyo nombre aparece al final de «Partidos del Gran…».
_AGLOMERADOS = (
    ("Ciudad Autónoma de Buenos Aires", (r"ciudad (?:autonoma )?de buenos aires", r"\bcaba\b")),
    ("Partidos del GBA", (r"partidos a partir del 98", r"partidos gba", r"partidos del gran buenos aires")),
    ("Partidos del conurbano", (r"partidos del conurbano",)),
    ("Gran Buenos Aires", (r"(?:^|\s)gran buenos aires$", r"^gba$")),
    ("Gran La Plata", (r"la plata",)),
    ("Bahía Blanca – Cerri", (r"bahia blanca",)),
    ("Mar del Plata – Batán", (r"mar del plata",)),
    ("San Nicolás – Villa Constitución", (r"san nicolas",)),
    ("Gran Rosario", (r"rosario",)),
    ("Gran Santa Fe", (r"gran santa fe",)),
    ("Gran Paraná", (r"parana",)),
    ("Concordia", (r"concordia",)),
    ("Gran Córdoba", (r"gran cordoba",)),
    ("Río Cuarto", (r"rio cuarto",)),
    ("Gran Mendoza", (r"mendoza",)),
    ("Gran San Juan", (r"san juan",)),
    ("San Luis – El Chorrillo", (r"san luis",)),
    ("Gran Tucumán – Tafí Viejo", (r"tucuman",)),
    ("Jujuy – Palpalá", (r"jujuy",)),
    ("Salta", (r"\bsalta\b",)),
    ("Santiago del Estero – La Banda", (r"santiago del estero",)),
    ("Gran Catamarca", (r"catamarca",)),
    ("La Rioja", (r"la rioja",)),
    ("Corrientes", (r"corrientes",)),
    ("Formosa", (r"formosa",)),
    ("Gran Resistencia", (r"resistencia",)),
    ("Posadas", (r"posadas",)),
    ("Santa Rosa – Toay", (r"santa rosa",)),
    ("Neuquén – Plottier", (r"neuquen|nuequen",)),
    ("Rawson – Trelew", (r"rawson",)),
    ("Comodoro Rivadavia – Rada Tilly", (r"comodoro",)),
    ("Río Gallegos", (r"gallegos",)),
    ("Ushuaia – Río Grande", (r"ushuaia",)),
    ("Viedma – Carmen de Patagones", (r"viedma",)),
)

_REGIONES = (
    ("Cuyo", (r"\bcuyo\b",)),
    ("Pampeana", (r"\bpampeana\b",)),
    ("Noreste", (r"\bnord?este\b",)),
    ("Noroeste", (r"\bnoroeste\b",)),
    ("Patagonia", (r"\bpatagonia\b",)),
)

_INTERIOR = "Total aglomerados del interior"
_MAS_500 = "Aglomerados de más de 500.000 habitantes"
_MENOS_500 = "Aglomerados de menos de 500.000 habitantes"

_TOTALES = (
    ("", (r"^$", r"^total$", r"^total aglomerados( urbanos)?$")),
    (_INTERIOR, (r"interior", r"^aip$")),
    (_MAS_500, (r"mas de 500",)),
    (_MENOS_500, (r"menos de 500",)),
)


def _busca(texto: str, tabla) -> str | None:
    for nombre, patrones in tabla:
        if any(re.search(patron, texto) for patron in patrones):
            return nombre
    return None


def _cobertura(texto: str) -> tuple[str, str] | None:
    """(grupo, nombre) de un total, una región o un aglomerado; el total nacional tiene nombre vacío."""
    texto = plano(texto).strip(" .")
    total = _busca(texto, _TOTALES)
    if total is not None or texto == "":
        return TOTAL_AGLOMERADOS, total or ""
    region = _busca(texto, _REGIONES)
    if region:
        return REGIONES, region
    aglomerado = _busca(texto, _AGLOMERADOS)
    if aglomerado:
        return AGLOMERADOS, aglomerado
    return None


# ===================================================================================================
# Tasas del mercado de trabajo (EPH)
# ===================================================================================================
TASAS_INDICADOR = "Tasas del mercado de trabajo (EPH)"

# clave → (apertura, nombre de la tasa)
_TASAS = {
    "actividad": ("Actividad", "Tasa de actividad"),
    "empleo": ("Empleo", "Tasa de empleo"),
    "desocupacion": ("Desocupación", "Tasa de desocupación"),
    "subocupacion": ("Subocupación", "Tasa de subocupación"),
    "subocupacion_demandante": ("Subocupación demandante", "Tasa de subocupación demandante"),
    "subocupacion_no_demandante": ("Subocupación no demandante", "Tasa de subocupación no demandante"),
    "asalarizacion": ("Asalarización", "Tasa de asalarización"),
    "sin_descuento": ("Asalariados sin descuento jubilatorio",
                      "Tasa de asalariados sin descuento jubilatorio"),
}


def _tasa(clave: str, cobertura: tuple[str, str], historica: bool = False) -> Clasificacion:
    apertura, nombre = _TASAS[clave]
    grupo, lugar = cobertura
    return Clasificacion(TEMA, TASAS_INDICADOR, unir(nombre, lugar),
                         cobertura=HISTORICAS if historica else grupo, apertura=apertura, unidad=PROPORCION)


_HOJA_TASA_03 = {
    "TA 03-": "actividad", "TE 03-": "empleo", "TD 03-": "desocupacion", "TS 03-": "subocupacion",
    "TSD 03-": "subocupacion_demandante", "TSND 03-": "subocupacion_no_demandante",
}
_HOJA_TASA_74 = {
    "TA 74-03": "actividad", "TE 74-03": "empleo", "TD 74-03": "desocupacion", "TS 74-03": "subocupacion",
}


def _tasa_desde_2003(serie: Serie) -> Clasificacion | None:
    """Hojas ``TA 03-`` … ``TSND 03-``: «Tasa de … total <aglomerado>. En porcentaje.»"""
    partes = re.split(r"\btotal\b", serie.descripcion, maxsplit=1, flags=re.IGNORECASE)
    if len(partes) != 2:
        return None
    cobertura = _cobertura(partes[1].replace("En porcentaje", ""))
    return _tasa(_HOJA_TASA_03[serie.hoja], cobertura) if cobertura else None


def _tasa_puntual(serie: Serie) -> Clasificacion | None:
    """Hojas ``TA 74-03`` … ``TS 74-03``: la descripción es el aglomerado, con la provincia adelante."""
    cobertura = _cobertura(serie.descripcion)
    return _tasa(_HOJA_TASA_74[serie.hoja], cobertura, historica=True) if cobertura else None


def _tasa_eph(serie: Serie) -> Clasificacion | None:
    """Hoja ``EPH``: Total, GBA y aglomerados del interior (AIP); anual empalmada, semestral puntual y trimestral."""
    m = re.fullmatch(r"eph_(puntual_continua|puntual|continua)_(\w+)", serie.variable)
    if not m:
        return None
    version, resto = m.groups()
    palabras = resto.split("_")
    if palabras[0] not in ("actividad", "empleo", "desocupacion", "subocupacion"):
        return None
    clave = palabras[0]
    if clave == "subocupacion" and "demandante" in palabras:
        clave = "subocupacion_no_demandante" if "no" in palabras else "subocupacion_demandante"
    lugar = {"aip": _INTERIOR, "gba": "Gran Buenos Aires"}.get(palabras[-1], "")
    cobertura = _cobertura(lugar)
    # La anual (1982 en adelante) empalma la puntual con la continua y sigue vigente; la semestral
    # es la EPH puntual cerrada en 2003.
    return _tasa(clave, cobertura, historica=version == "puntual")


def _asalarizacion(serie: Serie) -> Clasificacion | None:
    clave = {"tasa_asalarizacion": "asalarizacion",
             "asalariados_sin_descuento_jubilatorio": "sin_descuento"}.get(serie.variable)
    return _tasa(clave, _cobertura("")) if clave else None


def _asalarizacion_regiones(serie: Serie) -> Clasificacion | None:
    clave = "sin_descuento" if serie.variable.startswith("asalariados_sin_") else (
        "asalarizacion" if serie.variable.startswith("tasa_asalarizacion") else None)
    if clave is None:
        return None
    lugar = serie.descripcion.split("Trimestral.", 1)[-1].replace("Total Aglomerados Interior",
                                                                  "Total aglomerados interior")
    cobertura = _cobertura(lugar)
    return _tasa(clave, cobertura) if cobertura else None


# ===================================================================================================
# Población según condición de actividad (EPH)
# ===================================================================================================
POBLACION_INDICADOR = "Población según condición de actividad (EPH)"
TOTAL_URBANO = "Total urbano"

_POBLACIONES = {
    "poblacion_total": "Población total",
    "pea": "Población económicamente activa",
    "ocupados": "Población ocupada",
    "desocupados": "Población desocupada",
    "subocupados": "Población subocupada",
    "subocupados_demandantes": "Población subocupada demandante",
    "subocupados_no_demandantes": "Población subocupada no demandante",
}


def _poblacion(serie: Serie) -> Clasificacion | None:
    if serie.hoja == "EPH-Poblaciones":
        m = re.fullmatch(r"total_aglomerados_eph_(\w+)", serie.variable)
        grupo, lugar = TOTAL_AGLOMERADOS, ""
    else:
        m = re.fullmatch(r"(\w+)_total_urbano", serie.variable)
        grupo, lugar = (HISTORICAS if serie.hoja.endswith("1991-2010") else TOTAL_URBANO), TOTAL_URBANO
    nombre = _POBLACIONES.get(m.group(1)) if m else None
    if nombre is None:
        return None
    return Clasificacion(TEMA, POBLACION_INDICADOR, unir(nombre, lugar), cobertura=grupo,
                         unidad=MILES_DE_PERSONAS)


# ===================================================================================================
# Empleo (EIL)
# ===================================================================================================
EIL_INDICADOR = "Empleo (EIL)"
EIL_NOMBRE = "Índice de empleo"
EIL_UNIDAD = "Índice ago-2001=100"

_EIL_SECTORES = {
    "indice_total": ("Total", ""),
    "indice_industria": ("Sectores de actividad", "Industria"),
    "indice_construccion": ("Sectores de actividad", "Construcción"),
    "indice_comercio_servicios": ("Sectores de actividad", "Comercio y servicios"),
}
_EIL_AGLOMERADOS = {
    "indice_gran_buenos_aires": "Gran Buenos Aires", "indice_gran_cordoba": "Gran Córdoba",
    "indice_gran_mendoza": "Gran Mendoza", "indice_gran_rosario": "Gran Rosario",
}


def _eil(serie: Serie) -> Clasificacion | None:
    if serie.hoja == "EIL - Sector":
        datos = _EIL_SECTORES.get(serie.variable)
        if datos is None:
            return None
        grupo, lugar = datos
    else:
        lugar = _EIL_AGLOMERADOS.get(serie.variable)
        if lugar is None:
            return None
        grupo = AGLOMERADOS
    return Clasificacion(TEMA, EIL_INDICADOR, unir(EIL_NOMBRE, lugar), cobertura=grupo, unidad=EIL_UNIDAD)


# ===================================================================================================
# Pobreza e indigencia
# ===================================================================================================
POBREZA_INDICADOR = "Pobreza e indigencia"

_POBREZA_HOJAS = {
    "LP - Hog": ("Pobreza", "Incidencia de la pobreza", "Hogares"),
    "LP - Pers": ("Pobreza", "Incidencia de la pobreza", "Personas"),
    "LI - Hog": ("Indigencia", "Incidencia de la indigencia", "Hogares"),
    "LI - Pers": ("Indigencia", "Incidencia de la indigencia", "Personas"),
}
_POBREZA_PUNTUAL = {
    "indigentes_hogares": ("Indigencia", "Incidencia de la indigencia", "Hogares"),
    "indigentes_poblacion": ("Indigencia", "Incidencia de la indigencia", "Personas"),
    "pobres_hogares": ("Pobreza", "Incidencia de la pobreza", "Hogares"),
    "pobres_personas": ("Pobreza", "Incidencia de la pobreza", "Personas"),
}
_LUGAR_POBREZA = re.compile(r"\(%\)\s+(?:de 2001 a 2003|desde 2003)\.\s*(.*?)\.?(?:\s*EPH (?:puntual|continua)\.?)?$")


def _pobreza(serie: Serie) -> Clasificacion | None:
    if serie.hoja == "EPH Puntual - PeI":
        datos = _POBREZA_PUNTUAL.get(serie.variable)
        cobertura = _cobertura("Gran Buenos Aires")
        historica = True
    else:
        datos = _POBREZA_HOJAS[serie.hoja]
        m = _LUGAR_POBREZA.search(serie.descripcion)
        cobertura = _cobertura(m.group(1)) if m else None
        # Los cuadros 63.1 y 64.1 (rotulados «EPH puntual 2001-2003») están excluidos de la publicación
        # (ver ``EXCLUIDAS``); la regla los deja en «Series históricas» sólo para que tengan clasificación.
        # Las hojas LI (cuadros 61.1 y 62.1, rotuladas «2001 a 2003») y las LP con 63.2 y 64.2 traen la
        # EPH continua 1.er semestre de 2003 a 2.º de 2006: son series vigentes por cobertura.
        historica = serie.id.startswith(("63.1_", "64.1_"))
    if datos is None or cobertura is None:
        return None
    apertura, concepto, unidad_de_cuenta = datos
    return Clasificacion(TEMA, POBREZA_INDICADOR, unir(concepto, unidad_de_cuenta, cobertura[1]),
                         cobertura=HISTORICAS if historica else cobertura[0],
                         apertura=apertura, unidad=PROPORCION)


# ===================================================================================================
# Distribución del ingreso
# ===================================================================================================
INGRESO_INDICADOR = "Distribución del ingreso"
_IPCF = "Ingreso per cápita familiar"
_IOP = "Ingreso de la ocupación principal"
_CONCEPTO = {"ipcf": _IPCF, "iop": _IOP, "oip": _IOP}

_DP = {
    "coeficiente_gini_iop": ("Coeficiente de Gini", _IOP, "Coeficiente (0 a 1)"),
    "coeficiente_gini_ipcf": ("Coeficiente de Gini", _IPCF, "Coeficiente (0 a 1)"),
    "brecha_ingresos_iop": ("Brecha de ingresos P90/P10", _IOP, "Cociente P90/P10"),
    "brecha_ingresos_ipcf": ("Brecha de ingresos P90/P10", _IPCF, "Cociente P90/P10"),
}


def _distribucion(serie: Serie) -> Clasificacion | None:
    if serie.hoja == "DP":
        datos = _DP.get(serie.variable)
        if datos is None:
            return None
        medida, concepto, unidad = datos
        return Clasificacion(TEMA, INGRESO_INDICADOR, unir(medida, concepto), cobertura=concepto, unidad=unidad)
    m = re.fullmatch(r"(decil_(\d+)|total)_(ipcf|oip)", serie.variable)
    if not m:
        return None
    concepto = _CONCEPTO[m.group(3)]
    # «Total deciles» es el ingreso medio de toda la población (es el promedio de los diez deciles).
    parte = "Total" if m.group(1) == "total" else f"Decil {m.group(2)}"
    return Clasificacion(TEMA, INGRESO_INDICADOR, unir(concepto, parte), cobertura=concepto,
                         unidad=PESOS_CORRIENTES)


# ===================================================================================================
# Salario mínimo, haberes y asignación
# ===================================================================================================
SALARIO_INDICADOR = "Salario mínimo y haberes"
_SMVM = {
    "salario_minimo_vital_movil_mensual": "Por mes",
    "salario_minimo_vital_movil_diario": "Por día",
    "salario_minimo_vital_movil_hora": "Por hora",
}


def _salario_minimo(serie: Serie) -> Clasificacion | None:
    base = _SMVM.get(serie.variable)
    if base is None:
        return None
    return Clasificacion(TEMA, SALARIO_INDICADOR, unir("Salario mínimo, vital y móvil", base),
                         unidad=PESOS_CORRIENTES)


def _haber_minimo(serie: Serie) -> Clasificacion | None:
    # Las dos columnas son el mismo haber en dos unidades (no son copias: ver ``UNIDADES``).
    if serie.variable not in ("mensual_pesos", "mensual_pesos_corrientes"):
        return None
    return Clasificacion(TEMA, SALARIO_INDICADOR, "Haber mínimo jubilatorio", unidad=PESOS_CORRIENTES)


def _auh(serie: Serie) -> Clasificacion | None:
    if serie.variable != "auh":
        return None
    return Clasificacion(TEMA, "Asignación Universal por Hijo", "Asignación Universal por Hijo",
                         unidad=PESOS_CORRIENTES)


# ===================================================================================================
# Canastas básicas (Precios)
# ===================================================================================================
CANASTAS_INDICADOR = "Canastas básicas"
# Mismos nombres que usa ``precios_otros`` para la hoja «CBA y CBT 2016».
_CANASTAS = {
    "canasta_basica_alimentaria": ("Canasta básica alimentaria · Adulto equivalente", PESOS_CORRIENTES),
    "canasta_basica_total": ("Canasta básica total · Adulto equivalente", PESOS_CORRIENTES),
    "linea_indigencia": ("Línea de indigencia · Hogar de referencia", PESOS_CORRIENTES),
    "linea_pobreza": ("Línea de pobreza · Hogar de referencia", PESOS_CORRIENTES),
    "inversa_coeficiente_engel": ("Inversa del coeficiente de Engel", "Cociente CBT/CBA"),
}
_REGIONES_CANASTA = {
    "cuyo": "Cuyo", "gran_buenos_aires": "GBA", "noreste": "Noreste", "noroeste": "Noroeste",
    "pampeana": "Pampeana", "patagonia": "Patagonia",
}
_CABA = "Ciudad Autónoma de Buenos Aires"
# La CBA y la CBT de la Ciudad de Buenos Aires (IDECBA, cuadro 444.1 hoja «CBA y CBT BA») son del «hogar tipo»
# (descripción del dataset en la API) y valen 4,2 veces las del INDEC por adulto equivalente de la región GBA
# (abr-2016: 6.319 contra 1.515; feb-2025: 639.601 contra 151.491): no son adulto equivalente.
_CANASTA_CABA = {
    "canasta_basica_alimentaria": ("Canasta básica alimentaria · Hogar tipo", PESOS_CORRIENTES),
    "canasta_basica_total": ("Canasta básica total · Hogar tipo", PESOS_CORRIENTES),
}


def _canastas(serie: Serie) -> Clasificacion | None:
    if serie.hoja == "CBA y CBT":
        datos = _CANASTAS.get(serie.variable)
        return Clasificacion(PRECIOS, CANASTAS_INDICADOR, datos[0], cobertura=HISTORICAS,
                             unidad=datos[1]) if datos else None
    if serie.hoja == "CBA y CBT BA":
        # Hogar tipo del IDECBA, no adulto equivalente (ver el comentario de ``_CANASTA_CABA``).
        datos, lugar = _CANASTA_CABA.get(serie.variable), _CABA
    else:
        lugar = _REGIONES_CANASTA.get(serie.variable)
        texto = plano(serie.descripcion)
        clave = ("canasta_basica_alimentaria" if texto.startswith("canasta basica alimentaria")
                 else "canasta_basica_total" if texto.startswith("canasta basica total") else None)
        datos = _CANASTAS.get(clave) if clave else None
    if datos is None or lugar is None:
        return None
    return Clasificacion(PRECIOS, CANASTAS_INDICADOR, unir(datos[0], lugar), cobertura=lugar, unidad=datos[1])


# ===================================================================================================
# Correcciones: series que no se publican y unidades
# ===================================================================================================
# Sufijos de los 39 ID de los cuadros 63.1 (hogares) y 64.1 (personas): el prefijo se completa abajo.
_SUFIJOS_PUNTUAL = (
    "AIP_0_0_25", "BBC_0_0_18", "CRRT_0_0_29",
    "C_0_0_10", "C_0_0_4", "C_0_0_9",
    "F_0_0_7", "GC_0_0_12", "GC_0_0_14",
    "GLP_0_0_13", "GM_0_0_12", "GP_0_0_11",
    "GR_0_0_12", "GR_0_0_16", "GSF_0_0_13",
    "GSJ_0_0_13", "GTTV_0_0_23", "G_0_0_3",
    "JP_0_0_13", "LR_0_0_8", "MPB_0_0_15",
    "NP_0_0_16", "PGBA_0_0_26", "P_0_0_7",
    "RC_0_0_10", "RG_0_0_12", "RNE_0_0_15",
    "RNO_0_0_15", "RP_0_0_15", "RP_0_0_16",
    "RT_0_0_13", "SELB_0_0_24", "SLEC_0_0_21",
    "SNVC_0_0_30", "SRT_0_0_15", "S_0_0_5",
    "T_0_0_5", "URG_0_0_18", "VCP_0_0_23",
)

_MOTIVO_PUNTUAL = (
    "Copia exacta de la serie de su par 63.2/64.2 (EPH continua, 1.er semestre 2003 a 2.º de 2006; 8 valores "
    "idénticos en las 39 series de cada hoja) rotulada «EPH puntual 2001 a 2003»: la API de series declara para "
    "este ID otro dato (EPH puntual, 5 ondas de may-2001 a may-2003, p. ej. pobreza de personas total 35,9 % a "
    "54,7 %) que BD.xlsx no contiene"
)

EXCLUIDAS = {
    **{(FUENTE, f"63.1_{sufijo}"): _MOTIVO_PUNTUAL for sufijo in _SUFIJOS_PUNTUAL},
    **{(FUENTE, f"64.1_{sufijo}"): _MOTIVO_PUNTUAL for sufijo in _SUFIJOS_PUNTUAL},
    # Hoja «CBA y CBT», cuadro 59.2 (4 series): BD.xlsx trae 8 valores fechados 1988-04 a 1991-07 que son los de
    # oct-2000 a jul-2002 (la API los da desde 2000-10, «desde 2000 hasta 2006»: 63,51 oct-2000 … 100,25 jul-2002;
    # coinciden con el promedio trimestral de la serie mensual 59.4 de la misma hoja, diferencia < 1e-12).
    # Las fechas no corresponden al dato y el dato correcto está en 59.4.
    **{(FUENTE, id): "Fechas erróneas (1988-1991): los valores son los de oct-2000 a jul-2002, iguales al "
                     "promedio trimestral de la serie mensual 59.4 de la misma hoja y a la API (desde 2000-10)"
       for id in ("59.2_CBA_0_D_26", "59.2_CBT_0_D_20", "59.2_LI_0_D_16", "59.2_LP_0_D_13")},
    # CBA de la región GBA (hoja «CBA y CBT Reg»): igual a la CBA de la hoja «CBA y CBT 2016» (módulo
    # precios_otros, que cubre hasta sep-2026) en los 123 meses en común (abr-2016 a jun-2026), diferencia 0.
    (FUENTE, "444.1_CANASTA_BARIAGBA_0_0_26_47"): (
        "Copia exacta de la CBA de la hoja «CBA y CBT 2016» (123 meses en común, diferencia 0); se conserva la más larga"
    ),
}

_MONEDA_DE_CADA_EPOCA = "Pesos corrientes (moneda de cada época)"
_MONEDA_ACTUAL = "Pesos corrientes (convertidos a la moneda actual)"

UNIDADES = {
    # SMVM: el mensual cae 1e4 en jun-1983 (700 contra 7.000.000), 1e3 en jul-1985 y 1e4 en ene-1992 (97 contra
    # 970.000): son los cambios de moneda (peso ley, peso argentino, austral, peso). La serie está en la moneda
    # de cada época, sin convertir; «Pesos corrientes» a secas la hace pasar por homogénea.
    (FUENTE, "57.1_SMVMM_0_M_34"): _MONEDA_DE_CADA_EPOCA,
    (FUENTE, "57.1_SMVMD_0_M_33"): _MONEDA_DE_CADA_EPOCA,
    (FUENTE, "57.1_SMVMH_0_M_31"): _MONEDA_DE_CADA_EPOCA,
    # HaberMin: no son copias. 58.1_MP_0_M_24 está en la moneda de cada época (170 en ene-1971) y 58.1_MP_0_M_13 es
    # la misma serie convertida a la moneda actual (1,7e-9 en ene-1971): el cociente es exactamente 1e11 hasta
    # may-1983, 1e7 hasta jun-1985, 1e4 hasta dic-1991 y 1 desde ene-1992 (los factores de las reformas monetarias);
    # desde 1992 coinciden (415 meses, una sola diferencia de 0,96 en dic-2024).
    (FUENTE, "58.1_MP_0_M_24"): _MONEDA_DE_CADA_EPOCA,
    (FUENTE, "58.1_MP_0_M_13"): _MONEDA_ACTUAL,
}


# ===================================================================================================
# Despacho por hoja
# ===================================================================================================
_REGLAS = {
    **{hoja: _tasa_desde_2003 for hoja in _HOJA_TASA_03},
    **{hoja: _tasa_puntual for hoja in _HOJA_TASA_74},
    "EPH": _tasa_eph,
    "EPH - Asal": _asalarizacion,
    "EPH - Asal regiones": _asalarizacion_regiones,
    "EPH-Poblaciones": _poblacion,
    "Expansion EPH a TU - 1991-2010": _poblacion,
    "Expansion EPH a TU - Desde 2010": _poblacion,
    "EIL - Aglo": _eil,
    "EIL - Sector": _eil,
    **{hoja: _pobreza for hoja in _POBREZA_HOJAS},
    "EPH Puntual - PeI": _pobreza,
    "DP": _distribucion,
    "DP Deciles": _distribucion,
    "SMVM": _salario_minimo,
    "HaberMin": _haber_minimo,
    "AUH": _auh,
    "CBA y CBT": _canastas,
    "CBA y CBT BA": _canastas,
    "CBA y CBT Reg": _canastas,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.archivo != ARCHIVO:
        return None
    regla = _REGLAS.get(serie.hoja)
    return regla(serie) if regla else None
