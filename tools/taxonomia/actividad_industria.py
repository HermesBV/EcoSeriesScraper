"""Reglas de taxonomía de la actividad agregada, la industria y la construcción.

Alcance (siempre por fuente, archivo y hoja; nunca por la clasificación actual):

* ``indec-emae``, ``indec-ipi-manufacturero`` e ``indec-isac``: todas sus hojas.
* ``datos.gob.ar``, archivo ``actividad.xlsx``: hojas 1.4, 1.4.1, 1.4.2 (EMAE), 1.13 (ventas),
  1.14, 1.14.d, 1.14b, 1.14c (EMI e IPI manufacturero), 1.15 y 1.15b (encuesta industrial),
  1.16, 1.16b y 1.16c (capacidad instalada), 1.17 (productos industriales), 1.18 y 1.18b (ISAC).

Tema «Actividad». Indicadores (nivel 2) y subárbol::

    EMAE                      Nivel general · Sectores · Series históricas > Base 1993
    IPI manufacturero         Nivel general · Divisiones · Subclases > <división> · Auxiliares
                              Series históricas > EMI base 2004 / EMI base 2012 > Nivel general · Sectores
    Utilización de la capacidad instalada
                              Nivel general · Sectores · Series históricas > Base 2006 / Base 2012 > …
    Encuesta industrial       Volumen físico · Obreros ocupados · Horas trabajadas · Salarios por obrero
    Producción industrial por producto
                              un grupo por rama (alimentos, petróleo, química…)
    Ventas al mercado interno un grupo por rubro (automotores, derivados del petróleo…)
    ISAC                      Nivel general · Insumos > <insumo> · Puestos de trabajo ·
                              Permisos de edificación · Series históricas > Serie hasta 2015 /
                              Permisos de 176 y de 60 municipios

Criterios que explican las decisiones (ver ``docs/taxonomia.md``):

* **Mismo indicador, mismo grupo y mismo nombre sin importar la fuente.** El libro del INDEC y
  datos.gob.ar publican el mismo EMAE, el mismo IPI manufacturero y el mismo ISAC (los valores
  coinciden mes a mes: se verificó el nivel general del ISAC, las divisiones del IPI y los
  sectores del EMAE). Cuando datos.gob.ar trae una copia congelada de la serie vigente (hoja 1.18b
  del ISAC, hasta feb-2023) va a los mismos grupos y con los mismos nombres que la vigente.
* **El EMI es el antecesor del IPI manufacturero**, no otro indicador: es el índice de volumen
  físico de la industria que el INDEC publicó hasta 2015 y relanzó en 2016 como IPI
  manufacturero. Mide lo mismo con otro operativo, así que sus hojas (1.14, 1.14b y 1.14c)
  cuelgan de «Series históricas» del IPI, con la versión «EMI base 2004» o «EMI base 2012» como
  nivel 4, y comparten los nombres («IPI manufacturero · Nivel general»).
  Sus sectores no son las divisiones de la CLaNAE (hay «Industria automotriz» o
  «Industria metalmecánica, excluida la automotriz»), por eso se abren como «Sectores» y no
  como «Divisiones».
* **Clasificadores oficiales.** Las divisiones y subclases del IPI usan la CLaNAE 2004 con los
  nombres que publica el INDEC, escritos siempre igual en las siete tablas del libro y en la
  hoja 1.14.d. Los sectores del EMAE son los de la CIIU rev. 3 que usa el INDEC (sin la letra).
  Las 16 divisiones del IPI y los 69 subgrupos que se abren por división dan grupos finales de
  hasta 52 series.
* **Subclase homónima.** La subclase «35110/120/200/300/990» se llama igual que su división
  («Otro equipo de transporte»); se escribe «Otro equipo de transporte, excepto motocicletas»
  (la otra subclase de la división es «Motocicletas»).
* **Medidas.** Sin sufijo es el nivel de la serie original; «(desestacionalizado)»,
  «(tendencia-ciclo)», «(var. % interanual)», «(var. % mensual)», «(var. % trimestral)»,
  «(var. % acumulada)». «Acumulada» es la variación del acumulado del año respecto del mismo
  acumulado del año anterior. Los rótulos del INDEC arrastran textos de celdas combinadas
  («acumulada del año…» en columnas que no lo son): la medida sale de la posición del texto
  clave, no del rótulo completo, y se verificó contra los valores.
* **Unidades.** datos.gob.ar publica como proporción (0,031 = 3,1 %) las variaciones del EMAE base
  2004 (hoja 1.4.1), del EMI base 2004 (1.14c), de la encuesta industrial revisada (1.15b) y la
  variación interanual del ISAC (1.18b); como porcentaje, las del EMAE base 1993 (1.4); la capacidad
  instalada (1.16*) es un porcentaje de 0 a 100. Se verificó por los valores: 1.4.1 y 1.18b ×100
  coinciden con el libro del INDEC (diferencia máxima 2e-15 y 0 puntos) y la mediana de las
  variaciones es 0,0077 (1.4.1), 0,065 (1.14c) y 0,048 (1.15b) contra 0,7 de la 1.4.
* **Hoja 1.17 (producción industrial por producto).** El libro del Ministerio no declara bien ni la
  frecuencia ni la unidad: los títulos de las distribuciones de la API están cruzados (la 41.1
  «Valores mensuales» está indexada por año y la 41.3 «Valores anuales», por mes) y la unidad que
  declara cada serie no siempre corresponde a sus valores. La anual es la suma de la mensual y la
  trimestral también, de modo que las tres frecuencias de un producto valen lo mismo. Se completa
  la unidad sólo donde otra hoja del mismo libro trae los mismos valores con su unidad
  (petróleo crudo y gas natural, idénticos a las hojas 1.24 y 1.25). Se excluyen el papel para uso
  sanitario (copia exacta de las ventas de gaseosas de la 1.13), el cemento portland y las naftas
  común, súper y ultra, cuyos valores no pueden ser lo que declaran y cuyo dato correcto está en
  las hojas 1.13 y 1.24 (ver ``EXCLUIDAS``). El resto queda sin unidad: ver el informe de auditoría.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, limpiar, plano, unir

TEMA = "Actividad"
DATOS = "datos.gob.ar"
ARCHIVO_DATOS = "actividad.xlsx"

# --- Vocabulario controlado de grupos -----------------------------------------------------
NIVEL_GENERAL = "Nivel general"
SECTORES = "Sectores"
DIVISIONES = "Divisiones"
SUBCLASES = "Subclases"
AUXILIARES = "Auxiliares"
SERIES_HISTORICAS = "Series históricas"
INSUMOS = "Insumos"
PUESTOS = "Puestos de trabajo"
PERMISOS = "Permisos de edificación"

# --- Medidas (van entre paréntesis en el nombre) ---------------------------------------------
DESESTACIONALIZADO = "desestacionalizado"
TENDENCIA_CICLO = "tendencia-ciclo"
INTERANUAL = "var. % interanual"
MENSUAL = "var. % mensual"
TRIMESTRAL = "var. % trimestral"
ACUMULADA = "var. % acumulada"
METODO_DIRECTO = "método directo"

# --- Unidades ------------------------------------------------------------------------------
PORCENTAJE = "%"
PROPORCION = "Proporción (1 = 100%)"


# --- Correcciones explícitas (ver tools/taxonomia/__init__.py: correcciones) --------------------------------
_AUX_IPI = (
    "Código (CLaNAE 2004) - Período: no es una serie. Son los números de fila de la hoja de Excel "
    "(6, 7, 8… un entero por mes, de ene-2016 a la fecha; 128 valores distintos y consecutivos), "
    "verificado en los valores de BD.xlsx y en el libro sh_ipi_manufacturero_2026.xls del INDEC."
)
_CUADROS_IPI_AUX = {
    "Cuadro 2": "cuadro-2-ipi-manufacturero-nivel-general-divisiones-y-subclases-serie-original-base-2004-100-en-numeros-indice-anos-2016-2026-codigo-clanae-2004-periodo",
    "Cuadro 3": "cuadro-3-ipi-manufacturero-nivel-general-divisiones-y-subclases-serie-original-base-2004-100-en-variacion-porcentual-interanual-anos-2016-2026",
    "Cuadro 4": "cuadro-4-ipi-manufacturero-nivel-general-divisiones-y-subclases-serie-original-base-2004-100-en-variacion-porcentual-interanual-acumulada-anos-2016-2026-codigo-clanae-2004-periodo",
    "Cuadro 5": "cuadro-5-ipi-manufacturero-nivel-general-y-divisiones-serie-desestacionalizada-base-2004-100-en-numeros-indice-y-variacion-porcentual-intermensual-anos-2016-2026-codigo-clanae-2004-periodo",
    "Cuadro 6": "cuadro-6-ipi-manufacturero-nivel-general-y-divisiones-serie-tendencia-ciclo-base-2004-100-en-numeros-indice-y-variacion-porcentual-intermensual-anos-2016-2026-codigo-clanae-2004-periodo",
}

# Hoja 1.17 «Indicadores de producción de la industria manufacturera» (datos.gob.ar): no se publica,
# salvo petróleo crudo y gas natural. Motivos, con los valores de la hoja:
#  * contenido erróneo comprobado en cuatro productos: «papel para uso sanitario» es copia exacta de
#    las ventas de gaseosas de la hoja 1.13 (317 meses); el cemento vale de 7 a 38 veces el de la 1.13
#    sin cociente constante; las naftas súper y ultra superan a todo el petróleo procesado;
#  * valores incompatibles con la unidad que declara la fuente en muchos otros (último año: urea
#    414.863 «miles de toneladas» contra 1.300 de producción del país; aluminio primario 4.024 contra
#    ~440; acero crudo 10.082 contra ~5.000; PVC 59.703 contra ~200; kerosene 6.921 miles de m3);
#  * la distribución «mensual» está indexada por año y la «anual» por mes, y la trimestral trae un solo
#    dato en 33 series.
# No hay otra fuente en el inventario para verificar los demás productos uno por uno. Petróleo crudo y
# gas natural se conservan porque son idénticos a las hojas 1.24 y 1.25 (368 meses) y coinciden con la
# unidad de la API. Para volver a publicar un producto hay que confirmarlo contra el libro original
# del Ministerio (actividad_ied.xlsx) o una estadística sectorial, y agregar su ID a la lista.
HOJAS_EXCLUIDAS: dict[tuple[str, str, str], tuple[str, frozenset[str]]] = {
    ("datos.gob.ar", "actividad.xlsx", "1.17 Productos ind."): (
        "Hoja con contenido erróneo comprobado en varios productos y sin fuente para verificar el resto",
        frozenset({"41.1_GN_0_M_11", "41.2_GN_0_T_11", "41.3_GN_0_A_11",
                   "41.1_PC_0_M_14", "41.2_PC_0_T_14", "41.3_PC_0_A_14"}),
    ),
}

EXCLUIDAS: dict[tuple[str, str], str] = {
    # Columnas que no son series: el rótulo «… Código (CLaNAE 2004) - Período» trae el número de fila.
    **{("indec-ipi-manufacturero", f"ipi_manufacturero.xls|{cuadro}|{slug}"): f"{cuadro}. {_AUX_IPI}"
       for cuadro, slug in _CUADROS_IPI_AUX.items()},
    # Hoja 1.17: «papel para uso sanitario y otros usos» es copia exacta de las ventas de gaseosas
    # (hoja 1.13, «38.x_G_1994_*_8», en miles de litros): 317 meses idénticos en la mensual y 11 años
    # en la anual, y termina en la misma fecha (may-2020). Además 2,96 millones de toneladas en 2019
    # superan toda la producción nacional de papel. La trimestral es la suma de la mensual.
    ("datos.gob.ar", "41.1_PPSOU_0_M_33"): "Papel para uso sanitario y otros usos: copia exacta de las ventas de gaseosas de la hoja 1.13 (317 meses idénticos; 2,96 millones de t en 2019, más que toda la producción de papel).",
    ("datos.gob.ar", "41.2_PPSOU_0_T_33"): "Papel para uso sanitario y otros usos: trimestral de la copia de las ventas de gaseosas de la hoja 1.13.",
    ("datos.gob.ar", "41.3_PPSOU_0_A_33"): "Papel para uso sanitario y otros usos: copia exacta de las ventas de gaseosas de la hoja 1.13 (317 meses idénticos; 173.342 en may-2020 en ambas).",
    # Hoja 1.17: cemento portland. Declara «miles de toneladas» pero vale 14.896 por mes y 194.914 en
    # 2025; el cemento de la hoja 1.13 (ventas de producción nacional, miles de toneladas) vale 816 en
    # jul-2026. El cociente no es constante (7 a 38 mensual, 14,6 a 29,7 anual): no es un cambio de
    # unidad y no se puede renombrar. El dato correcto está en la hoja 1.13.
    ("datos.gob.ar", "41.1_CP_0_M_16"): "Cemento portland de la hoja 1.17: 194.914 «miles de toneladas» en 2025 contra 10.014 en la hoja 1.13; cociente variable (14,6 a 29,7 por año), no es un cambio de unidad.",
    ("datos.gob.ar", "41.2_CP_0_T_16"): "Cemento portland de la hoja 1.17 (trimestral, suma de la mensual): no corresponde a miles de toneladas; ver la hoja 1.13.",
    ("datos.gob.ar", "41.3_CP_0_A_16"): "Cemento portland de la hoja 1.17: 14.896 «miles de toneladas» en jul-2026 contra 816 en la hoja 1.13; cociente variable (7 a 38), no es un cambio de unidad.",
    # Hoja 1.17: naftas. La súper vale 20.215 y la ultra 21.291 «miles de m³» por mes (jul-2026): más
    # que todo el petróleo procesado (3.696) y casi iguales entre sí; la hoja 1.24 (producción 571 y
    # 269) y la 1.13 (ventas 586 y 209, idénticas a las ventas de la 1.24) dan órdenes de magnitud
    # 35 y 100 veces menores. La común (1.17) supera al total de naftas de la misma hoja (32.223
    # contra 7.347 en 2012) y a las ventas de la 1.13 (131).
    ("datos.gob.ar", "41.3_NS_0_A_11"): "Nafta súper de la hoja 1.17: 20.215 miles de m³ en jul-2026 contra 571 de producción en la hoja 1.24 y más que el petróleo procesado (3.696).",
    ("datos.gob.ar", "41.2_NS_0_T_11"): "Nafta súper de la hoja 1.17 (trimestral, suma de la mensual): ver 41.3_NS_0_A_11.",
    ("datos.gob.ar", "41.3_NU_0_A_11"): "Nafta ultra de la hoja 1.17: 21.291 miles de m³ en jul-2026 contra 269 de producción en la hoja 1.24 y casi igual a la súper.",
    ("datos.gob.ar", "41.2_NU_0_T_11"): "Nafta ultra de la hoja 1.17 (trimestral, suma de la mensual): ver 41.3_NU_0_A_11.",
    ("datos.gob.ar", "41.1_NC_0_M_11"): "Nafta común de la hoja 1.17: 32.223 miles de m³ en 2012, más que el total de naftas de la misma hoja (7.347) y que las ventas de la hoja 1.13 (131).",
    ("datos.gob.ar", "41.2_NC_0_T_11"): "Nafta común de la hoja 1.17 (trimestral, suma de la mensual): ver 41.1_NC_0_M_11.",
    ("datos.gob.ar", "41.3_NC_0_A_11"): "Nafta común de la hoja 1.17 (mensual): mismo problema que 41.1_NC_0_M_11.",
}

# Unidades de la hoja 1.17 que se pudieron establecer: los valores son idénticos a los de otra hoja
# del mismo libro que declara la unidad (petróleo crudo: 1.24, «Producción de petróleo crudo», miles
# de m³, 368 meses iguales; gas natural: 1.25, «Producción de gas natural», millones de m³, 99 % iguales)
# y coinciden con la declarada por la API de series de tiempo.
_UNIDAD_PRODUCTOS = {
    "petroleo_crudo": "Miles de metros cúbicos",
    "gas_natural": "Millones de metros cúbicos",
}


def indice(base: int | str) -> str:
    return f"Índice {base}=100"


def _hoja(serie: Serie) -> str:
    """Hoja sin mayúsculas, tildes ni puntuación final; «Cuadro 6,2» se lee como «cuadro 6.2»."""
    return plano(serie.hoja).replace(",", ".").rstrip(". ")


def _crear(indicador: str, partes: tuple[str, ...], medidas: tuple[str, ...], *, nivel3: str = "",
           nivel4: str = "", nivel5: str = "", unidad: str = "") -> Clasificacion:
    nombre = con_medida(unir(indicador, *partes), ", ".join(medidas))
    return Clasificacion(tema=TEMA, indicador=indicador, nombre=nombre, cobertura=nivel3,
                         apertura=nivel4, medida=nivel5, unidad=unidad)


# ===================================================================================================
# EMAE
# ===================================================================================================
EMAE = "EMAE"

# Sectores del EMAE (CIIU rev. 3), con la grafía del INDEC y sin la letra.
_SECTORES_EMAE = (
    "Agricultura, ganadería, caza y silvicultura", "Pesca", "Explotación de minas y canteras",
    "Industria manufacturera", "Electricidad, gas y agua", "Construcción",
    "Comercio mayorista, minorista y reparaciones", "Hoteles y restaurantes",
    "Transporte y comunicaciones", "Intermediación financiera",
    "Actividades inmobiliarias, empresariales y de alquiler",
    "Administración pública y defensa; planes de seguridad social de afiliación obligatoria",
    "Enseñanza", "Servicios sociales y de salud",
    "Otras actividades de servicios comunitarios, sociales y personales",
    "Impuestos netos de subsidios",
)
_SECTOR_EMAE_POR_CLAVE = {plano(s): s for s in _SECTORES_EMAE}

# datos.gob.ar (hoja 1.4.2) rotula los sectores con nombres abreviados; la correspondencia se
# verificó contra la hoja «Tabla Letras» del INDEC (mismos valores).
_SECTOR_EMAE_DATOS = {
    "agricultura_ganaderia_caza_silvicultura": "Agricultura, ganadería, caza y silvicultura",
    "pesca": "Pesca",
    "explotacion_minas_canteras": "Explotación de minas y canteras",
    "industria_manufacturera": "Industria manufacturera",
    "electricidad_gas_agua": "Electricidad, gas y agua",
    "construccion": "Construcción",
    "comercio_mayorista_minorista_reparaciones": "Comercio mayorista, minorista y reparaciones",
    "hoteles_restaurantes": "Hoteles y restaurantes",
    "transporte_comunicaciones": "Transporte y comunicaciones",
    "intermediacion_financiera": "Intermediación financiera",
    "actividades_inmobiliarias_empresariales_alquiler":
        "Actividades inmobiliarias, empresariales y de alquiler",
    "admin_publica_planes_seguridad_social_afiliacion_obligatoria":
        "Administración pública y defensa; planes de seguridad social de afiliación obligatoria",
    "ensenianza": "Enseñanza",
    "servicios_sociales_salud": "Servicios sociales y de salud",
    "otras_actividades_servicios_comunitarias_sociales_personales":
        "Otras actividades de servicios comunitarios, sociales y personales",
    "impuestos_netos_subsidios": "Impuestos netos de subsidios",
}


def _emae_general(medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _crear(EMAE, (NIVEL_GENERAL,), medidas, nivel3=NIVEL_GENERAL, unidad=unidad)


def _emae_sector(sector: str, medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _crear(EMAE, (sector,), medidas, nivel3=SECTORES, unidad=unidad)


def _emae_historica(medidas: tuple[str, ...], unidad: str, base: str) -> Clasificacion:
    return _crear(EMAE, (NIVEL_GENERAL,), medidas, nivel3=SERIES_HISTORICAS, nivel4=f"Base {base}",
                  unidad=unidad)


def _sin_letra(rotulo: str) -> str:
    """«D - Industria manufacturera» → «Industria manufacturera»."""
    return re.sub(r"^[A-Z] - ", "", limpiar(rotulo))


def _emae_sector_indec(serie: Serie, medidas: tuple[str, ...], unidad: str) -> Clasificacion | None:
    sector = _SECTOR_EMAE_POR_CLAVE.get(plano(_sin_letra(serie.columna)))
    return _emae_sector(sector, medidas, unidad) if sector else None


def _emae_tabla_letras(serie: Serie) -> Clasificacion | None:
    return _emae_sector_indec(serie, (), indice(2004))


def _emae_tabla_var_letras(serie: Serie) -> Clasificacion | None:
    return _emae_sector_indec(serie, (INTERANUAL,), PORCENTAJE)


def _emae_tabla(serie: Serie) -> Clasificacion | None:
    """Nivel general del libro mensual del INDEC (base 2004).

    La variación «respecto al mes anterior» aparece dos veces: la primera es de la serie
    desestacionalizada (coincide con el cociente de los índices) y la segunda, con nota al pie
    «(2)» y sufijo «-2» en el ID, de la tendencia-ciclo.
    """
    columna = plano(serie.columna)
    if columna.startswith("indice serie original"):
        return _emae_general((), indice(2004))
    if columna.startswith("indice serie desestacionalizada"):
        return _emae_general((DESESTACIONALIZADO,), indice(2004))
    if columna.startswith("indice serie tendencia"):
        return _emae_general((TENDENCIA_CICLO,), indice(2004))
    if columna.startswith("var % respecto a igual periodo del ano anterior"):
        return _emae_general((INTERANUAL,), PORCENTAJE)
    if columna.startswith("var % respecto al mes anterior"):
        tendencia = serie.id.endswith("-2") or bool(re.search(r"\(\d+\)$", serie.columna))
        return _emae_general((TENDENCIA_CICLO if tendencia else DESESTACIONALIZADO, MENSUAL), PORCENTAJE)
    return None


# datos.gob.ar: hoja 1.4 (base 1993, discontinuada) y 1.4.1 (base 2004, vigente).
_EMAE_1993 = {
    "indice_serie_original_anual": ((), indice(1993)),
    "indice_serie_original_trimestral": ((), indice(1993)),
    "indice_serie_original_mensual": ((), indice(1993)),
    "indice_serie_desestacionalizada": ((DESESTACIONALIZADO,), indice(1993)),
    "indice_tendencia_suavizada": ((TENDENCIA_CICLO,), indice(1993)),
    "var_igual_periodo_anio_anterior": ((INTERANUAL,), PORCENTAJE),
    "var_mes_anterior_serie_original": ((MENSUAL,), PORCENTAJE),
    "var_mes_anterior_tendencia_suavizada": ((TENDENCIA_CICLO, MENSUAL), PORCENTAJE),
}
_EMAE_2004 = {
    "emae_original": ((), indice(2004)),
    "emae_desestacionalizada": ((DESESTACIONALIZADO,), indice(2004)),
    "emae_tendencia_ciclo": ((TENDENCIA_CICLO,), indice(2004)),
    "emae_original_via": ((INTERANUAL,), PROPORCION),
    "emae_desestacionalizada_vm": ((DESESTACIONALIZADO, MENSUAL), PROPORCION),
    "emae_tendencia_ciclo_vm": ((TENDENCIA_CICLO, MENSUAL), PROPORCION),
}


def _emae_1993(serie: Serie) -> Clasificacion | None:
    encontrado = _EMAE_1993.get(serie.variable)
    return _emae_historica(encontrado[0], encontrado[1], "1993") if encontrado else None


def _emae_2004(serie: Serie) -> Clasificacion | None:
    encontrado = _EMAE_2004.get(serie.variable)
    return _emae_general(*encontrado) if encontrado else None


def _emae_sectores_datos(serie: Serie) -> Clasificacion | None:
    sector = _SECTOR_EMAE_DATOS.get(serie.variable)
    return _emae_sector(sector, (), indice(2004)) if sector else None


# ===================================================================================================
# IPI manufacturero (y su antecesor, el EMI)
# ===================================================================================================
IPI = "IPI manufacturero"
BASE_IPI = indice(2004)

# Divisiones de la CLaNAE 2004 tal como las publica el INDEC en el IPI manufacturero.
_DIVISIONES = {
    "15": "Alimentos y bebidas",
    "16": "Productos de tabaco",
    "17": "Productos textiles",
    "18-19": "Prendas de vestir, cuero y calzado",
    "20-22": "Madera, papel, edición e impresión",
    "23": "Refinación del petróleo, coque y combustible nuclear",
    "24": "Sustancias y productos químicos",
    "25": "Productos de caucho y plástico",
    "26": "Productos minerales no metálicos",
    "27": "Industrias metálicas básicas",
    "28": "Productos de metal",
    "29": "Maquinaria y equipo",
    "30-33": "Otros equipos, aparatos e instrumentos",
    "34": "Vehículos automotores, carrocerías, remolques y autopartes",
    "35": "Otro equipo de transporte",
    "36-38": "Muebles y colchones, y otras industrias manufactureras",
}
# Primeros dos dígitos del código de una subclase → código de su división.
_DIVISION_DE_PREFIJO = {
    **{p: p for p in ("15", "16", "17", "23", "24", "25", "26", "27", "28", "29", "34", "35")},
    "18": "18-19", "19": "18-19", "20": "20-22", "21": "20-22", "22": "20-22",
    "30": "30-33", "31": "30-33", "32": "30-33", "33": "30-33",
    "36": "36-38", "37": "36-38", "38": "36-38",
}
_OTRO_EQUIPO_DE_TRANSPORTE = "Otro equipo de transporte"
_OTRO_EQUIPO_SIN_MOTOS = "Otro equipo de transporte, excepto motocicletas"
# Dos subclases tienen un nombre oficial que, con la medida, pasa los 120 caracteres que admite un
# nombre de serie: se abrevian sin cambiar su sentido.
_GASEOSAS = "Gaseosas, aguas, sodas, cervezas, jugos, sidras y bebidas espirituosas"
_INFORMATICA = "Equipos de informática, televisión y comunicaciones, y componentes electrónicos"
_ABREVIADOS = {
    plano("Gaseosas, aguas, sodas, cervezas, jugos para diluir, sidras y bebidas espirituosas"): _GASEOSAS,
    plano("Equipos y aparatos de informática, televisión y comunicaciones, y componentes electrónicos"):
        _INFORMATICA,
}

# Subclases (nombre → división) de datos.gob.ar (hoja 1.14.d), por el identificador de la variable.
# Los nombres son los del libro del INDEC; la correspondencia se verificó con los valores del Cuadro 2.
_SUBCLASES_DATOS = {
    "carne_vacuna": ("15", "Carne vacuna"), "carne_aviar": ("15", "Carne aviar"),
    "fiambres_embutidos": ("15", "Fiambres y embutidos"),
    "otros_productos_alimenticios": ("15", "Otros productos alimenticios"),
    "preparacion_frutas_hortalizas_legumbres": ("15", "Preparación de frutas, hortalizas y legumbres"),
    "molienda_oleaginosas": ("15", "Molienda de oleaginosas"),
    "productos_lacteos": ("15", "Productos lácteos"),
    "molienda_cereales": ("15", "Molienda de cereales"),
    "galletitas_panaderia_pastas": ("15", "Galletitas, productos de panadería y pastas"),
    "azucar_confiteria_chocolate": ("15", "Azúcar, productos de confitería y chocolate"),
    "yerba_te_cafe": ("15", "Yerba mate, té y café"),
    "gaseosas_aguas_sodas_cervezas_jugos_sidras_espirituosas":
        ("15", _GASEOSAS),
    "vino": ("15", "Vino"),
    "preparacion_hojas_tabaco": ("16", "Preparación de hojas de tabaco"),
    "cigarrillos": ("16", "Cigarrillos"),
    "preparacion_fibras_uso_textil": ("17", "Preparación de fibras de uso textil"),
    "hilados_algodon": ("17", "Hilados de algodón"),
    "tejidos_acabado_productos_textiles": ("17", "Tejidos y acabado de productos textiles"),
    "otros_productos_textiles": ("17", "Otros productos textiles"),
    "prendas_vestir": ("18-19", "Prendas de vestir"),
    "curtido_articulos_cuero": ("18-19", "Curtido y artículos de cuero, excepto prendas"),
    "calzado": ("18-19", "Calzado y sus partes"),
    "productos_madera_corcho": ("20-22", "Madera y productos de madera y corcho, excepto muebles"),
    "productos_papel": ("20-22", "Papel y productos de papel"),
    "edicion_impresion": ("20-22", "Edición e impresión"),
    "otros_productos_refinacion":
        ("23", "Otros productos de la refinación del petróleo, coque y combustible nuclear"),
    "asfaltos": ("23", "Asfaltos"), "fueloil": ("23", "Fueloil"), "gasoil": ("23", "Gasoil"),
    "naftas": ("23", "Naftas"),
    "productos_quimicos_basicos": ("24", "Productos químicos básicos"),
    "gases_industriales": ("24", "Gases industriales"), "agroquimicos": ("24", "Agroquímicos"),
    "materias_primas_plasticas_y_caucho_sintetico": ("24", "Materias primas plásticas y caucho sintético"),
    "pinturas": ("24", "Pinturas"), "productos_farmaceuticos": ("24", "Productos farmacéuticos"),
    "detergentes_jabones_productos_personales": ("24", "Detergentes, jabones y productos personales"),
    "otros_productos_quimicos": ("24", "Otros productos químicos"),
    "neumaticos": ("25", "Neumáticos"), "otros_productos_caucho": ("25", "Otros productos de caucho"),
    "manufacturas_plastico": ("25", "Manufacturas de plástico"),
    "productos_vidrio": ("26", "Vidrio y productos de vidrio"),
    "productos_arcilla_ceramica": ("26", "Productos de arcilla y cerámica no refractaria"),
    "otros_productos_minerales_no_metalicos": ("26", "Otros productos minerales no metálicos"),
    "cemento": ("26", "Cemento"), "cal_yeso": ("26", "Cal y yeso"),
    "articulos_cemento_yeso": ("26", "Artículos de cemento y de yeso"),
    "industria_siderurgica": ("27", "Industria siderúrgica"),
    "aluminio_otros_metales_no_ferrosos": ("27", "Aluminio y otros metales no ferrosos"),
    "fundicion_metales": ("27", "Fundición de metales"),
    "productos_metalicos_uso_estructural": ("28", "Productos metálicos para uso estructural"),
    "otros_productos_metal_y_servicios": ("28", "Otros productos de metal y servicios de trabajo de metales"),
    "envases_metalicos": ("28", "Envases metálicos"),
    "maquinaria_uso_general": ("29", "Maquinaria de uso general"),
    "otra_maquinaria_uso_especial": ("29", "Otra maquinaria de uso especial"),
    "maquinaria_agropecuaria": ("29", "Maquinaria agropecuaria"),
    "aparatos_uso_domestico": ("29", "Aparatos de uso doméstico"),
    "equipos_informatica":
        ("30-33", _INFORMATICA),
    "equipos_electricos": ("30-33", "Equipos y aparatos eléctricos"),
    "instrumentos_medicos_opticos_precision": ("30-33", "Instrumentos médicos, ópticos y de precisión"),
    "vehiculos_automotores": ("34", "Vehículos automotores"),
    "carrocerias_remolques_semirremolques": ("34", "Carrocerías, remolques y semirremolques"),
    "autopartes": ("34", "Autopartes"),
    "otro_equipo_transporte": ("35", _OTRO_EQUIPO_SIN_MOTOS),
    "motocicletas": ("35", "Motocicletas"),
    "muebles_colchones": ("36-38", "Muebles y colchones"),
    "otras_industrias_manufactureras": ("36-38", "Otras industrias manufactureras"),
    "recliclamiento_reparacion_instalacion_mantenimiento":
        ("36-38", "Reciclamiento y reparación, instalación y mantenimiento"),
}
# Divisiones de datos.gob.ar por identificador de variable (hoja 1.14.d).
_DIVISIONES_DATOS = {
    "alimentos_bebidas": "15", "productos_tabaco": "16", "productos_textiles": "17",
    "prendas_vestir_cuero_calzado": "18-19", "madera_papel_edicion_impresion": "20-22",
    "refinacion_petroleo_coque_combustible_nuclear": "23", "sustancias_productos_quimicos": "24",
    "productos_caucho_plastico": "25", "productos_minerales_no_metalicos": "26",
    "industrias_metalicas_basicas": "27", "productos_metal": "28", "maquinaria_equipo": "29",
    "otros_equipos_aparatos_instrumentos": "30-33",
    "vehiculos_automotores_carrocerias_remolques_autopartes": "34",
    "otro_equipo_de_transporte": "35", "muebles_colchones_otras_industrias_manufactureras": "36-38",
}


def _ipi(partes: tuple[str, ...], medidas: tuple[str, ...], nivel3: str, nivel4: str = "",
         unidad: str = BASE_IPI, nivel5: str = "") -> Clasificacion:
    return _crear(IPI, partes, medidas, nivel3=nivel3, nivel4=nivel4, nivel5=nivel5, unidad=unidad)


def _ipi_division(codigo: str, medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _ipi((_DIVISIONES[codigo],), medidas, DIVISIONES, unidad=unidad)


def _ipi_subclase(codigo_division: str, nombre: str, medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _ipi((nombre,), medidas, SUBCLASES, _DIVISIONES[codigo_division], unidad=unidad)


def _ipi_general(medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _ipi((NIVEL_GENERAL,), medidas, NIVEL_GENERAL, unidad=unidad)


def _ipi_auxiliar(unidad: str = "") -> Clasificacion:
    return _ipi(("Número de período",), (), AUXILIARES, unidad=unidad)


# Rótulos de los cuadros del INDEC con el nivel general en filas (cuadros 1 de IPI e ISAC).
def _medidas_cuadro_1(columna: str) -> tuple[tuple[str, ...], str] | None:
    """(medidas, unidad) de una columna del «Cuadro 1»: serie original, desestacionalizada o tendencia-ciclo."""
    texto = plano(columna)
    if texto.startswith("serie original"):
        base: tuple[str, ...] = ()
    elif texto.startswith("serie desestacionalizada"):
        base = (DESESTACIONALIZADO,)
    elif texto.startswith("serie tendencia"):
        base = (TENDENCIA_CICLO,)
    else:
        return None
    if "variacion porcentual" not in texto:
        return base, ""
    if "respecto al mes anterior" in texto:
        return (*base, MENSUAL), PORCENTAJE
    if "respecto al mismo mes" in texto:
        return (*base, INTERANUAL), PORCENTAJE
    if "acumulada del ano respecto a igual acumulado" in texto and not base:
        return (ACUMULADA,), PORCENTAJE
    return None


def _ipi_cuadro_1(serie: Serie) -> Clasificacion | None:
    encontrado = _medidas_cuadro_1(serie.columna)
    if encontrado is None:
        return None
    medidas, unidad = encontrado
    return _ipi_general(medidas, unidad or BASE_IPI)


def _ipi_cuadro_7(serie: Serie) -> Clasificacion | None:
    """Nivel general desestacionalizado y de tendencia-ciclo por el método directo."""
    texto = plano(serie.columna)
    if texto.startswith("serie desestacionalizada"):
        base: tuple[str, ...] = (DESESTACIONALIZADO, METODO_DIRECTO)
    elif texto.startswith("serie tendencia"):
        base = (TENDENCIA_CICLO, METODO_DIRECTO)
    else:
        return None
    if "variacion porcentual" in texto:
        return _ipi_general((*base, MENSUAL), PORCENTAJE)
    return _ipi_general(base, BASE_IPI)


def _partir_rotulo(rotulo: str) -> tuple[str, str] | None:
    """«15111 - Carne vacuna - %» → («15111», «Carne vacuna»). None si no es de la forma «código - nombre»."""
    texto = limpiar(rotulo)
    if " - " not in texto:
        return None
    codigo, nombre = texto.split(" - ", 1)
    nombre = re.sub(r"\s+-\s+%$", "", nombre)
    return codigo.strip(), nombre.strip()


def _ipi_por_codigo(codigo: str, nombre: str, medidas: tuple[str, ...], unidad: str) -> Clasificacion | None:
    """Nivel general, división o subclase del IPI según el código CLaNAE del rótulo."""
    if codigo == "D":
        return _ipi_general(medidas, unidad)
    if codigo in _DIVISIONES:
        return _ipi_division(codigo, medidas, unidad)
    division = _DIVISION_DE_PREFIJO.get(re.sub(r"\D", "", codigo)[:2])
    if division is None:
        return None
    if plano(nombre) == plano(_OTRO_EQUIPO_DE_TRANSPORTE):
        nombre = _OTRO_EQUIPO_SIN_MOTOS
    nombre = _ABREVIADOS.get(plano(nombre), nombre)
    return _ipi_subclase(division, nombre, medidas, unidad)


def _ipi_cuadro_numerico(medidas: tuple[str, ...], unidad: str):
    """Cuadros 2 a 4: nivel general, divisiones y subclases con una sola medida."""
    def regla(serie: Serie) -> Clasificacion | None:
        if plano(serie.columna).startswith("cuadro"):
            return _ipi_auxiliar()   # columna auxiliar con el número de fila de la hoja
        partes = _partir_rotulo(serie.columna)
        return _ipi_por_codigo(*partes, medidas, unidad) if partes else None
    return regla


def _ipi_cuadro_desestacionalizado_o_tendencia(base: tuple[str, ...]):
    """Cuadros 5 y 6: nivel general y divisiones, índice y variación mensual de cada serie."""
    def regla(serie: Serie) -> Clasificacion | None:
        if plano(serie.columna).startswith("cuadro"):
            return _ipi_auxiliar()
        partes = _partir_rotulo(serie.columna)
        if partes is None:
            return None
        codigo, nombre = partes
        if plano(nombre).startswith("variacion porcentual"):
            return _ipi_por_codigo(codigo, "", (*base, MENSUAL), PORCENTAJE)
        # El rótulo del índice termina en «- %» por un arrastre de la fuente; es un índice.
        return _ipi_por_codigo(codigo, nombre, base, BASE_IPI)
    return regla


def _ipi_sectores_datos(serie: Serie) -> Clasificacion | None:
    division = _DIVISIONES_DATOS.get(serie.variable)
    if division:
        return _ipi_division(division, (), BASE_IPI)
    subclase = _SUBCLASES_DATOS.get(serie.variable)
    if subclase:
        return _ipi_subclase(subclase[0], subclase[1], (), BASE_IPI)
    return None


def _ipi_general_datos(serie: Serie) -> Clasificacion | None:
    medida = {"serie_original": (), "serie_desestacionalizada": (DESESTACIONALIZADO,),
              "serie_tendencia_ciclo": (TENDENCIA_CICLO,)}.get(serie.variable)
    return _ipi_general(medida, BASE_IPI) if medida is not None else None


# --- EMI (hojas 1.14, 1.14b y 1.14c) --------------------------------------------------------
# Sectores del EMI. «minerales_metalicos» es «Productos minerales no metálicos»: la descripción
# de datos.gob.ar dice «Minerales metálicos» pero el EMI no tiene un sector metálico y la serie
# vecina de la capacidad instalada se llama «minerales_no_metalicos».
_SECTORES_EMI = {
    "alimentos_bebidas": "Alimentos y bebidas",
    "automotores": "Industria automotriz",
    "caucho_plastico": "Productos de caucho y plástico",
    "edicion_impresion": "Edición e impresión",
    "industria_metalicas_basicas": "Industrias metálicas básicas",
    "metalmecanica_exc_automotor": "Industria metalmecánica, excluida la automotriz",
    "minerales_metalicos": "Productos minerales no metálicos",
    "papel_carton": "Papel y cartón",
    "productos_tabaco": "Productos de tabaco",
    "productos_textiles": "Productos textiles",
    "refinacion_petroleo": "Refinación del petróleo",
    "sustancias_productos_quimicos": "Sustancias y productos químicos",
}


def _emi(base: str, proporcion: bool = False):
    version = f"EMI base {base}"

    def regla(serie: Serie) -> Clasificacion | None:
        variable = serie.variable
        if variable in ("emi", "emi_desestac"):
            medidas = (DESESTACIONALIZADO,) if variable == "emi_desestac" else ()
            partes, nivel5 = (NIVEL_GENERAL,), NIVEL_GENERAL
        elif variable in _SECTORES_EMI:
            medidas, partes, nivel5 = (), (_SECTORES_EMI[variable],), SECTORES
        else:
            return None
        if proporcion:
            medidas, unidad = (INTERANUAL,), PROPORCION
        else:
            unidad = indice(base)
        return _crear(IPI, partes, medidas, nivel3=SERIES_HISTORICAS, nivel4=version, nivel5=nivel5,
                      unidad=unidad)
    return regla


# ===================================================================================================
# Utilización de la capacidad instalada en la industria
# ===================================================================================================
UCII = "Utilización de la capacidad instalada"
_SECTORES_UCII = {
    "ucii_nivel_general": None,
    "ucii_productos_alimenticios_bebidas": "Productos alimenticios y bebidas",
    "ucii_productos_tabaco": "Productos del tabaco",
    "ucii_productos_textiles": "Productos textiles",
    "ucii_papel_carton": "Papel y cartón",
    "ucii_edicion_impresion": "Edición e impresión",
    "ucii_refinacion_petroleo": "Refinación del petróleo",
    "ucii_sustancias_productos_quimicos": "Sustancias y productos químicos",
    "ucii_caucho_plastico": "Productos de caucho y plástico",
    "ucii_minerales_no_metalicos": "Productos minerales no metálicos",
    "ucii_industrias_metalicas_basicas": "Industrias metálicas básicas",
    "ucii_vehiculosautomotores": "Vehículos automotores",
    "ucii_metalmecanica_no_industria_automotriz": "Industria metalmecánica, excluida la automotriz",
}


def _ucii(version: str = ""):
    def regla(serie: Serie) -> Clasificacion | None:
        if serie.variable not in _SECTORES_UCII:
            return None
        sector = _SECTORES_UCII[serie.variable]
        grupo, partes = (NIVEL_GENERAL, (NIVEL_GENERAL,)) if sector is None else (SECTORES, (sector,))
        if version:
            return _crear(UCII, partes, (), nivel3=SERIES_HISTORICAS, nivel4=f"Base {version}",
                          nivel5=grupo, unidad=PORCENTAJE)
        return _crear(UCII, partes, (), nivel3=grupo, unidad=PORCENTAJE)
    return regla


# ===================================================================================================
# Encuesta industrial (hojas 1.15 y 1.15b)
# ===================================================================================================
ENCUESTA = "Encuesta industrial"
BASE_ENCUESTA = indice(1997)   # la base sale del ID de datos.gob.ar («…_1997_…»)

# Rubros (divisiones de la CIIU rev. 3) sin «Fabricación de» y abreviados para que el nombre con la
# medida no pase los 120 caracteres.
# La fuente los escribe con faltas y de cinco maneras distintas; se reconocen por palabras clave
# (el orden importa: las más específicas primero).
_RUBROS_ENCUESTA = (
    (("indice general",), NIVEL_GENERAL),
    (("alimenticios",), "Productos alimenticios y bebidas"),
    (("tabaco",), "Productos de tabaco"),
    (("textiles",), "Productos textiles"),
    (("prendas",), "Prendas de vestir; terminación y teñido de pieles"),
    (("curtido",), "Cueros, marroquinería, talabartería y calzado"),
    (("madera",), "Madera y corcho, excepto muebles; artículos de paja"),
    (("papel",), "Papel y productos de papel"),
    (("edicion",), "Edición e impresión; reproducción de grabaciones"),
    (("coque",), "Coque, refinación del petróleo y combustible nuclear"),
    (("quimicos",), "Sustancias y productos químicos"),
    (("caucho",), "Productos de caucho y plástico"),
    (("minerales no metalicos",), "Productos minerales no metálicos"),
    (("metales comunes",), "Metales comunes"),
    (("elaborados de metal",), "Productos de metal, excepto maquinaria y equipo"),
    (("oficina",), "Maquinaria de oficina, contabilidad e informática"),
    (("aparatos electronicos",), "Maquinaria y aparatos eléctricos"),
    (("maquinaria y equipos",), "Maquinaria y equipo"),
    (("radio",), "Equipos de radio, televisión y comunicaciones"),
    (("instrumentos",), "Instrumentos médicos, ópticos y de precisión; relojes"),
    (("vehiculos",), "Vehículos automotores, remolques y semirremolques"),
    (("equipos de transporte",), "Equipos de transporte"),
    (("muebles",), "Muebles y colchones; otras industrias manufactureras"),
)
_MEDIDAS_ENCUESTA = (
    ("volumen", "Volumen físico"),
    ("obreros", "Obreros ocupados"),
    ("horas", "Horas trabajadas"),
    ("salario", "Salarios por obrero"),
)
_RUBRO_DESCRIPCION = re.compile(r"\((?:IVF|IOO|IHT|ISO)\)(?:\s*\(revisado\))?\.\s*(.+)$", re.IGNORECASE)
_COLA_DESCRIPCION = re.compile(r"\.\s*(?:Anuales|Trimestrales|Variaciones [^.]*)\.?\s*$", re.IGNORECASE)
_VARIACION_ENCUESTA = re.compile(r"Variaciones (interanuales|trimestrales)", re.IGNORECASE)


def _encuesta(serie: Serie) -> Clasificacion | None:
    # La medida sale del título del dataset: la descripción de las series revisadas de obreros
    # ocupados dice «Salarios por obrero» por un error de copia.
    medida = next((nombre for clave, nombre in _MEDIDAS_ENCUESTA if clave in plano(serie.dataset)), None)
    encontrado = _RUBRO_DESCRIPCION.search(serie.descripcion)
    if medida is None or encontrado is None:
        return None
    texto = plano(_COLA_DESCRIPCION.sub("", encontrado.group(1)))
    rubro = next((nombre for claves, nombre in _RUBROS_ENCUESTA if all(c in texto for c in claves)), None)
    if rubro is None:
        return None
    if plano(serie.hoja).startswith("1.15b"):
        # Hoja de series revisadas: sólo variaciones, interanuales o respecto del trimestre anterior.
        variaciones = _VARIACION_ENCUESTA.findall(serie.descripcion)
        if not variaciones:
            return None
        medidas, unidad = ((INTERANUAL,) if variaciones[-1].lower() == "interanuales" else (TRIMESTRAL,)), PROPORCION
    else:
        medidas, unidad = (), BASE_ENCUESTA
    return _crear(ENCUESTA, (medida, rubro), medidas, nivel3=medida, unidad=unidad)


# ===================================================================================================
# Ventas al mercado interno de producción nacional (hoja 1.13)
# ===================================================================================================
VENTAS = "Ventas al mercado interno"
_AUTOMOTORES = "Automotores"
_DERIVADOS = "Derivados del petróleo"
_ALIMENTOS = "Alimentos, bebidas y tabaco"
_OTROS = "Otros productos"
# variable → (grupo, nombre, unidad). El gasoil dice «metros cúbicos» en la descripción pero los
# valores (≈1.180 en 2026, ≈1,2 millones de m³ por mes) son miles de metros cúbicos.
_VENTAS = {
    "automot_nac_automoviles": (_AUTOMOTORES, "Automóviles de producción nacional", "Unidades"),
    "automot_nac_carga_pasaj": (_AUTOMOTORES, "Automotores de carga y pasajeros de producción nacional", "Unidades"),
    "automot_nac_utilitarios": (_AUTOMOTORES, "Utilitarios de producción nacional", "Unidades"),
    "automot_nac_total": (_AUTOMOTORES, "Automotores de producción nacional", "Unidades"),
    "automot_nac_imp_automoviles": (_AUTOMOTORES, "Automóviles nacionales e importados", "Unidades"),
    "automot_nac_imp_carga_pasaj": (_AUTOMOTORES, "Automotores de carga y pasajeros nacionales e importados", "Unidades"),
    "automot_nac_imp_utilitarios": (_AUTOMOTORES, "Utilitarios nacionales e importados", "Unidades"),
    "automot_nac_imp_total": (_AUTOMOTORES, "Automotores nacionales e importados", "Unidades"),
    "gasoil": (_DERIVADOS, "Gasoil", "Miles de metros cúbicos"),
    "naftas": (_DERIVADOS, "Naftas", "Miles de metros cúbicos"),
    "nafta_comun": (_DERIVADOS, "Nafta común", "Miles de metros cúbicos"),
    "nafta_super": (_DERIVADOS, "Nafta súper", "Miles de metros cúbicos"),
    "nafta_ultra": (_DERIVADOS, "Nafta ultra", "Miles de metros cúbicos"),
    "asfalto": (_DERIVADOS, "Asfalto", "Miles de toneladas"),
    "azucar": (_ALIMENTOS, "Azúcar", "Miles de toneladas"),
    "cerveza": (_ALIMENTOS, "Cerveza", "Miles de hectolitros"),
    "gaseosas": (_ALIMENTOS, "Gaseosas", "Miles de litros"),
    "vino": (_ALIMENTOS, "Vino", "Miles de hectolitros"),
    "cigarrillos": (_ALIMENTOS, "Cigarrillos", "Millones de paquetes"),
    "cigarrillos_1er_cuartil": (_ALIMENTOS, "Cigarrillos, 1.er cuartil", "Millones de paquetes"),
    "cigarrillos_2do_cuartil": (_ALIMENTOS, "Cigarrillos, 2.º cuartil", "Millones de paquetes"),
    "cigarrillos_3er_cuartil": (_ALIMENTOS, "Cigarrillos, 3.er cuartil", "Millones de paquetes"),
    "cigarrillos_4to_cuartil": (_ALIMENTOS, "Cigarrillos, 4.º cuartil", "Millones de paquetes"),
    "cemento": (_OTROS, "Cemento", "Miles de toneladas"),
    "energia_electrica": (_OTROS, "Energía eléctrica", "GWh"),
    "productos_farma": (_OTROS, "Productos farmacéuticos", "Millones de unidades"),
    "residuos": (_OTROS, "Residuos", "Toneladas"),
    "maquinas_herramientas": (_OTROS, "Máquinas y herramientas", indice(1985)),
}


def _ventas(serie: Serie) -> Clasificacion | None:
    encontrado = _VENTAS.get(serie.variable)
    if encontrado is None:
        return None
    grupo, nombre, unidad = encontrado
    return _crear(VENTAS, (nombre,), (), nivel3=grupo, unidad=unidad)


# ===================================================================================================
# Producción industrial por producto (hoja 1.17)
# ===================================================================================================
PRODUCTOS = "Producción industrial por producto"
_P_ALIMENTOS = "Alimentos, bebidas y tabaco"
_P_PETROLEO = "Petróleo, gas y derivados"
_P_QUIMICA = "Productos químicos"
_P_AUTOS = "Automotores"
_P_METALES = "Siderurgia y metales"
_P_CONSTRUCCION = "Materiales de construcción"
_P_PAPEL = "Papel"
_P_ELECTRO = "Electrodomésticos y electrónica"
_P_OTROS = "Otros productos"
# variable → (grupo, nombre). Las unidades se completan sólo donde están confirmadas (ver _UNIDAD_PRODUCTOS). «Grasas» y
# «Otros artículos» no se pueden ubicar en una rama con los datos que trae la fuente.
_PRODUCTOS = {
    "aceites_vegetales": (_P_ALIMENTOS, "Aceites vegetales"),
    "subproductos_oleaginosos": (_P_ALIMENTOS, "Subproductos oleaginosos"),
    "harintrigo": (_P_ALIMENTOS, "Harina de trigo"),
    "dulce_leche": (_P_ALIMENTOS, "Dulce de leche"),
    "crema": (_P_ALIMENTOS, "Crema"), "manteca": (_P_ALIMENTOS, "Manteca"),
    "quesos": (_P_ALIMENTOS, "Quesos"), "yogur": (_P_ALIMENTOS, "Yogur"),
    "postres_lacteos_flanes": (_P_ALIMENTOS, "Postres lácteos y flanes"),
    "leche_industrial_parconsumo": (_P_ALIMENTOS, "Leche industrial y para consumo"),
    "faena_aves": (_P_ALIMENTOS, "Faena de aves"),
    "faena_carne_vacuna": (_P_ALIMENTOS, "Faena de carne vacuna"),
    "faena_ovinos": (_P_ALIMENTOS, "Faena de ovinos"),
    "faena_porcinos": (_P_ALIMENTOS, "Faena de porcinos"),
    "bebidas_espirituosas": (_P_ALIMENTOS, "Bebidas espirituosas"),
    "despachos_gaseosas": (_P_ALIMENTOS, "Despachos de gaseosas"),
    "despachos_autorizados_vino_mercado_interno":
        (_P_ALIMENTOS, "Despachos autorizados de vino en el mercado interno"),
    "mercado_externo": (_P_ALIMENTOS, "Despachos autorizados de vino en el mercado externo"),
    "despachos_cigarrillos": (_P_ALIMENTOS, "Despachos de cigarrillos"),
    "aerokerosene": (_P_PETROLEO, "Aerokerosene"), "asfalto": (_P_PETROLEO, "Asfalto"),
    "butano": (_P_PETROLEO, "Butano"), "coque": (_P_PETROLEO, "Coque"),
    "fuel_oil": (_P_PETROLEO, "Fueloil"), "gas_natural": (_P_PETROLEO, "Gas natural"),
    "gas_oil": (_P_PETROLEO, "Gasoil"), "gas_refineria": (_P_PETROLEO, "Gas de refinería"),
    "kerosene": (_P_PETROLEO, "Kerosene"), "lubricantes": (_P_PETROLEO, "Lubricantes"),
    "nafta_comun": (_P_PETROLEO, "Nafta común"), "nafta_super": (_P_PETROLEO, "Nafta súper"),
    "nafta_ultra": (_P_PETROLEO, "Nafta ultra"), "naftas_total": (_P_PETROLEO, "Naftas"),
    "otros_productos_livianos": (_P_PETROLEO, "Otros productos livianos"),
    "otros_productos_medianos": (_P_PETROLEO, "Otros productos medianos"),
    "otros_productos_pesados": (_P_PETROLEO, "Otros productos pesados"),
    "petroleo_crudo": (_P_PETROLEO, "Petróleo crudo"),
    "petroleo_procesado": (_P_PETROLEO, "Petróleo procesado"),
    "propano": (_P_PETROLEO, "Propano"),
    "etileno": (_P_QUIMICA, "Etileno"), "polietileno": (_P_QUIMICA, "Polietileno"),
    "pvc": (_P_QUIMICA, "PVC"), "caucho_sintetico": (_P_QUIMICA, "Caucho sintético"),
    "negro_humo": (_P_QUIMICA, "Negro de humo"), "urea": (_P_QUIMICA, "Urea"),
    "isopropanol": (_P_QUIMICA, "Isopropanol"), "sec_butanol": (_P_QUIMICA, "Sec-butanol"),
    "solventes_alifaticos": (_P_QUIMICA, "Solventes alifáticos"),
    "solventes_hexano": (_P_QUIMICA, "Solventes hexano"),
    "solventes_aromaticos": (_P_QUIMICA, "Solventes aromáticos"),
    "aguarras": (_P_QUIMICA, "Aguarrás"), "oxigeno": (_P_QUIMICA, "Oxígeno"),
    "nitrogeno": (_P_QUIMICA, "Nitrógeno"),
    "pinturas_parconstruccion": (_P_QUIMICA, "Pinturas para construcción"),
    "pinturas_parotros_usos": (_P_QUIMICA, "Pinturas para otros usos"),
    "detergentes_limpiadores_incluye_lavandinas":
        (_P_QUIMICA, "Detergentes y limpiadores (incluye lavandinas)"),
    "jabones_tocador_lavar": (_P_QUIMICA, "Jabones de tocador y de lavar"),
    "automotores_automoviles": (_P_AUTOS, "Automóviles"), "utilitarios": (_P_AUTOS, "Utilitarios"),
    "carga_pasajeros": (_P_AUTOS, "Carga y pasajeros"), "tractores": (_P_AUTOS, "Tractores"),
    "hierro_primario": (_P_METALES, "Hierro primario"), "acero_crudo": (_P_METALES, "Acero crudo"),
    "aluminio_primario": (_P_METALES, "Aluminio primario"),
    "laminados_frio": (_P_METALES, "Laminados en frío"),
    "laminados_caliente_no_planos": (_P_METALES, "Laminados en caliente no planos"),
    "planos": (_P_METALES, "Laminados en caliente planos"),
    "cemento_portland": (_P_CONSTRUCCION, "Cemento portland"),
    "ladrillos_ceramicos_huecos": (_P_CONSTRUCCION, "Ladrillos cerámicos huecos"),
    "placas_yeso": (_P_CONSTRUCCION, "Placas de yeso"),
    "ceramicos_pisos_revestimientos": (_P_CONSTRUCCION, "Cerámicos para pisos y revestimientos"),
    "articulos_sanitarios_ceramica": (_P_CONSTRUCCION, "Artículos sanitarios de cerámica"),
    "papel_pardiarios_impresion": (_P_PAPEL, "Papel para diarios e impresión"),
    "papel_parembalaje": (_P_PAPEL, "Papel para embalaje"),
    "papel_paruso_sanitario_otros_usos": (_P_PAPEL, "Papel para uso sanitario y otros usos"),
    "acondicionadores_aire": (_P_ELECTRO, "Acondicionadores de aire"),
    "acondicionadores_aire_parauto": (_P_ELECTRO, "Acondicionadores de aire para automóviles"),
    "calefactores_estufas": (_P_ELECTRO, "Calefactores y estufas"),
    "camaras_fotograficas": (_P_ELECTRO, "Cámaras fotográficas"),
    "cocinas_gas_familiar": (_P_ELECTRO, "Cocinas a gas familiar"),
    "calefones_termotanques": (_P_ELECTRO, "Calefones y termotanques"),
    "discos_compactos": (_P_ELECTRO, "Discos compactos"),
    "equipos_audio": (_P_ELECTRO, "Equipos de audio"),
    "hornos_anafes": (_P_ELECTRO, "Hornos y anafes"),
    "heladeras_freezers": (_P_ELECTRO, "Heladeras y freezers"),
    "hornos_microondas": (_P_ELECTRO, "Hornos a microondas"),
    "lavarropas_secarropas": (_P_ELECTRO, "Lavarropas y secarropas"),
    "monitores_lcd_parpc": (_P_ELECTRO, "Monitores LCD para PC"),
    "notebooks_netbooks_tablet_pc": (_P_ELECTRO, "Notebooks, netbooks y tablets"),
    "autorradios": (_P_ELECTRO, "Autorradios"),
    "reproductores_video": (_P_ELECTRO, "Reproductores de video"),
    "telefonos_celulares": (_P_ELECTRO, "Teléfonos celulares"),
    "televisores": (_P_ELECTRO, "Televisores"),
    "grasas": (_P_OTROS, "Grasas"), "otros_articulos": (_P_OTROS, "Otros artículos"),
}


def _productos(serie: Serie) -> Clasificacion | None:
    encontrado = _PRODUCTOS.get(serie.variable)
    if encontrado is None:
        return None
    grupo, nombre = encontrado
    return _crear(PRODUCTOS, (nombre,), (), nivel3=grupo, unidad=_UNIDAD_PRODUCTOS.get(serie.variable, ""))


# ===================================================================================================
# ISAC (indicador sintético de la actividad de la construcción)
# ===================================================================================================
ISAC = "ISAC"
BASE_ISAC = indice(2004)

_INSUMOS = (
    "Artículos sanitarios de cerámica", "Asfalto", "Cales", "Cemento portland",
    "Hierro redondo y aceros para la construcción", "Hormigón elaborado", "Ladrillos huecos",
    "Mosaicos graníticos y calcáreos", "Pinturas para construcción", "Pisos y revestimientos cerámicos",
    "Placas de yeso", "Resto", "Yeso",
)
_INSUMO_POR_CLAVE = {plano(i): i for i in _INSUMOS}
# datos.gob.ar (hoja 1.18b): la variable es «isac_<insumo>».
_INSUMO_DATOS = {
    "isac_articulos_sanitarios_de_ceramica": "Artículos sanitarios de cerámica",
    "isac_asfalto": "Asfalto", "isac_cales": "Cales", "isac_cemento_portland": "Cemento portland",
    "isac_hierro_redondo_y_aceros_para_la_construccion": "Hierro redondo y aceros para la construcción",
    "isac_hormigon_elaborado": "Hormigón elaborado", "isac_ladrillos_huecos": "Ladrillos huecos",
    "isac_mosaicos_graniticos_y_calcareos": "Mosaicos graníticos y calcáreos",
    "isac_pinturas_para_construccion": "Pinturas para construcción",
    "isac_pisos_y_revestimientos_ceramicos": "Pisos y revestimientos cerámicos",
    "isac_placas_de_yeso": "Placas de yeso", "isac_resto": "Resto", "isac_yeso": "Yeso",
}
_VERSION_ISAC_VIEJA = "Serie hasta 2015 (base 2004)"
_TIPOS_DE_OBRA = "Tipos de obra"
# Aperturas por tipo de obra del ISAC anterior (hoja 1.18), con el nombre de datos.gob.ar.
_OBRAS_DATOS = {
    "isac_edificios_vivienda": "Edificios para vivienda",
    "isac_edificios_otros_destinos": "Edificios para otros destinos",
    "isac_obras_viales": "Obras viales",
    "isac_construcciones_petroleras": "Obras petroleras",
    "isac_otras_obras_infraestructura": "Otras obras de infraestructura",
}


def _isac_general(medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _crear(ISAC, (NIVEL_GENERAL,), medidas, nivel3=NIVEL_GENERAL, unidad=unidad)


def _isac_insumo(insumo: str, medidas: tuple[str, ...], unidad: str) -> Clasificacion:
    return _crear(ISAC, (INSUMOS, insumo), medidas, nivel3=INSUMOS, nivel4=insumo, unidad=unidad)


def _isac_cuadro_1(serie: Serie) -> Clasificacion | None:
    encontrado = _medidas_cuadro_1(serie.columna)
    if encontrado is None:
        return None
    medidas, unidad = encontrado
    return _isac_general(medidas, unidad or BASE_ISAC)


def _insumo_de_rotulo(columna: str) -> str | None:
    """«Resto (1) - %» → «Resto»; el rótulo puede traer nota al pie y el sufijo «- %»."""
    texto = re.sub(r"\s*\(\d+\)", "", limpiar(columna))
    texto = re.sub(r"\s+-\s+%$", "", texto)
    return _INSUMO_POR_CLAVE.get(plano(texto))


def _isac_insumos(medidas: tuple[str, ...], unidad: str):
    def regla(serie: Serie) -> Clasificacion | None:
        insumo = _insumo_de_rotulo(serie.columna)
        return _isac_insumo(insumo, medidas, unidad) if insumo else None
    return regla


def _isac_puestos(serie: Serie) -> Clasificacion | None:
    texto = plano(serie.columna)
    if texto == "puestos de trabajo":
        medidas, unidad = (), "Puestos de trabajo"
    elif "respecto al mismo mes" in texto:
        medidas, unidad = (INTERANUAL,), PORCENTAJE
    elif "acumulada del ano" in texto:
        medidas, unidad = (ACUMULADA,), PORCENTAJE
    else:
        return None
    return _crear(ISAC, (PUESTOS,), medidas, nivel3=PUESTOS, unidad=unidad)


def _permisos(serie: Serie, historica: bool) -> Clasificacion | None:
    texto = plano(serie.columna)
    if texto.startswith("superficie autorizada"):
        partes, medidas, unidad = ("Superficie autorizada",), (), "Metros cuadrados"
    elif texto.startswith("permisos otorgados"):
        # El rótulo arrastra «acumulada del año…» de una celda combinada, pero los valores son el
        # número de permisos de cada mes (no crecen dentro del año).
        partes, medidas, unidad = ("Permisos otorgados",), (), "Permisos"
    elif "respecto al mismo mes" in texto:
        partes, medidas, unidad = ("Superficie autorizada",), (INTERANUAL,), PORCENTAJE
    elif "acumulada del ano" in texto:
        partes, medidas, unidad = ("Superficie autorizada",), (ACUMULADA,), PORCENTAJE
    else:
        return None
    partes = (PERMISOS, *partes)
    if not historica:
        return _crear(ISAC, partes, medidas, nivel3=PERMISOS, unidad=unidad)
    municipios = re.search(r"de (\d+) municipios", serie.descripcion)
    version = f"Permisos de edificación de {municipios.group(1)} municipios" if municipios else None
    if version is None:
        return None
    return _crear(ISAC, partes, medidas, nivel3=SERIES_HISTORICAS, nivel4=version, unidad=unidad)


def _isac_permisos_vigente(serie: Serie) -> Clasificacion | None:
    return _permisos(serie, historica=False)


def _isac_permisos_historico(serie: Serie) -> Clasificacion | None:
    return _permisos(serie, historica=True)


# datos.gob.ar, hoja 1.18 (ISAC anterior, 1993-2015): nivel general y cinco tipos de obra.
def _isac_anterior(serie: Serie) -> Clasificacion | None:
    general = {"isac_nivel_general": (), "isac_sin_estacionalidad": (DESESTACIONALIZADO,),
               "isac_ciclo_tendencia": (TENDENCIA_CICLO,)}.get(serie.variable)
    if general is not None:
        return _crear(ISAC, (NIVEL_GENERAL,), general, nivel3=SERIES_HISTORICAS,
                      nivel4=_VERSION_ISAC_VIEJA, nivel5=NIVEL_GENERAL, unidad=BASE_ISAC)
    obra = _OBRAS_DATOS.get(serie.variable)
    if obra is None:
        return None
    return _crear(ISAC, (obra,), (), nivel3=SERIES_HISTORICAS, nivel4=_VERSION_ISAC_VIEJA,
                  nivel5=_TIPOS_DE_OBRA, unidad=BASE_ISAC)


# datos.gob.ar, hoja 1.18b: copia congelada (hasta feb-2023) del ISAC vigente.
def _isac_revisado(serie: Serie) -> Clasificacion | None:
    general = {"isac_nivel_general": ((), BASE_ISAC), "isac_sin_estacionalidad": ((DESESTACIONALIZADO,), BASE_ISAC),
               "isac_ciclo_tendencia": ((TENDENCIA_CICLO,), BASE_ISAC),
               "isac_variacion_interanual": ((INTERANUAL,), PROPORCION)}.get(serie.variable)
    if general is not None:
        return _isac_general(*general)
    insumo = _INSUMO_DATOS.get(serie.variable)
    if insumo is None:
        return None
    descripcion = plano(serie.descripcion)
    if descripcion.endswith("serie sin estacionalidad"):
        medidas = (DESESTACIONALIZADO,)
    elif descripcion.endswith("serie tendencia ciclo"):
        medidas = (TENDENCIA_CICLO,)
    else:
        medidas = ()
    return _isac_insumo(insumo, medidas, BASE_ISAC)


# ===================================================================================================
# Despacho
# ===================================================================================================
_INDEC_EMAE = {
    "tabla letras": _emae_tabla_letras,
    "tabla var letras": _emae_tabla_var_letras,
    "tabla": _emae_tabla,
}
_INDEC_IPI = {
    "cuadro 1": _ipi_cuadro_1,
    "cuadro 2": _ipi_cuadro_numerico((), BASE_IPI),
    "cuadro 3": _ipi_cuadro_numerico((INTERANUAL,), PORCENTAJE),
    "cuadro 4": _ipi_cuadro_numerico((ACUMULADA,), PORCENTAJE),
    "cuadro 5": _ipi_cuadro_desestacionalizado_o_tendencia((DESESTACIONALIZADO,)),
    "cuadro 6": _ipi_cuadro_desestacionalizado_o_tendencia((TENDENCIA_CICLO,)),
    "cuadro 7": _ipi_cuadro_7,
}
_INDEC_ISAC = {
    "cuadro 1": _isac_cuadro_1,
    "cuadro 2.1": _isac_insumos((), BASE_ISAC),
    "cuadro 2.2": _isac_insumos((INTERANUAL,), PORCENTAJE),
    "cuadro 2.3": _isac_insumos((ACUMULADA,), PORCENTAJE),
    "cuadro 3.1": _isac_insumos((DESESTACIONALIZADO,), BASE_ISAC),
    "cuadro 3.2": _isac_insumos((DESESTACIONALIZADO, MENSUAL), PORCENTAJE),
    "cuadro 4.1": _isac_insumos((TENDENCIA_CICLO,), BASE_ISAC),
    "cuadro 4.2": _isac_insumos((TENDENCIA_CICLO, MENSUAL), PORCENTAJE),
    "cuadro 5": _isac_puestos,
    "cuadro 6.1": _isac_permisos_vigente,
    "cuadro 6.2": _isac_permisos_historico,
    "cuadro 6.3": _isac_permisos_historico,
}
_POR_FUENTE = {
    "indec-emae": _INDEC_EMAE,
    "indec-ipi-manufacturero": _INDEC_IPI,
    "indec-isac": _INDEC_ISAC,
}
# datos.gob.ar, actividad.xlsx: la hoja se compara tal cual (con mayúsculas y puntos).
_DATOS_ACTIVIDAD = {
    "1.4 Emae BASE 1993": _emae_1993,
    "1.4.1 Emae BASE 2004": _emae_2004,
    "1.4.2 Emae BASE 2004 - Sectores": _emae_sectores_datos,
    "1.13 Ventas": _ventas,
    "1.14 Emi": _emi("2004"),
    "1.14.d IPI Manuf. - NG": _ipi_general_datos,
    "1.14.d IPI Manuf. - sectores": _ipi_sectores_datos,
    "1.14b Emi 2012": _emi("2012"),
    "1.14c Emi 2004": _emi("2004", proporcion=True),
    "1.15 Enc. industrial": _encuesta,
    "1.15b Enc. industrial Rev.": _encuesta,
    "1.16 UCII": _ucii("2006"),
    "1.16b UCII 2012": _ucii("2012"),
    "1.16c UCII 2004": _ucii(),
    "1.17 Productos ind.": _productos,
    "1.18 ISAC": _isac_anterior,
    "1.18b ISAC rev.": _isac_revisado,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    regla = None
    if serie.fuente == DATOS:
        if serie.archivo == ARCHIVO_DATOS:
            regla = _DATOS_ACTIVIDAD.get(serie.hoja)
    elif serie.fuente in _POR_FUENTE:
        regla = _POR_FUENTE[serie.fuente].get(_hoja(serie))
    return regla(serie) if regla else None
