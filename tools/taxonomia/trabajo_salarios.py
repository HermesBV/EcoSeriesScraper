"""Reglas de salarios, empleo registrado y cuenta de generación del ingreso.

Tema «Trabajo e ingresos». Alcance (coincidencia exacta con fuente, archivo y hoja):

* ``indec-salarios`` (cuadros 1 a 3), ``mch-ripte`` y ``mch-sipa`` (todas las hojas);
* ``datos.gob.ar`` · ``empleo_ingresos.xlsx``: ``IS``, ``IS oct16=100``, ``RIPTE``, ``OEDE *``,
  ``RT/RN/CS/Puestos x Sector`` y ``x Prov``, ``CGI *``, ``EIM``, ``EIM Nuevo`` y las hojas de
  salarios industriales de 1960 a 1990.

Árbol (indicador > nivel 3 > nivel 4 > nivel 5)::

    Índice de salarios       Nivel general · Sector registrado · Sector privado registrado ·
                             Sector público · Sector público nacional · Sector público provincial ·
                             Sector privado no registrado · Series históricas
    RIPTE                    (una sola serie, publicada por el MCH y por datos.gob.ar)
    Empleo registrado (SIPA) Por modalidad ocupacional · Por rama de actividad ·
                             Por agrupación de ramas · Por provincia
    Remuneraciones del empleo registrado
                             Total · Por rama de actividad · Series históricas > versión > concepto
    Puestos de trabajo registrados
                             Total · Por rama de actividad · Por agrupación de ramas · Series históricas
    Cuenta de generación del ingreso
                             Total de la economía · Por rama de actividad > componente
    Obreros ocupados y horas trabajadas en la industria
    Salarios industriales    Industria manufacturera · Series históricas > versión

Criterios que explican las decisiones (ver ``docs/taxonomia.md``):

* Misma serie, misma ubicación y mismo nombre sin importar quién la publique. Se comprobó contra
  los valores de ``BD.xlsx``: el MCH (SIPA) y las hojas OEDE de datos.gob.ar publican las mismas
  series (asalariados privados por modalidad, rama y provincia; remuneración mediana y promedio);
  el MCH ``A.6`` repite, con otro rótulo, siete ramas de ``A.2`` (valores idénticos), que por eso
  llevan el nombre de la rama CIIU y no el de la agrupación del SIPA; ``OEDE Puestos Sector`` repite
  cinco ramas de ``OEDE Puestos Rama``. Las «agrupaciones» que no coinciden con ninguna rama
  (p. ej. «Actividades agropecuarias y pesca» = agricultura + pesca) van aparte.
* Las hojas ``CGI Remun``, ``CGI Remun netas``, ``CGI CostoSalarial`` y ``CGI Puestos`` (sector
  privado, 2004-2016, cerradas) son la misma medida que ``RT/RN/CS/Puestos x Sector`` (remuneración
  total, neta, costo salarial y puestos promedio del sector privado): los valores coinciden salvo
  la revisión. Por eso no van a la cuenta de generación del ingreso sino a «Remuneraciones del
  empleo registrado» y «Puestos de trabajo registrados», en «Series históricas». La cuenta de
  generación del ingreso queda con ``CGI VABpb`` (componentes del valor agregado bruto),
  ``CGI-2016`` (distribución funcional) y ``CGI ManodeObra`` (insumo de mano de obra), vigentes.
* ``CGI VABpb`` se queda en «Trabajo e ingresos»: la hoja es la cuenta de generación del ingreso
  completa (remuneraciones, excedente, ingreso mixto, impuestos y su suma, el VAB), y el VAB es
  el saldo que la cierra, no una serie de producción independiente.
* Ramas según la ClaNAE-04 con la denominación de las cuentas nacionales del INDEC, escritas
  siempre igual (``RAMA``). Provincias con la grafía oficial (``PROVINCIA``).
* Desestacionalizado: el SIPA publica «con estacionalidad» (serie original, sin sufijo) y «sin
  estacionalidad» (desestacionalizada: «(desestacionalizado)»).
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

TEMA = "Trabajo e ingresos"

# --- Indicadores -------------------------------------------------------------------------------
IS = "Índice de salarios"
RIPTE = "RIPTE"
EMPLEO_IND = "Empleo registrado (SIPA)"
REMUNERACION_IND = "Remuneraciones del empleo registrado"
PUESTOS_IND = "Puestos de trabajo registrados"
CGI_IND = "Cuenta de generación del ingreso"
OBREROS_IND = "Obreros ocupados y horas trabajadas en la industria"
SALARIOS_IND = "Salarios industriales"

# Prefijos de nombre cuando difieren del indicador.
EMPLEO = "Empleo registrado"
CGI = "CGI"

# --- Vocabulario controlado de grupos -------------------------------------------------------------
TOTAL = "Total"
POR_MODALIDAD = "Por modalidad ocupacional"
POR_RAMA = "Por rama de actividad"
POR_AGRUPACION = "Por agrupación de ramas"
POR_PROVINCIA = "Por provincia"
TOTAL_ECONOMIA = "Total de la economía"
SERIES_HISTORICAS = "Series históricas"
INDUSTRIA = "Industria manufacturera"

# --- Medidas ---------------------------------------------------------------------------------------
DESESTACIONALIZADO = "desestacionalizado"
VAR_MENSUAL = "var. % mensual"
VAR_TRIMESTRAL = "var. % trimestral"
VAR_INTERANUAL = "var. % interanual"
VAR_ACUMULADA = "var. % acumulada"
VAR_PERIODO = "var. % respecto del período anterior"
VAR = "var. %"
PRECIOS_CONSTANTES = "precios constantes"
PCT_PIB = "% del PIB a costo de factores"

# --- Unidades que completan lo que la fuente no informa -----------------------------------------------
PESOS = "Pesos"
MILLONES_DE_PESOS = "Millones de pesos"
MILES_DE_PERSONAS = "Miles de personas"
PUESTOS = "Puestos de trabajo"
MILES_DE_PUESTOS = "Miles de puestos"
PORCENTAJE = "%"
PROPORCION = "Proporción (1 = 100%)"
INDICE = "Índice"


def _indice(base: str) -> str:
    return f"Índice {base}"


# --- Ramas de actividad ---------------------------------------------------------------------------
# Secciones de la ClaNAE-04 con la denominación de las cuentas nacionales del INDEC.
RAMA = {
    "A": "Agricultura, ganadería, caza y silvicultura",
    "B": "Pesca y servicios conexos",
    "C": "Explotación de minas y canteras",
    "D": "Industria manufacturera",
    "E": "Electricidad, gas y agua",
    "F": "Construcción",
    "G": "Comercio mayorista, minorista y reparaciones",
    "H": "Hoteles y restaurantes",
    "I": "Transporte, almacenamiento y comunicaciones",
    "J": "Intermediación financiera y otros servicios financieros",
    "K": "Servicios inmobiliarios, empresariales y de alquiler",
    "L": "Administración pública, defensa y seguridad social obligatoria",
    "M": "Enseñanza",
    "N": "Servicios sociales y de salud",
    "O": "Servicios comunitarios, sociales y personales",
    "P": "Hogares privados con servicio doméstico",
}
SIN_ESPECIFICAR = "Sin especificar"
SIN_CLASIFICAR = "Sin clasificar"
SIN_DISTRIBUIR = "Sin distribuir"
SECTOR_PUBLICO = "Sector público"
SIN_SECTOR_PUBLICO = "Total excluido el sector público"

# --- Provincias (grafía oficial; «Buenos Aires» es la provincia) --------------------------------------
CABA = "Ciudad Autónoma de Buenos Aires"
PROVINCIA = {
    "buenos aires": "Buenos Aires", "catamarca": "Catamarca", "caba": CABA, "chaco": "Chaco",
    "chubut": "Chubut", "cordoba": "Córdoba", "corrientes": "Corrientes",
    "entre rios": "Entre Ríos", "formosa": "Formosa", "jujuy": "Jujuy", "la pampa": "La Pampa",
    "la rioja": "La Rioja", "mendoza": "Mendoza", "misiones": "Misiones", "neuquen": "Neuquén",
    "rio negro": "Río Negro", "salta": "Salta", "san juan": "San Juan", "san luis": "San Luis",
    "santa cruz": "Santa Cruz", "santa fe": "Santa Fe", "santiago del estero": "Santiago del Estero",
    "tierra del fuego": "Tierra del Fuego", "tucuman": "Tucumán",
}
_PROVINCIA_ALIAS = {
    "cdad. autonoma de buenos aires": "caba", "capital federal": "caba",
    "provincia de buenos aires": "buenos aires", "provincia buenos aires": "buenos aires",
    "tierra fuego": "tierra del fuego", "santiago estero": "santiago del estero",
}


def _provincia(clave: str) -> str | None:
    clave = _PROVINCIA_ALIAS.get(clave, clave)
    return PROVINCIA.get(clave)


def _clave(texto: str) -> str:
    """Minúsculas sin tildes ni llamadas de nota («CATAMARCA 3/») ni punto final."""
    return re.sub(r"\s*\d+/\.?$", "", plano(texto)).strip(" .")


def _rama_de(tabla: dict[str, str], clave: str) -> str | None:
    letra_o_nombre = tabla.get(clave)
    if letra_o_nombre is None:
        return None
    return RAMA.get(letra_o_nombre, letra_o_nombre)


# =================================================================================================
# Índice de salarios
# =================================================================================================

_SECTORES_IS = {
    "nivel general": "Nivel general",
    "registrado": "Sector registrado",
    "privado registrado": "Sector privado registrado",
    "publico": SECTOR_PUBLICO,
    "privado no registrado": "Sector privado no registrado",
    "publico nacional": "Sector público nacional",
    "publico provincial": "Sector público provincial",
}


def _is(sector: str, medida: str, historica: bool = False, unidad: str = "") -> Clasificacion:
    grupo = _SECTORES_IS[sector]
    return Clasificacion(
        tema=TEMA, indicador=IS, cobertura=SERIES_HISTORICAS if historica else grupo,
        nombre=con_medida(unir(IS, grupo), medida), unidad=unidad,
    )


_INDICE_2016 = _indice("oct-2016=100")


def _indec_salarios(serie: Serie) -> Clasificacion | None:
    columna = plano(serie.columna)
    hoja = plano(serie.hoja)
    if "igual mes del ano anterior" in columna:
        medida = VAR_INTERANUAL
    elif "diciembre del ano anterior" in columna:
        medida = VAR_ACUMULADA
    elif "mes anterior" in columna:
        medida = VAR_MENSUAL
    elif "numero indice" in columna:
        medida = ""
    else:
        return None
    unidad = _INDICE_2016 if not medida else PORCENTAJE

    if hoja == "cuadro 1":
        if columna.startswith("sector privado no registrado"):
            sector = "privado no registrado"
        elif columna.startswith("total indice de salarios"):
            sector = "nivel general"
        elif columna.startswith("sector registrado - sector privado registrado"):
            sector = "privado registrado"
        elif columna.startswith("sector registrado - sector publico"):
            sector = "publico"
        elif columna.startswith("sector registrado - total registrado"):
            sector = "registrado"
        else:
            return None
        return _is(sector, medida, unidad=unidad)

    if hoja == "cuadro 2":
        if columna.startswith("sector privado registrado"):
            sector = "privado registrado"
        elif columna.startswith("sector publico"):
            sector = "publico"
        elif columna.startswith("total registrado"):
            # Las columnas con llamada (9) a (12) son, por orden, el sector privado no registrado
            # y el nivel general (variación interanual y respecto de diciembre de cada uno): el
            # rótulo del INDEC repite «Total registrado (1)». Se verificó recalculando las
            # variaciones desde los índices del cuadro 1.
            llamada = re.search(r"%\s*\((\d+)\)\s*$", columna)
            if llamada is None:
                sector = "registrado"
            elif llamada.group(1) in ("9", "10"):
                sector = "privado no registrado"
            elif llamada.group(1) in ("11", "12"):
                sector = "nivel general"
            else:
                return None
        else:
            return None
        return _is(sector, medida, unidad=unidad)

    if hoja == "cuadro 3":
        if columna.startswith("publico nacional"):
            sector = "publico nacional"
        elif columna.startswith("publico provincial"):
            sector = "publico provincial"
        else:
            return None
        return _is(sector, medida, unidad=unidad)
    return None


# Hoja «IS» (datos.gob.ar): índice con y sin las modificaciones del Decreto 381/07, base 2012.
_IS_VIEJO = {
    "nivel_gral": "nivel general", "sector_pub": "publico",
    "sector_priv_no_regis": "privado no registrado", "sector_priv_regis": "privado registrado",
}


def _is_base_2012(serie: Serie) -> Clasificacion | None:
    for prefijo, sector in _IS_VIEJO.items():
        if serie.variable == prefijo + "_mod_decreto":
            return _is(sector, "", historica=True, unidad=_indice("2012=100"))
        if serie.variable == prefijo + "_sin_mod_decreto":
            return _is(sector, "sin modificaciones del Decreto 381/07", historica=True,
                       unidad=_indice("2012=100"))
    return None


_IS_OCT16 = {
    "indice_salarios": "nivel general",
    "indice_salarios_registrado": "registrado",
    "indice_salarios_registrado_sector_privado": "privado registrado",
    "indice_salarios_no_registrado_sector_privado": "privado no registrado",
    "indice_salarios_registrado_sector_publico": "publico",
}


def _is_oct16(serie: Serie) -> Clasificacion | None:
    sector = _IS_OCT16.get(serie.variable)
    return _is(sector, "", unidad=_INDICE_2016) if sector else None


# =================================================================================================
# RIPTE
# =================================================================================================

def _ripte(serie: Serie) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=RIPTE, nombre=RIPTE, unidad=PESOS)


# =================================================================================================
# Empleo registrado (SIPA): personas con trabajo registrado
# =================================================================================================

ASALARIADOS_PRIVADOS = "Asalariados privados"
MODALIDAD = {
    "privado": ASALARIADOS_PRIVADOS,
    "publico": "Asalariados públicos",
    "casas particulares": "Asalariados de casas particulares",
    "autonomos": "Independientes autónomos",
    "monotributo": "Independientes monotributistas",
    "monotributo social": "Independientes monotributistas sociales",
    "total": TOTAL,
}


def _empleo(grupo: str, partes: tuple[str, ...], desestacionalizado: bool,
            unidad: str = "") -> Clasificacion:
    nombre = con_medida(unir(EMPLEO, *partes), DESESTACIONALIZADO if desestacionalizado else "")
    return Clasificacion(tema=TEMA, indicador=EMPLEO_IND, cobertura=grupo, nombre=nombre, unidad=unidad)


def _empleo_modalidad(modalidad: str, desestacionalizado: bool, unidad: str = "") -> Clasificacion:
    return _empleo(POR_MODALIDAD, (MODALIDAD[modalidad],), desestacionalizado, unidad)


def _empleo_rama(rama: str, desestacionalizado: bool, unidad: str = "") -> Clasificacion:
    """Rama (o «Total», que es el total de asalariados privados) de los asalariados privados."""
    if rama == TOTAL:
        return _empleo_modalidad("privado", desestacionalizado, unidad)
    return _empleo(POR_RAMA, (ASALARIADOS_PRIVADOS, rama), desestacionalizado, unidad)


def _empleo_provincia(provincia: str, desestacionalizado: bool, unidad: str = "") -> Clasificacion:
    return _empleo(POR_PROVINCIA, (ASALARIADOS_PRIVADOS, provincia), desestacionalizado, unidad)


# Ramas del SIPA (secciones de la CIIU Rev. 3) tal como las rotulan las hojas A.2 y A.3.
_RAMAS_SIPA = {
    "agricultura, ganaderia, caza y silvicultura": "A", "pesca": "B",
    "explotacion de minas y canteras": "C", "industrias manufactureras": "D",
    "suministro de electricidad, gas y agua": "E", "construccion": "F",
    "comercio y reparaciones": "G", "hoteles y restaurantes": "H",
    "transporte, almacenamiento y comunicacion": "I", "transporte, almacen. y comunicacion": "I",
    "intermediacion financiera": "J",
    "actividades inmobiliarias, empresariales y de alquiler": "K", "ensenanza": "M",
    "servicios sociales y de salud": "N", "servicios comunitarios, sociales y personales": "O",
    "sin especificar": SIN_ESPECIFICAR, "total": TOTAL,
}

# Hoja A.6: agrupación de ramas del SIPA. Las ocho que siguen coinciden mes a mes con una rama de
# A.2 (se comparó la serie completa) y llevan el nombre de esa rama.
_AGRUPACION_IGUAL_A_RAMA = {
    "construccion": "F", "comercio": "G", "hoteleria y restaurantes": "H",
    "servicios financieros": "J", "educacion": "M", "salud": "N", "mineria y petroleo": "C",
    "sin especificar": SIN_ESPECIFICAR,
}
_AGRUPACION_PROPIA = {
    "actividades agropecuarias y pesca": "Actividades agropecuarias y pesca",
    "actividades de informatica": "Actividades de informática",
    "actividades inmobiliarias y de alquiler": "Actividades inmobiliarias y de alquiler",
    "alimentos y tabaco": "Alimentos y tabaco",
    "automotores y neumaticos": "Automotores y neumáticos",
    "comunicaciones": "Comunicaciones",
    "madera y papel": "Madera y papel",
    "metalmecanica": "Metalmecánica",
    "otras manufacturas": "Otras manufacturas",
    "otros servicios": "Otros servicios",
    "otros servicios a empresas": "Otros servicios a empresas",
    "otros servicios publicos y sociales": "Otros servicios públicos y sociales",
    "quimica y petroquimica": "Química y petroquímica",
    "servicios culturales": "Servicios culturales",
    "servicios de investigacion, seguridad y limpieza": "Servicios de investigación, seguridad y limpieza",
    "servicios tecnicos y/o profesionales a empresas": "Servicios técnicos y/o profesionales a empresas",
    "textiles, confecciones, cuero y calzado": "Textiles, confecciones, cuero y calzado",
    "transportes": "Transportes",
}

_MODALIDAD_SIPA = {
    "empleo asalariado en el sector privado": "privado",
    "empleo asalariado en el sector publico": "publico",
    "empleo en casas particulares": "casas particulares",
    "trabajo independientes autonomos": "autonomos",
    "trabajo independientes monotributo": "monotributo",
    "trabajo independientes monotributo social": "monotributo social",
    "total": "total",
}


def _sipa_empleo(serie: Serie) -> Clasificacion | None:
    hoja = serie.hoja
    columna = serie.columna
    ultimo = _clave(columna.split(" - ")[-1])
    desestacionalizado = hoja.endswith(".2")
    if hoja in ("A.1", "T.1"):
        unidad = ""
        if "desestacionalizada" in plano(columna):
            desestacionalizado = True
        elif "serie original" not in plano(columna):
            return None
        if hoja == "A.1":
            return _empleo_modalidad("privado", desestacionalizado, unidad)
        return _empleo_modalidad("total", desestacionalizado, unidad)
    if hoja in ("A.2.1", "A.2.2", "A.3.1", "A.3.2"):
        rama = _rama_de(_RAMAS_SIPA, ultimo)
        return _empleo_rama(rama, desestacionalizado) if rama else None
    if hoja in ("A.5.1", "A.5.2"):
        provincia = _provincia(ultimo)
        return _empleo_provincia(provincia, desestacionalizado) if provincia else None
    if hoja in ("A.6.1", "A.6.2"):
        rama = _rama_de(_AGRUPACION_IGUAL_A_RAMA, ultimo)
        if rama:
            return _empleo_rama(rama, desestacionalizado)
        agrupacion = _AGRUPACION_PROPIA.get(ultimo)
        if agrupacion:
            return _empleo(POR_AGRUPACION, (ASALARIADOS_PRIVADOS, agrupacion), desestacionalizado)
        return None
    if hoja in ("T.2.1", "T.2.2", "T.3.1", "T.3.2"):
        modalidad = _MODALIDAD_SIPA.get(ultimo)
        return _empleo_modalidad(modalidad, desestacionalizado) if modalidad else None
    return None


# =================================================================================================
# Remuneraciones del empleo registrado
# =================================================================================================

BRUTA_PROMEDIO = "Remuneración bruta promedio"
BRUTA_MEDIANA = "Remuneración bruta mediana"
NETA_PROMEDIO = "Remuneración neta promedio"
COSTO_SALARIAL = "Costo salarial promedio"

# Versiones viejas (nivel 4 de «Series históricas»).
RAMAS_AGRUPADAS = "Ramas agrupadas"
PROVINCIAS_HIST = "Provincias"
RAMAS_CGI = "Ramas de la cuenta de generación del ingreso"


def _remuneracion(concepto: str, partes: tuple[str, ...] = (), desestacionalizado: bool = False,
                  grupo: str = TOTAL, version: str = "") -> Clasificacion:
    nombre = con_medida(unir(concepto, *partes), DESESTACIONALIZADO if desestacionalizado else "")
    if version:
        return Clasificacion(tema=TEMA, indicador=REMUNERACION_IND, cobertura=SERIES_HISTORICAS,
                             apertura=version, medida=concepto, nombre=nombre, unidad=PESOS)
    return Clasificacion(tema=TEMA, indicador=REMUNERACION_IND, cobertura=grupo, nombre=nombre,
                         unidad=PESOS)


def _sipa_remuneracion(serie: Serie) -> Clasificacion | None:
    columna = plano(serie.columna)
    if "mediana" in columna:
        concepto = BRUTA_MEDIANA
    elif "promedio" in columna:
        concepto = BRUTA_PROMEDIO
    else:
        return None
    if "sin estacionalidad" in columna:
        desestacionalizado = True
    elif "con estacionalidad" in columna:
        desestacionalizado = False
    else:
        return None
    return _remuneracion(concepto, desestacionalizado=desestacionalizado)


def _oede_remuneraciones(serie: Serie) -> Clasificacion | None:
    concepto = {"asalariados_priv_remuneracion_mediana": BRUTA_MEDIANA,
                "asalariados_priv_remuneracion_promedio": BRUTA_PROMEDIO}.get(serie.variable)
    return _remuneracion(concepto) if concepto else None


# Hoja OEDE Remuneraciones Rama: «rem_bruta_asal_<rama>» (con estacionalidad) y
# «rem_bruta_asal_sin_est_<rama>» (sin estacionalidad), con rótulos abreviados distintos.
_RAMAS_OEDE_REM = {
    "agricultura_ganaderia_caza_silvicultura": "A", "agric_ganaderia_caza_silvicultura": "A",
    "acti_inmobiliarias_empresariales_alquiler": "K", "acti_inmo_empresariales_alquiler": "K",
    "comercio_reparaciones": "G", "construccion": "F", "ensenianza": "M",
    "explotacion_minas_canteras": "C", "hoteles_restaurantes": "H",
    "intermediacion_financiera": "J", "industrias_manufactureras": "D",
    "otras_acti_comunitarias_soc_personales": "O", "otras_acti_comun_soc_personales": "O",
    "pesca": "B", "servicios_sociales_salud": "N", "suministro_electricidad_gas_agua": "E",
    "total": TOTAL, "transporte_almacenamiento_comunicaciones": "I",
    "transporte_almacenamiento_comunic": "I",
}


def _oede_remuneraciones_rama(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if not variable.startswith("rem_bruta_asal_"):
        return None
    cola = variable[len("rem_bruta_asal_"):]
    desestacionalizado = cola.startswith("sin_est_")
    if desestacionalizado:
        cola = cola[len("sin_est_"):]
    rama = _rama_de(_RAMAS_OEDE_REM, cola)
    if rama is None:
        return None
    if rama == TOTAL:
        return _remuneracion(BRUTA_PROMEDIO, desestacionalizado=desestacionalizado)
    return _remuneracion(BRUTA_PROMEDIO, (rama,), desestacionalizado, grupo=POR_RAMA)


# --- Series históricas del INDEC: sectores agrupados y provincias (2006-2015) -------------------------
_SECTORES_AGRUPADOS = {
    "alimentos_bebidas_tabaco": "Alimentos, bebidas y tabaco",
    "agricultura_ganaderia_caza_silvicultura": "A",
    "actividades_inmobiliarias_empresariales_alquiler": "K",
    "admin_publica_defensa_orgs_organos_extraterritoriales":
        "Administración pública, defensa y organizaciones y órganos extraterritoriales",
    "comercio_mayorista_minorista_reparaciones": "G",
    "construccion": "F",
    "derivados_petroleo_quimicos": "Derivados del petróleo y químicos",
    "explotacion_minas_canteras": "C",
    "hoteles_restaurantes": "H",
    "intermediacion_financiera": "J",
    "metalicas_basicas_productos_metalicos":
        "Metálicas básicas y productos metálicos, excepto maquinaria y equipo",
    "madera_papel_imprenta_editoriales": "Madera, papel, imprenta y editoriales",
    "material_transporte": "Material de transporte",
    "maquinarias_equipos": "Maquinarias y equipos",
    "otras_actividades_servicios_comunitarios_sociales_personales": "O",
    "otras_industrias": "Otras industrias",
    "pesca_servicios_conexos": "B",
    "sin_clasificar": SIN_CLASIFICAR,
    "suministro_electricidad_gas_agua": "E",
    "sector_privado": TOTAL,
    "servicios_sociales_ensenianza_saludos_privados": "Servicios sociales, enseñanza y salud privados",
    "transporte_almacenamiento_comunicaciones": "I",
    "total_industria_manufacturera": "D",
    "total_sectores_productores_bienes": "Sectores productores de bienes",
    "total_sectores_productores_servicios": "Sectores productores de servicios",
    "total_sistema": "Total del sistema",
    "textiles_cuero": "Textiles y cueros",
}
_TOTAL_PROVINCIAS = "total_sector_privado"

# Hoja → (concepto, es de remuneración). Para los puestos no hay nivel de concepto.
_HISTORICAS_INDEC = {
    "RT x Sector": (BRUTA_PROMEDIO, RAMAS_AGRUPADAS), "RN x Sector": (NETA_PROMEDIO, RAMAS_AGRUPADAS),
    "CS x Sector": (COSTO_SALARIAL, RAMAS_AGRUPADAS), "Puestos x Sector": (None, RAMAS_AGRUPADAS),
    "RT x Prov": (BRUTA_PROMEDIO, PROVINCIAS_HIST), "RN x Prov": (NETA_PROMEDIO, PROVINCIAS_HIST),
    "CS x Prov": (COSTO_SALARIAL, PROVINCIAS_HIST), "Puestos x Prov": (None, PROVINCIAS_HIST),
}


def _historica_indec(serie: Serie) -> Clasificacion | None:
    concepto, version = _HISTORICAS_INDEC[serie.hoja]
    if version == RAMAS_AGRUPADAS:
        rama = _rama_de(_SECTORES_AGRUPADOS, serie.variable)
        if rama is None:
            return None
        sufijo = () if rama == TOTAL else (rama,)
    else:
        if serie.variable == _TOTAL_PROVINCIAS:
            sufijo = ()
        elif serie.variable == "sin_clasificar":
            sufijo = (SIN_CLASIFICAR,)
        else:
            provincia = _provincia(serie.variable.replace("_", " "))
            if provincia is None:
                return None
            sufijo = (provincia,)
    if concepto is None:
        return _puestos(sufijo, grupo=SERIES_HISTORICAS, version=version, unidad=MILES_DE_PUESTOS)
    return _remuneracion(concepto, sufijo, version=version)


# --- Cuenta de generación del ingreso, sector privado (2004-2016): 65 sectores ---------------------
_RAMAS_CGI_ANTIGUO = {
    "actividades inmobiliarias, empresariales y de alquiler": "K",
    "administracion publica y defensa; planes de seguridad social de afiliacion obligatoria": "L",
    "agricultura, ganaderia, caza y servicios conexos": "Agricultura, ganadería, caza y servicios conexos",
    "agricultura, ganaderia, caza y silvicultura": "A",
    "asociaciones": "Asociaciones",
    "captacion , depuracion y distribucion de agua": "Captación, depuración y distribución de agua",
    "comercio mayorista, minorista y reparaciones": "G",
    "comunicaciones": "Comunicaciones",
    "construccion": "F",
    "curtido y terminacion de cueros; fabricacion de articulos de marroquineria, talabarteria y calzado y de sus partes":
        "Cueros, marroquinería, talabartería y calzado",
    "edicion e impresion; reproduccion de grabaciones": "Edición e impresión; reproducción de grabaciones",
    "elaboracion de productos alimenticios y bebidas": "Elaboración de productos alimenticios y bebidas",
    "elaboracion de productos de tabaco": "Elaboración de productos de tabaco",
    "electricidad, gas y agua": "E",
    "eliminacion de desperdicios y aguas residuales, saneamiento y servicios similares":
        "Eliminación de desperdicios y aguas residuales, saneamiento y servicios similares",
    "ensenanza": "M",
    "ensenanza privada": "Enseñanza privada",
    "ensenanza publica": "Enseñanza pública",
    "explotacion de minas y canteras": "C",
    "extraccion de carbon y lignito; extraccion de turba. extraccion de petroleo crudo y gas natural; actividades de servicios relacionadas con la extraccion de petroleo y gas, excepto las actividades de prospeccion":
        "Extracción de carbón, lignito, turba, petróleo crudo y gas natural",
    "extraccion de minerales metaliferos. explotacion de minas y canteras n.c.p.":
        "Extracción de minerales metalíferos; explotación de minas y canteras n.c.p.",
    "fabricacion de coque, productos de la refinacion del petroleo y combustible nuclear":
        "Fabricación de coque, productos de la refinación del petróleo y combustible nuclear",
    "fabricacion de equipo de transporte n.c.p.": "Fabricación de equipo de transporte n.c.p.",
    "fabricacion de equipos y aparatos de radio, television y comunicaciones":
        "Fabricación de equipos y aparatos de radio, televisión y comunicaciones",
    "fabricacion de gas ; distribucion de combustibles gaseosos por tuberias":
        "Fabricación de gas; distribución de combustibles gaseosos por tuberías",
    "fabricacion de instrumentos medicos, opticos y de precision; fabricacion de relojes":
        "Fabricación de instrumentos médicos, ópticos y de precisión; fabricación de relojes",
    "fabricacion de maquinaria de oficina, contabilidad e informatica":
        "Fabricación de maquinaria de oficina, contabilidad e informática",
    "fabricacion de maquinaria y aparatos electricos n.c.p.":
        "Fabricación de maquinaria y aparatos eléctricos n.c.p.",
    "fabricacion de maquinaria y equipo n.c.p.": "Fabricación de maquinaria y equipo n.c.p.",
    "fabricacion de metales comunes": "Fabricación de metales comunes",
    "fabricacion de muebles y colchones; industrias manufactureras n.c.p.":
        "Fabricación de muebles y colchones; industrias manufactureras n.c.p.",
    "fabricacion de papel y de productos de papel": "Fabricación de papel y de productos de papel",
    "fabricacion de prendas de vestir; terminacion y tenido de pieles":
        "Fabricación de prendas de vestir; terminación y teñido de pieles",
    "fabricacion de productos de caucho y plastico": "Fabricación de productos de caucho y plástico",
    "fabricacion de productos elaborados de metal, excepto maquinaria y equipo":
        "Fabricación de productos elaborados de metal, excepto maquinaria y equipo",
    "fabricacion de productos minerales no metalicos": "Fabricación de productos minerales no metálicos",
    "fabricacion de productos textiles": "Fabricación de productos textiles",
    "fabricacion de sustancias y productos quimicos": "Fabricación de sustancias y productos químicos",
    "fabricacion de vehiculos automotores, remolques y semirremolques":
        "Fabricación de vehículos automotores, remolques y semirremolques",
    "generacion captacion y distribucion de energia electrica":
        "Generación, captación y distribución de energía eléctrica",
    "hoteles ; campamentos y otros tipos de hospedaje temporal":
        "Hoteles; campamentos y otros tipos de hospedaje temporal",
    "hoteles y restaurantes": "H",
    "industria manufacturera": "D",
    "intermediacion financiera": "Intermediación financiera",
    "intermediacion financiera y otros servicios financieros": "J",
    "no clasificados": "No clasificados",
    "otras actividades de servicios comunitarias, sociales y personales": "O",
    "pesca": "B",
    "produccion de madera y fabricacion de productos de madera y corcho, excepto muebles; fabricacion de articulos de paja y de materiales trenzables":
        "Madera y productos de madera y corcho, excepto muebles; artículos de paja",
    "reciclamiento": "Reciclamiento",
    "reparacion, mantenimiento e instalacion de maquinas y equipos":
        "Reparación, mantenimiento e instalación de máquinas y equipos",
    "restaurantes, bares y cantinas": "Restaurantes, bares y cantinas",
    "resto": "Resto",
    "salud privada": "Salud privada",
    "salud publica": "Salud pública",
    "servicios auxiliares a la actividad financiera": "Servicios auxiliares a la actividad financiera",
    "servicios culturales y deportivos. otras actividades": "Servicios culturales y deportivos; otras actividades",
    "servicios de organizaciones y organos extraterritoriales":
        "Servicios de organizaciones y órganos extraterritoriales",
    "servicios de seguros": "Servicios de seguros",
    "servicios inmobiliarios con bienes propios y arrendados":
        "Servicios inmobiliarios con bienes propios y arrendados",
    "servicios sociales y de salud": "N",
    "silvicultura, extraccion de madera y servicios conexos":
        "Silvicultura, extracción de madera y servicios conexos",
    "total": TOTAL,
    "transporte": "Transporte",
    "transporte y comunicaciones": "I",
}
# La clave de consulta pierde el punto final («n.c.p.»): se lo quita también de la tabla.
_RAMAS_CGI_ANTIGUO = {clave.rstrip("."): valor for clave, valor in _RAMAS_CGI_ANTIGUO.items()}
_CONCEPTO_CGI_ANTIGUO = {
    "CGI Remun": BRUTA_PROMEDIO, "CGI Remun netas": NETA_PROMEDIO,
    "CGI CostoSalarial": COSTO_SALARIAL, "CGI Puestos": None,
}


def _cgi_antiguo(serie: Serie) -> Clasificacion | None:
    rama = _rama_de(_RAMAS_CGI_ANTIGUO, _clave(serie.descripcion))
    if rama is None:
        return None
    sufijo = () if rama == TOTAL else (rama,)
    concepto = _CONCEPTO_CGI_ANTIGUO[serie.hoja]
    if concepto is None:
        return _puestos(sufijo, grupo=SERIES_HISTORICAS, version=RAMAS_CGI, unidad=PUESTOS)
    return _remuneracion(concepto, sufijo, version=RAMAS_CGI)


# =================================================================================================
# Puestos de trabajo registrados
# =================================================================================================

PUESTOS_NOMBRE = "Puestos de trabajo registrados"


def _puestos(partes: tuple[str, ...] = (), desestacionalizado: bool = False, grupo: str = TOTAL,
             version: str = "", unidad: str = PUESTOS) -> Clasificacion:
    nombre = con_medida(unir(PUESTOS_NOMBRE, *partes), DESESTACIONALIZADO if desestacionalizado else "")
    return Clasificacion(tema=TEMA, indicador=PUESTOS_IND, cobertura=grupo, apertura=version,
                         nombre=nombre, unidad=unidad)


_RAMAS_OEDE_PUESTOS = {
    "agricultura_ganaderia_caza_silvicultura": "A", "comercio_por_mayor_por_menor": "G",
    "construccion": "F", "ensenianza": "M", "explotacion_minas_canteras": "C",
    "electricidad_gas_agua": "E", "hoteleria_restaurantes": "H",
    "intermediacion_financiera_otros_ser_finan": "J", "industria_manufacturera": "D",
    "pesca_servicios_conexos": "B", "servicios_inmobiliarios_empresariales_alquiler": "K",
    "servicios_comunitarios_sociales_personales": "O", "servicios_sociales_salud": "N",
    "total": TOTAL, "transporte_almacenamiento_comunicaciones": "I",
}
# Hoja OEDE Puestos Sector: cinco de los siete sectores coinciden con una rama (mismos valores).
_SECTORES_OEDE_PUESTOS = {
    "comercio": "G", "construccion": "F", "electricidad_gas_agua": "E", "industria": "D",
    "mineria_petroleo": "C", "total": TOTAL,
}
_AGRUPACIONES_OEDE_PUESTOS = {
    "agricultura_ganaderia_pesca": "Agricultura, ganadería y pesca",
    "servicios": "Servicios",
}


def _puestos_rama(rama: str, desestacionalizado: bool) -> Clasificacion:
    if rama == TOTAL:
        return _puestos((), desestacionalizado)
    return _puestos((rama,), desestacionalizado, grupo=POR_RAMA)


def _oede_puestos_rama(serie: Serie) -> Clasificacion | None:
    rama = _rama_de(_RAMAS_OEDE_PUESTOS, serie.variable)
    return _puestos_rama(rama, False) if rama else None


def _oede_puestos_sector(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if not variable.startswith("empleo_registrado_priv_"):
        return None
    cola = variable[len("empleo_registrado_priv_"):]
    desestacionalizado = cola.startswith("sin_estac_")
    if desestacionalizado:
        cola = cola[len("sin_estac_"):]
    rama = _rama_de(_SECTORES_OEDE_PUESTOS, cola)
    if rama:
        return _puestos_rama(rama, desestacionalizado)
    agrupacion = _AGRUPACIONES_OEDE_PUESTOS.get(cola)
    if agrupacion:
        return _puestos((agrupacion,), desestacionalizado, grupo=POR_AGRUPACION)
    return None


# =================================================================================================
# Empleo registrado: hojas OEDE (modalidad, rama y provincia)
# =================================================================================================

def _oede_total(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    desestacionalizado = variable.endswith("_sin_estac")
    base = re.sub(r"_(?:sin_estac|estacionalidad)$", "", variable)
    modalidad = {
        "asalariados_pub": "publico", "asalariados_priv": "privado",
        "asalariados_casas_particulares": "casas particulares",
        "independientes_autonomos": "autonomos", "independientes_monotributo": "monotributo",
        "independientes_monotributo_social": "monotributo social", "total": "total",
    }.get(base)
    if modalidad is None:
        return None
    return _empleo_modalidad(modalidad, desestacionalizado, MILES_DE_PERSONAS)


def _oede_asalariados_provincia(serie: Serie) -> Clasificacion | None:
    cola = serie.variable
    if not cola.startswith("asalariados_priv_"):
        return None
    cola = cola[len("asalariados_priv_"):]
    desestacionalizado = cola.startswith("sin_estac_")
    if desestacionalizado:
        cola = cola[len("sin_estac_"):]
    clave = cola.replace("_", " ")
    provincia = _provincia(clave)
    return _empleo_provincia(provincia, desestacionalizado, MILES_DE_PERSONAS) if provincia else None


_RAMAS_OEDE_ASALARIADOS = {
    "agricultura_ganaderia_caza_silvicultura": "A",
    "actividades_inmobiliarias_empresariales_alquiler": "K", "comercio_reparaciones": "G",
    "construccion": "F", "ensenanza": "M", "explotacion_minas_canteras": "C",
    "hoteles_restaurantes": "H", "intermediacion_financiera": "J", "industrias_manufactureras": "D",
    "pesca": "B", "sin_especificar": SIN_ESPECIFICAR, "servicios_sociales_salud": "N",
    "servicios_comunitarios_sociales_personales": "O", "suministro_electricidad_gas_agua": "E",
    "total": TOTAL, "transporte_almacen_comunicacion": "I",
}


def _oede_asalariados_rama(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if variable.endswith("_sin_est"):
        cola, desestacionalizado = variable[: -len("_sin_est")], True
    elif variable.endswith("_est"):
        cola, desestacionalizado = variable[: -len("_est")], False
    else:
        return None
    rama = _rama_de(_RAMAS_OEDE_ASALARIADOS, cola)
    return _empleo_rama(rama, desestacionalizado, MILES_DE_PERSONAS) if rama else None


# =================================================================================================
# Cuenta de generación del ingreso (INDEC, base 2004, desde 2016)
# =================================================================================================

VAB = "Valor agregado bruto"
REMUNERACION_TRABAJO = "Remuneración del trabajo asalariado"
EXCEDENTE = "Excedente bruto de explotación"
INGRESO_MIXTO = "Ingreso mixto bruto"
IMPUESTOS = "Impuestos netos de subsidios a la actividad"
PIB_COSTO_FACTORES = "PIB a costo de factores"
PUESTOS_TOTAL = "Puestos de trabajo"
PUESTOS_REGISTRADOS = "Puestos de trabajo asalariados registrados"
PUESTOS_NO_REGISTRADOS = "Puestos de trabajo asalariados no registrados"
PUESTOS_NO_ASALARIADOS = "Puestos de trabajo no asalariados"

_LETRA_SECTOR = {letra: RAMA[letra.upper()] for letra in "abcdefghijklmnop"}
_AGREGADOS_CGI = (
    ("sin_distribuir_", SIN_DISTRIBUIR),
    ("total_excluido_sector_publico_", SIN_SECTOR_PUBLICO),
    ("total_excl_sector_pub_", SIN_SECTOR_PUBLICO),
    ("total_general_", ""),
    ("total_sector_publico_", SECTOR_PUBLICO),
)


def _sector_cgi(variable: str) -> str | None:
    """Rama del prefijo de la variable («k_act_inmob…»); '' para el total de la economía."""
    for prefijo, nombre in _AGREGADOS_CGI:
        if variable.startswith(prefijo):
            return nombre
    if len(variable) > 2 and variable[1] == "_" and variable[0] in _LETRA_SECTOR:
        return _LETRA_SECTOR[variable[0]]
    return None


def _cgi(componente: str, sector: str, medida: str = "", unidad: str = "") -> Clasificacion:
    nombre = con_medida(unir(CGI, componente, sector), medida)
    if not sector:
        return Clasificacion(tema=TEMA, indicador=CGI_IND, cobertura=TOTAL_ECONOMIA, nombre=nombre,
                             unidad=unidad)
    return Clasificacion(tema=TEMA, indicador=CGI_IND, cobertura=POR_RAMA, apertura=componente,
                         nombre=nombre, unidad=unidad)


_COMPONENTES_VAB = (
    ("valor agregado bruto pb", VAB), ("remuneracion al trabajo asalariado", REMUNERACION_TRABAJO),
    ("excedente de explotacion bruto", EXCEDENTE), ("ingreso bruto mixto", INGRESO_MIXTO),
    ("impuestos netos de subsidios a la actividad", IMPUESTOS),
)


def _cgi_vab(serie: Serie) -> Clasificacion | None:
    sector = _sector_cgi(serie.variable)
    descripcion = plano(serie.descripcion)
    if sector is None:
        return None
    for cola, componente in _COMPONENTES_VAB:
        if descripcion.endswith(cola):
            return _cgi(componente, sector, unidad=MILLONES_DE_PESOS)
    return None


_CGI_2016 = {
    "total_remuneracion_trabajo_asalariado": REMUNERACION_TRABAJO,
    "excedente_bruto_explotacion": EXCEDENTE,
    "total_ingreso_mixto_bruto": INGRESO_MIXTO,
    "total_impuestos_netos_subsidios_actividad": IMPUESTOS,
    "total_pib_cf": PIB_COSTO_FACTORES,
}


def _cgi_2016(serie: Serie) -> Clasificacion | None:
    componente = _CGI_2016.get(serie.variable)
    return _cgi(componente, "", PCT_PIB, PROPORCION) if componente else None


def _cgi_mano_de_obra(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    sector = _sector_cgi(variable)
    if sector is None:
        return None
    # El orden importa: «asalariados_no_reg» y «puestos_trab_no_asal» contienen «asal».
    if "no_asal" in variable:
        componente = PUESTOS_NO_ASALARIADOS
    elif "no_reg" in variable:
        componente = PUESTOS_NO_REGISTRADOS
    elif re.search(r"asal(?:ariado)?_reg", variable):
        componente = PUESTOS_REGISTRADOS
    elif variable.endswith(("puestos_trabajo", "puesto_trabajo")):
        componente = PUESTOS_TOTAL
    else:
        return None
    return _cgi(componente, sector, unidad=MILES_DE_PUESTOS)


# =================================================================================================
# Industria: obreros, horas y salarios
# =================================================================================================

OBREROS = "Obreros ocupados en la industria"
HORAS = "Horas trabajadas en la industria"
SALARIO_OBRERO = "Salario por obrero ocupado"
OFICIAL = "Salario básico de convenio del oficial"
PEON = "Salario básico de convenio del peón"
BASICO = "Salario básico de convenio"
HORA_NORMAL = "Salario hora normal"
CAPITAL_FEDERAL = "Capital Federal"

# Versiones de «Series históricas».
CONVENIOS_POR_RAMA = "Salarios básicos de convenio por rama"
CONVENIOS_CAPITAL = "Convenios de la Capital Federal"
HORA_NORMAL_VERSION = "Salario hora normal"

_BASE_EIM = _indice("1997=100")


def _eim(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    concepto = {"obreros_ocupados": OBREROS, "horas_trabajadas": HORAS,
                "salario_obrero": SALARIO_OBRERO}
    for clave, nombre in concepto.items():
        if variable == f"indice_{clave}":
            medida, unidad = "", _BASE_EIM
        elif variable == f"variaciones_anuales_{clave}":
            medida, unidad = VAR_INTERANUAL, PORCENTAJE
        elif variable == f"variaciones_trimestrales_{clave}":
            medida, unidad = VAR_TRIMESTRAL, PORCENTAJE
        else:
            continue
        if nombre is SALARIO_OBRERO:
            return Clasificacion(tema=TEMA, indicador=SALARIOS_IND, cobertura=INDUSTRIA,
                                 nombre=con_medida(unir(SALARIOS_IND, nombre), medida), unidad=unidad)
        return Clasificacion(tema=TEMA, indicador=OBREROS_IND, nombre=con_medida(nombre, medida),
                             unidad=unidad)
    return None


_RAMAS_CONVENIO = {
    "alimentacion": "Alimentación", "calzado": "Calzado", "carne": "Carne", "caucho": "Caucho",
    "ceramistas": "Ceramistas", "confeccion": "Confección", "cueros": "Cueros",
    "graficos": "Gráficos", "lechera": "Lechera", "maderas": "Maderas",
    "mecanicos_transporte_automotor": "Mecánicos, transporte y automotor",
    "metalurgicos": "Metalúrgicos", "mosaistas": "Mosaístas",
    "papel_carton_quimicos_afines": "Papel, cartón, químicos y afines",
    "promedio": "Promedio de las ramas", "quimicos": "Químicos", "tabaco": "Tabaco",
    "textil_algodon": "Textil algodón", "textil_lana": "Textil lana", "vidrio": "Vidrio",
    "vitivinicola": "Vitivinícola",
}


def _convenio_por_rama(serie: Serie) -> Clasificacion | None:
    rama = _RAMAS_CONVENIO.get(serie.variable)
    if rama is None:
        return None
    categoria, etiqueta = (OFICIAL, "Oficial") if serie.hoja.startswith("Salario oficial") else (PEON, "Peón")
    return Clasificacion(
        tema=TEMA, indicador=SALARIOS_IND, cobertura=SERIES_HISTORICAS, apertura=CONVENIOS_POR_RAMA,
        medida=etiqueta, nombre=unir(SALARIOS_IND, categoria, rama), unidad=_indice("1960=100"),
    )


def _convenio_capital(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if serie.hoja == "SalariosInd-Indices 62-67":
        categoria = {"basico_convenio": BASICO, "oficiales": OFICIAL, "peones": PEON}.get(variable)
        medida, unidad = "", INDICE
    elif serie.hoja == "SalariosInd-var65-66":
        categoria = {"basico_convenio": BASICO,
                     "real_sin_basico_convenio": "Salario real sin básico de convenio"}.get(variable)
        medida, unidad = VAR, PORCENTAJE
    else:
        return None
    if categoria is None:
        return None
    return Clasificacion(
        tema=TEMA, indicador=SALARIOS_IND, cobertura=SERIES_HISTORICAS, apertura=CONVENIOS_CAPITAL,
        nombre=con_medida(unir(SALARIOS_IND, categoria, CAPITAL_FEDERAL), medida), unidad=unidad,
    )


_HORA_NORMAL = {
    # variable → (concepto, medida, unidad)
    "nominal_a": (HORA_NORMAL, (), ""),
    "real_base_1986": (HORA_NORMAL, (PRECIOS_CONSTANTES,), _indice("1986=100")),
    "poder_adquisitivo_base_1986": ("Poder adquisitivo del salario hora normal", (), _indice("1986=100")),
    "variacion_salario_real_a": (HORA_NORMAL, (PRECIOS_CONSTANTES, VAR_PERIODO), PORCENTAJE),
    "variacion_salario_real_b": (HORA_NORMAL, (PRECIOS_CONSTANTES, VAR_INTERANUAL), PORCENTAJE),
    "variacion_poder_adquisitivo_a": ("Poder adquisitivo del salario hora normal", (VAR_PERIODO,), PORCENTAJE),
    "variacion_poder_adquisitivo_b": ("Poder adquisitivo del salario hora normal", (VAR_INTERANUAL,), PORCENTAJE),
}


def _hora_normal(serie: Serie) -> Clasificacion | None:
    definicion = _HORA_NORMAL.get(serie.variable)
    if definicion is None:
        return None
    concepto, medida, unidad = definicion
    return Clasificacion(
        tema=TEMA, indicador=SALARIOS_IND, cobertura=SERIES_HISTORICAS, apertura=HORA_NORMAL_VERSION,
        nombre=con_medida(unir(SALARIOS_IND, concepto), ", ".join(medida)), unidad=unidad,
    )


# =================================================================================================
# Despacho
# =================================================================================================

_DATOS_GOB = "datos.gob.ar"
_ARCHIVO = "empleo_ingresos.xlsx"

_POR_HOJA = {
    "IS": _is_base_2012, "IS oct16=100": _is_oct16, "RIPTE": _ripte,
    "OEDE Total": _oede_total, "OEDE Asal prov": _oede_asalariados_provincia,
    "OEDE Asal rama": _oede_asalariados_rama, "OEDE Remuneraciones": _oede_remuneraciones,
    "OEDE Remuneraciones Rama": _oede_remuneraciones_rama,
    "OEDE Puestos Rama": _oede_puestos_rama, "OEDE Puestos Sector": _oede_puestos_sector,
    **{hoja: _historica_indec for hoja in _HISTORICAS_INDEC},
    **{hoja: _cgi_antiguo for hoja in _CONCEPTO_CGI_ANTIGUO},
    "CGI VABpb": _cgi_vab, "CGI-2016": _cgi_2016, "CGI ManodeObra": _cgi_mano_de_obra,
    "EIM": _eim, "EIM Nuevo": _eim,
    "Salario oficial60-75": _convenio_por_rama, "Salario peon60-75": _convenio_por_rama,
    "Salario real Ind85-90": _hora_normal,
    "SalariosInd-Indices 62-67": _convenio_capital, "SalariosInd-var65-66": _convenio_capital,
}

_SIPA_REMUNERACION = ("A.4",)


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == "indec-salarios":
        return _indec_salarios(serie)
    if serie.fuente == "mch-ripte":
        return _ripte(serie)
    if serie.fuente == "mch-sipa":
        if serie.hoja in _SIPA_REMUNERACION:
            return _sipa_remuneracion(serie)
        return _sipa_empleo(serie)
    if serie.fuente == _DATOS_GOB and serie.archivo == _ARCHIVO:
        regla = _POR_HOJA.get(serie.hoja)
        return regla(serie) if regla else None
    return None
