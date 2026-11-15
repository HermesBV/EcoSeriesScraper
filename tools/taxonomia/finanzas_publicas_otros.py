"""Reglas de las finanzas públicas de ``finanzas_publicas.xlsx`` (datos.gob.ar) que no son la cuenta base caja.

Hojas y árbol (tema «Finanzas públicas»):

    Cuenta ahorro-inversión-financiamiento (base devengado)
        AC_Dev_61, AN_Dev_61, CE_Dev_61, EP_Dev_61, ISS_Dev_61, OD_Dev_61, PROV_MCBA_Dev_61,
        SPA_Dev_61, SPA_Dev_93: series históricas 1961-1992 (y el resultado financiero 1993-2018).
        Nivel 3 = subsector; nivel 4 = bloque (Recursos, Erogaciones, Resultados, Financiamiento).
    Gasto público consolidado
        GPC_$, GPC_%, GPN_$, GPN_%, GPP_$, GPP_%, GPM_$, GPM_%: gasto por finalidad y función.
        Nivel 3 = nivel de gobierno; nivel 4 = finalidad (Funcionamiento del Estado, Servicios sociales…).
    Gasto en política de ingresos
        Gasto Pol. Ing: devengado del Gobierno nacional por programa. Nivel 3 = tipo de programa.
    Prestaciones de la seguridad social
        JUB, PNC; SEG DES., AUH-AUE, FUENTE ANSES PRESTACIONES, Fuente Cuenta Inversion- Presta,
        PROGRESAR: cantidad de beneficios o de beneficiarios. Nivel 3 = tipo de prestación.
    Recaudación tributaria
        RECA, RECA_SUB, RECA_HISTORICA: Total, Por dirección general, Por tributo (con una
        apertura por familia de tributos) y Series históricas.

Cómo se deriva cada dimensión: los cuadros del Ministerio de Economía no traen un encabezado
jerárquico, así que la identidad de cada serie está en ``Variable`` (el código de la fuente,
estable entre las publicaciones de un mismo cuadro). Cada hoja tiene un diccionario explícito
de variable a partida con su nombre en castellano; una variable que no esté en el diccionario
queda sin regla (``None``) en lugar de salir con un nombre armado a medias.

Convenciones de nombre:

* La cuenta devengado se llama «<Partida> · <subpartida> · <subsector>», sin el subsector para
  el Sector Público Argentino. Una misma partida en dos períodos (1961-1976, 1977-1986…) lleva
  el mismo nombre: el período está en las fechas de la serie, no en el nombre.
* Sin sufijo es el valor en pesos corrientes; «(% del PIB)» es el gasto como porcentaje del PIB;
  «(var. % interanual)» es la variación; «(cantidad de beneficios)» y «(cantidad de beneficiarios)»
  distinguen los conteos de los montos.
* Dos publicaciones de la misma estadística (ANSES-Transparencia y ANSES-Abiertos) llevan el
  mismo nombre: miden lo mismo y sólo difieren en el período que cubren.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, unir

FUENTE = "datos.gob.ar"
ARCHIVO = "finanzas_publicas.xlsx"
TEMA = "Finanzas públicas"

MILLONES_DE_PESOS = "Millones de pesos"
PORCENTAJE_DEL_PIB = "% del PIB"
PROPORCION = "Proporción"
BENEFICIOS = "Beneficios"
BENEFICIARIOS = "Beneficiarios"
PERSONAS = "Personas"

TOTAL = "Total"
DEL_PIB = "% del PIB"
INTERANUAL = "var. % interanual"


def _clasificacion(indicador: str, cobertura: str, apertura: str, nombre: str, unidad: str) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=indicador, cobertura=cobertura, apertura=apertura,
                         nombre=nombre, unidad=unidad)


# ---------------------------------------------------------------------------
# Cuenta ahorro-inversión-financiamiento, base devengado (1961-1992)
# ---------------------------------------------------------------------------

AIF_DEVENGADO = "Cuenta ahorro-inversión-financiamiento (base devengado)"

RECURSOS = "Recursos"
EROGACIONES = "Erogaciones"
RESULTADOS = "Resultados"
FINANCIAMIENTO = "Financiamiento"

_SECTOR_PUBLICO_ARGENTINO = "Sector Público Argentino"
_SUBSECTORES = {
    "AC_Dev_61": "Administración central",
    "AN_Dev_61": "Administración nacional",
    "CE_Dev_61": "Cuentas especiales",
    "EP_Dev_61": "Empresas públicas",
    "ISS_Dev_61": "Instituciones de la seguridad social",
    "OD_Dev_61": "Organismos descentralizados",
    "PROV_MCBA_Dev_61": "Provincias y Municipalidad de la Ciudad de Buenos Aires",
    "SPA_Dev_61": _SECTOR_PUBLICO_ARGENTINO,
}

_FIGURATIVAS = "operaciones figurativas"
_TRANSFERENCIAS = "Erogaciones corrientes · Transferencias"

# Variable de la fuente: (bloque, partida). Las erogaciones y los recursos «después de figurativas»
# suman a los totales las transferencias entre entidades del mismo sector (verificado con los datos:
# Total de erogaciones + Erogaciones figurativas = Total de erogaciones después de figurativas).
# El Resultado es el resultado financiero: igual a menos la suma del aumento de pasivos financieros
# netos y del financiamiento del Banco Central (verificado con los datos).
_PARTIDAS_AIF = {
    # Recursos
    "rec_corr_total_rec_corr": (RECURSOS, "Recursos corrientes"),
    "rec_corr_ingr_tribut": (RECURSOS, "Recursos corrientes · Ingresos tributarios"),
    "rec_corr_ingr_notribut": (RECURSOS, "Recursos corrientes · Ingresos no tributarios"),
    "recursos_capital": (RECURSOS, "Recursos de capital"),
    "total_recursos": (RECURSOS, "Recursos totales"),
    "total_rec_desp_figur": (RECURSOS, f"Recursos totales después de {_FIGURATIVAS}"),
    # Erogaciones
    "erog_corr_total_erog_corr": (EROGACIONES, "Erogaciones corrientes"),
    "erog_corr_personal": (EROGACIONES, "Erogaciones corrientes · Personal"),
    "erog_corr_bys_noper": (EROGACIONES, "Erogaciones corrientes · Bienes y servicios no personales"),
    "erog_corr_int_deu_int": (EROGACIONES, "Erogaciones corrientes · Intereses de deudas internas"),
    "erog_corr_int_deu_ext_prop": (EROGACIONES, "Erogaciones corrientes · Intereses de deudas externas propias"),
    "erog_corr_transctescap_pasiv": (EROGACIONES, f"{_TRANSFERENCIAS} · Pasividades"),
    "erog_corr_transctescap_enspriv": (EROGACIONES, f"{_TRANSFERENCIAS} · Enseñanza privada"),
    "erog_corr_transctescap_otras": (EROGACIONES, f"{_TRANSFERENCIAS} · Otras"),
    "erog_corr_desq_cteemppubl": (EROGACIONES, "Erogaciones corrientes · Desequilibrio corriente de empresas públicas"),
    "erog_cap_total_erog_capital": (EROGACIONES, "Erogaciones de capital"),
    "erog_cap_inv_real_bscapital": (EROGACIONES, "Erogaciones de capital · Inversión real · Bienes de capital"),
    "erog_cap_inv_real_constr": (EROGACIONES, "Erogaciones de capital · Inversión real · Construcciones"),
    "erog_cap_inv_bspreexist": (EROGACIONES, "Erogaciones de capital · Inversión en bienes preexistentes"),
    "erog_cap_inv_financ": (EROGACIONES, "Erogaciones de capital · Inversión financiera"),
    "total_erogaciones": (EROGACIONES, "Erogaciones totales"),
    "erog_figurativas": (EROGACIONES, "Erogaciones figurativas"),
    "total_erog_desp_figur": (EROGACIONES, f"Erogaciones totales después de {_FIGURATIVAS}"),
    "total_erog_prim_desp_figur": (EROGACIONES, f"Erogaciones primarias después de {_FIGURATIVAS}"),
    # Resultados
    "ahorro": (RESULTADOS, "Ahorro corriente"),
    "resultado": (RESULTADOS, "Resultado financiero"),
    "superavit_prim_total": (RESULTADOS, "Superávit primario"),
    "superavit_prim_sin_rec_cap": (RESULTADOS, "Superávit primario sin recursos de capital"),
    "neces_finan": (RESULTADOS, "Necesidad de financiamiento"),
    "neces_finan_antes_figur": (RESULTADOS, f"Necesidad de financiamiento antes de {_FIGURATIVAS}"),
    # Financiamiento
    "fin_neto_total_finan_neto": (FINANCIAMIENTO, "Financiamiento neto"),
    "fin_neto_usonetocr_int_usocred": (FINANCIAMIENTO, "Uso neto del crédito interno · Uso del crédito"),
    "fin_neto_usonetocr_int_amor": (FINANCIAMIENTO, "Uso neto del crédito interno · Amortización"),
    "fin_neto_usonetocr_ext_usocred": (FINANCIAMIENTO, "Uso neto del crédito externo · Uso del crédito"),
    "fin_neto_usonetocr_ext_amor": (FINANCIAMIENTO, "Uso neto del crédito externo · Amortización"),
    "fin_neto_usonetoad_provcont": (FINANCIAMIENTO, "Adelantos a proveedores y contratistas"),
    "fin_neto_usonetoad_provcontejeant": (FINANCIAMIENTO, "Adelantos a proveedores y contratistas del ejercicio anterior"),
    "finan_contrib_figur_ac": (FINANCIAMIENTO, "Contribuciones figurativas · De administración central"),
    "finan_contrib_figur_ce": (FINANCIAMIENTO, "Contribuciones figurativas · De cuentas especiales"),
    "finan_contrib_figur_od": (FINANCIAMIENTO, "Contribuciones figurativas · De organismos descentralizados"),
    "finan_emerg_econ": (FINANCIAMIENTO, "Financiamiento por emergencia económica"),
    "aum_pas_fin_netos": (FINANCIAMIENTO, "Aumento de pasivos financieros netos"),
    "bco_ctral_rep_arg": (FINANCIAMIENTO, "Financiamiento del Banco Central de la República Argentina"),
}


def _aif_devengado(serie: Serie) -> Clasificacion | None:
    subsector = _SUBSECTORES[serie.hoja]
    definicion = _PARTIDAS_AIF.get(serie.variable)
    if definicion is None:
        return None
    bloque, partida = definicion
    nombre = partida if subsector == _SECTOR_PUBLICO_ARGENTINO else unir(partida, subsector)
    return _clasificacion(AIF_DEVENGADO, subsector, bloque, nombre, MILLONES_DE_PESOS)


def _resultado_1993_2018(serie: Serie) -> Clasificacion:
    # Una sola serie: «Resultado financiero devengado (1993-2018)» del Sector Público Argentino,
    # la continuación del «Resultado» de la hoja SPA_Dev_61.
    return _clasificacion(AIF_DEVENGADO, _SECTOR_PUBLICO_ARGENTINO, RESULTADOS,
                          "Resultado financiero", MILLONES_DE_PESOS)


# ---------------------------------------------------------------------------
# Gasto público consolidado, por nivel de gobierno, finalidad y función
# ---------------------------------------------------------------------------

GASTO_PUBLICO = "Gasto público consolidado"
_NOMBRE_GASTO = "Gasto público"

_NIVELES_DE_GOBIERNO = {
    # letra de la hoja: (grupo del árbol, cobertura en el nombre de la serie)
    "C": ("Consolidado", ""),
    "N": ("Nación", "Nación"),
    "P": ("Provincias y Ciudad Autónoma de Buenos Aires", "Provincias y Ciudad Autónoma de Buenos Aires"),
    "M": ("Municipios", "Municipios"),
}

FUNCIONAMIENTO = "Funcionamiento del Estado"
SERVICIOS_SOCIALES = "Servicios sociales"
SERVICIOS_ECONOMICOS = "Servicios económicos"
DEUDA = "Servicios de la deuda pública"

# Variable (sin prefijo ni sufijo «_pib»): (finalidad, nombre). Cuando la finalidad y el nombre
# coinciden, la serie es el subtotal de la finalidad. Verificado con los datos: el total es la suma
# de las cuatro finalidades y cada finalidad es la suma de sus funciones.
_FUNCIONES = {
    "funcionamiento_estado": (FUNCIONAMIENTO, FUNCIONAMIENTO),
    "administracion_general": (FUNCIONAMIENTO, "Administración general"),
    "justicia": (FUNCIONAMIENTO, "Justicia"),
    "defensa_seguridad": (FUNCIONAMIENTO, "Defensa y seguridad"),
    "servicios_sociales": (SERVICIOS_SOCIALES, SERVICIOS_SOCIALES),
    "educacion_basica": (SERVICIOS_SOCIALES, "Educación básica"),
    "educacion_universitaria": (SERVICIOS_SOCIALES, "Educación superior y universitaria"),
    "educacion_cultura_sin_discriminar": (SERVICIOS_SOCIALES, "Educación y cultura sin discriminar"),
    "cultura": (SERVICIOS_SOCIALES, "Cultura"),
    "ciencia_tecnica": (SERVICIOS_SOCIALES, "Ciencia y técnica"),
    "atencion_salud": (SERVICIOS_SOCIALES, "Atención pública de la salud"),
    "obras_sociales_salud": (SERVICIOS_SOCIALES, "Atención de la salud de las obras sociales"),
    "inssjyp_salud": (SERVICIOS_SOCIALES, "Atención de la salud del INSSJyP"),
    "agua_alcantarillado": (SERVICIOS_SOCIALES, "Agua potable y alcantarillado"),
    "vivienda_urbanismo": (SERVICIOS_SOCIALES, "Vivienda y urbanismo"),
    "otros_servicios_urbanos": (SERVICIOS_SOCIALES, "Otros servicios urbanos"),
    "promocion_asistencia_social": (SERVICIOS_SOCIALES, "Promoción y asistencia social pública"),
    "prevision_social": (SERVICIOS_SOCIALES, "Previsión social"),
    "obras_sociales_prestaciones": (SERVICIOS_SOCIALES, "Prestaciones sociales de las obras sociales"),
    "inssjyp_prestaciones": (SERVICIOS_SOCIALES, "Prestaciones sociales del INSSJyP"),
    "asignaciones_familiares": (SERVICIOS_SOCIALES, "Asignaciones familiares"),
    "programas_empleo": (SERVICIOS_SOCIALES, "Programas de empleo y seguros de empleo"),
    "servicios_economicos": (SERVICIOS_ECONOMICOS, SERVICIOS_ECONOMICOS),
    "energia_combustible": (SERVICIOS_ECONOMICOS, "Energía y combustible"),
    "comunicaciones": (SERVICIOS_ECONOMICOS, "Comunicaciones"),
    "transporte": (SERVICIOS_ECONOMICOS, "Transporte"),
    "industria": (SERVICIOS_ECONOMICOS, "Industria"),
    "produccion_primaria": (SERVICIOS_ECONOMICOS, "Producción primaria"),
    "otros_servicios_economicos": (SERVICIOS_ECONOMICOS, "Otros gastos en servicios económicos"),
    "servicios_deuda_publica": (DEUDA, DEUDA),
}

_HOJA_GASTO = re.compile(r"^GP([CNPM])_([$%])$")


def _funcion_del_gasto(variable: str, letra: str) -> str | None:
    """Clave de la función en ``_FUNCIONES`` («» para el total), o None si la variable no corresponde."""
    prefijo = f"gp{letra.lower()}"
    if variable == prefijo:
        return ""
    if not variable.startswith(prefijo + "_"):
        return None
    resto = variable[len(prefijo) + 1:]
    if resto == "pib":
        return ""
    return resto.removesuffix("_pib")


def _gasto_publico(serie: Serie) -> Clasificacion | None:
    encontrado = _HOJA_GASTO.match(serie.hoja)
    if encontrado is None:
        return None
    letra, medida = encontrado.groups()
    porcentaje = medida == "%"
    funcion = _funcion_del_gasto(serie.variable, letra)
    if funcion is None:
        return None
    grupo, cobertura = _NIVELES_DE_GOBIERNO[letra]
    if funcion == "":
        finalidad, apertura = "", TOTAL
    else:
        definicion = _FUNCIONES.get(funcion)
        if definicion is None:
            return None
        finalidad, apertura = definicion
    nombre = con_medida(unir(_NOMBRE_GASTO, apertura, cobertura), DEL_PIB if porcentaje else "")
    return _clasificacion(GASTO_PUBLICO, grupo, finalidad, nombre,
                          PORCENTAJE_DEL_PIB if porcentaje else MILLONES_DE_PESOS)


# ---------------------------------------------------------------------------
# Gasto en política de ingresos (Gobierno nacional, devengado, por programa)
# ---------------------------------------------------------------------------

POLITICA_DE_INGRESOS = "Gasto en política de ingresos"

# Tipos de prestación o programa; los mismos grupos que usa «Prestaciones de la seguridad social».
JUBILACIONES_Y_PENSIONES = "Jubilaciones y pensiones"
PENSIONES_NO_CONTRIBUTIVAS = "Pensiones no contributivas"
SEGURO_DE_DESEMPLEO = "Seguro de desempleo"
ASIGNACIONES = "Asignaciones familiares y universales"
BECAS = "Becas y programas para estudiantes"
EMPLEO = "Programas de empleo e inclusión social"

# Variable: (grupo, programa). Los nombres de los programas son los de la fuente, sin siglas
# crípticas ni llamadas al pie.
_PROGRAMAS = {
    "total_politica_ingresos": (TOTAL, ""),
    "jubilaciones_pensiones_sist_int_previsional_argentino":
        (JUBILACIONES_Y_PENSIONES, "Jubilaciones y pensiones del SIPA"),
    "jubilaciones_pensiones_otros_regimenes_nacionales":
        (JUBILACIONES_Y_PENSIONES, "Jubilaciones y pensiones de otros regímenes nacionales"),
    "pension_universal_adulto_mayor": (JUBILACIONES_Y_PENSIONES, "Pensión Universal para el Adulto Mayor"),
    "pensiones_no_contributivas":
        (PENSIONES_NO_CONTRIBUTIVAS, "Pensiones no contributivas"),
    "asignaciones_familiares": (ASIGNACIONES, "Asignaciones familiares"),
    "asignacion_universal_hijo_proteccion_social":
        (ASIGNACIONES, "Asignación Universal por Hijo para Protección Social"),
    "seguro_desempleo": (SEGURO_DE_DESEMPLEO, "Seguro por desempleo"),
    "seguro_desempleo_renatre": (SEGURO_DE_DESEMPLEO, "Seguro por desempleo del RENATRE"),
    "becas": (BECAS, "Becas por condición de vulnerabilidad"),
    "programa_respaldo_estudiantes_argentina": (BECAS, "PROGRESAR"),
    "argentina_trabaja": (EMPLEO, "Argentina Trabaja"),
    "jovenes_mas_mejor_trabajo": (EMPLEO, "Programa Jóvenes con Más y Mejor Trabajo"),
    "programa_jefas_jefes_hogar_desocupados": (EMPLEO, "Programa Jefas y Jefes de Hogar Desocupados"),
    "plan_familias_inclusion_social": (EMPLEO, "Plan Familias por la Inclusión Social"),
    "proyectos_productivos_comunitarios": (EMPLEO, "Proyectos Productivos Comunitarios"),
    "seguro_capacitacion_empleo": (EMPLEO, "Seguro de capacitación y empleo"),
    "otras_politicas_empleo_minist_trabajo_empleo_seg_soc":
        (EMPLEO, "Otras políticas de empleo del Ministerio de Trabajo, Empleo y Seguridad Social"),
}


def _politica_de_ingresos(serie: Serie) -> Clasificacion | None:
    definicion = _PROGRAMAS.get(serie.variable)
    if definicion is None:
        return None
    grupo, programa = definicion
    nombre = unir(POLITICA_DE_INGRESOS, programa or TOTAL)
    return _clasificacion(POLITICA_DE_INGRESOS, grupo, "", nombre, MILLONES_DE_PESOS)


# ---------------------------------------------------------------------------
# Prestaciones de la seguridad social (cantidad de beneficios o de beneficiarios)
# ---------------------------------------------------------------------------

PRESTACIONES = "Prestaciones de la seguridad social"

_BENEFICIOS = "cantidad de beneficios"
_BENEFICIARIOS = "cantidad de beneficiarios"
_PERSONAS = "cantidad de personas"

POR_TIPO_DE_PENSION = "Por tipo de pensión"
POR_JURISDICCION = "Por jurisdicción"
ASIGNACION_POR_HIJO_FAMILIAR = "Asignaciones familiares por hijo"
ASIGNACION_UNIVERSAL_POR_HIJO = "Asignación Universal por Hijo"
ASIGNACION_UNIVERSAL_POR_EMBARAZO = "Asignación Universal por Embarazo"
ASIGNACION_PRENATAL = "Asignación familiar prenatal"

_UNIDAD_DE = {_BENEFICIOS: BENEFICIOS, _BENEFICIARIOS: BENEFICIARIOS, _PERSONAS: PERSONAS}


def _prestacion(grupo: str, apertura: str, partes: tuple[str, ...], medida: str = _BENEFICIOS) -> Clasificacion:
    return _clasificacion(PRESTACIONES, grupo, apertura, con_medida(unir(*partes), medida), _UNIDAD_DE[medida])


_JUBILACIONES = JUBILACIONES_Y_PENSIONES
_SIPA = "SIPA"
_OTROS_REGIMENES = "Otros regímenes nacionales"

# Clave común a las publicaciones de jubilaciones y pensiones del SIPA (BESS y ANSES).
_CONCEPTOS_PREVISIONALES = {
    "jubilaciones": _prestacion(_JUBILACIONES, "", (_JUBILACIONES, "Jubilaciones", _SIPA)),
    "pensiones": _prestacion(_JUBILACIONES, "", (_JUBILACIONES, "Pensiones", _SIPA)),
    "total": _prestacion(_JUBILACIONES, "", (_JUBILACIONES, TOTAL, _SIPA)),
}

_PNC = PENSIONES_NO_CONTRIBUTIVAS
_TIPOS_DE_PNC = {
    "asistenciales_invalidez": "Asistenciales por invalidez",
    "asistenciales_vejez": "Asistenciales por vejez",
    "asistenciales_madre_7_hijos": "Asistenciales a madres de 7 o más hijos",
    "graciables": "Graciables",
    "leyes_especiales": "Por leyes especiales",
    "ex_combatientes": "A ex combatientes",
}
_JURISDICCIONES_PNC = {
    "buenos_aires": "Buenos Aires", "caba": "Ciudad Autónoma de Buenos Aires",
    "catamarca": "Catamarca", "chaco": "Chaco", "chubut": "Chubut", "cordoba": "Córdoba",
    "corrientes": "Corrientes", "entre_rios": "Entre Ríos", "formosa": "Formosa", "jujuy": "Jujuy",
    "la_pampa": "La Pampa", "la_rioja": "La Rioja", "mendoza": "Mendoza", "misiones": "Misiones",
    "neuquen": "Neuquén", "rio_negro": "Río Negro", "salta": "Salta", "san_juan": "San Juan",
    "san_luis": "San Luis", "santa_cruz": "Santa Cruz", "santa_fe": "Santa Fe",
    "santiago_estero": "Santiago del Estero", "tierra_fuego": "Tierra del Fuego",
    "tucuman": "Tucumán", "sin_especificar": "Sin especificar",
}
_PREFIJO_PNC = "pensiones_no_contributivas_"


def _bess(serie: Serie) -> Clasificacion | None:
    """Cuadro «Jubilaciones, PNC y seguro de desempleo» del Boletín Estadístico de la Seguridad Social."""
    variable = serie.variable
    if variable == "beneficiarios_sistema_integral_prestaciones_desempleo":
        return _prestacion(SEGURO_DE_DESEMPLEO, "", (SEGURO_DE_DESEMPLEO,), _BENEFICIARIOS)
    if variable == "jubilaciones_sistema_intregrado_previsional_argentino":
        return _CONCEPTOS_PREVISIONALES["jubilaciones"]
    if variable == "pensiones_sistema_integrado_previsional_argentino":
        return _CONCEPTOS_PREVISIONALES["pensiones"]
    if variable == "total_jubilaciones_pensiones_sistema_previsional_argentino":
        return _CONCEPTOS_PREVISIONALES["total"]
    if variable == "total_pensiones_no_contributivas":
        return _prestacion(_PNC, "", (_PNC, TOTAL))
    if variable.startswith(_PREFIJO_PNC):
        clave = variable[len(_PREFIJO_PNC):]
        if clave in _TIPOS_DE_PNC:
            return _prestacion(_PNC, POR_TIPO_DE_PENSION, (_PNC, _TIPOS_DE_PNC[clave]))
        if clave in _JURISDICCIONES_PNC:
            return _prestacion(_PNC, POR_JURISDICCION, (_PNC, TOTAL, _JURISDICCIONES_PNC[clave]))
    return None


def _auh_aue(serie: Serie) -> Clasificacion | None:
    """Hoja «AUH-AUE»: la AUH y la AUE del BESS y la AUH mensual desde agosto de 2019."""
    if serie.variable == "aue_asignacion_embarazo":
        return _prestacion(ASIGNACIONES, ASIGNACION_UNIVERSAL_POR_EMBARAZO, (ASIGNACION_UNIVERSAL_POR_EMBARAZO,))
    if serie.variable in ("auh_asignacion_universal_hijo_hijo_discapacitado", "AUH (Agosto 19 - Hoy)"):
        return _prestacion(ASIGNACIONES, ASIGNACION_UNIVERSAL_POR_HIJO, (ASIGNACION_UNIVERSAL_POR_HIJO, TOTAL))
    return None


# Hoja «FUENTE ANSES PRESTACIONES». Las publicaciones «Transparencia» y «Abiertos» de ANSES
# miden lo mismo (se solapan en 2013-2015), por eso comparten nombre.
_AAFF = ASIGNACION_POR_HIJO_FAMILIAR
_AUH = ASIGNACION_UNIVERSAL_POR_HIJO
_AUE = ASIGNACION_UNIVERSAL_POR_EMBARAZO
_ANSES = {
    # Asignaciones familiares por hijo (o hijo con discapacidad)
    "asignacion_familiar_hijo_hijo_discapacitado": (_AAFF, (_AAFF, TOTAL), _BENEFICIOS),
    "asignacion_familiar_hijo_discapacitado": (_AAFF, (_AAFF, TOTAL), _BENEFICIOS),
    "total_aaff": (_AAFF, (_AAFF, TOTAL), _BENEFICIOS),
    "asignaciones_familiares_suaf_relacion_dependencia":
        (_AAFF, (_AAFF, "Adulto en relación de dependencia"), _BENEFICIOS),
    "asignaciones_familiares_suaf_monotributo": (_AAFF, (_AAFF, "Adulto monotributista"), _BENEFICIOS),
    "asignaciones_familiares_pasivos_sipa": (_AAFF, (_AAFF, "Adulto pasivo del SIPA"), _BENEFICIOS),
    "asignaciones_familiares_pasivos_pension_no_contributiva":
        (_AAFF, (_AAFF, "Adulto pasivo con pensión no contributiva"), _BENEFICIOS),
    "asignaciones_familiares_pasivos_ex_combatientes":
        (_AAFF, (_AAFF, "Adulto pasivo ex combatiente"), _BENEFICIOS),
    "asignaciones_familiares_desempleo": (_AAFF, (_AAFF, "Adulto en situación de desempleo"), _BENEFICIOS),
    # Asignación familiar prenatal
    "asignacion_familiar_prenatal": (ASIGNACION_PRENATAL, (ASIGNACION_PRENATAL,), _BENEFICIOS),
    "asignacion_familiar_prenatal_datos_abiertos": (ASIGNACION_PRENATAL, (ASIGNACION_PRENATAL,), _BENEFICIOS),
    # Asignación Universal por Hijo
    "asignacion_universal_hijo_hijo_discapacitado_trasparencia": (_AUH, (_AUH, TOTAL), _BENEFICIOS),
    "asignacion_universal_hijo_hijo_discapacitado_datos_abiertos": (_AUH, (_AUH, TOTAL), _BENEFICIOS),
    "total_auh": (_AUH, (_AUH, TOTAL), _BENEFICIOS),
    "asignacion_universal_hijo_monotributo_social":
        (_AUH, (_AUH, "Adulto con monotributo social"), _BENEFICIOS),
    "asignacion_universal_hijo_servicio_domestico":
        (_AUH, (_AUH, "Adulto en servicio doméstico"), _BENEFICIOS),
    "asignacion_universal_hijo_monotributo_soc_servicio_domestico":
        (_AUH, (_AUH, "Adulto con monotributo social o en servicio doméstico"), _BENEFICIOS),
    "asignacion_universal_hijo_otra_condicion":
        (_AUH, (_AUH, "Adulto con otra condición"), _BENEFICIOS),
    # Asignación Universal por Embarazo
    "asignacion_universal_embarazo": (_AUE, (_AUE,), _BENEFICIOS),
    "asignacion_universal_por_embarazo": (_AUE, (_AUE,), _BENEFICIOS),
}


def _anses(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if variable in _ANSES:
        apertura, partes, medida = _ANSES[variable]
        return _prestacion(ASIGNACIONES, apertura, partes, medida)
    if variable == "total_aaff_auh":
        return _prestacion(ASIGNACIONES, "", (ASIGNACIONES, TOTAL))
    if variable == "ninios_adp":
        return _prestacion(ASIGNACIONES, "",
                           (ASIGNACIONES, "Menores de 18 años en el Administrador de Personas"), _PERSONAS)
    if variable in ("jubilaciones_sipa_transparencia", "jubilaciones_sipa_datos_abiertos"):
        return _CONCEPTOS_PREVISIONALES["jubilaciones"]
    if variable in ("pensiones_sipa_transparencia", "pensiones_sipa_datos_abiertos"):
        return _CONCEPTOS_PREVISIONALES["pensiones"]
    if variable in ("jubilaciones_pensiones_sipa", "total_jubilaciones_pensiones_sipa_datos_abiertos"):
        return _CONCEPTOS_PREVISIONALES["total"]
    if variable == "pension_universal_adulto_mayor":
        return _prestacion(_JUBILACIONES, "", (_JUBILACIONES, "Pensión Universal para el Adulto Mayor"))
    if variable == "prestaciones_seguro_desempleo":
        return _prestacion(SEGURO_DE_DESEMPLEO, "", (SEGURO_DE_DESEMPLEO,))
    if variable == "titulares_progresar":
        return _prestacion(BECAS, "", ("PROGRESAR",), _BENEFICIARIOS)
    return None


def _cuenta_de_inversion(serie: Serie) -> Clasificacion | None:
    """Hoja «Fuente Cuenta Inversion- Presta»: prestaciones de la Cuenta de Inversión."""
    if serie.variable == "jubilaciones_pensiones_retiros_otros_regimenes_nacionales":
        return _prestacion(_JUBILACIONES, "", (_JUBILACIONES, TOTAL, _OTROS_REGIMENES))
    if serie.variable == "becas_estudiantiles":
        return _prestacion(BECAS, "", ("Becas estudiantiles",))
    return None


def _progresar(serie: Serie) -> Clasificacion:
    return _prestacion(BECAS, "", ("PROGRESAR",), _BENEFICIARIOS)


# ---------------------------------------------------------------------------
# Recaudación tributaria
# ---------------------------------------------------------------------------

RECAUDACION = "Recaudación tributaria"
_RECAUDACION = "Recaudación"

TOTALES = "Total"
POR_DIRECCION = "Por dirección general"
POR_TRIBUTO = "Por tributo"
SERIES_HISTORICAS = "Series históricas"

GANANCIAS = "Ganancias"
IVA = "IVA"
SEGURIDAD_SOCIAL = "Seguridad social"
COMERCIO_EXTERIOR = "Comercio exterior"
COMBUSTIBLES = "Combustibles"
OTROS_IMPUESTOS = "Otros impuestos"
AJUSTES = "Devoluciones, reintegros y otros conceptos"

_DGI = "Dirección General Impositiva"
_DGA = "Dirección General de Aduanas"

# Variable de la hoja RECA (cuadro «Recursos tributarios totales y por tributo», pesos corrientes):
# (apertura del árbol, nombre). Las denominaciones son las de AFIP/ARCA. Las variables
# «faciliades_de_pago» y la descripción alternativa de «intereses_pagados» son variantes de la
# fuente en la serie mensual.
_TRIBUTOS = {
    "total_recursos_tributarios": (TOTALES, (TOTAL,)),
    "total_con_capitalizacion_transitorios": (TOTALES, ("Total con capitalización y transitorios",)),
    "ganancias": (GANANCIAS, (GANANCIAS,)),
    "ganancias_dgi": (GANANCIAS, (GANANCIAS, _DGI)),
    "ganancias_dga": (GANANCIAS, (GANANCIAS, _DGA)),
    "iva": (IVA, (IVA,)),
    "iva_dgi": (IVA, (IVA, _DGI)),
    "iva_dga": (IVA, (IVA, _DGA)),
    "sistema_seguridad_social": (SEGURIDAD_SOCIAL, (SEGURIDAD_SOCIAL,)),
    "aportes_personales": (SEGURIDAD_SOCIAL, ("Aportes personales",)),
    "contribuciones_patronales": (SEGURIDAD_SOCIAL, ("Contribuciones patronales",)),
    "otros_ingresos_seguridad_social": (SEGURIDAD_SOCIAL, ("Otros ingresos de la seguridad social",)),
    "otros_sipa": (SEGURIDAD_SOCIAL, ("Otros del SIPA",)),
    "sistema_capitalizacion": (SEGURIDAD_SOCIAL, ("Sistema de capitalización",)),
    "derechos_exportacion": (COMERCIO_EXTERIOR, ("Derechos de exportación",)),
    "derechos_importacion_otros": (COMERCIO_EXTERIOR, ("Derechos de importación y otros",)),
    "total_combustibles": (COMBUSTIBLES, (COMBUSTIBLES,)),
    "combustibles_ley_23966_naftas": (COMBUSTIBLES, (COMBUSTIBLES, "Ley 23.966", "Naftas")),
    "combustibles_ley_23966_otros": (COMBUSTIBLES, (COMBUSTIBLES, "Ley 23.966", "Otros")),
    "otros_s_combustibles": (COMBUSTIBLES, (COMBUSTIBLES, "Otros sobre combustibles")),
    "bienes_personales": (OTROS_IMPUESTOS, ("Bienes personales",)),
    "ganancia_minima_presunta": (OTROS_IMPUESTOS, ("Ganancia mínima presunta",)),
    "creditos_debitos_cta_cte": (OTROS_IMPUESTOS, ("Créditos y débitos en cuenta corriente",)),
    "internos_coparticipados": (OTROS_IMPUESTOS, ("Impuestos internos coparticipados",)),
    "otros_coparticipados": (OTROS_IMPUESTOS, ("Otros impuestos coparticipados",)),
    "impuesto_pais": (OTROS_IMPUESTOS, ("Impuesto PAIS",)),
    "otros_impuestos": (OTROS_IMPUESTOS, (OTROS_IMPUESTOS,)),
    "devoluciones": (AJUSTES, ("Devoluciones",)),
    "reintegros": (AJUSTES, ("Reintegros",)),
    "facilidades_pago": (AJUSTES, ("Facilidades de pago",)),
    "faciliades_de_pago": (AJUSTES, ("Facilidades de pago",)),
    "intereses_pagados": (AJUSTES, ("Intereses pagados",)),
}


def _recaudacion_clasificacion(apertura: str, partes: tuple[str, ...], medida: str, unidad: str) -> Clasificacion:
    nombre = con_medida(unir(_RECAUDACION, *partes), medida)
    if apertura in (TOTALES, POR_DIRECCION):
        return _clasificacion(RECAUDACION, apertura, "", nombre, unidad)
    return _clasificacion(RECAUDACION, POR_TRIBUTO, apertura, nombre, unidad)


def _reca(serie: Serie) -> Clasificacion | None:
    definicion = _TRIBUTOS.get(serie.variable)
    if definicion is None:
        return None
    apertura, partes = definicion
    return _recaudacion_clasificacion(apertura, partes, "", MILLONES_DE_PESOS)


# Hoja RECA_SUB: recaudación por dirección general (el total y la seguridad social son los mismos
# datos que «Total recursos tributarios» y «Sistema seguridad social» de la hoja RECA) y su variación.
_SUBGRUPOS = {
    "total_recaudacion": (TOTALES, (TOTAL,)),
    "subtotal_dgi": (POR_DIRECCION, (_DGI,)),
    "subtotal_dga": (POR_DIRECCION, (_DGA,)),
    "seguridad_social": (SEGURIDAD_SOCIAL, (SEGURIDAD_SOCIAL,)),
}


def _reca_sub(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    variacion = variable.endswith("_var_ia")
    definicion = _SUBGRUPOS.get(variable.removesuffix("_var_ia"))
    if definicion is None:
        return None
    apertura, partes = definicion
    return _recaudacion_clasificacion(apertura, partes, INTERANUAL if variacion else "",
                                      PROPORCION if variacion else MILLONES_DE_PESOS)


def _reca_historica(serie: Serie) -> Clasificacion:
    # «Recaudación historia impuestos (1932-1969)»: total de la recaudación de impuestos nacionales,
    # en millones de pesos moneda nacional (la unidad vigente hasta 1969).
    return _clasificacion(RECAUDACION, SERIES_HISTORICAS, "", unir(_RECAUDACION, TOTAL),
                          "Millones de pesos moneda nacional")


# ---------------------------------------------------------------------------

_POR_HOJA = {hoja: _aif_devengado for hoja in _SUBSECTORES}
_POR_HOJA.update({
    "SPA_Dev_93": _resultado_1993_2018,
    "Gasto Pol. Ing": _politica_de_ingresos,
    "JUB, PNC; SEG DES.": _bess,
    "AUH-AUE": _auh_aue,
    "FUENTE ANSES PRESTACIONES": _anses,
    "Fuente Cuenta Inversion- Presta": _cuenta_de_inversion,
    "PROGRESAR": _progresar,
    "RECA": _reca,
    "RECA_SUB": _reca_sub,
    "RECA_HISTORICA": _reca_historica,
})
_POR_HOJA.update({f"GP{letra}_{medida}": _gasto_publico for letra in "CNPM" for medida in ("$", "%")})


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.archivo != ARCHIVO:
        return None
    regla = _POR_HOJA.get(serie.hoja)
    return regla(serie) if regla else None
