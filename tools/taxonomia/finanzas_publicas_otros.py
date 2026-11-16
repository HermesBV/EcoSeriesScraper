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
* Dos publicaciones de la misma estadística (ANSES-Transparencia y ANSES-Abiertos, más el Boletín
  mensual de AUH) llevan el mismo nombre porque miden lo mismo. En el solapamiento (2013-01 a 2015-06)
  jubilaciones, pensiones y su total coinciden dentro del 0,1 %; las asignaciones (AUH, AAFF,
  AUE, prenatal) difieren entre 1 % y 6 % en mediana y hasta 30 % en meses sueltos, y la AUH y las AAFF
  de Abiertos van corridas uno o dos meses respecto del Boletín. No son copias: no se excluye ninguna.
  Las series de Transparencia traen ceros de relleno desde 2015-07 y las del Boletín antes de 2014.
* Auditoría: la unidad de la cuenta devengado 1961-1992 y de los gastos en pesos de 1980-1986 se
  corrige con ``UNIDADES`` (ver más abajo); SPA_Dev_93 queda como continuación del «Resultado
  financiero» del Sector Público Argentino sin haberse podido confirmar que tenga el mismo perímetro
  (no figura en la API y el libro original no se pudo bajar).
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
# Auditoría (contraste con los informes mensuales de ARCA de diciembre de 2023 y 2024: total,
# IVA, Ganancias, Seguridad social, Derechos de exportación y de importación, Créditos y débitos,
# Combustibles, PAIS, aportes, contribuciones y «Otros SIPA» coinciden al millón):
# * «Otros SIPA» es la denominación de ARCA («Otros SIPA (-)»): se resta de la seguridad social
#   (Seguridad social = Aportes + Contribuciones + Otros ingresos − Otros SIPA, desde 2009).
# * «devoluciones» son las devoluciones de IVA: IVA = IVA DGI + IVA DGA − Devoluciones en los
#   357 meses (diferencia nula), y ARCA las muestra bajo el IVA. Por eso cuelgan del IVA.
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
    "otros_sipa": (SEGURIDAD_SOCIAL, ("Otros SIPA",)),
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
    "devoluciones": (IVA, (IVA, "Devoluciones")),
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
# Unidades corregidas (auditoría de noviembre de 2026)
# ---------------------------------------------------------------------------
#
# Cuenta devengado 1961-1992: el inventario decía «Millones de pesos» para los cuatro tramos, pero
# cada uno está en la moneda y la escala de su época. Evidencia:
#   1. Los metadatos de la API de series de tiempo (descripción de cada distribución) dicen
#      «1961-1976 en Australes», «1977-1986 en miles de Australes», «1987-1989 en miles de Pesos» y
#      «1990-1992 en millones de Pesos» (ver nota sobre SPA_Dev_61 más abajo: sus títulos están corridos,
#      pero la descripción trae el período y la unidad correctos).
#   2. Los valores: la razón entre el total de erogaciones del Sector Público Argentino y el gasto
#      nacional de la hoja GPN_$ (millones de pesos desde 1987) es 12.000 en 1980-1984, 11 en
#      1985-1986, 1.100 en 1987-1989 y 1,2 en 1990-1992; y las erogaciones de los ocho subsectores se
#      multiplican por 0,0025 en 1977, 0,0073 en 1985, 0,24 en 1987 y 0,019 en 1990 mientras que la
#      inflación daba 3, 7, 2,4 y 24: saltos de 1.000, 1.000, 10 y 1.000, que son exactamente los
#      cambios de moneda (1 austral = 1.000 pesos argentinos; 1 peso = 10.000 australes).
#   3. Hallazgo: dentro del tramo 1977-1986 hay un corte en 1985 (los valores de 1985-1986 están en
#      millones de australes, 1.000 veces más grandes que los de 1977-1984 en miles de australes),
#      aunque la fuente rotula todo el tramo «miles de Australes».
# La regla no distingue el tramo; sí lo hace el ID (``<dataset>.<tramo>_…``).
#
# Gasto público consolidado en pesos (GPC_$, GPN_$, GPP_$, GPM_$): 1980-1986 está en miles de pesos
# y 1987-2023 en millones (la API y el inventario dicen «millones» para todo). Evidencia: el PIB
# implícito (gasto en pesos / gasto en % del PIB) pasa de 10.168 en 1986 a 23,6 en 1987 cuando el
# crecimiento nominal fue 2,4 (factor 1.000), y desde 1993 coincide con el PIB corriente del INDEC
# (236.505 millones en 1993); y el cociente con la cuenta devengado de arriba cambia de la misma manera.

_IDS_TRAMO_1961_1976 = (
    "432.1_AHORRORRO_DEVE_0_6_6", "432.1_AUM_PAS_FITOS_DEVE_0_17_9", "432.1_BCO_CTRAL_ARG_DEVE_0_17_56",
    "432.1_EROG_CAP_IANC_DEVE_0_19_78", "432.1_EROG_CAP_IIST_DEVE_0_23_97",
    "432.1_EROG_CAP_ISTR_DEVE_0_24_18", "432.1_EROG_CAP_ITAL_DEVE_0_27_83",
    "432.1_EROG_CAP_TTAL_DEVE_0_27_32", "432.1_EROG_CORR_INT_DEVE_0_21_19",
    "432.1_EROG_CORR_NAL_DEVE_0_18_59", "432.1_EROG_CORR_ORR_DEVE_0_25_92",
    "432.1_EROG_CORR_PER_DEVE_0_19_26", "432.1_EROG_CORR_RAS_DEVE_0_28_10",
    "432.1_EROG_CORR_RIV_DEVE_0_30_16", "432.1_EROG_CORR_ROP_DEVE_0_26_81",
    "432.1_EROG_CORR_SIV_DEVE_0_28_77", "432.1_EROG_FIGURVAS_DEVE_0_16_52",
    "432.1_FINAN_EMERCON_DEVE_0_16_27", "432.1_FIN_NETO_TETO_DEVE_0_25_47",
    "432.1_FIN_NETO_UMOR_DEVE_0_27_76", "432.1_FIN_NETO_UMOR_DEVE_0_27_83",
    "432.1_FIN_NETO_URED_DEVE_0_30_91", "432.1_FIN_NETO_URED_DEVE_0_30_98",
    "432.1_NECES_FINAGUR_DEVE_0_23_99", "432.1_NECES_FINANAN_DEVE_0_11_98",
    "432.1_RECURSOS_CTAL_DEVE_0_16_48", "432.1_REC_CORR_IBUT_DEVE_0_20_16",
    "432.1_REC_CORR_IBUT_DEVE_0_22_91", "432.1_REC_CORR_TORR_DEVE_0_23_6", "432.1_RESULTADOADO_DEVE_0_9_73",
    "432.1_SUPERAVIT_CAP_DEVE_0_26_57", "432.1_SUPERAVIT_TAL_DEVE_0_20_100",
    "432.1_TOTAL_EROGGUR_DEVE_0_21_11", "432.1_TOTAL_EROGGUR_DEVE_0_26_14",
    "432.1_TOTAL_EROGNES_DEVE_0_17_38", "432.1_TOTAL_RECUSOS_DEVE_0_14_60",
    "432.1_TOTAL_REC_GUR_DEVE_0_20_6", "433.1_AHORRORRO_DEVE_0_6_43", "433.1_AUM_PAS_FITOS_DEVE_0_17_38",
    "433.1_EROG_CAP_IANC_DEVE_0_19_55", "433.1_EROG_CAP_ISTR_DEVE_0_24_31",
    "433.1_EROG_CAP_ITAL_DEVE_0_27_17", "433.1_EROG_CAP_TTAL_DEVE_0_27_35",
    "433.1_EROG_CORR_INT_DEVE_0_21_93", "433.1_EROG_CORR_NAL_DEVE_0_18_98",
    "433.1_EROG_CORR_ORR_DEVE_0_25_6", "433.1_EROG_CORR_PER_DEVE_0_19_3", "433.1_EROG_CORR_RAS_DEVE_0_28_2",
    "433.1_EROG_CORR_ROP_DEVE_0_26_100", "433.1_EROG_CORR_SIV_DEVE_0_28_96",
    "433.1_EROG_FIGURVAS_DEVE_0_16_52", "433.1_FINAN_CONT_AC_DEVE_0_22_14",
    "433.1_FINAN_CONT_CE_DEVE_0_22_12", "433.1_FINAN_CONT_OD_DEVE_0_22_31",
    "433.1_FIN_NETO_TETO_DEVE_0_25_70", "433.1_FIN_NETO_UMOR_DEVE_0_27_33",
    "433.1_FIN_NETO_UMOR_DEVE_0_27_54", "433.1_FIN_NETO_URED_DEVE_0_30_33",
    "433.1_FIN_NETO_URED_DEVE_0_30_83", "433.1_NECES_FINAGUR_DEVE_0_23_68",
    "433.1_NECES_FINANAN_DEVE_0_11_50", "433.1_RECURSOS_CTAL_DEVE_0_16_7",
    "433.1_REC_CORR_IBUT_DEVE_0_20_8", "433.1_REC_CORR_IBUT_DEVE_0_22_70",
    "433.1_REC_CORR_TORR_DEVE_0_23_71", "433.1_RESULTADOADO_DEVE_0_9_11",
    "433.1_SUPERAVIT_CAP_DEVE_0_26_78", "433.1_SUPERAVIT_TAL_DEVE_0_20_18",
    "433.1_TOTAL_EROGGUR_DEVE_0_21_33", "433.1_TOTAL_EROGGUR_DEVE_0_26_36",
    "433.1_TOTAL_EROGNES_DEVE_0_17_2", "433.1_TOTAL_RECUSOS_DEVE_0_14_76",
    "433.1_TOTAL_REC_GUR_DEVE_0_20_21", "434.1_AHORRORRO_DEVE_0_6_45", "434.1_AUM_PAS_FITOS_DEVE_0_17_88",
    "434.1_EROG_CAP_IANC_DEVE_0_19_10", "434.1_EROG_CAP_ISTR_DEVE_0_24_9",
    "434.1_EROG_CAP_ITAL_DEVE_0_27_60", "434.1_EROG_CAP_TTAL_DEVE_0_27_89",
    "434.1_EROG_CORR_INT_DEVE_0_21_43", "434.1_EROG_CORR_NAL_DEVE_0_18_61",
    "434.1_EROG_CORR_ORR_DEVE_0_25_80", "434.1_EROG_CORR_PER_DEVE_0_19_60",
    "434.1_EROG_CORR_RAS_DEVE_0_28_33", "434.1_EROG_CORR_ROP_DEVE_0_26_64",
    "434.1_EROG_FIGURVAS_DEVE_0_16_55", "434.1_FINAN_CONT_AC_DEVE_0_22_69",
    "434.1_FINAN_CONT_CE_DEVE_0_22_76", "434.1_FINAN_CONT_OD_DEVE_0_22_22",
    "434.1_FIN_NETO_TETO_DEVE_0_25_11", "434.1_FIN_NETO_UMOR_DEVE_0_27_28",
    "434.1_FIN_NETO_UMOR_DEVE_0_27_58", "434.1_FIN_NETO_URED_DEVE_0_30_6",
    "434.1_FIN_NETO_URED_DEVE_0_30_76", "434.1_NECES_FINAGUR_DEVE_0_23_51",
    "434.1_NECES_FINANAN_DEVE_0_11_2", "434.1_RECURSOS_CTAL_DEVE_0_16_39",
    "434.1_REC_CORR_IBUT_DEVE_0_20_14", "434.1_REC_CORR_IBUT_DEVE_0_22_55",
    "434.1_REC_CORR_TORR_DEVE_0_23_49", "434.1_RESULTADOADO_DEVE_0_9_64",
    "434.1_SUPERAVIT_CAP_DEVE_0_26_50", "434.1_SUPERAVIT_TAL_DEVE_0_20_59",
    "434.1_TOTAL_EROGGUR_DEVE_0_21_5", "434.1_TOTAL_EROGGUR_DEVE_0_26_14",
    "434.1_TOTAL_EROGNES_DEVE_0_17_12", "434.1_TOTAL_RECUSOS_DEVE_0_14_34",
    "434.1_TOTAL_REC_GUR_DEVE_0_20_95", "435.1_AHORRORRO_DEVE_0_6_66", "435.1_AUM_PAS_FITOS_DEVE_0_17_72",
    "435.1_EROG_CORR_ORR_DEVE_0_25_43", "435.1_EROG_CORR_RAS_DEVE_0_28_23",
    "435.1_EROG_CORR_SIV_DEVE_0_28_41", "435.1_FINAN_CONT_AC_DEVE_0_22_77",
    "435.1_NECES_FINAGUR_DEVE_0_23_32", "435.1_NECES_FINANAN_DEVE_0_11_41",
    "435.1_RECURSOS_CTAL_DEVE_0_16_99", "435.1_REC_CORR_IBUT_DEVE_0_20_84",
    "435.1_REC_CORR_IBUT_DEVE_0_22_95", "435.1_REC_CORR_TORR_DEVE_0_23_21",
    "435.1_RESULTADOADO_DEVE_0_9_13", "435.1_SUPERAVIT_CAP_DEVE_0_26_25", "435.1_SUPERAVIT_TAL_DEVE_0_20_9",
    "435.1_TOTAL_EROGGUR_DEVE_0_21_15", "435.1_TOTAL_EROGGUR_DEVE_0_26_25",
    "435.1_TOTAL_EROGNES_DEVE_0_17_2", "435.1_TOTAL_RECUSOS_DEVE_0_14_30",
    "435.1_TOTAL_REC_GUR_DEVE_0_20_47", "436.1_AHORRORRO_DEVE_0_6_28", "436.1_AUM_PAS_FITOS_DEVE_0_17_14",
    "436.1_BCO_CTRAL_ARG_DEVE_0_17_53", "436.1_EROG_CAP_IANC_DEVE_0_19_11",
    "436.1_EROG_CAP_ISTR_DEVE_0_24_6", "436.1_EROG_CAP_ITAL_DEVE_0_27_3",
    "436.1_EROG_CAP_TTAL_DEVE_0_27_81", "436.1_EROG_CORR_INT_DEVE_0_21_5",
    "436.1_EROG_CORR_NAL_DEVE_0_18_70", "436.1_EROG_CORR_ORR_DEVE_0_25_25",
    "436.1_EROG_CORR_PER_DEVE_0_19_2", "436.1_EROG_CORR_RAS_DEVE_0_28_97",
    "436.1_EROG_CORR_RIV_DEVE_0_30_21", "436.1_EROG_CORR_ROP_DEVE_0_26_39",
    "436.1_EROG_CORR_SIV_DEVE_0_28_85", "436.1_EROG_FIGURVAS_DEVE_0_16_58",
    "436.1_FINAN_CONT_AC_DEVE_0_22_61", "436.1_FINAN_CONT_CE_DEVE_0_22_43",
    "436.1_FINAN_CONT_OD_DEVE_0_22_38", "436.1_FIN_NETO_TETO_DEVE_0_25_100",
    "436.1_FIN_NETO_UMOR_DEVE_0_27_52", "436.1_FIN_NETO_UMOR_DEVE_0_27_89",
    "436.1_FIN_NETO_URED_DEVE_0_30_16", "436.1_FIN_NETO_URED_DEVE_0_30_48",
    "436.1_NECES_FINAGUR_DEVE_0_23_28", "436.1_NECES_FINANAN_DEVE_0_11_7",
    "436.1_RECURSOS_CTAL_DEVE_0_16_48", "436.1_REC_CORR_IBUT_DEVE_0_20_33",
    "436.1_REC_CORR_IBUT_DEVE_0_22_91", "436.1_REC_CORR_TORR_DEVE_0_23_17",
    "436.1_RESULTADOADO_DEVE_0_9_60", "436.1_SUPERAVIT_CAP_DEVE_0_26_32",
    "436.1_SUPERAVIT_TAL_DEVE_0_20_97", "436.1_TOTAL_EROGGUR_DEVE_0_21_24",
    "436.1_TOTAL_EROGGUR_DEVE_0_26_78", "436.1_TOTAL_EROGNES_DEVE_0_17_83",
    "436.1_TOTAL_RECUSOS_DEVE_0_14_51", "436.1_TOTAL_REC_GUR_DEVE_0_20_3", "437.1_AHORRORRO_DEVE_0_6_40",
    "437.1_AUM_PAS_FITOS_DEVE_0_17_90", "437.1_EROG_CAP_IANC_DEVE_0_19_15",
    "437.1_EROG_CAP_ISTR_DEVE_0_24_8", "437.1_EROG_CAP_ITAL_DEVE_0_27_40",
    "437.1_EROG_CAP_TTAL_DEVE_0_27_79", "437.1_EROG_CORR_INT_DEVE_0_21_82",
    "437.1_EROG_CORR_NAL_DEVE_0_18_79", "437.1_EROG_CORR_ORR_DEVE_0_25_89",
    "437.1_EROG_CORR_PER_DEVE_0_19_70", "437.1_EROG_CORR_RAS_DEVE_0_28_66",
    "437.1_EROG_CORR_ROP_DEVE_0_26_65", "437.1_FINAN_CONT_AC_DEVE_0_22_17",
    "437.1_FINAN_CONT_CE_DEVE_0_22_49", "437.1_FINAN_CONT_OD_DEVE_0_22_46",
    "437.1_FIN_NETO_TETO_DEVE_0_25_37", "437.1_FIN_NETO_UMOR_DEVE_0_27_6",
    "437.1_FIN_NETO_UMOR_DEVE_0_27_67", "437.1_FIN_NETO_URED_DEVE_0_30_32",
    "437.1_FIN_NETO_URED_DEVE_0_30_88", "437.1_NECES_FINAGUR_DEVE_0_23_51",
    "437.1_NECES_FINANAN_DEVE_0_11_9", "437.1_RECURSOS_CTAL_DEVE_0_16_35",
    "437.1_REC_CORR_IBUT_DEVE_0_20_66", "437.1_REC_CORR_IBUT_DEVE_0_22_31",
    "437.1_REC_CORR_TORR_DEVE_0_23_67", "437.1_RESULTADOADO_DEVE_0_9_66",
    "437.1_SUPERAVIT_CAP_DEVE_0_26_98", "437.1_SUPERAVIT_TAL_DEVE_0_20_78",
    "437.1_TOTAL_EROGGUR_DEVE_0_21_29", "437.1_TOTAL_EROGGUR_DEVE_0_26_19",
    "437.1_TOTAL_EROGNES_DEVE_0_17_43", "437.1_TOTAL_RECUSOS_DEVE_0_14_31",
    "437.1_TOTAL_REC_GUR_DEVE_0_20_1", "438.1_AHORRORRO_DEVE_0_6_30", "438.1_AUM_PAS_FITOS_DEVE_0_17_55",
    "438.1_BCO_CTRAL_ARG_DEVE_0_17_22", "438.1_EROG_CAP_IANC_DEVE_0_19_51",
    "438.1_EROG_CAP_ISTR_DEVE_0_24_72", "438.1_EROG_CAP_ITAL_DEVE_0_27_74",
    "438.1_EROG_CAP_TTAL_DEVE_0_27_37", "438.1_EROG_CORR_INT_DEVE_0_21_33",
    "438.1_EROG_CORR_ORR_DEVE_0_25_71", "438.1_EROG_CORR_ROP_DEVE_0_26_91",
    "438.1_EROG_CORR_UBL_DEVE_0_25_55", "438.1_FINAN_CONT_AC_DEVE_0_22_86",
    "438.1_FINAN_CONT_CE_DEVE_0_22_47", "438.1_FINAN_CONT_OD_DEVE_0_22_4",
    "438.1_FIN_NETO_TETO_DEVE_0_25_33", "438.1_FIN_NETO_UMOR_DEVE_0_27_59",
    "438.1_FIN_NETO_UMOR_DEVE_0_27_7", "438.1_FIN_NETO_URED_DEVE_0_30_43",
    "438.1_FIN_NETO_URED_DEVE_0_30_91", "438.1_NECES_FINAGUR_DEVE_0_23_62",
    "438.1_NECES_FINANAN_DEVE_0_11_98", "438.1_RECURSOS_CTAL_DEVE_0_16_97",
    "438.1_RESULTADOADO_DEVE_0_9_39", "438.1_SUPERAVIT_CAP_DEVE_0_26_25",
    "438.1_SUPERAVIT_TAL_DEVE_0_20_45", "438.1_TOTAL_EROGGUR_DEVE_0_21_49",
    "438.1_TOTAL_EROGGUR_DEVE_0_26_51", "438.1_TOTAL_EROGNES_DEVE_0_17_59",
    "438.1_TOTAL_RECUSOS_DEVE_0_14_71", "438.1_TOTAL_REC_GUR_DEVE_0_20_70", "439.1_AHORRORRO_DEVE_0_6_15",
    "439.1_AUM_PAS_FITOS_DEVE_0_17_78", "439.1_BCO_CTRAL_ARG_DEVE_0_17_43",
    "439.1_EROG_CAP_IANC_DEVE_0_19_21", "439.1_EROG_CAP_ISTR_DEVE_0_24_100",
    "439.1_EROG_CAP_ITAL_DEVE_0_27_44", "439.1_EROG_CAP_TTAL_DEVE_0_27_33",
    "439.1_EROG_CORR_INT_DEVE_0_21_25", "439.1_EROG_CORR_NAL_DEVE_0_18_70",
    "439.1_EROG_CORR_ORR_DEVE_0_25_83", "439.1_EROG_CORR_PER_DEVE_0_19_21",
    "439.1_EROG_CORR_RAS_DEVE_0_28_26", "439.1_EROG_CORR_RIV_DEVE_0_30_36",
    "439.1_EROG_CORR_ROP_DEVE_0_26_64", "439.1_EROG_CORR_SIV_DEVE_0_28_88",
    "439.1_EROG_CORR_UBL_DEVE_0_25_43", "439.1_EROG_FIGURVAS_DEVE_0_16_28",
    "439.1_FINAN_CONT_AC_DEVE_0_22_19", "439.1_FINAN_CONT_CE_DEVE_0_22_18",
    "439.1_FINAN_CONT_OD_DEVE_0_22_91", "439.1_FIN_NETO_TETO_DEVE_0_25_76",
    "439.1_FIN_NETO_UMOR_DEVE_0_27_26", "439.1_FIN_NETO_UMOR_DEVE_0_27_95",
    "439.1_FIN_NETO_URED_DEVE_0_30_18", "439.1_FIN_NETO_URED_DEVE_0_30_83",
    "439.1_NECES_FINAGUR_DEVE_0_23_58", "439.1_NECES_FINANAN_DEVE_0_11_81",
    "439.1_RECURSOS_CTAL_DEVE_0_16_74", "439.1_REC_CORR_IBUT_DEVE_0_20_17",
    "439.1_REC_CORR_IBUT_DEVE_0_22_7", "439.1_REC_CORR_TORR_DEVE_0_23_32", "439.1_RESULTADOADO_DEVE_0_9_87",
    "439.1_SUPERAVIT_CAP_DEVE_0_26_1", "439.1_SUPERAVIT_TAL_DEVE_0_20_67",
    "439.1_TOTAL_EROGGUR_DEVE_0_21_1", "439.1_TOTAL_EROGGUR_DEVE_0_26_49",
    "439.1_TOTAL_EROGNES_DEVE_0_17_67", "439.1_TOTAL_RECUSOS_DEVE_0_14_93",
    "439.1_TOTAL_REC_GUR_DEVE_0_20_30",
)

_IDS_TRAMO_1977_1986 = (
    "432.2_AHORRORRO_DEVE_0_6_6", "432.2_AUM_PAS_FITOS_DEVE_0_17_33", "432.2_BCO_CTRAL_ARG_DEVE_0_17_30",
    "432.2_EROG_CAP_IANC_DEVE_0_19_83", "432.2_EROG_CAP_IIST_DEVE_0_23_99",
    "432.2_EROG_CAP_ISTR_DEVE_0_24_27", "432.2_EROG_CAP_ITAL_DEVE_0_27_76",
    "432.2_EROG_CAP_TTAL_DEVE_0_27_62", "432.2_EROG_CORR_INT_DEVE_0_21_85",
    "432.2_EROG_CORR_NAL_DEVE_0_18_25", "432.2_EROG_CORR_ORR_DEVE_0_25_40",
    "432.2_EROG_CORR_PER_DEVE_0_19_55", "432.2_EROG_CORR_RAS_DEVE_0_28_76",
    "432.2_EROG_CORR_RIV_DEVE_0_30_97", "432.2_EROG_CORR_ROP_DEVE_0_26_43",
    "432.2_EROG_CORR_SIV_DEVE_0_28_79", "432.2_EROG_FIGURVAS_DEVE_0_16_6",
    "432.2_FINAN_EMERCON_DEVE_0_16_83", "432.2_FIN_NETO_TETO_DEVE_0_25_91",
    "432.2_FIN_NETO_UMOR_DEVE_0_27_11", "432.2_FIN_NETO_UMOR_DEVE_0_27_42",
    "432.2_FIN_NETO_UONT_DEVE_0_27_90", "432.2_FIN_NETO_URED_DEVE_0_30_33",
    "432.2_FIN_NETO_URED_DEVE_0_30_50", "432.2_NECES_FINAGUR_DEVE_0_23_83",
    "432.2_NECES_FINANAN_DEVE_0_11_98", "432.2_RECURSOS_CTAL_DEVE_0_16_36",
    "432.2_REC_CORR_IBUT_DEVE_0_20_94", "432.2_REC_CORR_IBUT_DEVE_0_22_16",
    "432.2_REC_CORR_TORR_DEVE_0_23_90", "432.2_RESULTADOADO_DEVE_0_9_100",
    "432.2_SUPERAVIT_CAP_DEVE_0_26_88", "432.2_SUPERAVIT_TAL_DEVE_0_20_40",
    "432.2_TOTAL_EROGGUR_DEVE_0_21_8", "432.2_TOTAL_EROGGUR_DEVE_0_26_70",
    "432.2_TOTAL_EROGNES_DEVE_0_17_40", "432.2_TOTAL_RECUSOS_DEVE_0_14_74",
    "432.2_TOTAL_REC_GUR_DEVE_0_20_33", "433.2_AHORRORRO_DEVE_0_6_34", "433.2_AUM_PAS_FITOS_DEVE_0_17_48",
    "433.2_EROG_CAP_IANC_DEVE_0_19_0", "433.2_EROG_CAP_IIST_DEVE_0_23_46",
    "433.2_EROG_CAP_ISTR_DEVE_0_24_97", "433.2_EROG_CAP_ITAL_DEVE_0_27_31",
    "433.2_EROG_CAP_TTAL_DEVE_0_27_69", "433.2_EROG_CORR_INT_DEVE_0_21_31",
    "433.2_EROG_CORR_NAL_DEVE_0_18_32", "433.2_EROG_CORR_ORR_DEVE_0_25_78",
    "433.2_EROG_CORR_PER_DEVE_0_19_75", "433.2_EROG_CORR_RAS_DEVE_0_28_14",
    "433.2_EROG_CORR_ROP_DEVE_0_26_86", "433.2_EROG_CORR_SIV_DEVE_0_28_73",
    "433.2_EROG_FIGURVAS_DEVE_0_16_33", "433.2_FINAN_CONT_AC_DEVE_0_22_7",
    "433.2_FINAN_CONT_CE_DEVE_0_22_55", "433.2_FINAN_CONT_OD_DEVE_0_22_90",
    "433.2_FIN_NETO_TETO_DEVE_0_25_63", "433.2_FIN_NETO_UMOR_DEVE_0_27_38",
    "433.2_FIN_NETO_UMOR_DEVE_0_27_6", "433.2_FIN_NETO_UONT_DEVE_0_27_53",
    "433.2_FIN_NETO_URED_DEVE_0_30_83", "433.2_NECES_FINAGUR_DEVE_0_23_0",
    "433.2_NECES_FINANAN_DEVE_0_11_96", "433.2_RECURSOS_CTAL_DEVE_0_16_60",
    "433.2_REC_CORR_IBUT_DEVE_0_20_97", "433.2_REC_CORR_IBUT_DEVE_0_22_94",
    "433.2_REC_CORR_TORR_DEVE_0_23_66", "433.2_RESULTADOADO_DEVE_0_9_38",
    "433.2_SUPERAVIT_CAP_DEVE_0_26_50", "433.2_SUPERAVIT_TAL_DEVE_0_20_93",
    "433.2_TOTAL_EROGGUR_DEVE_0_21_13", "433.2_TOTAL_EROGGUR_DEVE_0_26_64",
    "433.2_TOTAL_EROGNES_DEVE_0_17_93", "433.2_TOTAL_RECUSOS_DEVE_0_14_56",
    "433.2_TOTAL_REC_GUR_DEVE_0_20_64", "434.2_AHORRORRO_DEVE_0_6_76", "434.2_AUM_PAS_FITOS_DEVE_0_17_78",
    "434.2_EROG_CAP_IANC_DEVE_0_19_73", "434.2_EROG_CAP_IIST_DEVE_0_23_66",
    "434.2_EROG_CAP_ISTR_DEVE_0_24_1", "434.2_EROG_CAP_ITAL_DEVE_0_27_91",
    "434.2_EROG_CAP_TTAL_DEVE_0_27_26", "434.2_EROG_CORR_INT_DEVE_0_21_7",
    "434.2_EROG_CORR_NAL_DEVE_0_18_11", "434.2_EROG_CORR_ORR_DEVE_0_25_31",
    "434.2_EROG_CORR_PER_DEVE_0_19_17", "434.2_EROG_CORR_RAS_DEVE_0_28_20",
    "434.2_EROG_CORR_ROP_DEVE_0_26_24", "434.2_EROG_FIGURVAS_DEVE_0_16_74",
    "434.2_FINAN_CONT_AC_DEVE_0_22_30", "434.2_FINAN_CONT_CE_DEVE_0_22_72",
    "434.2_FINAN_CONT_OD_DEVE_0_22_36", "434.2_FIN_NETO_TETO_DEVE_0_25_87",
    "434.2_FIN_NETO_UANT_DEVE_0_33_2", "434.2_FIN_NETO_UMOR_DEVE_0_27_43",
    "434.2_FIN_NETO_UMOR_DEVE_0_27_76", "434.2_FIN_NETO_UONT_DEVE_0_27_60",
    "434.2_FIN_NETO_URED_DEVE_0_30_69", "434.2_FIN_NETO_URED_DEVE_0_30_90",
    "434.2_NECES_FINAGUR_DEVE_0_23_33", "434.2_NECES_FINANAN_DEVE_0_11_44",
    "434.2_RECURSOS_CTAL_DEVE_0_16_33", "434.2_REC_CORR_IBUT_DEVE_0_20_68",
    "434.2_REC_CORR_IBUT_DEVE_0_22_38", "434.2_REC_CORR_TORR_DEVE_0_23_50",
    "434.2_RESULTADOADO_DEVE_0_9_40", "434.2_SUPERAVIT_CAP_DEVE_0_26_59", "434.2_SUPERAVIT_TAL_DEVE_0_20_8",
    "434.2_TOTAL_EROGGUR_DEVE_0_21_23", "434.2_TOTAL_EROGGUR_DEVE_0_26_11",
    "434.2_TOTAL_EROGNES_DEVE_0_17_28", "434.2_TOTAL_RECUSOS_DEVE_0_14_35",
    "434.2_TOTAL_REC_GUR_DEVE_0_20_72", "435.2_AHORRORRO_DEVE_0_6_3", "435.2_AUM_PAS_FITOS_DEVE_0_17_68",
    "435.2_EROG_CORR_ORR_DEVE_0_25_80", "435.2_EROG_CORR_SIV_DEVE_0_28_80",
    "435.2_FINAN_CONT_AC_DEVE_0_22_78", "435.2_NECES_FINAGUR_DEVE_0_23_52",
    "435.2_NECES_FINANAN_DEVE_0_11_97", "435.2_REC_CORR_IBUT_DEVE_0_20_53",
    "435.2_REC_CORR_IBUT_DEVE_0_22_100", "435.2_REC_CORR_TORR_DEVE_0_23_12",
    "435.2_RESULTADOADO_DEVE_0_9_14", "435.2_SUPERAVIT_CAP_DEVE_0_26_56",
    "435.2_SUPERAVIT_TAL_DEVE_0_20_64", "435.2_TOTAL_EROGGUR_DEVE_0_21_89",
    "435.2_TOTAL_EROGGUR_DEVE_0_26_77", "435.2_TOTAL_EROGNES_DEVE_0_17_31",
    "435.2_TOTAL_RECUSOS_DEVE_0_14_43", "435.2_TOTAL_REC_GUR_DEVE_0_20_85", "436.2_AHORRORRO_DEVE_0_6_11",
    "436.2_AUM_PAS_FITOS_DEVE_0_17_58", "436.2_BCO_CTRAL_ARG_DEVE_0_17_40",
    "436.2_EROG_CAP_IANC_DEVE_0_19_54", "436.2_EROG_CAP_IIST_DEVE_0_23_43",
    "436.2_EROG_CAP_ISTR_DEVE_0_24_90", "436.2_EROG_CAP_ITAL_DEVE_0_27_39",
    "436.2_EROG_CAP_TTAL_DEVE_0_27_8", "436.2_EROG_CORR_INT_DEVE_0_21_100",
    "436.2_EROG_CORR_NAL_DEVE_0_18_61", "436.2_EROG_CORR_ORR_DEVE_0_25_38",
    "436.2_EROG_CORR_PER_DEVE_0_19_86", "436.2_EROG_CORR_RAS_DEVE_0_28_59",
    "436.2_EROG_CORR_RIV_DEVE_0_30_27", "436.2_EROG_CORR_ROP_DEVE_0_26_89",
    "436.2_EROG_CORR_SIV_DEVE_0_28_71", "436.2_EROG_FIGURVAS_DEVE_0_16_97",
    "436.2_FINAN_CONT_AC_DEVE_0_22_15", "436.2_FINAN_CONT_CE_DEVE_0_22_61",
    "436.2_FINAN_CONT_OD_DEVE_0_22_60", "436.2_FIN_NETO_TETO_DEVE_0_25_8",
    "436.2_FIN_NETO_UANT_DEVE_0_33_38", "436.2_FIN_NETO_UMOR_DEVE_0_27_40",
    "436.2_FIN_NETO_UMOR_DEVE_0_27_49", "436.2_FIN_NETO_UONT_DEVE_0_27_9",
    "436.2_FIN_NETO_URED_DEVE_0_30_49", "436.2_FIN_NETO_URED_DEVE_0_30_86",
    "436.2_NECES_FINAGUR_DEVE_0_23_56", "436.2_NECES_FINANAN_DEVE_0_11_10",
    "436.2_RECURSOS_CTAL_DEVE_0_16_8", "436.2_REC_CORR_IBUT_DEVE_0_20_96",
    "436.2_REC_CORR_IBUT_DEVE_0_22_3", "436.2_REC_CORR_TORR_DEVE_0_23_75", "436.2_RESULTADOADO_DEVE_0_9_45",
    "436.2_SUPERAVIT_CAP_DEVE_0_26_49", "436.2_SUPERAVIT_TAL_DEVE_0_20_99",
    "436.2_TOTAL_EROGGUR_DEVE_0_21_76", "436.2_TOTAL_EROGGUR_DEVE_0_26_7",
    "436.2_TOTAL_EROGNES_DEVE_0_17_4", "436.2_TOTAL_RECUSOS_DEVE_0_14_10",
    "436.2_TOTAL_REC_GUR_DEVE_0_20_85", "437.2_AHORRORRO_DEVE_0_6_98", "437.2_AUM_PAS_FITOS_DEVE_0_17_86",
    "437.2_EROG_CAP_IANC_DEVE_0_19_75", "437.2_EROG_CAP_IIST_DEVE_0_23_91",
    "437.2_EROG_CAP_ISTR_DEVE_0_24_87", "437.2_EROG_CAP_ITAL_DEVE_0_27_4",
    "437.2_EROG_CAP_TTAL_DEVE_0_27_54", "437.2_EROG_CORR_INT_DEVE_0_21_61",
    "437.2_EROG_CORR_NAL_DEVE_0_18_17", "437.2_EROG_CORR_ORR_DEVE_0_25_10",
    "437.2_EROG_CORR_PER_DEVE_0_19_26", "437.2_EROG_CORR_RAS_DEVE_0_28_21",
    "437.2_EROG_CORR_RIV_DEVE_0_30_96", "437.2_EROG_CORR_ROP_DEVE_0_26_80",
    "437.2_EROG_CORR_SIV_DEVE_0_28_28", "437.2_FINAN_CONT_AC_DEVE_0_22_1",
    "437.2_FINAN_CONT_CE_DEVE_0_22_7", "437.2_FINAN_CONT_OD_DEVE_0_22_17",
    "437.2_FIN_NETO_TETO_DEVE_0_25_89", "437.2_FIN_NETO_UANT_DEVE_0_33_26",
    "437.2_FIN_NETO_UMOR_DEVE_0_27_26", "437.2_FIN_NETO_UMOR_DEVE_0_27_86",
    "437.2_FIN_NETO_UONT_DEVE_0_27_69", "437.2_FIN_NETO_URED_DEVE_0_30_10",
    "437.2_FIN_NETO_URED_DEVE_0_30_50", "437.2_NECES_FINAGUR_DEVE_0_23_18",
    "437.2_NECES_FINANAN_DEVE_0_11_12", "437.2_RECURSOS_CTAL_DEVE_0_16_58",
    "437.2_REC_CORR_IBUT_DEVE_0_20_55", "437.2_REC_CORR_IBUT_DEVE_0_22_3",
    "437.2_REC_CORR_TORR_DEVE_0_23_91", "437.2_RESULTADOADO_DEVE_0_9_94",
    "437.2_SUPERAVIT_CAP_DEVE_0_26_63", "437.2_SUPERAVIT_TAL_DEVE_0_20_75",
    "437.2_TOTAL_EROGGUR_DEVE_0_21_1", "437.2_TOTAL_EROGGUR_DEVE_0_26_62",
    "437.2_TOTAL_EROGNES_DEVE_0_17_96", "437.2_TOTAL_RECUSOS_DEVE_0_14_98",
    "437.2_TOTAL_REC_GUR_DEVE_0_20_98", "438.2_AHORRORRO_DEVE_0_6_69", "438.2_AUM_PAS_FITOS_DEVE_0_17_54",
    "438.2_EROG_CAP_IANC_DEVE_0_19_7", "438.2_EROG_CAP_ISTR_DEVE_0_24_1",
    "438.2_EROG_CAP_ITAL_DEVE_0_27_36", "438.2_EROG_CAP_TTAL_DEVE_0_27_28",
    "438.2_EROG_CORR_INT_DEVE_0_21_31", "438.2_EROG_CORR_ORR_DEVE_0_25_54",
    "438.2_EROG_CORR_ROP_DEVE_0_26_48", "438.2_EROG_CORR_UBL_DEVE_0_25_67",
    "438.2_FINAN_CONT_AC_DEVE_0_22_92", "438.2_FINAN_CONT_CE_DEVE_0_22_9",
    "438.2_FINAN_CONT_OD_DEVE_0_22_60", "438.2_FIN_NETO_TETO_DEVE_0_25_29",
    "438.2_FIN_NETO_UANT_DEVE_0_33_28", "438.2_FIN_NETO_UMOR_DEVE_0_27_31",
    "438.2_FIN_NETO_UMOR_DEVE_0_27_67", "438.2_FIN_NETO_UONT_DEVE_0_27_44",
    "438.2_FIN_NETO_URED_DEVE_0_30_64", "438.2_FIN_NETO_URED_DEVE_0_30_92",
    "438.2_NECES_FINAGUR_DEVE_0_23_66", "438.2_NECES_FINANAN_DEVE_0_11_49",
    "438.2_RECURSOS_CTAL_DEVE_0_16_35", "438.2_RESULTADOADO_DEVE_0_9_86",
    "438.2_SUPERAVIT_CAP_DEVE_0_26_62", "438.2_SUPERAVIT_TAL_DEVE_0_20_19",
    "438.2_TOTAL_EROGGUR_DEVE_0_21_82", "438.2_TOTAL_EROGGUR_DEVE_0_26_80",
    "438.2_TOTAL_EROGNES_DEVE_0_17_47", "438.2_TOTAL_RECUSOS_DEVE_0_14_92",
    "438.2_TOTAL_REC_GUR_DEVE_0_20_71", "439.2_AHORRORRO_DEVE_0_6_92", "439.2_AUM_PAS_FITOS_DEVE_0_17_92",
    "439.2_BCO_CTRAL_ARG_DEVE_0_17_16", "439.2_EROG_CAP_IANC_DEVE_0_19_8",
    "439.2_EROG_CAP_IIST_DEVE_0_23_38", "439.2_EROG_CAP_ISTR_DEVE_0_24_81",
    "439.2_EROG_CAP_ITAL_DEVE_0_27_2", "439.2_EROG_CAP_TTAL_DEVE_0_27_61",
    "439.2_EROG_CORR_INT_DEVE_0_21_17", "439.2_EROG_CORR_NAL_DEVE_0_18_77",
    "439.2_EROG_CORR_ORR_DEVE_0_25_13", "439.2_EROG_CORR_PER_DEVE_0_19_17",
    "439.2_EROG_CORR_RAS_DEVE_0_28_75", "439.2_EROG_CORR_RIV_DEVE_0_30_17",
    "439.2_EROG_CORR_ROP_DEVE_0_26_48", "439.2_EROG_CORR_SIV_DEVE_0_28_27",
    "439.2_EROG_CORR_UBL_DEVE_0_25_55", "439.2_EROG_FIGURVAS_DEVE_0_16_74",
    "439.2_FINAN_CONT_AC_DEVE_0_22_78", "439.2_FINAN_CONT_CE_DEVE_0_22_1",
    "439.2_FINAN_CONT_OD_DEVE_0_22_17", "439.2_FIN_NETO_TETO_DEVE_0_25_21",
    "439.2_FIN_NETO_UANT_DEVE_0_33_58", "439.2_FIN_NETO_UMOR_DEVE_0_27_21",
    "439.2_FIN_NETO_UMOR_DEVE_0_27_82", "439.2_FIN_NETO_UONT_DEVE_0_27_0",
    "439.2_FIN_NETO_URED_DEVE_0_30_13", "439.2_FIN_NETO_URED_DEVE_0_30_54",
    "439.2_NECES_FINAGUR_DEVE_0_23_92", "439.2_NECES_FINANAN_DEVE_0_11_87",
    "439.2_RECURSOS_CTAL_DEVE_0_16_25", "439.2_REC_CORR_IBUT_DEVE_0_20_9",
    "439.2_REC_CORR_IBUT_DEVE_0_22_74", "439.2_REC_CORR_TORR_DEVE_0_23_90",
    "439.2_RESULTADOADO_DEVE_0_9_99", "439.2_SUPERAVIT_CAP_DEVE_0_26_13",
    "439.2_SUPERAVIT_TAL_DEVE_0_20_67", "439.2_TOTAL_EROGGUR_DEVE_0_21_42",
    "439.2_TOTAL_EROGGUR_DEVE_0_26_10", "439.2_TOTAL_EROGNES_DEVE_0_17_56",
    "439.2_TOTAL_RECUSOS_DEVE_0_14_14", "439.2_TOTAL_REC_GUR_DEVE_0_20_77",
)

_IDS_TRAMO_1987_1989 = (
    "432.3_AHORRORRO_DEVE_0_6_61", "432.3_AUM_PAS_FITOS_DEVE_0_17_40", "432.3_EROG_CAP_IANC_DEVE_0_19_16",
    "432.3_EROG_CAP_IIST_DEVE_0_23_78", "432.3_EROG_CAP_ISTR_DEVE_0_24_75",
    "432.3_EROG_CAP_ITAL_DEVE_0_27_10", "432.3_EROG_CAP_TTAL_DEVE_0_27_28",
    "432.3_EROG_CORR_INT_DEVE_0_21_6", "432.3_EROG_CORR_NAL_DEVE_0_18_97",
    "432.3_EROG_CORR_ORR_DEVE_0_25_25", "432.3_EROG_CORR_PER_DEVE_0_19_53",
    "432.3_EROG_CORR_RAS_DEVE_0_28_98", "432.3_EROG_CORR_RIV_DEVE_0_30_35",
    "432.3_EROG_CORR_ROP_DEVE_0_26_91", "432.3_EROG_CORR_SIV_DEVE_0_28_67",
    "432.3_EROG_FIGURVAS_DEVE_0_16_5", "432.3_FINAN_CONT_CE_DEVE_0_22_59",
    "432.3_FINAN_CONT_OD_DEVE_0_22_32", "432.3_FINAN_EMERCON_DEVE_0_16_91",
    "432.3_FIN_NETO_TETO_DEVE_0_25_57", "432.3_FIN_NETO_UMOR_DEVE_0_27_82",
    "432.3_FIN_NETO_UMOR_DEVE_0_27_93", "432.3_FIN_NETO_UONT_DEVE_0_27_7",
    "432.3_FIN_NETO_URED_DEVE_0_30_38", "432.3_FIN_NETO_URED_DEVE_0_30_58",
    "432.3_NECES_FINAGUR_DEVE_0_23_84", "432.3_NECES_FINANAN_DEVE_0_11_13",
    "432.3_RECURSOS_CTAL_DEVE_0_16_20", "432.3_REC_CORR_IBUT_DEVE_0_20_81",
    "432.3_REC_CORR_IBUT_DEVE_0_22_100", "432.3_REC_CORR_TORR_DEVE_0_23_58",
    "432.3_RESULTADOADO_DEVE_0_9_58", "432.3_SUPERAVIT_CAP_DEVE_0_26_2", "432.3_SUPERAVIT_TAL_DEVE_0_20_0",
    "432.3_TOTAL_EROGGUR_DEVE_0_21_52", "432.3_TOTAL_EROGGUR_DEVE_0_26_72",
    "432.3_TOTAL_EROGNES_DEVE_0_17_63", "432.3_TOTAL_RECUSOS_DEVE_0_14_36",
    "432.3_TOTAL_REC_GUR_DEVE_0_20_28", "433.3_AHORRORRO_DEVE_0_6_100", "433.3_AUM_PAS_FITOS_DEVE_0_17_94",
    "433.3_EROG_CAP_IANC_DEVE_0_19_51", "433.3_EROG_CAP_IIST_DEVE_0_23_42",
    "433.3_EROG_CAP_ISTR_DEVE_0_24_64", "433.3_EROG_CAP_ITAL_DEVE_0_27_41",
    "433.3_EROG_CAP_TTAL_DEVE_0_27_70", "433.3_EROG_CORR_INT_DEVE_0_21_2",
    "433.3_EROG_CORR_NAL_DEVE_0_18_18", "433.3_EROG_CORR_ORR_DEVE_0_25_4",
    "433.3_EROG_CORR_PER_DEVE_0_19_62", "433.3_EROG_CORR_RAS_DEVE_0_28_49",
    "433.3_EROG_CORR_ROP_DEVE_0_26_86", "433.3_EROG_CORR_SIV_DEVE_0_28_32",
    "433.3_EROG_FIGURVAS_DEVE_0_16_89", "433.3_FINAN_CONT_AC_DEVE_0_22_12",
    "433.3_FINAN_CONT_CE_DEVE_0_22_1", "433.3_FINAN_CONT_OD_DEVE_0_22_5",
    "433.3_FIN_NETO_TETO_DEVE_0_25_100", "433.3_FIN_NETO_UMOR_DEVE_0_27_37",
    "433.3_FIN_NETO_URED_DEVE_0_30_61", "433.3_NECES_FINAGUR_DEVE_0_23_91",
    "433.3_NECES_FINANAN_DEVE_0_11_72", "433.3_RECURSOS_CTAL_DEVE_0_16_41",
    "433.3_REC_CORR_IBUT_DEVE_0_20_41", "433.3_REC_CORR_IBUT_DEVE_0_22_64",
    "433.3_REC_CORR_TORR_DEVE_0_23_37", "433.3_RESULTADOADO_DEVE_0_9_76",
    "433.3_SUPERAVIT_CAP_DEVE_0_26_91", "433.3_SUPERAVIT_TAL_DEVE_0_20_45",
    "433.3_TOTAL_EROGGUR_DEVE_0_21_8", "433.3_TOTAL_EROGGUR_DEVE_0_26_15",
    "433.3_TOTAL_EROGNES_DEVE_0_17_50", "433.3_TOTAL_RECUSOS_DEVE_0_14_5",
    "433.3_TOTAL_REC_GUR_DEVE_0_20_64", "434.3_AHORRORRO_DEVE_0_6_71", "434.3_AUM_PAS_FITOS_DEVE_0_17_57",
    "434.3_EROG_CAP_IANC_DEVE_0_19_20", "434.3_EROG_CAP_IIST_DEVE_0_23_56",
    "434.3_EROG_CAP_ISTR_DEVE_0_24_53", "434.3_EROG_CAP_ITAL_DEVE_0_27_19",
    "434.3_EROG_CAP_TTAL_DEVE_0_27_39", "434.3_EROG_CORR_INT_DEVE_0_21_9",
    "434.3_EROG_CORR_NAL_DEVE_0_18_18", "434.3_EROG_CORR_ORR_DEVE_0_25_100",
    "434.3_EROG_CORR_PER_DEVE_0_19_28", "434.3_EROG_CORR_RAS_DEVE_0_28_97",
    "434.3_EROG_CORR_ROP_DEVE_0_26_33", "434.3_EROG_FIGURVAS_DEVE_0_16_9",
    "434.3_FINAN_CONT_AC_DEVE_0_22_55", "434.3_FINAN_CONT_CE_DEVE_0_22_89",
    "434.3_FINAN_CONT_OD_DEVE_0_22_89", "434.3_FINAN_EMERCON_DEVE_0_16_19",
    "434.3_FIN_NETO_TETO_DEVE_0_25_65", "434.3_FIN_NETO_UMOR_DEVE_0_27_63",
    "434.3_FIN_NETO_UMOR_DEVE_0_27_71", "434.3_FIN_NETO_UONT_DEVE_0_27_17",
    "434.3_FIN_NETO_URED_DEVE_0_30_14", "434.3_FIN_NETO_URED_DEVE_0_30_43",
    "434.3_NECES_FINAGUR_DEVE_0_23_20", "434.3_NECES_FINANAN_DEVE_0_11_28",
    "434.3_RECURSOS_CTAL_DEVE_0_16_65", "434.3_REC_CORR_IBUT_DEVE_0_20_20",
    "434.3_REC_CORR_IBUT_DEVE_0_22_89", "434.3_REC_CORR_TORR_DEVE_0_23_66",
    "434.3_RESULTADOADO_DEVE_0_9_47", "434.3_SUPERAVIT_CAP_DEVE_0_26_13",
    "434.3_SUPERAVIT_TAL_DEVE_0_20_18", "434.3_TOTAL_EROGGUR_DEVE_0_21_42",
    "434.3_TOTAL_EROGGUR_DEVE_0_26_100", "434.3_TOTAL_EROGNES_DEVE_0_17_75",
    "434.3_TOTAL_RECUSOS_DEVE_0_14_77", "434.3_TOTAL_REC_GUR_DEVE_0_20_100", "435.3_AHORRORRO_DEVE_0_6_58",
    "435.3_AUM_PAS_FITOS_DEVE_0_17_45", "435.3_EROG_CORR_ORR_DEVE_0_25_4",
    "435.3_EROG_CORR_SIV_DEVE_0_28_32", "435.3_NECES_FINAGUR_DEVE_0_23_74",
    "435.3_NECES_FINANAN_DEVE_0_11_35", "435.3_REC_CORR_IBUT_DEVE_0_20_36",
    "435.3_REC_CORR_IBUT_DEVE_0_22_62", "435.3_REC_CORR_TORR_DEVE_0_23_33",
    "435.3_RESULTADOADO_DEVE_0_9_47", "435.3_SUPERAVIT_CAP_DEVE_0_26_93",
    "435.3_SUPERAVIT_TAL_DEVE_0_20_50", "435.3_TOTAL_EROGGUR_DEVE_0_21_42",
    "435.3_TOTAL_EROGGUR_DEVE_0_26_47", "435.3_TOTAL_EROGNES_DEVE_0_17_23",
    "435.3_TOTAL_RECUSOS_DEVE_0_14_99", "435.3_TOTAL_REC_GUR_DEVE_0_20_52", "436.3_AHORRORRO_DEVE_0_6_80",
    "436.3_AUM_PAS_FITOS_DEVE_0_17_17", "436.3_EROG_CAP_IANC_DEVE_0_19_55",
    "436.3_EROG_CAP_IIST_DEVE_0_23_8", "436.3_EROG_CAP_ISTR_DEVE_0_24_36",
    "436.3_EROG_CAP_ITAL_DEVE_0_27_30", "436.3_EROG_CAP_TTAL_DEVE_0_27_46",
    "436.3_EROG_CORR_INT_DEVE_0_21_97", "436.3_EROG_CORR_NAL_DEVE_0_18_94",
    "436.3_EROG_CORR_ORR_DEVE_0_25_58", "436.3_EROG_CORR_PER_DEVE_0_19_47",
    "436.3_EROG_CORR_RAS_DEVE_0_28_52", "436.3_EROG_CORR_RIV_DEVE_0_30_53",
    "436.3_EROG_CORR_ROP_DEVE_0_26_98", "436.3_EROG_CORR_SIV_DEVE_0_28_49",
    "436.3_EROG_FIGURVAS_DEVE_0_16_20", "436.3_FINAN_CONT_AC_DEVE_0_22_18",
    "436.3_FINAN_CONT_CE_DEVE_0_22_57", "436.3_FINAN_CONT_OD_DEVE_0_22_72",
    "436.3_FINAN_EMERCON_DEVE_0_16_85", "436.3_FIN_NETO_TETO_DEVE_0_25_40",
    "436.3_FIN_NETO_UMOR_DEVE_0_27_62", "436.3_FIN_NETO_UMOR_DEVE_0_27_93",
    "436.3_FIN_NETO_UONT_DEVE_0_27_29", "436.3_FIN_NETO_URED_DEVE_0_30_0",
    "436.3_FIN_NETO_URED_DEVE_0_30_64", "436.3_NECES_FINAGUR_DEVE_0_23_31",
    "436.3_NECES_FINANAN_DEVE_0_11_92", "436.3_RECURSOS_CTAL_DEVE_0_16_70",
    "436.3_REC_CORR_IBUT_DEVE_0_20_46", "436.3_REC_CORR_IBUT_DEVE_0_22_11",
    "436.3_REC_CORR_TORR_DEVE_0_23_80", "436.3_RESULTADOADO_DEVE_0_9_79",
    "436.3_SUPERAVIT_CAP_DEVE_0_26_72", "436.3_SUPERAVIT_TAL_DEVE_0_20_74",
    "436.3_TOTAL_EROGGUR_DEVE_0_21_63", "436.3_TOTAL_EROGGUR_DEVE_0_26_34",
    "436.3_TOTAL_EROGNES_DEVE_0_17_18", "436.3_TOTAL_RECUSOS_DEVE_0_14_29",
    "436.3_TOTAL_REC_GUR_DEVE_0_20_35", "437.3_AHORRORRO_DEVE_0_6_39", "437.3_AUM_PAS_FITOS_DEVE_0_17_99",
    "437.3_EROG_CAP_IANC_DEVE_0_19_35", "437.3_EROG_CAP_IIST_DEVE_0_23_98",
    "437.3_EROG_CAP_ISTR_DEVE_0_24_49", "437.3_EROG_CAP_ITAL_DEVE_0_27_42",
    "437.3_EROG_CAP_TTAL_DEVE_0_27_87", "437.3_EROG_CORR_INT_DEVE_0_21_11",
    "437.3_EROG_CORR_NAL_DEVE_0_18_60", "437.3_EROG_CORR_ORR_DEVE_0_25_54",
    "437.3_EROG_CORR_PER_DEVE_0_19_10", "437.3_EROG_CORR_RAS_DEVE_0_28_2",
    "437.3_EROG_CORR_RIV_DEVE_0_30_20", "437.3_EROG_CORR_SIV_DEVE_0_28_42",
    "437.3_FINAN_CONT_AC_DEVE_0_22_89", "437.3_FINAN_CONT_CE_DEVE_0_22_28",
    "437.3_FINAN_CONT_OD_DEVE_0_22_7", "437.3_FIN_NETO_TETO_DEVE_0_25_99",
    "437.3_FIN_NETO_UMOR_DEVE_0_27_26", "437.3_FIN_NETO_URED_DEVE_0_30_59",
    "437.3_NECES_FINAGUR_DEVE_0_23_10", "437.3_NECES_FINANAN_DEVE_0_11_56",
    "437.3_RECURSOS_CTAL_DEVE_0_16_55", "437.3_REC_CORR_IBUT_DEVE_0_20_66",
    "437.3_REC_CORR_IBUT_DEVE_0_22_5", "437.3_REC_CORR_TORR_DEVE_0_23_91", "437.3_RESULTADOADO_DEVE_0_9_20",
    "437.3_SUPERAVIT_CAP_DEVE_0_26_57", "437.3_SUPERAVIT_TAL_DEVE_0_20_6",
    "437.3_TOTAL_EROGGUR_DEVE_0_21_96", "437.3_TOTAL_EROGGUR_DEVE_0_26_25",
    "437.3_TOTAL_EROGNES_DEVE_0_17_10", "437.3_TOTAL_RECUSOS_DEVE_0_14_18",
    "437.3_TOTAL_REC_GUR_DEVE_0_20_29", "438.3_AHORRORRO_DEVE_0_6_39", "438.3_AUM_PAS_FITOS_DEVE_0_17_46",
    "438.3_EROG_CAP_IANC_DEVE_0_19_58", "438.3_EROG_CAP_ISTR_DEVE_0_24_17",
    "438.3_EROG_CAP_ITAL_DEVE_0_27_28", "438.3_EROG_CAP_TTAL_DEVE_0_27_93",
    "438.3_EROG_CORR_INT_DEVE_0_21_81", "438.3_EROG_CORR_ORR_DEVE_0_25_92",
    "438.3_EROG_CORR_ROP_DEVE_0_26_22", "438.3_EROG_CORR_UBL_DEVE_0_25_29",
    "438.3_FINAN_CONT_AC_DEVE_0_22_66", "438.3_FINAN_CONT_CE_DEVE_0_22_1",
    "438.3_FIN_NETO_TETO_DEVE_0_25_88", "438.3_FIN_NETO_UMOR_DEVE_0_27_100",
    "438.3_FIN_NETO_UMOR_DEVE_0_27_20", "438.3_FIN_NETO_UONT_DEVE_0_27_57",
    "438.3_FIN_NETO_URED_DEVE_0_30_45", "438.3_FIN_NETO_URED_DEVE_0_30_83",
    "438.3_NECES_FINAGUR_DEVE_0_23_72", "438.3_NECES_FINANAN_DEVE_0_11_30",
    "438.3_RECURSOS_CTAL_DEVE_0_16_82", "438.3_RESULTADOADO_DEVE_0_9_71",
    "438.3_SUPERAVIT_CAP_DEVE_0_26_72", "438.3_SUPERAVIT_TAL_DEVE_0_20_16",
    "438.3_TOTAL_EROGGUR_DEVE_0_21_97", "438.3_TOTAL_EROGGUR_DEVE_0_26_91",
    "438.3_TOTAL_EROGNES_DEVE_0_17_43", "438.3_TOTAL_RECUSOS_DEVE_0_14_10",
    "438.3_TOTAL_REC_GUR_DEVE_0_20_11", "439.3_AHORRORRO_DEVE_0_6_47", "439.3_AUM_PAS_FITOS_DEVE_0_17_48",
    "439.3_EROG_CAP_IANC_DEVE_0_19_28", "439.3_EROG_CAP_IIST_DEVE_0_23_22",
    "439.3_EROG_CAP_ISTR_DEVE_0_24_8", "439.3_EROG_CAP_ITAL_DEVE_0_27_38",
    "439.3_EROG_CAP_TTAL_DEVE_0_27_16", "439.3_EROG_CORR_INT_DEVE_0_21_12",
    "439.3_EROG_CORR_NAL_DEVE_0_18_60", "439.3_EROG_CORR_ORR_DEVE_0_25_21",
    "439.3_EROG_CORR_PER_DEVE_0_19_12", "439.3_EROG_CORR_RAS_DEVE_0_28_99",
    "439.3_EROG_CORR_RIV_DEVE_0_30_20", "439.3_EROG_CORR_ROP_DEVE_0_26_56",
    "439.3_EROG_CORR_SIV_DEVE_0_28_23", "439.3_EROG_CORR_UBL_DEVE_0_25_42",
    "439.3_EROG_FIGURVAS_DEVE_0_16_60", "439.3_FINAN_CONT_AC_DEVE_0_22_57",
    "439.3_FINAN_CONT_CE_DEVE_0_22_54", "439.3_FINAN_CONT_OD_DEVE_0_22_87",
    "439.3_FINAN_EMERCON_DEVE_0_16_0", "439.3_FIN_NETO_TETO_DEVE_0_25_62",
    "439.3_FIN_NETO_UMOR_DEVE_0_27_53", "439.3_FIN_NETO_UMOR_DEVE_0_27_71",
    "439.3_FIN_NETO_UONT_DEVE_0_27_15", "439.3_FIN_NETO_URED_DEVE_0_30_33",
    "439.3_FIN_NETO_URED_DEVE_0_30_39", "439.3_NECES_FINAGUR_DEVE_0_23_38",
    "439.3_NECES_FINANAN_DEVE_0_11_41", "439.3_RECURSOS_CTAL_DEVE_0_16_94",
    "439.3_REC_CORR_IBUT_DEVE_0_20_6", "439.3_REC_CORR_IBUT_DEVE_0_22_73",
    "439.3_REC_CORR_TORR_DEVE_0_23_65", "439.3_RESULTADOADO_DEVE_0_9_56",
    "439.3_SUPERAVIT_CAP_DEVE_0_26_91", "439.3_SUPERAVIT_TAL_DEVE_0_20_16",
    "439.3_TOTAL_EROGGUR_DEVE_0_21_2", "439.3_TOTAL_EROGGUR_DEVE_0_26_14",
    "439.3_TOTAL_EROGNES_DEVE_0_17_94", "439.3_TOTAL_RECUSOS_DEVE_0_14_32",
    "439.3_TOTAL_REC_GUR_DEVE_0_20_61",
)

_IDS_TRAMO_1990_1992 = (
    "432.4_AHORRORRO_DEVE_0_6_14", "432.4_AUM_PAS_FITOS_DEVE_0_17_1", "432.4_EROG_CAP_IANC_DEVE_0_19_83",
    "432.4_EROG_CAP_IIST_DEVE_0_23_39", "432.4_EROG_CAP_ISTR_DEVE_0_24_36",
    "432.4_EROG_CAP_ITAL_DEVE_0_27_27", "432.4_EROG_CAP_TTAL_DEVE_0_27_16",
    "432.4_EROG_CORR_INT_DEVE_0_21_69", "432.4_EROG_CORR_NAL_DEVE_0_18_33",
    "432.4_EROG_CORR_ORR_DEVE_0_25_17", "432.4_EROG_CORR_PER_DEVE_0_19_83",
    "432.4_EROG_CORR_RAS_DEVE_0_28_52", "432.4_EROG_CORR_RIV_DEVE_0_30_41",
    "432.4_EROG_CORR_ROP_DEVE_0_26_27", "432.4_EROG_CORR_SIV_DEVE_0_28_71",
    "432.4_EROG_FIGURVAS_DEVE_0_16_61", "432.4_FINAN_CONT_CE_DEVE_0_22_38",
    "432.4_FINAN_CONT_OD_DEVE_0_22_40", "432.4_FINAN_EMERCON_DEVE_0_16_96",
    "432.4_FIN_NETO_TETO_DEVE_0_25_42", "432.4_FIN_NETO_UMOR_DEVE_0_27_10",
    "432.4_FIN_NETO_UMOR_DEVE_0_27_27", "432.4_FIN_NETO_UONT_DEVE_0_27_69",
    "432.4_FIN_NETO_URED_DEVE_0_30_78", "432.4_FIN_NETO_URED_DEVE_0_30_88",
    "432.4_NECES_FINAGUR_DEVE_0_23_34", "432.4_NECES_FINANAN_DEVE_0_11_60",
    "432.4_RECURSOS_CTAL_DEVE_0_16_57", "432.4_REC_CORR_IBUT_DEVE_0_20_87",
    "432.4_REC_CORR_IBUT_DEVE_0_22_43", "432.4_REC_CORR_TORR_DEVE_0_23_32",
    "432.4_RESULTADOADO_DEVE_0_9_22", "432.4_SUPERAVIT_CAP_DEVE_0_26_29",
    "432.4_SUPERAVIT_TAL_DEVE_0_20_95", "432.4_TOTAL_EROGGUR_DEVE_0_21_86",
    "432.4_TOTAL_EROGGUR_DEVE_0_26_87", "432.4_TOTAL_EROGNES_DEVE_0_17_94",
    "432.4_TOTAL_RECUSOS_DEVE_0_14_30", "432.4_TOTAL_REC_GUR_DEVE_0_20_34", "433.4_AHORRORRO_DEVE_0_6_83",
    "433.4_AUM_PAS_FITOS_DEVE_0_17_56", "433.4_EROG_CAP_IANC_DEVE_0_19_33",
    "433.4_EROG_CAP_IIST_DEVE_0_23_15", "433.4_EROG_CAP_ISTR_DEVE_0_24_6",
    "433.4_EROG_CAP_ITAL_DEVE_0_27_13", "433.4_EROG_CAP_TTAL_DEVE_0_27_69",
    "433.4_EROG_CORR_NAL_DEVE_0_18_21", "433.4_EROG_CORR_ORR_DEVE_0_25_7",
    "433.4_EROG_CORR_PER_DEVE_0_19_36", "433.4_EROG_CORR_RAS_DEVE_0_28_54",
    "433.4_EROG_CORR_ROP_DEVE_0_26_25", "433.4_EROG_CORR_SIV_DEVE_0_28_11",
    "433.4_EROG_FIGURVAS_DEVE_0_16_88", "433.4_FINAN_CONT_AC_DEVE_0_22_7",
    "433.4_FINAN_CONT_CE_DEVE_0_22_73", "433.4_FINAN_CONT_OD_DEVE_0_22_67",
    "433.4_FIN_NETO_TETO_DEVE_0_25_69", "433.4_FIN_NETO_UMOR_DEVE_0_27_73",
    "433.4_FIN_NETO_URED_DEVE_0_30_44", "433.4_FIN_NETO_URED_DEVE_0_30_89",
    "433.4_NECES_FINAGUR_DEVE_0_23_26", "433.4_NECES_FINANAN_DEVE_0_11_35",
    "433.4_RECURSOS_CTAL_DEVE_0_16_56", "433.4_REC_CORR_IBUT_DEVE_0_20_52",
    "433.4_REC_CORR_IBUT_DEVE_0_22_15", "433.4_REC_CORR_TORR_DEVE_0_23_0", "433.4_RESULTADOADO_DEVE_0_9_89",
    "433.4_SUPERAVIT_CAP_DEVE_0_26_23", "433.4_SUPERAVIT_TAL_DEVE_0_20_27",
    "433.4_TOTAL_EROGGUR_DEVE_0_21_46", "433.4_TOTAL_EROGGUR_DEVE_0_26_62",
    "433.4_TOTAL_EROGNES_DEVE_0_17_72", "433.4_TOTAL_RECUSOS_DEVE_0_14_73",
    "433.4_TOTAL_REC_GUR_DEVE_0_20_38", "434.4_AHORRORRO_DEVE_0_6_8", "434.4_AUM_PAS_FITOS_DEVE_0_17_1",
    "434.4_EROG_CAP_IANC_DEVE_0_19_71", "434.4_EROG_CAP_IIST_DEVE_0_23_11",
    "434.4_EROG_CAP_ISTR_DEVE_0_24_82", "434.4_EROG_CAP_ITAL_DEVE_0_27_5",
    "434.4_EROG_CAP_TTAL_DEVE_0_27_76", "434.4_EROG_CORR_INT_DEVE_0_21_40",
    "434.4_EROG_CORR_NAL_DEVE_0_18_31", "434.4_EROG_CORR_ORR_DEVE_0_25_58",
    "434.4_EROG_CORR_PER_DEVE_0_19_65", "434.4_EROG_CORR_RAS_DEVE_0_28_32",
    "434.4_EROG_CORR_ROP_DEVE_0_26_25", "434.4_EROG_FIGURVAS_DEVE_0_16_45",
    "434.4_FINAN_CONT_AC_DEVE_0_22_53", "434.4_FINAN_CONT_CE_DEVE_0_22_52",
    "434.4_FINAN_CONT_OD_DEVE_0_22_10", "434.4_FIN_NETO_TETO_DEVE_0_25_49",
    "434.4_FIN_NETO_UMOR_DEVE_0_27_11", "434.4_FIN_NETO_UONT_DEVE_0_27_11",
    "434.4_FIN_NETO_URED_DEVE_0_30_51", "434.4_NECES_FINAGUR_DEVE_0_23_32",
    "434.4_NECES_FINANAN_DEVE_0_11_43", "434.4_RECURSOS_CTAL_DEVE_0_16_70",
    "434.4_REC_CORR_IBUT_DEVE_0_20_8", "434.4_REC_CORR_IBUT_DEVE_0_22_24",
    "434.4_REC_CORR_TORR_DEVE_0_23_61", "434.4_RESULTADOADO_DEVE_0_9_70",
    "434.4_SUPERAVIT_CAP_DEVE_0_26_73", "434.4_SUPERAVIT_TAL_DEVE_0_20_100",
    "434.4_TOTAL_EROGGUR_DEVE_0_21_68", "434.4_TOTAL_EROGGUR_DEVE_0_26_13",
    "434.4_TOTAL_EROGNES_DEVE_0_17_80", "434.4_TOTAL_RECUSOS_DEVE_0_14_14",
    "434.4_TOTAL_REC_GUR_DEVE_0_20_55", "435.4_AHORRORRO_DEVE_0_6_32", "435.4_AUM_PAS_FITOS_DEVE_0_17_73",
    "435.4_EROG_CORR_ORR_DEVE_0_25_11", "435.4_EROG_CORR_SIV_DEVE_0_28_50",
    "435.4_NECES_FINAGUR_DEVE_0_23_71", "435.4_NECES_FINANAN_DEVE_0_11_31",
    "435.4_REC_CORR_IBUT_DEVE_0_20_89", "435.4_REC_CORR_IBUT_DEVE_0_22_58",
    "435.4_REC_CORR_TORR_DEVE_0_23_35", "435.4_RESULTADOADO_DEVE_0_9_88",
    "435.4_SUPERAVIT_CAP_DEVE_0_26_98", "435.4_SUPERAVIT_TAL_DEVE_0_20_10",
    "435.4_TOTAL_EROGGUR_DEVE_0_21_59", "435.4_TOTAL_EROGGUR_DEVE_0_26_88",
    "435.4_TOTAL_EROGNES_DEVE_0_17_43", "435.4_TOTAL_RECUSOS_DEVE_0_14_25",
    "435.4_TOTAL_REC_GUR_DEVE_0_20_35", "436.4_AHORRORRO_DEVE_0_6_15", "436.4_AUM_PAS_FITOS_DEVE_0_17_82",
    "436.4_EROG_CAP_IANC_DEVE_0_19_0", "436.4_EROG_CAP_IIST_DEVE_0_23_32",
    "436.4_EROG_CAP_ISTR_DEVE_0_24_88", "436.4_EROG_CAP_ITAL_DEVE_0_27_63",
    "436.4_EROG_CAP_TTAL_DEVE_0_27_77", "436.4_EROG_CORR_INT_DEVE_0_21_15",
    "436.4_EROG_CORR_NAL_DEVE_0_18_11", "436.4_EROG_CORR_ORR_DEVE_0_25_86",
    "436.4_EROG_CORR_PER_DEVE_0_19_35", "436.4_EROG_CORR_RAS_DEVE_0_28_31",
    "436.4_EROG_CORR_RIV_DEVE_0_30_78", "436.4_EROG_CORR_ROP_DEVE_0_26_17",
    "436.4_EROG_CORR_SIV_DEVE_0_28_33", "436.4_EROG_FIGURVAS_DEVE_0_16_33",
    "436.4_FINAN_CONT_AC_DEVE_0_22_72", "436.4_FINAN_CONT_CE_DEVE_0_22_12",
    "436.4_FINAN_CONT_OD_DEVE_0_22_55", "436.4_FINAN_EMERCON_DEVE_0_16_2",
    "436.4_FIN_NETO_TETO_DEVE_0_25_26", "436.4_FIN_NETO_UMOR_DEVE_0_27_22",
    "436.4_FIN_NETO_UMOR_DEVE_0_27_6", "436.4_FIN_NETO_UONT_DEVE_0_27_41",
    "436.4_FIN_NETO_URED_DEVE_0_30_12", "436.4_FIN_NETO_URED_DEVE_0_30_72",
    "436.4_NECES_FINAGUR_DEVE_0_23_24", "436.4_NECES_FINANAN_DEVE_0_11_90",
    "436.4_RECURSOS_CTAL_DEVE_0_16_26", "436.4_REC_CORR_IBUT_DEVE_0_20_83",
    "436.4_REC_CORR_IBUT_DEVE_0_22_4", "436.4_REC_CORR_TORR_DEVE_0_23_5", "436.4_RESULTADOADO_DEVE_0_9_73",
    "436.4_SUPERAVIT_CAP_DEVE_0_26_95", "436.4_SUPERAVIT_TAL_DEVE_0_20_15",
    "436.4_TOTAL_EROGGUR_DEVE_0_21_4", "436.4_TOTAL_EROGGUR_DEVE_0_26_65",
    "436.4_TOTAL_EROGNES_DEVE_0_17_36", "436.4_TOTAL_RECUSOS_DEVE_0_14_10",
    "436.4_TOTAL_REC_GUR_DEVE_0_20_80", "437.4_AHORRORRO_DEVE_0_6_37", "437.4_AUM_PAS_FITOS_DEVE_0_17_60",
    "437.4_EROG_CAP_IANC_DEVE_0_19_33", "437.4_EROG_CAP_IIST_DEVE_0_23_52",
    "437.4_EROG_CAP_ISTR_DEVE_0_24_68", "437.4_EROG_CAP_ITAL_DEVE_0_27_11",
    "437.4_EROG_CAP_TTAL_DEVE_0_27_55", "437.4_EROG_CORR_INT_DEVE_0_21_85",
    "437.4_EROG_CORR_NAL_DEVE_0_18_92", "437.4_EROG_CORR_ORR_DEVE_0_25_46",
    "437.4_EROG_CORR_PER_DEVE_0_19_2", "437.4_EROG_CORR_RAS_DEVE_0_28_15",
    "437.4_EROG_CORR_RIV_DEVE_0_30_53", "437.4_EROG_CORR_SIV_DEVE_0_28_64",
    "437.4_FINAN_CONT_AC_DEVE_0_22_66", "437.4_FINAN_CONT_CE_DEVE_0_22_68",
    "437.4_FINAN_CONT_OD_DEVE_0_22_79", "437.4_FIN_NETO_TETO_DEVE_0_25_19",
    "437.4_FIN_NETO_UMOR_DEVE_0_27_3", "437.4_FIN_NETO_URED_DEVE_0_30_82",
    "437.4_NECES_FINAGUR_DEVE_0_23_21", "437.4_NECES_FINANAN_DEVE_0_11_7",
    "437.4_RECURSOS_CTAL_DEVE_0_16_82", "437.4_REC_CORR_IBUT_DEVE_0_20_58",
    "437.4_REC_CORR_IBUT_DEVE_0_22_97", "437.4_REC_CORR_TORR_DEVE_0_23_39",
    "437.4_RESULTADOADO_DEVE_0_9_95", "437.4_SUPERAVIT_CAP_DEVE_0_26_35",
    "437.4_SUPERAVIT_TAL_DEVE_0_20_57", "437.4_TOTAL_EROGGUR_DEVE_0_21_31",
    "437.4_TOTAL_EROGGUR_DEVE_0_26_31", "437.4_TOTAL_EROGNES_DEVE_0_17_7",
    "437.4_TOTAL_RECUSOS_DEVE_0_14_91", "437.4_TOTAL_REC_GUR_DEVE_0_20_72", "438.4_AHORRORRO_DEVE_0_6_14",
    "438.4_AUM_PAS_FITOS_DEVE_0_17_52", "438.4_EROG_CAP_IANC_DEVE_0_19_65",
    "438.4_EROG_CAP_ISTR_DEVE_0_24_35", "438.4_EROG_CAP_ITAL_DEVE_0_27_1",
    "438.4_EROG_CAP_TTAL_DEVE_0_27_88", "438.4_EROG_CORR_INT_DEVE_0_21_75",
    "438.4_EROG_CORR_ORR_DEVE_0_25_88", "438.4_EROG_CORR_ROP_DEVE_0_26_85",
    "438.4_EROG_CORR_UBL_DEVE_0_25_44", "438.4_FINAN_CONT_AC_DEVE_0_22_10",
    "438.4_FIN_NETO_TETO_DEVE_0_25_36", "438.4_FIN_NETO_UMOR_DEVE_0_27_2",
    "438.4_FIN_NETO_UMOR_DEVE_0_27_83", "438.4_FIN_NETO_UONT_DEVE_0_27_21",
    "438.4_FIN_NETO_URED_DEVE_0_30_37", "438.4_FIN_NETO_URED_DEVE_0_30_88",
    "438.4_NECES_FINAGUR_DEVE_0_23_88", "438.4_NECES_FINANAN_DEVE_0_11_6",
    "438.4_RECURSOS_CTAL_DEVE_0_16_94", "438.4_RESULTADOADO_DEVE_0_9_72",
    "438.4_SUPERAVIT_CAP_DEVE_0_26_11", "438.4_SUPERAVIT_TAL_DEVE_0_20_2",
    "438.4_TOTAL_EROGGUR_DEVE_0_21_35", "438.4_TOTAL_EROGGUR_DEVE_0_26_14",
    "438.4_TOTAL_EROGNES_DEVE_0_17_41", "438.4_TOTAL_RECUSOS_DEVE_0_14_47",
    "438.4_TOTAL_REC_GUR_DEVE_0_20_21", "439.4_AHORRORRO_DEVE_0_6_11", "439.4_AUM_PAS_FITOS_DEVE_0_17_53",
    "439.4_EROG_CAP_IANC_DEVE_0_19_44", "439.4_EROG_CAP_IIST_DEVE_0_23_2",
    "439.4_EROG_CAP_ISTR_DEVE_0_24_62", "439.4_EROG_CAP_ITAL_DEVE_0_27_35",
    "439.4_EROG_CAP_TTAL_DEVE_0_27_47", "439.4_EROG_CORR_INT_DEVE_0_21_63",
    "439.4_EROG_CORR_NAL_DEVE_0_18_72", "439.4_EROG_CORR_ORR_DEVE_0_25_77",
    "439.4_EROG_CORR_PER_DEVE_0_19_80", "439.4_EROG_CORR_RAS_DEVE_0_28_97",
    "439.4_EROG_CORR_RIV_DEVE_0_30_37", "439.4_EROG_CORR_ROP_DEVE_0_26_9",
    "439.4_EROG_CORR_SIV_DEVE_0_28_82", "439.4_EROG_CORR_UBL_DEVE_0_25_0",
    "439.4_EROG_FIGURVAS_DEVE_0_16_44", "439.4_FINAN_CONT_AC_DEVE_0_22_15",
    "439.4_FINAN_CONT_CE_DEVE_0_22_50", "439.4_FINAN_CONT_OD_DEVE_0_22_100",
    "439.4_FINAN_EMERCON_DEVE_0_16_13", "439.4_FIN_NETO_TETO_DEVE_0_25_45",
    "439.4_FIN_NETO_UMOR_DEVE_0_27_27", "439.4_FIN_NETO_UMOR_DEVE_0_27_81",
    "439.4_FIN_NETO_UONT_DEVE_0_27_81", "439.4_FIN_NETO_URED_DEVE_0_30_66",
    "439.4_FIN_NETO_URED_DEVE_0_30_91", "439.4_NECES_FINAGUR_DEVE_0_23_73",
    "439.4_NECES_FINANAN_DEVE_0_11_48", "439.4_RECURSOS_CTAL_DEVE_0_16_63",
    "439.4_REC_CORR_IBUT_DEVE_0_20_95", "439.4_REC_CORR_IBUT_DEVE_0_22_60",
    "439.4_REC_CORR_TORR_DEVE_0_23_63", "439.4_RESULTADOADO_DEVE_0_9_42",
    "439.4_SUPERAVIT_CAP_DEVE_0_26_93", "439.4_SUPERAVIT_TAL_DEVE_0_20_28",
    "439.4_TOTAL_EROGGUR_DEVE_0_21_22", "439.4_TOTAL_EROGGUR_DEVE_0_26_32",
    "439.4_TOTAL_EROGNES_DEVE_0_17_47", "439.4_TOTAL_RECUSOS_DEVE_0_14_27",
    "439.4_TOTAL_REC_GUR_DEVE_0_20_97",
)

_IDS_GASTO_EN_PESOS = (
    "451.1_GPCGPC_0_0_3_66", "451.1_GPC_ADMINIRAL_0_0_26_69", "451.1_GPC_AGUA_AADO_0_0_23_65",
    "451.1_GPC_ASIGNARES_0_0_27_81", "451.1_GPC_ATENCILUD_0_0_18_73", "451.1_GPC_CIENCIICA_0_0_19_53",
    "451.1_GPC_COMUNINES_0_0_18_91", "451.1_GPC_CULTURURA_0_0_11_84", "451.1_GPC_DEFENSDAD_0_0_21_1",
    "451.1_GPC_EDUCACICA_0_0_20_19", "451.1_GPC_EDUCACNAR_0_0_37_73", "451.1_GPC_EDUCACRIA_0_0_27_11",
    "451.1_GPC_ENERGIBLE_0_0_23_53", "451.1_GPC_FUNCIOADO_0_0_25_7", "451.1_GPC_INDUSTRIA_0_0_13_67",
    "451.1_GPC_INSSJYLUD_0_0_17_22", "451.1_GPC_INSSJYNES_0_0_24_45", "451.1_GPC_JUSTICCIA_0_0_12_84",
    "451.1_GPC_OBRAS_LUD_0_0_24_25", "451.1_GPC_OBRAS_NES_0_0_31_10", "451.1_GPC_OTROS_COS_0_0_30_59",
    "451.1_GPC_OTROS_NOS_0_0_27_42", "451.1_GPC_PREVISIAL_0_0_20_88", "451.1_GPC_PRODUCRIA_0_0_23_15",
    "451.1_GPC_PROGRALEO_0_0_20_86", "451.1_GPC_PROMOCIAL_0_0_31_1", "451.1_GPC_SERVICCOS_0_0_24_98",
    "451.1_GPC_SERVICICA_0_0_27_71", "451.1_GPC_SERVICLES_0_0_22_30", "451.1_GPC_TRANSPRTE_0_0_14_53",
    "451.1_GPC_VIVIENSMO_0_0_22_12", "451.3_GPNGPN_0_0_3_30", "451.3_GPN_ADMINIRAL_0_0_26_28",
    "451.3_GPN_AGUA_AADO_0_0_23_55", "451.3_GPN_ASIGNARES_0_0_27_98", "451.3_GPN_ATENCILUD_0_0_18_84",
    "451.3_GPN_CIENCIICA_0_0_19_94", "451.3_GPN_COMUNINES_0_0_18_88", "451.3_GPN_CULTURURA_0_0_11_24",
    "451.3_GPN_DEFENSDAD_0_0_21_36", "451.3_GPN_EDUCACICA_0_0_20_53", "451.3_GPN_EDUCACNAR_0_0_37_1",
    "451.3_GPN_EDUCACRIA_0_0_27_84", "451.3_GPN_ENERGIBLE_0_0_23_9", "451.3_GPN_FUNCIOADO_0_0_25_32",
    "451.3_GPN_INDUSTRIA_0_0_13_70", "451.3_GPN_INSSJYLUD_0_0_17_96", "451.3_GPN_INSSJYNES_0_0_24_96",
    "451.3_GPN_JUSTICCIA_0_0_12_92", "451.3_GPN_OBRAS_LUD_0_0_24_43", "451.3_GPN_OBRAS_NES_0_0_31_59",
    "451.3_GPN_OTROS_COS_0_0_30_91", "451.3_GPN_OTROS_NOS_0_0_27_63", "451.3_GPN_PREVISIAL_0_0_20_49",
    "451.3_GPN_PRODUCRIA_0_0_23_60", "451.3_GPN_PROGRALEO_0_0_20_79", "451.3_GPN_PROMOCIAL_0_0_31_3",
    "451.3_GPN_SERVICCOS_0_0_24_38", "451.3_GPN_SERVICICA_0_0_27_85", "451.3_GPN_SERVICLES_0_0_22_42",
    "451.3_GPN_TRANSPRTE_0_0_14_72", "451.3_GPN_VIVIENSMO_0_0_22_83", "451.5_GPPGPP_0_0_3_41",
    "451.5_GPP_ADMINIRAL_0_0_26_85", "451.5_GPP_AGUA_AADO_0_0_23_99", "451.5_GPP_ASIGNARES_0_0_27_88",
    "451.5_GPP_ATENCILUD_0_0_18_19", "451.5_GPP_CIENCIICA_0_0_19_33", "451.5_GPP_COMUNINES_0_0_18_77",
    "451.5_GPP_CULTURURA_0_0_11_3", "451.5_GPP_DEFENSDAD_0_0_21_77", "451.5_GPP_EDUCACICA_0_0_20_34",
    "451.5_GPP_EDUCACNAR_0_0_37_61", "451.5_GPP_EDUCACRIA_0_0_27_58", "451.5_GPP_ENERGIBLE_0_0_23_75",
    "451.5_GPP_FUNCIOADO_0_0_25_67", "451.5_GPP_INDUSTRIA_0_0_13_61", "451.5_GPP_INSSJYLUD_0_0_17_29",
    "451.5_GPP_INSSJYNES_0_0_24_88", "451.5_GPP_JUSTICCIA_0_0_12_99", "451.5_GPP_OBRAS_LUD_0_0_24_92",
    "451.5_GPP_OBRAS_NES_0_0_31_54", "451.5_GPP_OTROS_COS_0_0_30_88", "451.5_GPP_OTROS_NOS_0_0_27_9",
    "451.5_GPP_PREVISIAL_0_0_20_74", "451.5_GPP_PRODUCRIA_0_0_23_78", "451.5_GPP_PROGRALEO_0_0_20_10",
    "451.5_GPP_PROMOCIAL_0_0_31_14", "451.5_GPP_SERVICCOS_0_0_24_35", "451.5_GPP_SERVICICA_0_0_27_14",
    "451.5_GPP_SERVICLES_0_0_22_89", "451.5_GPP_TRANSPRTE_0_0_14_21", "451.5_GPP_VIVIENSMO_0_0_22_46",
    "451.7_GPMGPM_0_0_3_100", "451.7_GPM_ADMINIRAL_0_0_26_42", "451.7_GPM_AGUA_AADO_0_0_23_4",
    "451.7_GPM_ASIGNARES_0_0_27_36", "451.7_GPM_ATENCILUD_0_0_18_56", "451.7_GPM_CIENCIICA_0_0_19_13",
    "451.7_GPM_COMUNINES_0_0_18_8", "451.7_GPM_CULTURURA_0_0_11_41", "451.7_GPM_DEFENSDAD_0_0_21_62",
    "451.7_GPM_EDUCACICA_0_0_20_87", "451.7_GPM_EDUCACNAR_0_0_37_82", "451.7_GPM_EDUCACRIA_0_0_27_48",
    "451.7_GPM_ENERGIBLE_0_0_23_54", "451.7_GPM_FUNCIOADO_0_0_25_0", "451.7_GPM_INDUSTRIA_0_0_13_11",
    "451.7_GPM_INSSJYLUD_0_0_17_15", "451.7_GPM_INSSJYNES_0_0_24_58", "451.7_GPM_JUSTICCIA_0_0_12_75",
    "451.7_GPM_OBRAS_LUD_0_0_24_54", "451.7_GPM_OBRAS_NES_0_0_31_60", "451.7_GPM_OTROS_COS_0_0_30_17",
    "451.7_GPM_OTROS_NOS_0_0_27_88", "451.7_GPM_PREVISIAL_0_0_20_6", "451.7_GPM_PRODUCRIA_0_0_23_8",
    "451.7_GPM_PROGRALEO_0_0_20_47", "451.7_GPM_PROMOCIAL_0_0_31_55", "451.7_GPM_SERVICCOS_0_0_24_20",
    "451.7_GPM_SERVICICA_0_0_27_93", "451.7_GPM_SERVICLES_0_0_22_88", "451.7_GPM_TRANSPRTE_0_0_14_49",
    "451.7_GPM_VIVIENSMO_0_0_22_88",
)

_AUSTRALES = "Australes"
_MILES_DE_AUSTRALES = "Miles de australes (1985-1986: millones de australes)"
_MILES_DE_PESOS = "Miles de pesos"
_GASTO_EN_PESOS = "Millones de pesos (1980-1986: miles de pesos)"

UNIDADES = {
    (FUENTE, id_): unidad
    for unidad, ids in (
        (_AUSTRALES, _IDS_TRAMO_1961_1976),
        (_MILES_DE_AUSTRALES, _IDS_TRAMO_1977_1986),
        (_MILES_DE_PESOS, _IDS_TRAMO_1987_1989),
        (MILLONES_DE_PESOS, _IDS_TRAMO_1990_1992),
        (_GASTO_EN_PESOS, _IDS_GASTO_EN_PESOS),
    )
    for id_ in ids
}


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
