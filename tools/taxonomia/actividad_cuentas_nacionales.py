"""Cuentas nacionales: PIB, oferta y demanda globales, valor agregado por rama e ingreso nacional.

Fuentes (se reconocen por ``fuente``, ``archivo`` y ``hoja``, nunca por la clasificación previa):

* ``indec-pib``, ``latest.xls``, cuadros 1 a 15 (informe técnico del PIB trimestral, base 2004).
* ``datos.gob.ar``, ``actividad.xlsx``, las hojas 1.1 a 1.9.1 de cuentas nacionales (ver ``_HOJAS``).
  Las hojas 1.10 en adelante son de otros módulos.

Árbol (Tema > Indicador > nivel 3 > nivel 4 > nivel 5)::

    Actividad > PIB > Oferta y demanda globales
                    > Formación bruta de capital fijo
                    > Por sector de actividad > Precios corrientes | Precios constantes
                    > Valor agregado bruto por rama > Precios corrientes | Precios constantes > Secciones | Divisiones y grupos
                    > Valor bruto de producción por rama > Precios corrientes | Precios constantes > Secciones | Divisiones y grupos
                    > En dólares y per cápita
                    > Series históricas > Base 1993 > Oferta y demanda globales | Por sector de actividad
    Actividad > Ingreso nacional y ahorro > Ingreso, ahorro y préstamo neto
                                          > Ahorro del resto del mundo
                                          > Series históricas > Base 1993
    Precios > Precios implícitos del PIB > Oferta y demanda globales | Valor agregado por sector
                                         | Valor agregado por rama > Secciones | Divisiones y grupos
                                         | Series históricas > Base 1993
      (la misma rama que arma ``precios_otros`` para los deflactores de ``precios.xlsx``)

Decisiones de diseño:

* «Oferta y demanda globales», «Valor agregado bruto» y «Valor bruto de producción» son
  aperturas del PIB y no indicadores propios: el PIB, la oferta global y la demanda global son
  una sola identidad contable y el mismo PIB aparece en todas las tablas; con indicadores
  separados el PIB tendría nombres distintos según la hoja. Como los grupos de «Por sector de
  actividad» y de «Valor agregado bruto por rama» superan las 150 series (corrientes y constantes
  de cuatro tablas), se abren por valoración. La formación bruta de capital fijo se separa de
  oferta y demanda por tamaño y porque es un concepto con su propio detalle.
* Los nombres siguen «PIB · componente» para la oferta y la demanda («PIB · Consumo privado»),
  «VAB · rama» y «VBP · rama» (siglas del valor agregado bruto y del valor bruto de producción:
  con el nombre completo de la rama y las medidas no entran en los 120 caracteres). El valor
  corriente va sin sufijo; «(precios constantes)», «(desestacionalizado)», «(var. % interanual)»,
  «(% del PIB)», «(en dólares)» y «(per cápita)» distinguen el resto.
* Mismo concepto, mismo nombre entre tablas y bases: la hoja 1.3.1 y el cuadro 4 del INDEC son
  la misma serie y llevan el mismo nombre. Se verificó contra los datos que el PIB, el consumo,
  la inversión, las exportaciones y las importaciones de las cuentas de ingreso y ahorro son
  idénticos a los de oferta y demanda (por eso llevan el nombre «PIB · …» aunque cuelguen de
  «Ingreso nacional y ahorro»), y que las hojas de oferta y demanda (1.1.1 y 1.5.1) coinciden con
  los cuadros 1 y 8 del INDEC.
* Las ramas se escriben con la denominación de las cuentas nacionales (CIIU rev. 3 / ClaNAE).
  Los nombres de las divisiones y grupos no repiten su sección («VAB · Fabricación de papel y de
  productos de papel»); el árbol sí la separa: «Secciones» y «Divisiones y grupos».
  La sección L se abrevia a «Administración pública y defensa» en los nombres del PIB (el nombre
  oficial completo no entra con las medidas) y se escribe completa en Precios implícitos,
  igual que en precios_otros.
* En las hojas por rama, la serie «Agricultura, ganadería, caza y servicios conexos» es en realidad
  los cultivos agrícolas: sumada con la silvicultura y los cuatro rubros pecuarios da la sección A
  (se verificó en 1.3.2 y en 1.6.2), y el valor bruto de producción la publica como «Cultivos agrícolas».
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, unir

FUENTE_INDEC = "indec-pib"
FUENTE_DATOS = "datos.gob.ar"
ARCHIVO_DATOS = "actividad.xlsx"

ACTIVIDAD = "Actividad"
PRECIOS = "Precios"

# Indicadores
PIB = "PIB"
VAB = "VAB"
VBP = "VBP"
INGRESO = "Ingreso nacional y ahorro"
IMPLICITOS = "Precios implícitos del PIB"

# Nivel 3 del PIB
OFERTA_DEMANDA = "Oferta y demanda globales"
INVERSION = "Formación bruta de capital fijo"
POR_SECTOR = "Por sector de actividad"
VAB_POR_RAMA = "Valor agregado bruto por rama"
VBP_POR_RAMA = "Valor bruto de producción por rama"
DOLARES = "En dólares y per cápita"
HISTORICAS = "Series históricas"
BASE_1993 = "Base 1993"
CORRIENTES = "Precios corrientes"
CONSTANTES_GRUPO = "Precios constantes"

# Nivel 3 de Precios implícitos del PIB (los mismos grupos que arma precios_otros)
IMP_SECTOR = "Valor agregado por sector"
IMP_RAMA = "Valor agregado por rama"

# Nivel 3 de Ingreso nacional y ahorro
CUENTA_INGRESO = "Ingreso, ahorro y préstamo neto"
RESTO_DEL_MUNDO = "Ahorro del resto del mundo"

# Medidas del nombre
CONSTANTES = "precios constantes"
DESESTACIONALIZADO = "desestacionalizado"
INTERANUAL = "var. % interanual"
PCT_PIB = "% del PIB"
EN_DOLARES = "en dólares"
PER_CAPITA = "per cápita"

# Unidades
M_CORRIENTES = "Millones de pesos"
M_2004 = "Millones de pesos de 2004"
M_1993 = "Millones de pesos de 1993"
INDICE_2004 = "Índice 2004=100"
INDICE_1993 = "Índice 1993=100"
PORCENTAJE = "%"


def _nombre(*partes: str, medida: tuple[str, ...] = ()) -> str:
    return con_medida(unir(*partes), ", ".join(medida))


# =================================================================================================
# Ramas de actividad (CIIU rev. 3 / ClaNAE, como las publican las cuentas nacionales)
# =================================================================================================
SECCIONES = {
    "A": "Agricultura, ganadería, caza y silvicultura",
    "B": "Pesca",
    "C": "Explotación de minas y canteras",
    "D": "Industria manufacturera",
    "E": "Suministro de electricidad, gas y agua",
    "F": "Construcción",
    "G": "Comercio mayorista y minorista y reparaciones",
    "H": "Hoteles y restaurantes",
    "I": "Transporte, almacenamiento y comunicaciones",
    "J": "Intermediación financiera",
    "K": "Actividades inmobiliarias, empresariales y de alquiler",
    "L": "Administración pública y defensa; planes de seguridad social de afiliación obligatoria",
    "M": "Enseñanza",
    "N": "Servicios sociales y de salud",
    "O": "Otras actividades de servicios comunitarias, sociales y personales",
    "P": "Hogares privados con servicio doméstico",
}

# Divisiones y grupos, con la sección de la que cuelgan. Cada clave es el nombre de la rama.
_SUBRAMAS = (
    ("A", "Cultivos agrícolas"),
    ("A", "Cría de animales"),
    ("A", "Servicios agrícolas y pecuarios, excepto los veterinarios"),
    ("A", "Caza, repoblación de animales de caza y servicios conexos"),
    ("A", "Construcciones agropecuarias"),
    ("A", "Silvicultura, extracción de madera y servicios conexos"),
    ("C", "Extracción de carbón, lignito, petróleo crudo y gas natural y actividades relacionadas"),
    ("C", "Extracción de minerales metalíferos; explotación de minas y canteras n.c.p"),
    ("D", "Elaboración de productos alimenticios y bebidas"),
    ("D", "Elaboración de productos de tabaco"),
    ("D", "Fabricación de productos textiles"),
    ("D", "Fabricación de prendas de vestir; terminación y teñido de pieles"),
    ("D", "Curtido y terminación de cueros; marroquinería, talabartería y calzado"),
    ("D", "Producción de madera y fabricación de productos de madera, corcho y paja, excepto muebles"),
    ("D", "Fabricación de papel y de productos de papel"),
    ("D", "Edición e impresión; reproducción de grabaciones"),
    ("D", "Fabricación de coque, productos de la refinación del petróleo y combustible nuclear"),
    ("D", "Fabricación de sustancias y productos químicos"),
    ("D", "Fabricación de productos de caucho y plástico"),
    ("D", "Fabricación de productos minerales no metálicos"),
    ("D", "Fabricación de metales comunes"),
    ("D", "Fabricación de productos elaborados de metal, excepto maquinaria y equipo"),
    ("D", "Fabricación de maquinaria y equipo n.c.p"),
    ("D", "Fabricación de maquinaria de oficina, contabilidad e informática"),
    ("D", "Fabricación de maquinaria y aparatos eléctricos n.c.p"),
    ("D", "Fabricación de equipos y aparatos de radio, televisión y comunicaciones"),
    ("D", "Fabricación de instrumentos médicos, ópticos y de precisión; fabricación de relojes"),
    ("D", "Fabricación de vehículos automotores, remolques y semirremolques"),
    ("D", "Fabricación de equipo de transporte n.c.p"),
    ("D", "Fabricación de muebles y colchones; industrias manufactureras n.c.p"),
    ("D", "Reciclamiento"),
    ("D", "Reparación, mantenimiento e instalación de máquinas y equipos"),
    ("E", "Generación, captación y distribución de energía eléctrica"),
    ("E", "Fabricación de gas; distribución de combustibles gaseosos por tuberías"),
    ("E", "Captación, depuración y distribución de agua"),
    ("H", "Hoteles; campamentos y otros tipos de hospedaje temporal"),
    ("H", "Restaurantes, bares y cantinas"),
    ("I", "Transporte"),
    ("I", "Comunicaciones"),
    ("J", "Intermediación financiera y otros servicios financieros"),
    ("J", "Servicios de seguros"),
    ("J", "Servicios auxiliares a la actividad financiera"),
    ("K", "Servicios inmobiliarios con bienes propios y arrendados"),
    ("K", "Resto de actividades inmobiliarias, empresariales y de alquiler"),
    ("M", "Enseñanza pública"),
    ("M", "Enseñanza privada"),
    ("N", "Salud pública"),
    ("N", "Salud privada"),
    ("O", "Eliminación de desperdicios y aguas residuales, saneamiento y servicios similares"),
    ("O", "Asociaciones"),
    ("O", "Servicios culturales y deportivos; otras actividades"),
)
_SECCION_DE_SUBRAMA = {nombre: seccion for seccion, nombre in _SUBRAMAS}

_TOTAL = "total"

# Cómo reconocer cada rama en la variable de las hojas por rama (1.3.2, 1.3.3, 1.6.2, 1.6.3 y
# 1.7.2). Cada hoja nombra las ramas a su manera (a veces con el prefijo de su sección, a veces
# truncadas), por eso la regla es «la primera que contiene alguno de estos fragmentos».
# Las reglas van de lo más específico a lo más general.
_REGLAS_RAMA = (
    # Totales de la tabla
    (("valor_agregado_bruto_precios_basicos", "valor_bruto_produccion_precios_basicos"), _TOTAL),
    # Sección A y sus partes
    (("agricultura_ganaderia_caza_silvicultura_total", "agri_ganad_caza_silvi_total", "total_agricultura"), "A"),
    (("agricultura_ganaderia_caza_servicios_conexos", "agri_ganad_caza_silvi_agri_ganad_caza_serv_conexos",
      "cultivos_agricolas"), "Cultivos agrícolas"),
    (("cria_animales",), "Cría de animales"),
    (("servicios_agricolas_pecuarios",), "Servicios agrícolas y pecuarios, excepto los veterinarios"),
    (("caza_repoblacion",), "Caza, repoblación de animales de caza y servicios conexos"),
    (("construccionesagropecuarias", "construcciones_agropecuarias"), "Construcciones agropecuarias"),
    (("silvicultura_madera", "silvicultura_extraccion", "silvi_extra_madera"),
     "Silvicultura, extracción de madera y servicios conexos"),
    # Sección C
    (("explotacion_minas_canteras_total", "explo_minas_cant_total", "total_explotacion_minas_canteras"), "C"),
    (("extraccion_carbon", "expl_min_cant_extr_ca_lg"),
     "Extracción de carbón, lignito, petróleo crudo y gas natural y actividades relacionadas"),
    (("extraccion_minerales_metaliferos", "explo_minas_cant_extra_mine_metal"),
     "Extracción de minerales metalíferos; explotación de minas y canteras n.c.p"),
    # Sección D
    (("industria_manufacturera_total", "ind_manufac_total", "total_industria_manufacturera"), "D"),
    (("alimenticios",), "Elaboración de productos alimenticios y bebidas"),
    (("tabaco",), "Elaboración de productos de tabaco"),
    (("textiles",), "Fabricación de productos textiles"),
    (("prendas",), "Fabricación de prendas de vestir; terminación y teñido de pieles"),
    (("curtido",), "Curtido y terminación de cueros; marroquinería, talabartería y calzado"),
    (("produccion_madera", "prod_madera", "prod_mad_"),
     "Producción de madera y fabricación de productos de madera, corcho y paja, excepto muebles"),
    (("papel",), "Fabricación de papel y de productos de papel"),
    (("edicion",), "Edición e impresión; reproducción de grabaciones"),
    (("coque",), "Fabricación de coque, productos de la refinación del petróleo y combustible nuclear"),
    (("quimicos",), "Fabricación de sustancias y productos químicos"),
    (("caucho",), "Fabricación de productos de caucho y plástico"),
    (("minerales_no_metalicos",), "Fabricación de productos minerales no metálicos"),
    (("metales_comunes",), "Fabricación de metales comunes"),
    (("elaborados_metal",), "Fabricación de productos elaborados de metal, excepto maquinaria y equipo"),
    (("maquinaria_equipo_ncp", "maquinarias_equipo_ncp", "maq_equipo_ncp"), "Fabricación de maquinaria y equipo n.c.p"),
    (("oficina",), "Fabricación de maquinaria de oficina, contabilidad e informática"),
    (("aparatos_electricos", "aparatos_electronicos"), "Fabricación de maquinaria y aparatos eléctricos n.c.p"),
    (("radio",), "Fabricación de equipos y aparatos de radio, televisión y comunicaciones"),
    (("instrumentos", "instr_medicos"), "Fabricación de instrumentos médicos, ópticos y de precisión; fabricación de relojes"),
    (("vehiculos",), "Fabricación de vehículos automotores, remolques y semirremolques"),
    (("equipo_transporte_ncp", "equipos_transporte_ncp", "equipo_transp_ncp"), "Fabricación de equipo de transporte n.c.p"),
    (("muebles",), "Fabricación de muebles y colchones; industrias manufactureras n.c.p"),
    (("reciclamiento",), "Reciclamiento"),
    (("reparacion_mantenimiento",), "Reparación, mantenimiento e instalación de máquinas y equipos"),
    # Sección E
    (("electricidad_gas_agua_total", "elec_gas_agua_total", "total_electricidad_agua_gas"), "E"),
    (("generacion_captacion",), "Generación, captación y distribución de energía eléctrica"),
    (("fabricacion_gas", "fab_gas"), "Fabricación de gas; distribución de combustibles gaseosos por tuberías"),
    (("captacion_depuracion",), "Captación, depuración y distribución de agua"),
    # Secciones B, F, G, L, P
    (("pesca",), "B"),
    (("construccion",), "F"),
    (("comercio_mayorista",), "G"),
    (("administracion_publica", "adm_pub_defensa", "admi_publica"), "L"),
    (("hogares_privados",), "P"),
    # Sección H
    (("hoteles_restaurantes_total", "hoteles_restau_total", "total_hoteles_restaurantes"), "H"),
    (("hoteles_campamentos", "hoteles_restau_hoteles_camp", "hoteles_campamentos_otros"),
     "Hoteles; campamentos y otros tipos de hospedaje temporal"),
    (("restaurantes_bares", "hoteles_restau_restau"), "Restaurantes, bares y cantinas"),
    # Sección I
    (("transporte_almacenamiento_comunicaciones_total", "transp_comu_total", "total_transporte_comunicaciones"), "I"),
    (("comunicaciones", "transp_comu_comu"), "Comunicaciones"),
    (("transporte", "transp_comu_transp"), "Transporte"),
    # Sección J
    (("intermediacion_financiera_total", "intermediacion_finan_total", "total_intermediacion_financiera"), "J"),
    (("otros_servicios_financieros", "intermed_finan_otros"), "Intermediación financiera y otros servicios financieros"),
    (("servicios_seguros", "serv_seguros"), "Servicios de seguros"),
    (("auxiliares",), "Servicios auxiliares a la actividad financiera"),
    # Sección K
    (("actividades_inmobiliarias_empresariales_alquiler_total", "act_inmobiliarias_emp_alquiler_total",
      "total_actividades_inmobiliarias"), "K"),
    (("servicios_inmobiliarios_bienes_propios", "serv_inmo_con_bienes"),
     "Servicios inmobiliarios con bienes propios y arrendados"),
    (("resto",), "Resto de actividades inmobiliarias, empresariales y de alquiler"),
    # Sección M
    (("ensenianza_total", "total_ensenianza"), "M"),
    (("ensenianza_privada", "ensenianza_ensenianza_privada"), "Enseñanza privada"),
    (("ensenianza_publica", "ensenianza_ensenianza_pub"), "Enseñanza pública"),
    # Sección N
    (("servicios_sociales_salud_total", "serv_soc_salud_total", "total_servicios_sociales"), "N"),
    (("salud_privada", "serv_soc_salud_salud_privada"), "Salud privada"),
    (("salud_publica", "serv_soc_salud_salud_pub"), "Salud pública"),
    # Sección O
    (("otras_act_serv_comunitarias_soc_pers_total", "total_otras_actividades", "otras_act_serv_comunitarias_soc_personales_serv_domestico",
      "otras_act_ser_comunitarias_soc_personales_ser_domesticos", "otras_act_comunitarias_sociales_personales_ser_domestico"), "O"),
    (("eliminacion_desperdicios", "elimin_desperdicios", "eli_des_agu"),
     "Eliminación de desperdicios y aguas residuales, saneamiento y servicios similares"),
    (("asociaciones",), "Asociaciones"),
    (("servicios_culturales", "serv_cul_deport"), "Servicios culturales y deportivos; otras actividades"),
)


def _rama(variable: str) -> str | None:
    """Clave de la rama (letra de sección, nombre de subrama o «total») a partir de la variable."""
    for fragmentos, clave in _REGLAS_RAMA:
        if any(fragmento in variable for fragmento in fragmentos):
            return clave
    return None


_ADMINISTRACION_CORTA = "Administración pública y defensa"


def _nombre_rama(clave: str) -> str:
    """Nombre de la rama en los nombres de serie. La sección L va abreviada (el nombre completo,
    con los planes de seguridad social, no entra en los 120 caracteres con las medidas)."""
    if clave == "L":
        return _ADMINISTRACION_CORTA
    return SECCIONES.get(clave, clave)


SOLO_SECCIONES = "Secciones"
DIVISIONES = "Divisiones y grupos"


def _grupo_de_rama(clave: str) -> str:
    """Nivel 5 del árbol: las secciones (con el total) por un lado y su detalle por otro. Los
    nombres de serie no repiten la sección de la que cuelga cada división (no entra en los 120
    caracteres), pero cada denominación es la oficial y no se repite entre secciones."""
    return SOLO_SECCIONES if clave == _TOTAL or clave in SECCIONES else DIVISIONES


# =================================================================================================
# Oferta y demanda globales y formación bruta de capital fijo
# =================================================================================================
# Clave del componente -> partes del nombre después de «PIB».
_ITEM_OFERTA_DEMANDA = {
    "pib": (),
    "importaciones": ("Importaciones",),
    "oferta": ("Oferta global",),
    "consumo_privado": ("Consumo privado",),
    "consumo_publico": ("Consumo público",),
    "exportaciones": ("Exportaciones",),
    "demanda": ("Demanda global",),
    "fbcf": ("Formación bruta de capital fijo",),
    "existencias": ("Variación de existencias",),
    "objetos": ("Objetos valiosos",),
    "discrepancia": ("Discrepancia estadística",),
    "existencias_discrepancia": ("Variación de existencias y discrepancia estadística",),
}
# Componentes de la inversión, con los nombres que usa precios_otros para los deflactores.
_ITEM_INVERSION = {
    "construcciones": ("Inversión en construcciones",),
    "otras_construcciones": ("Inversión en otras construcciones",),
    "equipo_durable": ("Inversión en equipos durables de producción",),
    "equipo_durable_nacional": ("Inversión en equipos durables de producción", "Nacionales"),
    "equipo_durable_importado": ("Inversión en equipos durables de producción", "Importados"),
    "maquinaria": ("Inversión en maquinaria y equipo",),
    "maquinaria_nacional": ("Inversión en maquinaria y equipo", "Nacionales"),
    "maquinaria_importado": ("Inversión en maquinaria y equipo", "Importados"),
    "transporte": ("Inversión en material de transporte",),
    "transporte_nacional": ("Inversión en material de transporte", "Nacionales"),
    "transporte_importado": ("Inversión en material de transporte", "Importados"),
    "otros_activos": ("Inversión en otros activos fijos",),
}
_ITEM_COMPONENTE = {**_ITEM_OFERTA_DEMANDA, **_ITEM_INVERSION}

# Variable de datos.gob.ar -> clave del componente.
_VARIABLE_COMPONENTE = {
    "oferta_global_pib": "pib",
    "oferta_global_importacion": "importaciones",
    "oferta_global_total": "oferta",
    "demanda_global_consumo_priv": "consumo_privado",
    "manda_global_consumo_priv": "consumo_privado",
    "demanda_global_consumo_publico": "consumo_publico",
    "demanda_global_exportacion": "exportaciones",
    "demanda_global_total": "demanda",
    "demanda_global_ibif": "fbcf",
    "demanda_global_ibif_total": "fbcf",
    "variacion_existencias": "existencias",
    "objetos_valiosos": "objetos",
    "discrepancia_estadistica": "discrepancia",
    "discrepancia_estadistica_variacion_existencias": "existencias_discrepancia",
    "demanda_global_ibif_construccion": "construcciones",
    "demanda_global_ibif_otras_construcciones": "otras_construcciones",
    "demanda_global_ibif_equipo_durable_produccion": "equipo_durable",
    "demanda_global_ibif_equipo_durable_produccion_nacional": "equipo_durable_nacional",
    "demanda_global_ibif_equipo_durable_produccion_importado": "equipo_durable_importado",
    "dg_ibif_equipo_durable_prod_maquinaria_equipo_total": "maquinaria",
    "dg_ibif_equipo_durable_prod_maquinaria_equipo_nacional": "maquinaria_nacional",
    "dg_ibif_equipo_durable_prod_maquinaria_equipo_importado": "maquinaria_importado",
    "dg_ibif_equipo_durable_prod_material_transporte_total": "transporte",
    "dg_ibif_equipo_durable_prod_material_transporte_nacional": "transporte_nacional",
    "dg_ibif_equipo_durable_prod_material_transporte_importado": "transporte_importado",
    "demanda_global_ibif_otros_activos_fijos": "otros_activos",
}

# Código de fila de los cuadros 1 a 3, 8 a 11 del INDEC -> clave del componente.
_CODIGO_OFERTA_DEMANDA = {
    "B1b": "pib",
    "P7": "importaciones",
    "oferta-global": "oferta",
    "P3_S14_S15": "consumo_privado",
    "P3_S13": "consumo_publico",
    "P6": "exportaciones",
    "demanda-global": "demanda",
    "P51b": "fbcf",
    "P52": "existencias",
    "P53": "objetos",
    "discrepancia-estadistica-4": "discrepancia",
}
# Cuadros 6, 7, 14 y 15 del INDEC (inversión). Los pares «Nacional» e «Importado» sin sufijo son
# los de maquinaria y equipo y los que llevan un código al final son los de equipo de transporte
# (se verificó con las sumas del cuadro 14: nacional + importado = maquinaria y equipo, y
# nacional-xxxx + importado-xxxx = equipo de transporte; los cuadros 6, 7 y 15 siguen el mismo orden).
_CODIGO_INVERSION = {
    "P51b": "fbcf",
    "AN111 / AN112": "construcciones",
    "AN1122": "otras_construcciones",
    "AN113 / AN114": "maquinaria",
    "AN1131": "transporte",
    "AN115 / AN1171 / AN1172": "otros_activos",
    "nacional": "maquinaria_nacional",
    "importado": "maquinaria_importado",
}


def _componente_inversion_indec(codigo: str) -> str | None:
    if codigo in _CODIGO_INVERSION:
        return _CODIGO_INVERSION[codigo]
    if re.fullmatch(r"nacional-\w+", codigo):
        return "transporte_nacional"
    if re.fullmatch(r"importado-\w+", codigo):
        return "transporte_importado"
    return None


def _nombre_pib(clave: str, medida: tuple[str, ...] = ()) -> str:
    return _nombre(PIB, *_ITEM_COMPONENTE[clave], medida=medida)


def _nombre_implicito(clave: str, medida: tuple[str, ...] = ()) -> str:
    partes = _ITEM_COMPONENTE[clave] or ("PIB",)
    return _nombre("Precios implícitos", *partes, medida=medida)


def _es_inversion(clave: str) -> bool:
    return clave in _ITEM_INVERSION


def _clasif_pib(nombre: str, cobertura: str, unidad: str, apertura: str = "", medida_nivel: str = "") -> Clasificacion:
    return Clasificacion(ACTIVIDAD, PIB, nombre, cobertura=cobertura, apertura=apertura, medida=medida_nivel, unidad=unidad)


def _clasif_implicito(nombre: str, cobertura: str, unidad: str, apertura: str = "") -> Clasificacion:
    return Clasificacion(PRECIOS, IMPLICITOS, nombre, cobertura=cobertura, apertura=apertura, unidad=unidad)


def _historica_pib(nombre: str, grupo: str, unidad: str) -> Clasificacion:
    return Clasificacion(ACTIVIDAD, PIB, nombre, cobertura=HISTORICAS, apertura=BASE_1993, medida=grupo, unidad=unidad)


# =================================================================================================
# PIB por sector de actividad (secciones, agregados e impuestos)
# =================================================================================================
# Variable de datos.gob.ar -> (clase, nombre). Clase «vab»: valor agregado de la sección o del
# agregado; «pib»: componente del PIB a precios de mercado que no es valor agregado (impuestos y
# derechos); «total»: el valor agregado bruto; «producto»: el propio PIB.
_VARIABLE_SECTOR = {
    "agricultura_ganaderia_caza_silvicultura": ("vab", SECCIONES["A"]),
    "pesca": ("vab", SECCIONES["B"]),
    "explotacion_minas_canteras": ("vab", SECCIONES["C"]),
    "industria_manufacturera": ("vab", SECCIONES["D"]),
    "suministro_electricidad_gas_agua": ("vab", SECCIONES["E"]),
    "construccion": ("vab", SECCIONES["F"]),
    "comercio_mayorista_minorista_reparaciones": ("vab", SECCIONES["G"]),
    "hoteles_restaurantes": ("vab", SECCIONES["H"]),
    "transporte_almacenamiento_comunicaciones": ("vab", SECCIONES["I"]),
    "intermediacion_financiera": ("vab", SECCIONES["J"]),
    "actividades_inmobiliarias_empresariales_alquiler": ("vab", SECCIONES["K"]),
    "admin_publica_planes_seguridad_social_afiliacion_obligatoria": ("vab", SECCIONES["L"]),
    "administracion_publica_defensa": ("vab", SECCIONES["L"]),
    "ensenianza": ("vab", SECCIONES["M"]),
    "servicios_sociales_salud": ("vab", SECCIONES["N"]),
    "otras_actividades_servicios_comunitarias_sociales_personales": ("vab", SECCIONES["O"]),
    "hogares_privs_servicio_domestico": ("vab", SECCIONES["P"]),
    "sectores_productores_bienes": ("vab", "Sectores productores de bienes"),
    "sectores_productores_servicios": ("vab", "Sectores productores de servicios"),
    "ensenianza_servicios_sociales_salud": ("vab", "Enseñanza, servicios sociales y de salud"),
    "otras_actividades_servicios_soc_personales_domestico":
        ("vab", "Otras actividades de servicios comunitarias, sociales y personales, y servicio doméstico"),
    "impuestos_productos_netos_subsidios": ("pib", "Impuestos a los productos netos de subsidios"),
    "impuesto_valor_agregado": ("pib", "Impuesto al valor agregado"),
    "derechos_importacion": ("pib", "Derechos de importación"),
    "impuesto_importaciones": ("pib", "Derechos de importación"),
    "servicios_intermediacion_financiera_medidos_indirectamente":
        ("pib", "Servicios de intermediación financiera medidos indirectamente"),
    "valor_agregado_bruto_precios_basicos": ("total", "Valor agregado bruto a precios básicos"),
    "valor_agregado_bruto_precios_productor": ("total", "Valor agregado bruto a precios de productor"),
    "producto_interno_bruto_precios_mercado": ("producto", "PIB"),
}

# Código de fila de los cuadros 4, 5, 12 y 13 del INDEC -> (clase, nombre).
_CODIGO_SECTOR = {
    **{letra: ("vab", nombre) for letra, nombre in SECCIONES.items()},
    "B1b": ("producto", "PIB"),
    "D211": ("pib", "Impuesto al valor agregado"),
    "D212": ("pib", "Derechos de importación"),
    "D214 - D31": ("pib", "Impuestos a los productos netos de subsidios"),
}


def _nombre_sector(clase: str, nombre: str, medida: tuple[str, ...] = ()) -> str:
    if clase == "producto":
        return _nombre(PIB, medida=medida)
    if clase == "total":
        return _nombre(VAB, medida=medida)
    if clase == "pib":
        return _nombre(PIB, nombre, medida=medida)
    return _nombre(VAB, _ADMINISTRACION_CORTA if nombre == SECCIONES["L"] else nombre, medida=medida)


def _nombre_sector_implicito(clase: str, nombre: str, medida: tuple[str, ...] = ()) -> str:
    return _nombre("Precios implícitos", nombre, medida=medida)


# =================================================================================================
# Reglas de datos.gob.ar (actividad.xlsx)
# =================================================================================================
def _oferta_demanda(serie: Serie, constantes: str, unidad: str, medida: tuple[str, ...],
                    historica: bool) -> Clasificacion | None:
    clave = _VARIABLE_COMPONENTE.get(serie.variable)
    if clave is None:
        return None
    nombre = _nombre_pib(clave, medida)
    if historica:
        return _historica_pib(nombre, OFERTA_DEMANDA, unidad)
    grupo = INVERSION if _es_inversion(clave) else OFERTA_DEMANDA
    return _clasif_pib(nombre, grupo, unidad)


def _hoja_1_1(serie):    # oferta y demanda, precios constantes, base 1993
    return _oferta_demanda(serie, "", M_1993, (CONSTANTES,), True)


def _hoja_1_1_1(serie):  # oferta y demanda, precios constantes, base 2004
    return _oferta_demanda(serie, "", M_2004, (CONSTANTES,), False)


def _hoja_1_2(serie):    # desestacionalizada, base 1993
    return _oferta_demanda(serie, "", M_1993, (CONSTANTES, DESESTACIONALIZADO), True)


def _hoja_1_2_1(serie):  # desestacionalizada, base 2004
    return _oferta_demanda(serie, "", M_2004, (CONSTANTES, DESESTACIONALIZADO), False)


def _hoja_1_5(serie):    # precios corrientes, base 1993
    return _oferta_demanda(serie, "", M_CORRIENTES, (), True)


def _hoja_1_5_1(serie):  # precios corrientes, base 2004
    return _oferta_demanda(serie, "", M_CORRIENTES, (), False)


def _por_sector(serie: Serie, medida: tuple[str, ...], unidad: str, historica: bool) -> Clasificacion | None:
    encontrado = _VARIABLE_SECTOR.get(serie.variable)
    if encontrado is None:
        return None
    nombre = _nombre_sector(*encontrado, medida=medida)
    if historica:
        return _historica_pib(nombre, POR_SECTOR, unidad)
    valoracion = CONSTANTES_GRUPO if CONSTANTES in medida else CORRIENTES
    return _clasif_pib(nombre, POR_SECTOR, unidad, apertura=valoracion)


def _hoja_1_3(serie):
    return _por_sector(serie, (CONSTANTES,), M_1993, True)


def _hoja_1_3_1(serie):
    return _por_sector(serie, (CONSTANTES,), M_2004, False)


def _hoja_1_6(serie):
    return _por_sector(serie, (), M_CORRIENTES, True)


def _hoja_1_6_1(serie):
    return _por_sector(serie, (), M_CORRIENTES, False)


def _por_rama(serie: Serie, grupo: str, prefijo: str, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    clave = _rama(serie.variable)
    if clave is None:
        return None
    nombre = _nombre(prefijo, medida=medida) if clave == _TOTAL else _nombre(prefijo, _nombre_rama(clave), medida=medida)
    valoracion = CONSTANTES_GRUPO if CONSTANTES in medida else CORRIENTES
    return _clasif_pib(nombre, grupo, unidad, apertura=valoracion, medida_nivel=_grupo_de_rama(clave))


def _hoja_1_3_2(serie):
    return _por_rama(serie, VAB_POR_RAMA, VAB, (CONSTANTES,), M_2004)


def _hoja_1_6_2(serie):
    return _por_rama(serie, VAB_POR_RAMA, VAB, (), M_CORRIENTES)


def _hoja_1_3_3(serie):
    return _por_rama(serie, VBP_POR_RAMA, VBP, (CONSTANTES,), M_2004)


def _hoja_1_6_3(serie):
    return _por_rama(serie, VBP_POR_RAMA, VBP, (), M_CORRIENTES)


def _hoja_1_7_2(serie):  # precios implícitos del valor agregado por rama, base 2004
    clave = _rama(serie.variable)
    if clave is None:
        return None
    if clave == _TOTAL:
        nombre = _nombre("Precios implícitos", "Valor agregado bruto a precios básicos")
    else:
        # Los deflactores llevan el nombre largo de la sección, como en precios_otros.
        nombre = _nombre("Precios implícitos", SECCIONES.get(clave, clave))
    return _clasif_implicito(nombre, IMP_RAMA, INDICE_2004, apertura=_grupo_de_rama(clave))


def _hoja_1_7_1(serie):  # precios implícitos por sector, base 2004
    encontrado = _VARIABLE_SECTOR.get(serie.variable)
    if encontrado is None:
        return None
    nombre = _nombre("Precios implícitos", "PIB" if encontrado[0] == "producto" else encontrado[1])
    return _clasif_implicito(nombre, IMP_SECTOR, INDICE_2004)


def _hoja_1_7(serie):    # precios implícitos por sector, base 1993
    encontrado = _VARIABLE_SECTOR.get(serie.variable)
    if encontrado is None:
        return None
    nombre = _nombre("Precios implícitos", "PIB" if encontrado[0] == "producto" else encontrado[1])
    return _clasif_implicito(nombre, HISTORICAS, INDICE_1993, apertura=BASE_1993)


def _hoja_1_7_1b(serie):  # precios implícitos de oferta y demanda, base 2004
    clave = _VARIABLE_COMPONENTE.get(serie.variable)
    if clave is None:
        return None
    return _clasif_implicito(_nombre_implicito(clave), OFERTA_DEMANDA, INDICE_2004)


def _hoja_1_7b(serie):   # precios implícitos de oferta y demanda, base 1993
    clave = _VARIABLE_COMPONENTE.get(serie.variable)
    if clave is None:
        return None
    return _clasif_implicito(_nombre_implicito(clave), HISTORICAS, INDICE_1993, apertura=BASE_1993)


# 1.8: PIB en dólares y per cápita (variable -> (nombre, unidad)).
_DOLARES = {
    "pib_precios_corrientes": (_nombre(PIB), M_CORRIENTES),
    "pib_precios_2004": (_nombre(PIB, medida=(CONSTANTES,)), M_2004),
    "pib_dolares_precios_corrientes": (_nombre(PIB, medida=(EN_DOLARES,)), "Millones de dólares"),
    "pib_per_capita_pesos_corrientes": (_nombre(PIB, medida=(PER_CAPITA,)), "Pesos"),
    "pib_per_capita_dolares_corrientes": (_nombre(PIB, medida=(EN_DOLARES, PER_CAPITA)), "Dólares"),
    "indice_precios_constantes": (_nombre(PIB, medida=(CONSTANTES, EN_DOLARES, PER_CAPITA)), INDICE_2004),
    # Insumos de la conversión, que la hoja publica junto con el PIB.
    "poblacion": (_nombre(PIB, "Población"), "Personas"),
    "tcn_pesos_dolares": (_nombre(PIB, "Tipo de cambio nominal"), "Pesos por dólar"),
}


def _hoja_1_8(serie):
    encontrado = _DOLARES.get(serie.variable)
    if encontrado is None:
        return None
    nombre, unidad = encontrado
    return _clasif_pib(nombre, DOLARES, unidad)


# -------------------------------------------------------------------------------------------------
# Ingreso nacional y ahorro
# -------------------------------------------------------------------------------------------------
def _ingreso(*partes: str, medida: tuple[str, ...] = ()) -> str:
    return _nombre(INGRESO, *partes, medida=medida)


# Conceptos de la cuenta de ingreso y ahorro que son los mismos de oferta y demanda (idénticos
# en los datos): llevan el nombre del PIB. El resto lleva el del indicador.
_INGRESO_COMUN = {
    "pib": _nombre_pib("pib"),
    "consumo_privado": _nombre_pib("consumo_privado"),
    "consumo_publico": _nombre_pib("consumo_publico"),
    "exportaciones": _nombre_pib("exportaciones"),
    "importaciones": _nombre_pib("importaciones"),
    "fbcf": _nombre_pib("fbcf"),
    "existencias": _nombre_pib("existencias"),
    "objetos": _nombre_pib("objetos"),
    "discrepancia": _nombre_pib("discrepancia"),
    "existencias_discrepancia": _nombre_pib("existencias_discrepancia"),
}
_ANB = _ingreso("Ahorro nacional bruto")
_RENTAS = _ingreso("Rentas netas de factores del exterior")
_TRANSFERENCIAS = _ingreso("Transferencias corrientes netas del exterior")
_PRESTAMO = _ingreso("Préstamo / endeudamiento neto")
_INB = _ingreso("Ingreso nacional bruto a precios de mercado")
_INBD = _ingreso("Ingreso nacional bruto disponible")
_CONSUMO_TOTAL = _ingreso("Consumo total, privado y público")
_AHORRO_RESTO = _ingreso("Ahorro del resto del mundo")
_AIB = _ingreso("Ahorro interno bruto")
_GANANCIA_INTERCAMBIO = "Ganancia o pérdida real del intercambio"


def _resto(*partes: str) -> str:
    return _ingreso(*partes, "Resto del mundo")


# 1.9.1 (base 2004), cuenta de ingreso, ahorro y préstamo neto. Variable -> nombre.
_INGRESO_2004 = {
    "adqui_menos_dispo_activos_no_finan_no_producidos":
        _ingreso("Adquisiciones menos disposiciones de activos no financieros no producidos"),
    "ahorro_nac_bruto": _ANB,
    "ahorro_nac_bruto_anb": _ANB,  # así se llama en la hoja trimestral
    "cons_efectivo_total_priv_pub": _CONSUMO_TOTAL,
    "cons_nac_priv": _INGRESO_COMUN["consumo_privado"],
    "cons_pub": _INGRESO_COMUN["consumo_publico"],
    "disc_estadistica": _INGRESO_COMUN["discrepancia"],
    "expo_bienes_serv": _INGRESO_COMUN["exportaciones"],
    "form_bruta_cap_fijo": _INGRESO_COMUN["fbcf"],
    "form_bruta_cap_fbc_fijo_var_exist_obj_val": _ingreso("Formación bruta de capital"),
    "ing_nac_bruto_precios_mercado": _INB,
    "ing_nac_bruto_disp_pm": _INBD,
    "impo_bienes_serv": _INGRESO_COMUN["importaciones"],
    "objetos_valiosos": _INGRESO_COMUN["objetos"],
    "otras_rentas": _ingreso("Otras rentas"),
    "pib": _INGRESO_COMUN["pib"],
    "prest_endeudamiento_neto": _PRESTAMO,
    "rentas_inv_pagar": _ingreso("Rentas de la inversión por pagar"),
    "rentas_inv_cobrar": _ingreso("Rentas de la inversión por cobrar"),
    "rentas_netas_factores_ext": _RENTAS,
    "transf_cap_netas": _ingreso("Transferencias de capital netas"),
    "transf_corr_netas_ext": _TRANSFERENCIAS,
    "var_existencias": _INGRESO_COMUN["existencias"],
    "var_patri_neto_debidas_ahorro_transf_cap":
        _ingreso("Variaciones del patrimonio neto debidas al ahorro y a las transferencias de capital"),
}

# 1.9.1 (base 2004), ahorro del resto del mundo. La cuenta está vista desde el resto del mundo:
# los saldos tienen el signo contrario al de la cuenta nacional (se verificó contra los datos),
# por eso llevan «Resto del mundo»; las exportaciones y las importaciones son las mismas.
_RESTO_2004 = {
    "adq_disp_activos_no_fcieros_no_ptivos":
        _resto("Adquisiciones menos disposiciones de activos no financieros no producidos"),
    "expo_bs_scios": _INGRESO_COMUN["exportaciones"],
    "impo_bs_scios": _INGRESO_COMUN["importaciones"],
    "prest_endeudam_neto": _resto("Préstamo / endeudamiento neto"),
    "rentas_netas_fact_ext": _resto("Rentas netas de factores del exterior"),
    # Saldo corriente con el exterior = importaciones - exportaciones + rentas - transferencias:
    # es el ahorro del resto del mundo de la base 1993 (mismo signo e igual identidad).
    "saldo_corr_ext": _AHORRO_RESTO,
    "saldo_ext_bs_scios": _resto("Saldo externo de bienes y servicios"),
    "transf_cap_net": _resto("Transferencias de capital netas"),
    "transf_corr_netas_ext": _resto("Transferencias corrientes netas del exterior"),
}

# 1.9 (base 1993), nominales.
_INGRESO_1993 = {
    "ahorro_nacional_bruto": _ANB,
    "cuenta_capital_balanza_pagos": _ingreso("Cuenta capital de la balanza de pagos"),
    "consumo_nacional": _CONSUMO_TOTAL,
    "formacion_bruta_capital_fijo": _INGRESO_COMUN["fbcf"],
    "ingreso_nacional_bruto_disponible": _INBD,
    "ingreso_nacional_bruto_precios_mercado": _INB,
    "producto_interno_bruto_precios_mercado": _INGRESO_COMUN["pib"],
    "prestamo_endeudamiento_neto": _PRESTAMO,
    "remuneracion_neta_factores_del_exterior": _RENTAS,
    "transferencias_corrientes_netas": _TRANSFERENCIAS,
    "variacion_existencias_discrepancia_estadistica": _INGRESO_COMUN["existencias_discrepancia"],
    "ahorro_interno_bruto": _AIB,
    "ahorro_resto_del_mundo": _AHORRO_RESTO,
    "discrepancia_estadistica_variacion_existencias": _INGRESO_COMUN["existencias_discrepancia"],
    "exportaciones_bienes_servicios": _INGRESO_COMUN["exportaciones"],
    "importaciones_bienes_servicios": _INGRESO_COMUN["importaciones"],
    "inversion_interna_bruta_fija": _INGRESO_COMUN["fbcf"],
    "remuneracion_neta_factores_exterior": _RENTAS,
}

# 1.9 (cont. 2), a precios constantes de 1993. La tabla trae dos bloques, el de ingreso («i_») y
# el de ahorro («a_»), que repiten cuatro conceptos con los mismos datos (ahorro nacional bruto,
# rentas, transferencias) o con el signo contrario (ganancia o pérdida del intercambio): sin
# distinguirlos dos series de la misma hoja tendrían el mismo nombre.
_EN_INGRESO = "cuenta de ingreso"
_EN_AHORRO = "cuenta de ahorro"
_INGRESO_REAL = {
    "a_ahorro_interno_bruto_real": (_AIB, ()),
    "a_ahorro_nacional_bruto_real": (_ANB, (_EN_AHORRO,)),
    "a_ahorro_real_resto_mundo": (_AHORRO_RESTO, ()),
    "a_bienes": (_ingreso(_GANANCIA_INTERCAMBIO, "Bienes"), ()),
    "a_servicios": (_ingreso(_GANANCIA_INTERCAMBIO, "Servicios"), ()),
    "a_discrepancia_estadistica_variacion_real_existencias": (_INGRESO_COMUN["existencias_discrepancia"], ()),
    "a_exportaciones_bienes_servicios_reales": (_INGRESO_COMUN["exportaciones"], ()),
    "a_ganancia_perdida_real_intercambio": (_ingreso(_GANANCIA_INTERCAMBIO), (_EN_AHORRO,)),
    "a_importaciones_bienes_servicios_reales": (_INGRESO_COMUN["importaciones"], ()),
    "a_inversion_interna_bruta_real_fija": (_INGRESO_COMUN["fbcf"], ()),
    "a_remuneracion_real_neta_factores_exterior": (_RENTAS, (_EN_AHORRO,)),
    "a_transferencias_corrientes_reales_netas": (_TRANSFERENCIAS, (_EN_AHORRO,)),
    "i_ahorro_nacional_bruto_real": (_ANB, (_EN_INGRESO,)),
    "i_consumo_total_real_priv_publico": (_CONSUMO_TOTAL, ()),
    "i_emuneracion_real_neta_factores_del_exterior": (_RENTAS, (_EN_INGRESO,)),
    "i_ganancia_perdida_real_del_intercambio": (_ingreso(_GANANCIA_INTERCAMBIO), (_EN_INGRESO,)),
    "i_ingreso_interno_bruto_real": (_ingreso("Ingreso interno bruto"), ()),
    "i_ingreso_nacional_bruto_real": (_INB, ()),
    "i_ingreso_nacional_disponible_bruto_real": (_INBD, ()),
    "i_producto_interno_bruto_precios_constantes": (_INGRESO_COMUN["pib"], ()),
    "i_transferencias_corrientes_reales_netas": (_TRANSFERENCIAS, (_EN_INGRESO,)),
}


def _clasif_ingreso(nombre: str, cobertura: str, unidad: str, apertura: str = "") -> Clasificacion:
    return Clasificacion(ACTIVIDAD, INGRESO, nombre, cobertura=cobertura, apertura=apertura, unidad=unidad)


def _hoja_1_9_1(serie):
    nombre = _INGRESO_2004.get(serie.variable)
    return None if nombre is None else _clasif_ingreso(nombre, CUENTA_INGRESO, M_CORRIENTES)


def _hoja_1_9_1_resto(serie):
    nombre = _RESTO_2004.get(serie.variable)
    return None if nombre is None else _clasif_ingreso(nombre, RESTO_DEL_MUNDO, M_CORRIENTES)


def _hoja_1_9(serie):
    nombre = _INGRESO_1993.get(serie.variable)
    return None if nombre is None else _clasif_ingreso(nombre, HISTORICAS, M_CORRIENTES, apertura=BASE_1993)


def _hoja_1_9_real(serie):
    encontrado = _INGRESO_REAL.get(serie.variable)
    if encontrado is None:
        return None
    nombre, calificativo = encontrado
    return _clasif_ingreso(con_medida(nombre, ", ".join((CONSTANTES, *calificativo))), HISTORICAS, M_1993,
                           apertura=BASE_1993)


_HOJAS = {
    "1.1 OyD real": _hoja_1_1,
    "1.1.1 OyD real BASE 2004": _hoja_1_1_1,
    "1.2 OyD real s.e.": _hoja_1_2,
    "1.2.1 OyD real s.e. BASE 2004": _hoja_1_2_1,
    "1.3 PIB real x sector": _hoja_1_3,
    "1.3.1 PIB real x sector BASE 04": _hoja_1_3_1,
    "1.3.2 VAB real x rama 2004": _hoja_1_3_2,
    "1.3.3 VBP real x rama 2004": _hoja_1_3_3,
    "1.5 OyD corr.": _hoja_1_5,
    "1.5.1 OyD corr. 2004": _hoja_1_5_1,
    "1.6 Pib corriente x sector": _hoja_1_6,
    "1.6.1 Pib corr x sector 2004": _hoja_1_6_1,
    "1.6.2 VAB corriente x rama 2004": _hoja_1_6_2,
    "1.6.3 VBP corriente x rama 2004": _hoja_1_6_3,
    "1.7 Precios impl": _hoja_1_7,
    "1.7.1 Precios impl BASE 2004": _hoja_1_7_1,
    "1.7.1b Precios impl BASE 2004": _hoja_1_7_1b,
    "1.7.2 Precios impl. x rama 2004": _hoja_1_7_2,
    "1.7b Precios impl": _hoja_1_7b,
    "1.8 PIB US$": _hoja_1_8,
    "1.9 Ingreso Nac": _hoja_1_9,
    "1.9 Ingreso Nac (cont. 1)": _hoja_1_9,
    "1.9 Ingreso Nac (cont. 2)": _hoja_1_9_real,
    "1.9.1 Ingreso Nac BASE 2004": _hoja_1_9_1,
    "1.9.1 Ing Nac (cont. 1) BASE 04": _hoja_1_9_1_resto,
}


# =================================================================================================
# Reglas del INDEC (informe técnico del PIB trimestral, ``latest.xls``)
# =================================================================================================
def _codigo(serie: Serie) -> str:
    """Código de la fila del cuadro: «cuadro-4|D211» -> «D211»."""
    return serie.id.split("|", 1)[1] if "|" in serie.id else ""


def _cuadro_oferta_demanda(serie: Serie, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    clave = _CODIGO_OFERTA_DEMANDA.get(_codigo(serie))
    if clave is None:
        return None
    return _clasif_pib(_nombre_pib(clave, medida), OFERTA_DEMANDA, unidad)


def _cuadro_inversion(serie: Serie, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    clave = _componente_inversion_indec(_codigo(serie))
    if clave is None:
        return None
    return _clasif_pib(_nombre_pib(clave, medida), INVERSION, unidad)


def _cuadro_sector(serie: Serie, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    encontrado = _CODIGO_SECTOR.get(_codigo(serie))
    if encontrado is None:
        return None
    valoracion = CONSTANTES_GRUPO if CONSTANTES in medida else CORRIENTES
    return _clasif_pib(_nombre_sector(*encontrado, medida=medida), POR_SECTOR, unidad, apertura=valoracion)


def _cuadro_implicito_oferta_demanda(serie: Serie, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    clave = _CODIGO_OFERTA_DEMANDA.get(_codigo(serie))
    if clave is None:
        return None
    return _clasif_implicito(_nombre_implicito(clave, medida), OFERTA_DEMANDA, unidad)


def _cuadro_implicito_inversion(serie: Serie, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    clave = _componente_inversion_indec(_codigo(serie))
    if clave is None:
        return None
    return _clasif_implicito(_nombre_implicito(clave, medida), OFERTA_DEMANDA, unidad)


def _cuadro_implicito_sector(serie: Serie, medida: tuple[str, ...], unidad: str) -> Clasificacion | None:
    encontrado = _CODIGO_SECTOR.get(_codigo(serie))
    if encontrado is None:
        return None
    return _clasif_implicito(_nombre_sector_implicito(*encontrado, medida=medida), IMP_SECTOR, unidad)


# Cuadro -> (regla, medida, unidad).
_CUADROS = {
    "cuadro 1": (_cuadro_oferta_demanda, (CONSTANTES,), M_2004),
    "cuadro 2": (_cuadro_oferta_demanda, (CONSTANTES, INTERANUAL), PORCENTAJE),
    "cuadro 3": (_cuadro_oferta_demanda, (CONSTANTES, PCT_PIB), PORCENTAJE),
    "cuadro 4": (_cuadro_sector, (CONSTANTES,), M_2004),
    "cuadro 5": (_cuadro_sector, (CONSTANTES, INTERANUAL), PORCENTAJE),
    "cuadro 6": (_cuadro_inversion, (CONSTANTES,), M_2004),
    "cuadro 7": (_cuadro_inversion, (CONSTANTES, INTERANUAL), PORCENTAJE),
    "cuadro 8": (_cuadro_oferta_demanda, (), M_CORRIENTES),
    "cuadro 9": (_cuadro_implicito_oferta_demanda, (), INDICE_2004),
    "cuadro 10": (_cuadro_implicito_oferta_demanda, (INTERANUAL,), PORCENTAJE),
    "cuadro 11": (_cuadro_oferta_demanda, (PCT_PIB,), PORCENTAJE),
    "cuadro 12": (_cuadro_sector, (), M_CORRIENTES),
    "cuadro 13": (_cuadro_implicito_sector, (), INDICE_2004),
    "cuadro 14": (_cuadro_inversion, (), M_CORRIENTES),
    "cuadro 15": (_cuadro_implicito_inversion, (), INDICE_2004),
}


def _clasificar_indec(serie: Serie) -> Clasificacion | None:
    definicion = _CUADROS.get(serie.hoja)
    if definicion is None:
        return None
    regla, medida, unidad = definicion
    return regla(serie, medida, unidad)


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == FUENTE_INDEC:
        return _clasificar_indec(serie)
    if serie.fuente == FUENTE_DATOS and serie.archivo == ARCHIVO_DATOS:
        regla = _HOJAS.get(serie.hoja)
        return regla(serie) if regla else None
    return None
