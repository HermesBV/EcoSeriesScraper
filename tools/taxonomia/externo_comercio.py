"""Reglas de comercio exterior, salvo las grandes tablas por NCM, país y provincia.

Cubre la fuente ``indec-ica`` y, de ``datos.gob.ar``, las hojas de ``sector_externo.xlsx``
(1, 2, 3, 10, 11, 12, 13, 17, 18, 19, 34, 35, 38, 39, 40 y 41) y de ``precios.xlsx`` (4.9.1,
4.9.2 y 4.9.4). La 4.9.3 la clasifica ``precios_otros``; acá se usa su mismo indicador y su
misma forma de nombre.

Árbol (tema «Sector externo» salvo indicación):

    Intercambio comercial argentino
    ├── Exportaciones
    │   ├── Total                       ICA (hoja 1 e ``indec-ica``) y total de la hoja 2
    │   ├── Por grandes rubros          Productos primarios, MOA, MOI, combustibles y energía
    │   ├── Por rubro                   hoja 2, con un grupo por gran rubro
    │   └── Por intensidad tecnológica  hoja 19, un grupo por clasificación (Lall, UNCTAD)
    ├── Importaciones
    │   ├── Total
    │   ├── Por uso económico           hojas 1 y 3
    │   ├── Por grupos de usos económicos   las sumas que publica la hoja 1
    │   └── Por intensidad tecnológica
    └── Saldo comercial > Total

    Índices de precios y cantidades del comercio exterior
    ├── (índices generales)             hojas 10 a 13, 4.9.1 y 4.9.2
    ├── Por grandes rubros              exportaciones (hoja 10)
    ├── Por uso económico               importaciones (hoja 11)
    (hoja 4.9.4: rotulada «Base 1993» pero en base 2004; mismo nombre que la hoja 13)

    Turismo internacional               hoja 17: Receptivo, Emisivo y Saldo
    Complejos exportadores de oleaginosas y cereales   hojas 34 y 35
    Empresas exportadoras               hojas 39 y 40 (y los totales de la 41)
    Índices de difusión de las exportaciones           hoja 41

    Precios > IPMP                      hoja 18
    Precios > Precios FOB oficiales     hoja 38

Lo que los datos mostraron y no dicen los rótulos de la fuente:

* ``indec-ica``: «Var.% mensual» es la variación respecto del mismo mes del año anterior (empieza
  en enero de 1991 y 23,47 % coincide con 10.036 / 8.128 de septiembre de 2025) y «Var.% acumulada»
  compara el acumulado del año con el del año anterior. «Saldo - Var.% acumulada» es en realidad
  el saldo mensual (10.036 − 7.060 = 2.976, el valor de la serie).
* Hoja 10: «Agro» e «Industria» son las manufacturas de origen agropecuario e industrial. Los
  cuatro rubros reproducen exactamente el índice de valor general con ponderaciones 34,4 % (agro),
  27,8 % (industria), 19,8 % (primarios) y 17,9 % (combustibles), las de 2004.
* Hoja 12: las descripciones de las variaciones trimestrales están cruzadas. Contra los índices:
  ``VAP`` es la variación interanual de los precios de exportación y ``VMP`` la trimestral;
  en importaciones y términos del intercambio ``VPP1`` y ``VPTI1`` son las trimestrales y
  ``VPP2`` y ``VPTI2`` las interanuales (al revés de lo que dicen sus descripciones).
* Hoja 13: ``VPA`` es la variación absoluta anual de la ganancia o pérdida del intercambio.
* Hojas 39 y 40: los rótulos de los rangos de exportación están corridos por celdas combinadas.
  Se reasignaron por los valores: las cantidades por rango suman el total de empresas (5.738 en
  agosto de 2018) y el monto por rango dividido por la cantidad da un promedio dentro del
  rango (74 M / 1.023 empresas = 0,07 M; 9.928 M / 54 = 184 M).
* Hoja 40 y los totales de la 39: son acumulados de los últimos 12 meses (el de agosto de 2018
  es 59.611 M contra 60.453 M de la suma de los 12 meses del ICA).
* Hoja 1: «Bienes de consumo y vehículos automotores de pasajeros» (IIBCVAPR) incluye también el
  resto: es IIBCO + IIVAP + IIR en los 417 meses.
* Hoja 4.9.4: rotulada «Base 1993», está en base 2004 (ver ``UNIDADES``). Hoja 18: el IPMP vigente
  tiene base diciembre de 2001.
* Hoja 38: las dos variantes de arroz «los demás no parbolizados» son copias de las «no parbolizado»
  (ver ``EXCLUIDAS``); la cebada «no cervecera» es la que el rótulo marca «(no)» y su par embolsado.
* Hoja 41: el «índice de difusión» es la proporción de empresas, destinos o productos con
  crecimiento interanual (3.499 / 5.738 = 0,6098).
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

EXTERNO = "Sector externo"
PRECIOS = "Precios"

# Unidades
MILLONES_USD = "Millones de dólares"
PORCENTAJE = "%"
INDICE_2004 = "Índice 2004=100"
USD_2004 = "Millones de dólares de 2004"
EMPRESAS = "Empresas"
PROPORCION = "Proporción"

# Indicadores
INTERCAMBIO = "Intercambio comercial argentino"
INDICES_COMERCIO = "Índices de precios y cantidades del comercio exterior"
TURISMO = "Turismo internacional"
GRANOS = "Complejos exportadores de oleaginosas y cereales"
EMPRESAS_EXPORTADORAS = "Empresas exportadoras"
DIFUSION = "Índices de difusión de las exportaciones"
IPMP = "IPMP"
FOB_OFICIALES = "Precios FOB oficiales"

# Vocabulario de grupos
EXPORTACIONES = "Exportaciones"
IMPORTACIONES = "Importaciones"
SALDO = "Saldo comercial"
TOTAL = "Total"
POR_GRANDES_RUBROS = "Por grandes rubros"
POR_RUBRO = "Por rubro"
POR_USO = "Por uso económico"
POR_GRUPOS_DE_USOS = "Por grupos de usos económicos"
POR_INTENSIDAD = "Por intensidad tecnológica"
POR_RANGO = "Por rango de exportación"
POR_SECTOR = "Por sector de actividad"
SERIES_HISTORICAS = "Series históricas"

# Medidas
INTERANUAL = "var. % interanual"
TRIMESTRAL = "var. % trimestral"
ACUMULADA = "var. % acumulada"


# =================================================================================================
# Clasificadores compartidos
# =================================================================================================
PRIMARIOS = "Productos primarios"
MOA = "Manufacturas de origen agropecuario"
MOI = "Manufacturas de origen industrial"
COMBUSTIBLES_Y_ENERGIA = "Combustibles y energía"

# Usos económicos (nomenclatura del ICA)
BIENES_DE_CAPITAL = "Bienes de capital"
BIENES_INTERMEDIOS = "Bienes intermedios"
COMBUSTIBLES_Y_LUBRICANTES = "Combustibles y lubricantes"
PIEZAS_Y_ACCESORIOS = "Piezas y accesorios para bienes de capital"
BIENES_DE_CONSUMO = "Bienes de consumo"
VEHICULOS = "Vehículos automotores de pasajeros"
RESTO = "Resto"

# Sectores de actividad (secciones A, B, C, D y G de la CIIU, con la grafía del clasificador)
SECTOR_A = "Agricultura, ganadería, caza y silvicultura"
SECTOR_B = "Pesca"
SECTOR_C = "Explotación de minas y canteras"
SECTOR_D = "Industria manufacturera"
SECTOR_G = "Comercio al por mayor y al por menor"
SECTOR_RESTO = "Resto"
SECTOR_SIN_CLASIFICAR = "Sin clasificar"

# Rangos de exportación anual por empresa
R_0 = "Con exportaciones de menos de USD 0,1 M"
R_1 = "Con exportaciones entre USD 0,1 M y USD 1 M"
R_2 = "Con exportaciones entre USD 1 M y USD 5 M"
R_3 = "Con exportaciones entre USD 5 M y USD 20 M"
R_4 = "Con exportaciones entre USD 20 M y USD 100 M"
R_5 = "Con exportaciones entre USD 100 M y USD 500 M"
R_6 = "Con exportaciones de más de USD 500 M"
R_MAS_100 = "Con exportaciones de más de USD 100 M"
R_MENOS_100 = "Con exportaciones de menos de USD 100 M"
R_MAS_1 = "Con exportaciones de más de USD 1 M"
R_MENOS_1 = "Con exportaciones de menos de USD 1 M"

# Condición de la empresa según su saldo comercial
CONDICIONES = {
    "superavit": "Con saldo superavitario",
    "deficit": "Con saldo deficitario",
    "equilibr": "Con saldo equilibrado",
}


def _clasificacion(tema, indicador, nombre, cobertura="", apertura="", medida="", unidad=""):
    return Clasificacion(tema, indicador, nombre, cobertura=cobertura, apertura=apertura,
                         medida=medida, unidad=unidad)


# =================================================================================================
# indec-ica
# =================================================================================================
_ICA_INDEC_MEDIDAS = {
    "Total mensual": ("", MILLONES_USD),
    "Total acumulado": ("acumulado del año", MILLONES_USD),
    "Var.% mensual": (INTERANUAL, PORCENTAJE),
    "Var.% acumulada": (ACUMULADA, PORCENTAJE),
}
_ICA_INDEC_FLUJOS = {"Exportaciones": EXPORTACIONES, "Importaciones": IMPORTACIONES, "Saldo": SALDO}


def _indec_ica(serie: Serie) -> Clasificacion | None:
    texto = f"{serie.columna} - {serie.nombre}"
    encontrado = re.search(r"\b(Exportaciones|Importaciones|Saldo) - (Total mensual|Total acumulado|"
                           r"Var\.% mensual|Var\.% acumulada)\b", texto)
    if not encontrado:
        return None
    flujo, cual = _ICA_INDEC_FLUJOS[encontrado.group(1)], encontrado.group(2)
    medida, unidad = _ICA_INDEC_MEDIDAS[cual]
    if flujo == SALDO:
        # El rótulo dice «Var.% acumulada» pero es el saldo del mes (exportaciones - importaciones).
        if cual != "Var.% acumulada":
            return None
        medida, unidad = "", MILLONES_USD
    return _clasificacion(EXTERNO, INTERCAMBIO, con_medida(flujo, medida), cobertura=flujo,
                          apertura=TOTAL, unidad=unidad)


# =================================================================================================
# Hoja 1. ICA, hoja 2. X Rubro y hoja 3. M usos
# =================================================================================================
def _codigo_id(serie: Serie, patron: str) -> str | None:
    encontrado = re.match(patron, serie.id)
    return encontrado.group(1) if encontrado else None


def _total(flujo: str) -> Clasificacion:
    return _clasificacion(EXTERNO, INTERCAMBIO, flujo, cobertura=flujo, apertura=TOTAL,
                          unidad=MILLONES_USD)


def _gran_rubro(nombre: str) -> Clasificacion:
    return _clasificacion(EXTERNO, INTERCAMBIO, unir(EXPORTACIONES, nombre), cobertura=EXPORTACIONES,
                          apertura=POR_GRANDES_RUBROS, unidad=MILLONES_USD)


def _uso(nombre: str) -> Clasificacion:
    return _clasificacion(EXTERNO, INTERCAMBIO, unir(IMPORTACIONES, nombre), cobertura=IMPORTACIONES,
                          apertura=POR_USO, unidad=MILLONES_USD)


def _grupo_de_usos(nombre: str) -> Clasificacion:
    return _clasificacion(EXTERNO, INTERCAMBIO, unir(IMPORTACIONES, nombre), cobertura=IMPORTACIONES,
                          apertura=POR_GRUPOS_DE_USOS, unidad=MILLONES_USD)


_ICA = {
    "IET": _total(EXPORTACIONES),
    "IEPP": _gran_rubro(PRIMARIOS),
    "IEMOA": _gran_rubro(MOA),
    "IEMOI": _gran_rubro(MOI),
    "IECE": _gran_rubro(COMBUSTIBLES_Y_ENERGIA),
    "IIT": _total(IMPORTACIONES),
    "IIBCA": _uso(BIENES_DE_CAPITAL),
    "IIBI": _uso(BIENES_INTERMEDIOS),
    "IICL": _uso(COMBUSTIBLES_Y_LUBRICANTES),
    "IIPABC": _uso(PIEZAS_Y_ACCESORIOS),
    "IIBCO": _uso(BIENES_DE_CONSUMO),
    "IIVAP": _uso(VEHICULOS),
    "IIR": _uso(RESTO),
    # Sumas de usos que publica el ICA.
    "IBCPP": _grupo_de_usos("Bienes de capital, piezas y accesorios"),
    "IBICL": _grupo_de_usos("Bienes intermedios, combustibles y lubricantes"),
    # La descripción de la fuente omite «resto»: IIBCVAPR = IIBCO + IIVAP + IIR exacto en los 417 meses
    # (y IIT = IBCPP + IBICL + IIBCVAPR); el mnemónico (BC + VAP + R) también lo dice.
    "IIBCVAPR": _grupo_de_usos("Bienes de consumo, vehículos automotores de pasajeros y resto"),
    "SC": _total(SALDO),
    "ISC": _total(SALDO),
}


def _ica(serie: Serie) -> Clasificacion | None:
    codigo = _codigo_id(serie, r"^74\.\d_([A-Z]+)_0_[ATM]_\d+$")
    return _ICA.get(codigo)


# Hoja 3: la misma nomenclatura de usos, con el código del ID.
_USOS = {
    "IBCA": BIENES_DE_CAPITAL,
    "IBC": BIENES_DE_CONSUMO,
    "IBI": BIENES_INTERMEDIOS,
    "IC": COMBUSTIBLES_Y_LUBRICANTES,
    "IPABC": PIEZAS_Y_ACCESORIOS,
    "IR": RESTO,
    "IVAP": VEHICULOS,
}


def _importaciones_por_uso(serie: Serie) -> Clasificacion | None:
    codigo = _codigo_id(serie, r"^76\.\d_([A-Z]+)_0_[ATM]_\d+$")
    if codigo == "ITG":
        return _total(IMPORTACIONES)
    return _uso(_USOS[codigo]) if codigo in _USOS else None


# Hoja 2: variable (sin los prefijos «ica_» y «exportaciones_») → (gran rubro, rubro)
_RUBROS = {
    # Productos primarios
    "animales_vivos": (PRIMARIOS, "Animales vivos"),
    "pescados_mariscos": (PRIMARIOS, "Pescados y mariscos sin elaborar"),
    "miel": (PRIMARIOS, "Miel"),
    "hortalizas_legumbres_sin_elaborar": (PRIMARIOS, "Hortalizas y legumbres sin elaborar"),
    "frutas_frescas": (PRIMARIOS, "Frutas frescas"),
    "cereales": (PRIMARIOS, "Cereales"),
    "semillas_frutos_oleaginosos": (PRIMARIOS, "Semillas y frutos oleaginosos"),
    "tabaco_sin_elaborar": (PRIMARIOS, "Tabaco sin elaborar"),
    "lanas_sucias": (PRIMARIOS, "Lanas sucias"),
    "fibra_algodon": (PRIMARIOS, "Fibras de algodón"),
    "mineral_cobre_concentrados": (PRIMARIOS, "Minerales de cobre y sus concentrados"),
    "resto_primarios": (PRIMARIOS, "Resto de productos primarios"),
    # Manufacturas de origen agropecuario
    "carnes": (MOA, "Carnes"),
    "pescados_mariscos_elaborados": (MOA, "Pescados y mariscos elaborados"),
    "productos_lacteos_huevos": (MOA, "Productos lácteos y huevos"),
    "otros_productos_origen_animal": (MOA, "Otros productos de origen animal"),
    "frutas_secas_procesadas": (MOA, "Frutas secas o procesadas"),
    "cafe_te_yerba_mate_especias": (MOA, "Café, té, yerba mate y especias"),
    "productos_molineria": (MOA, "Productos de molinería"),
    "grasas_aceites": (MOA, "Grasas y aceites"),
    "azucar_articulos_confiteria": (MOA, "Azúcar y artículos de confitería"),
    "hortalizas_legumbres_frutas_preparadas": (MOA, "Hortalizas, legumbres y frutas preparadas"),
    "bebidas_liquidos_alcoholicos_vinagre": (MOA, "Bebidas, líquidos alcohólicos y vinagre"),
    "residuos_desperdicios_industria_alimenticia":
        (MOA, "Residuos y desperdicios de la industria alimenticia"),
    "extractos_curtientes_tintor": (MOA, "Extractos curtientes y tintóreos"),
    "pieles_cueros": (MOA, "Pieles y cueros"),
    "lanas_elaboradas": (MOA, "Lanas elaboradas"),
    "resto_moa": (MOA, "Resto de manufacturas de origen agropecuario"),
    # Manufacturas de origen industrial
    "productos_quimicos_conexos": (MOI, "Productos químicos y conexos"),
    "materias_plasticas_artificiales": (MOI, "Materias plásticas y artificiales"),
    "caucho_sus_manufacturas": (MOI, "Caucho y sus manufacturas"),
    "manufacturas_cuero_marroquineria": (MOI, "Manufacturas de cuero y marroquinería"),
    "papel_carton_impresos_publicaciones": (MOI, "Papel, cartón, impresos y publicaciones"),
    "textiles_confecciones": (MOI, "Textiles y confecciones"),
    "calzado_partes_componentes": (MOI, "Calzado y sus partes componentes"),
    "manufacturas_piedra_yeso": (MOI, "Manufacturas de piedra, yeso y similares"),
    "piedras_metales_preciosos": (MOI, "Piedras y metales preciosos"),
    "metales_comunes_manufacturas": (MOI, "Metales comunes y sus manufacturas"),
    "maquinas_aparatos_materiales_electricos": (MOI, "Máquinas, aparatos y materiales eléctricos"),
    "material_transporte_terrestre": (MOI, "Material de transporte terrestre"),
    "vehiculos_navegacion_aerea_maritima_fluvial":
        (MOI, "Vehículos de navegación aérea, marítima y fluvial"),
    "resto_moi": (MOI, "Resto de manufacturas de origen industrial"),
    # Combustibles y energía
    "petroleo_crudo": (COMBUSTIBLES_Y_ENERGIA, "Petróleo crudo"),
    "carburantes_grasas_aceites_lubricantes":
        (COMBUSTIBLES_Y_ENERGIA, "Carburantes, grasas y aceites lubricantes"),
    "gas_petroleo_otros_hidrocarburos": (COMBUSTIBLES_Y_ENERGIA, "Gas de petróleo y otros hidrocarburos"),
    "energia_electrica": (COMBUSTIBLES_Y_ENERGIA, "Energía eléctrica"),
    "resto_combustibles": (COMBUSTIBLES_Y_ENERGIA, "Resto de combustibles"),
}
_TOTALES_RUBROS = {
    "total_general": _total(EXPORTACIONES),
    "total_productos_primarios": _gran_rubro(PRIMARIOS),
    "total_moa": _gran_rubro(MOA),
    "total_moi": _gran_rubro(MOI),
    "total_combustibles_energia": _gran_rubro(COMBUSTIBLES_Y_ENERGIA),
}


def _exportaciones_por_rubro(serie: Serie) -> Clasificacion | None:
    clave = re.sub(r"^ica_", "", serie.variable)
    clave = re.sub(r"^exportaciones_", "", clave)
    if clave in _TOTALES_RUBROS:
        return _TOTALES_RUBROS[clave]
    if clave not in _RUBROS:
        return None
    gran_rubro, rubro = _RUBROS[clave]
    return _clasificacion(EXTERNO, INTERCAMBIO, unir(EXPORTACIONES, rubro), cobertura=EXPORTACIONES,
                          apertura=POR_RUBRO, medida=gran_rubro, unidad=MILLONES_USD)


# =================================================================================================
# Hoja 19. Exportaciones e importaciones por intensidad tecnológica
# =================================================================================================
LALL = "Clasificación de Lall"
UNCTAD = "Clasificación de la UNCTAD"

_L_RECURSOS = "Manufacturas basadas en recursos naturales"
_L_BAJA = "Manufacturas de baja tecnología"
_L_MEDIA = "Manufacturas de media tecnología"
_L_ALTA = "Manufacturas de alta tecnología"
_U_MANUFACTURAS = "Productos manufacturados"
_U_MANO_DE_OBRA = "Mano de obra intensiva y uso intensivo de recursos"
_U_BAJA = "Baja especialización"
_U_MEDIA = "Media especialización"
_U_ALTA = "Alta especialización"
_U_ELECTRONICOS = "Electrónicos, excepto partes y componentes"
_U_PARTES = "Partes y componentes eléctricos y electrónicos"
_U_OTROS = "Otros, excepto electrónicos"

# Variable → (clasificación, camino). Los caminos se verificaron con los valores: cada total es la
# suma de sus hijos y los rubros suman el total de exportaciones o de importaciones.
_INTENSIDAD = {
    # Exportaciones, Lall
    "lall_exportaciones_productos_primarios": (EXPORTACIONES, LALL, (PRIMARIOS,)),
    "lall_expo_total_manufct_basadas_en_rec_nat": (EXPORTACIONES, LALL, (_L_RECURSOS,)),
    "lall_expo_manufct_basadas_en_rec_nat_basadas_en_agro":
        (EXPORTACIONES, LALL, (_L_RECURSOS, "Basadas en el agro")),
    "lall_expo_manufct_basadas_en_rec_nat_otras": (EXPORTACIONES, LALL, (_L_RECURSOS, "Otras")),
    "lall_exportaciones_total_manufacturas_baja_tecnologia": (EXPORTACIONES, LALL, (_L_BAJA,)),
    "lall_x_manu_baja_tecno_textil_indument_calzado_textil":
        (EXPORTACIONES, LALL, (_L_BAJA, "Textil, indumentaria y calzado")),
    "lall_expo_manufct_baja_tecno_otras_otras": (EXPORTACIONES, LALL, (_L_BAJA, "Otras")),
    "lall_exportaciones_total_manufacturas_media_tecnologia": (EXPORTACIONES, LALL, (_L_MEDIA,)),
    "lall_expo_manufct_media_tecno_automotor": (EXPORTACIONES, LALL, (_L_MEDIA, "Automotor")),
    "lall_expo_manufct_media_tecno_procesos": (EXPORTACIONES, LALL, (_L_MEDIA, "Procesos")),
    "lall_expo_manufct_media_tecno_ingenieria": (EXPORTACIONES, LALL, (_L_MEDIA, "Ingeniería")),
    "lall_exportaciones_total_manufacturas_alta_tecnologia": (EXPORTACIONES, LALL, (_L_ALTA,)),
    "lall_expo_manufct_alta_tecno_electronicas_electricas_":
        (EXPORTACIONES, LALL, (_L_ALTA, "Electrónicas y eléctricas")),
    "lall_exportaciones_manufacturas_alta_tecnologia_otras_": (EXPORTACIONES, LALL, (_L_ALTA, "Otras")),
    "lall_exportaciones_otras": (EXPORTACIONES, LALL, ("Otras",)),
    "lall_total_exportaciones": (EXPORTACIONES, LALL, (TOTAL,)),
    # Importaciones, Lall
    "lall_importaciones_total_productos_primarios": (IMPORTACIONES, LALL, (PRIMARIOS,)),
    "lall_impo_manufct_basadas_en_rec_nat": (IMPORTACIONES, LALL, (_L_RECURSOS,)),
    "lall_impo_manufct_basadas_en_rec_nat_basadas_en_agro":
        (IMPORTACIONES, LALL, (_L_RECURSOS, "Basadas en el agro")),
    "lall_impo_manufct_basadas_en_rec_nat_otras": (IMPORTACIONES, LALL, (_L_RECURSOS, "Otras")),
    "lall_importaciones_total_manufacturas_baja_tecnologia": (IMPORTACIONES, LALL, (_L_BAJA,)),
    "lall_m_manu_baja_tecno_textil_indument_calzado_textil":
        (IMPORTACIONES, LALL, (_L_BAJA, "Textil, indumentaria y calzado")),
    "lall_importaciones_manufacturas_baja_tecnologia_otras": (IMPORTACIONES, LALL, (_L_BAJA, "Otras")),
    "lall_importaciones_total_manufacturas_media_tecnologia": (IMPORTACIONES, LALL, (_L_MEDIA,)),
    "lall_impo_manufct_media_tecno_automotor": (IMPORTACIONES, LALL, (_L_MEDIA, "Automotor")),
    "lall_impo_manufct_media_tecno_procesos": (IMPORTACIONES, LALL, (_L_MEDIA, "Procesos")),
    "lall_impo_manufct_media_tecno_ingenieria": (IMPORTACIONES, LALL, (_L_MEDIA, "Ingeniería")),
    "lall_importaciones_manufacturas_alta_tecnologia": (IMPORTACIONES, LALL, (_L_ALTA,)),
    "lall_impo_manufct_alta_tecno_electronicas_electricas":
        (IMPORTACIONES, LALL, (_L_ALTA, "Electrónicas y eléctricas")),
    "lall_importaciones_manufacturas_alta_tecnologia_otras": (IMPORTACIONES, LALL, (_L_ALTA, "Otras")),
    "lall_importaciones_otras": (IMPORTACIONES, LALL, ("Otras",)),
    "lall_total_importaciones": (IMPORTACIONES, LALL, (TOTAL,)),
    # Exportaciones, UNCTAD
    "unctad_x_prod_primarios_piedras_preciosas_oro_no_monetario":
        (EXPORTACIONES, UNCTAD, ("Productos primarios, piedras preciosas y oro no monetario",)),
    "unctad_exportaciones_total_productos_manufacturados": (EXPORTACIONES, UNCTAD, (_U_MANUFACTURAS,)),
    "unctad_x_prod_manu_mano_obra_intens_uso_intens": (EXPORTACIONES, UNCTAD, (_U_MANO_DE_OBRA,)),
    "unctad_x_prod_manu_manu_intens_en_tecno_baja_especializ": (EXPORTACIONES, UNCTAD, (_U_BAJA,)),
    "unctad_x_prod_manu_total_manu_media_especializacion": (EXPORTACIONES, UNCTAD, (_U_MEDIA,)),
    "unctad_x_prod_manu_intens_en_tecno_media_especializ_electr":
        (EXPORTACIONES, UNCTAD, (_U_MEDIA, _U_ELECTRONICOS)),
    "unctad_x_prod_manu_intens_en_tecno_media_especliz_partes":
        (EXPORTACIONES, UNCTAD, (_U_MEDIA, _U_PARTES)),
    "unctad_x_prod_manu_intens_en_tecno_media_especializ_otros":
        (EXPORTACIONES, UNCTAD, (_U_MEDIA, _U_OTROS)),
    "unctad_x_prod_manu_total_manu_alta_especializacion": (EXPORTACIONES, UNCTAD, (_U_ALTA,)),
    "unctad_x_prod_manu_intens_en_tecno_alta_espcliz_elect_exc":
        (EXPORTACIONES, UNCTAD, (_U_ALTA, _U_ELECTRONICOS)),
    # Estas dos no dicen «alta»: se ubicaron porque con ellas los hijos suman el total de la rama.
    "unctad_x_prod_manu_partes_compon_para_bienes_elects_electr":
        (EXPORTACIONES, UNCTAD, (_U_ALTA, _U_PARTES)),
    "unctad_expo_prod_manufct_otros_excluyendo_electonicos": (EXPORTACIONES, UNCTAD, (_U_ALTA, _U_OTROS)),
    "unctad_exportaciones_otras": (EXPORTACIONES, UNCTAD, ("Otras",)),
    "unctad_total_exportaciones": (EXPORTACIONES, UNCTAD, (TOTAL,)),
    # Importaciones, UNCTAD
    "unctad_m_prod_primarios_piedras_preciosas_oro_no_monetario":
        (IMPORTACIONES, UNCTAD, ("Productos primarios, piedras preciosas y oro no monetario",)),
    "unctad_importaciones_total_productos_manufacturados": (IMPORTACIONES, UNCTAD, (_U_MANUFACTURAS,)),
    "unctad_m_prod_manu_mano_obra_intens_uso_intens_recur": (IMPORTACIONES, UNCTAD, (_U_MANO_DE_OBRA,)),
    "unctad_m_prod_manu_intens_tecno_baja_espcliz_baja_espcliz": (IMPORTACIONES, UNCTAD, (_U_BAJA,)),
    "unctad_m_prod_manu_total_manu_media_especializacion_": (IMPORTACIONES, UNCTAD, (_U_MEDIA,)),
    "unctad_m_manu_intens_tecno_media_espcliz_electr_exc_part":
        (IMPORTACIONES, UNCTAD, (_U_MEDIA, _U_ELECTRONICOS)),
    "unctad_m_manu_intens_en_tecno_media_espcliz_comp_electr":
        (IMPORTACIONES, UNCTAD, (_U_MEDIA, _U_PARTES)),
    "unctad_m_manu_intens_en_tecno_media_espcliz_otros": (IMPORTACIONES, UNCTAD, (_U_MEDIA, _U_OTROS)),
    "unctad_m_prod_manu_total_manu_alta_especializacion": (IMPORTACIONES, UNCTAD, (_U_ALTA,)),
    "unctad_m_manu_intens_en_tecno_alta_espcliz_electr_exc":
        (IMPORTACIONES, UNCTAD, (_U_ALTA, _U_ELECTRONICOS)),
    "unctad_m_manu_intens_en_tecno_alta_espcilz_comp_elect": (IMPORTACIONES, UNCTAD, (_U_ALTA, _U_PARTES)),
    "unctad_m_manu_intens_en_tecno_alta_espcliz_otros": (IMPORTACIONES, UNCTAD, (_U_ALTA, _U_OTROS)),
    "unctad_importaciones_otras": (IMPORTACIONES, UNCTAD, ("Otras",)),
    "unctad_total_importaciones": (IMPORTACIONES, UNCTAD, (TOTAL,)),
}


def _intensidad_tecnologica(serie: Serie) -> Clasificacion | None:
    regla = _INTENSIDAD.get(serie.variable)
    if regla is None:
        return None
    flujo, clasificacion, camino = regla
    return _clasificacion(EXTERNO, INTERCAMBIO, unir(flujo, clasificacion, *camino), cobertura=flujo,
                          apertura=POR_INTENSIDAD, medida=clasificacion, unidad=MILLONES_USD)


# =================================================================================================
# Índices de precios y cantidades del comercio exterior (hojas 10 a 13, 4.9.1, 4.9.2 y 4.9.4)
# =================================================================================================
_INDICE_DE = {"valor": "Índice de valor", "cantidad": "Índice de cantidad", "precio": "Índice de precios"}
_IPE = "Índice de precios de exportación"
_IPI = "Índice de precios de importación"
_TERMINOS = "Términos del intercambio"
_PODER_DE_COMPRA = "Poder de compra de las exportaciones"
_GANANCIA = "Ganancia o pérdida del intercambio"
_EXPORTACIONES_CONSTANTES = "Exportaciones (precios constantes)"


def _nombre_indice(tipo: str, flujo: str) -> str:
    """«Índice de valor de exportación», «Índice de precios de importación»…"""
    return f"{_INDICE_DE[tipo]} de {'exportación' if flujo == 'exportaciones' else 'importación'}"


_RUBROS_INDICES = {
    "Nivel general": "",
    "Productos primarios": PRIMARIOS,
    "Agro": MOA,
    "Industria": MOI,
    "Combustibles": COMBUSTIBLES_Y_ENERGIA,
    "Combustibles y lubricantes": COMBUSTIBLES_Y_LUBRICANTES,
    "Bienes de capital": BIENES_DE_CAPITAL,
    "Bienes de consumo": BIENES_DE_CONSUMO,
    "Bienes intermedios": BIENES_INTERMEDIOS,
    "Piezas y accesorios para bienes de capital": PIEZAS_Y_ACCESORIOS,
    "Vehículos automotores de pasajeros": VEHICULOS,
}


def _indices_por_rubro(serie: Serie) -> Clasificacion | None:
    """Hojas 10 (exportaciones) y 11 (importaciones): valor, precio y cantidad por rubro o uso."""
    encontrado = re.fullmatch(r"Índice de (valor|precio|cantidad) de (exportaciones|importaciones)\. (.+)",
                              serie.descripcion)
    if not encontrado or encontrado.group(3) not in _RUBROS_INDICES:
        return None
    tipo, flujo, rubro = encontrado.groups()
    rubro = _RUBROS_INDICES[rubro]
    nombre = unir(_nombre_indice(tipo, flujo), rubro)
    if not rubro:
        return _clasificacion(EXTERNO, INDICES_COMERCIO, nombre, unidad=INDICE_2004)
    apertura = POR_GRANDES_RUBROS if flujo == "exportaciones" else POR_USO
    return _clasificacion(EXTERNO, INDICES_COMERCIO, nombre, cobertura=apertura, unidad=INDICE_2004)


# Hoja 12: código del ID → (frecuencia → (nombre, medida)). Las medidas salen de recalcular las
# variaciones con los índices: las descripciones de la fuente están cruzadas.
_TERMINOS_DEL_INTERCAMBIO = {
    "IPE": {"A": (_IPE, ""), "T": (_IPE, "")},
    "IPI": {"A": (_IPI, ""), "T": (_IPI, "")},
    "ITI": {"A": (_TERMINOS, ""), "T": (_TERMINOS, "")},
    "VMP": {"A": (_IPE, INTERANUAL), "T": (_IPE, TRIMESTRAL)},
    "VAP": {"T": (_IPE, INTERANUAL)},
    "VPP1": {"A": (_IPI, INTERANUAL), "T": (_IPI, TRIMESTRAL)},
    "VPP2": {"T": (_IPI, INTERANUAL)},
    "VPTI1": {"A": (_TERMINOS, INTERANUAL), "T": (_TERMINOS, TRIMESTRAL)},
    "VPTI2": {"T": (_TERMINOS, INTERANUAL)},
}


def _terminos_del_intercambio(serie: Serie) -> Clasificacion | None:
    codigo = _codigo_id(serie, r"^82\.\d_([A-Z0-9]+)_2004_[AT]_\d+$")
    por_frecuencia = _TERMINOS_DEL_INTERCAMBIO.get(codigo)
    if not por_frecuencia or serie.frecuencia not in por_frecuencia:
        return None
    nombre, medida = por_frecuencia[serie.frecuencia]
    return _clasificacion(EXTERNO, INDICES_COMERCIO, con_medida(nombre, medida),
                          unidad=PORCENTAJE if medida else INDICE_2004)


# Hoja 13 (base 2004) y hoja 4.9.4 (rotulada base 1993, pero en base 2004: ver abajo).
_PODER_DE_COMPRA_2004 = {
    "EMD": (_EXPORTACIONES_CONSTANTES, USD_2004),
    "GPIMD": (_GANANCIA, USD_2004),
    "ITI": (_TERMINOS, INDICE_2004),
    "PCEMD": (_PODER_DE_COMPRA, USD_2004),
    "VPA": (con_medida(_GANANCIA, "variación interanual"), USD_2004),
}
_PODER_DE_COMPRA_1993 = {
    "ED1": (_EXPORTACIONES_CONSTANTES, USD_2004),
    "GPI": (_GANANCIA, USD_2004),
    "PCE": (_PODER_DE_COMPRA, USD_2004),
    "TI": (_TERMINOS, INDICE_2004),
}


def _poder_de_compra(serie: Serie) -> Clasificacion | None:
    codigo = _codigo_id(serie, r"^83\.\d_([A-Z0-9]+)_2004_[A-Z]_\d+$")
    if codigo not in _PODER_DE_COMPRA_2004:
        return None
    nombre, unidad = _PODER_DE_COMPRA_2004[codigo]
    return _clasificacion(EXTERNO, INDICES_COMERCIO, nombre, unidad=unidad)


def _poder_de_compra_1993(serie: Serie) -> Clasificacion | None:
    # Pese al «Base 1993» de la fuente, los valores están en dólares de 2004 e índice 2004=100: son los
    # de la hoja 13 (idénticos de 1986 a 2013), el índice vale 100 y la ganancia 0 en 2004, y la API
    # continúa estos ID hasta 2023 con los valores de la hoja 13. Mismo nombre que la hoja 13; la unidad
    # se corrige en ``UNIDADES``.
    codigo = _codigo_id(serie, r"^120\.\d_([A-Z0-9]+)_1993_0_\d+$")
    if codigo not in _PODER_DE_COMPRA_1993:
        return None
    nombre, unidad = _PODER_DE_COMPRA_1993[codigo]
    return _clasificacion(EXTERNO, INDICES_COMERCIO, nombre, unidad=unidad)


# Hojas 4.9.1 y 4.9.2 (precios.xlsx): los índices generales de valor y cantidad, base 2004.
_INDICES_GENERALES = {
    "117.1_ENGC_0_A_36": _nombre_indice("cantidad", "exportaciones"),
    "117.1_ENGV_0_A_33": _nombre_indice("valor", "exportaciones"),
    "118.1_INGC_0_A_36": _nombre_indice("cantidad", "importaciones"),
    "118.1_INGV_0_A_33": _nombre_indice("valor", "importaciones"),
}


def _indices_generales(serie: Serie) -> Clasificacion | None:
    nombre = _INDICES_GENERALES.get(serie.id)
    if nombre is None:
        return None
    return _clasificacion(EXTERNO, INDICES_COMERCIO, nombre, unidad=INDICE_2004)


# =================================================================================================
# Hoja 17. Encuesta de turismo internacional
# =================================================================================================
_FLUJOS_TURISMO = {"receptivo": "Receptivo", "emisivo": "Emisivo"}
_REGIONES_TURISMO = {
    "estados_unidos_canada": "Estados Unidos y Canadá",
    "resto_america": "Resto de América",
    "chile": "Chile",
    "restodelmundo": "Resto del mundo",
    "europa": "Europa",
    "brasil": "Brasil",
    "uruguay": "Uruguay",
    "paraguay": "Paraguay",
    "bolivia": "Bolivia",
}
POR_REGION = "Por país o región"
MILES_DE_TURISTAS = "Miles de turistas"


def _turismo(serie: Serie) -> Clasificacion | None:
    if serie.variable == "saldo":
        return _clasificacion(EXTERNO, TURISMO, unir(TURISMO, "Saldo"), cobertura="Saldo",
                              unidad=MILES_DE_TURISTAS)
    encontrado = re.fullmatch(r"turismo_(receptivo|emisivo)(?:_(.+))?", serie.variable)
    if not encontrado:
        return None
    flujo, resto = _FLUJOS_TURISMO[encontrado.group(1)], encontrado.group(2)
    if resto is None:   # relevamiento en Ezeiza y Aeroparque: el único con serie mensual
        return _clasificacion(EXTERNO, TURISMO, unir(TURISMO, flujo), cobertura=flujo, apertura=TOTAL,
                              unidad=MILES_DE_TURISTAS)
    if resto == "total":   # suma de las regiones: difiere del relevamiento de Ezeiza y Aeroparque en los primeros años
        nombre = unir(TURISMO, flujo, "Total de países y regiones")
    elif resto in _REGIONES_TURISMO:
        nombre = unir(TURISMO, flujo, _REGIONES_TURISMO[resto])
    else:
        return None
    return _clasificacion(EXTERNO, TURISMO, nombre, cobertura=flujo, apertura=POR_REGION,
                          unidad=MILES_DE_TURISTAS)


# =================================================================================================
# Hojas 34 y 35. Oleaginosas y cereales
# =================================================================================================
EMBARQUES = "Embarques"
LIQUIDACION = "Liquidación de divisas"
_EMBARQUES = {
    "poroto_soja": "Poroto de soja",
    "maiz": "Maíz",
    "trigo": "Trigo",
    "subproductos": "Subproductos",
    "resto": "Resto",
    "total": "",
}


def _embarques(serie: Serie) -> Clasificacion | None:
    if serie.variable not in _EMBARQUES:
        return None
    return _clasificacion(EXTERNO, GRANOS, unir(EMBARQUES, _EMBARQUES[serie.variable]),
                          cobertura=EMBARQUES, unidad="Millones de toneladas")


def _liquidacion(serie: Serie) -> Clasificacion | None:
    medida = {"acum_mensual": "", "promedio_diario": "promedio diario"}.get(serie.variable)
    if medida is None:
        return None
    return _clasificacion(EXTERNO, GRANOS, con_medida(LIQUIDACION, medida), cobertura=LIQUIDACION,
                          unidad=MILLONES_USD)


# =================================================================================================
# Hojas 39, 40 y 41. Empresas exportadoras e índices de difusión
# =================================================================================================
POR_SALDO = "Por saldo comercial"
POR_SECTOR_Y_SALDO = "Por sector y saldo comercial"
MONTO = "acumulado 12 meses"

# Hojas 39 y 40: variable → rango. Los rótulos de la fuente están corridos (ver el docstring).
_RANGOS_39 = {
    "detalle_por_rangos_de_exp_men_usd_1_mmenos_de_usd_01_m": R_0,
    "detalle_por_rangos_de_exp_men_usd_1_mentre_usd_01_m_usd_1_m": R_1,
    "detalle_por_rangos_de_exp_total_may_usd_1_m": R_2,
    "detalle_por_rangos_de_exp_may_usd_1_m_entre_usd_1_m_usd_5_m": R_3,
    "detalle_por_rangos_de_exp_entre_usd_5_m_usd_20_m": R_4,
    "detalle_por_rangos_de_exp_entre_usd_100_m_usd_500_m": R_5,
    "detalle_por_rangos_de_exp_mas_de_usd_500_m": R_6,
    "detalle_por_rangos_de_exp_entre_usd_20_m_usd_100_m": R_MAS_100,
    "detalle_por_rangos_de_exp_total_men_usd_1_m": R_MENOS_100,
}
_RANGOS_40 = {
    "rangos_exp_m_usd_exportan_men_usd_100_m": R_0,
    "rangos_exp_m_usd_entre_usd_01_m_usd_1_m": R_1,
    "rangos_exp_m_usd_entre_usd_1_m_usd_5_m": R_2,
    "rangos_exp_m_usd_entre_usd_5_m_usd_20_m": R_3,
    "rangos_exp_m_usd_entre_usd_20_m_usd_100_m": R_4,
    "rangos_exp_m_usd_exportan_may_usd_100_m": R_5,
    "rangos_exp_m_usd_mas_usd_500_m": R_6,
    "rangos_exp_m_usdtotal_may_usd_100_m": R_MAS_100,
    "rangos_exp_m_usd_total_men_usd_100_m": R_MENOS_100,
}


def _sector(texto: str) -> str | None:
    """Sector de actividad que nombra una descripción de la fuente."""
    claves = plano(texto)
    for clave, sector in (
        ("sin clasificar", SECTOR_SIN_CLASIFICAR), ("agric", SECTOR_A), ("pesca", SECTOR_B),
        ("minas", SECTOR_C), ("manuf", SECTOR_D), ("comercio", SECTOR_G), ("resto", SECTOR_RESTO),
    ):
        if clave in claves:
            return sector
    return None


def _condicion(texto: str) -> str | None:
    claves = plano(texto)
    for clave, nombre in CONDICIONES.items():
        if clave in claves:
            return nombre
    return None


def _empresas(corte: str, nombre: str, cantidad: bool) -> Clasificacion:
    """Un corte de las empresas exportadoras: la cantidad de empresas o su monto exportado."""
    medida = "" if cantidad else MONTO
    return _clasificacion(EXTERNO, EMPRESAS_EXPORTADORAS, con_medida(nombre, medida), cobertura=corte,
                          unidad=EMPRESAS if cantidad else MILLONES_USD)


def _nombre_empresas(rango: str = "", sector: str = "", condicion: str = "") -> tuple[str, str]:
    """(corte, nombre) de una serie de las hojas 39 y 40."""
    nombre = unir(EMPRESAS_EXPORTADORAS, rango, sector, condicion)
    if rango:
        return POR_RANGO, nombre
    if sector and condicion:
        return POR_SECTOR_Y_SALDO, nombre
    if sector:
        return POR_SECTOR, nombre
    if condicion:
        return POR_SALDO, nombre
    return TOTAL, nombre


def _empresas_exportadoras(serie: Serie) -> Clasificacion | None:
    cantidad = serie.hoja.startswith("39.")
    variable = serie.variable
    if variable in ("total_empresas_exportadoras", "exportaciones_m_usd"):
        corte, nombre = _nombre_empresas()
    elif variable in (_RANGOS_39 if cantidad else _RANGOS_40):
        corte, nombre = _nombre_empresas(rango=(_RANGOS_39 if cantidad else _RANGOS_40)[variable])
    else:
        sector, condicion = _sector(serie.descripcion), _condicion(serie.descripcion)
        if sector is None and condicion is None:
            return None
        corte, nombre = _nombre_empresas(sector=sector or "", condicion=condicion or "")
    return _empresas(corte, nombre, cantidad)


# Hoja 41. Las descripciones son limpias: se leen en vez de las variables.
_RANGOS_41 = {
    "Menor a USD 0,1 M": R_0,
    "Entre USD 0,1M y USD 1M": R_1,
    "Entre USD 1M y USD 5M": R_2,
    "Entre USD 5M y USD 20M": R_3,
    "Entre USD 20M y USD 100M": R_4,
    "Entre USD 100M y USD 500M": R_5,
    "Mayor a 500M": R_6,
    "Exportan > USD 1 M": R_MAS_1,
    "Exportan < USD 1 M": R_MENOS_1,
}
_SECTORES_41 = (
    ("A. Agric", SECTOR_A), ("B. Pesca", SECTOR_B), ("C. Minas", SECTOR_C), ("D. Ind", SECTOR_D),
    ("G. Comercio", SECTOR_G), ("Resto", SECTOR_RESTO), ("Sin clasificar", SECTOR_SIN_CLASIFICAR),
)
_CONTEOS_41 = {
    "Crecimiento": "cantidad con crecimiento interanual", "Crecimiento i.a.": "cantidad con crecimiento interanual",
    "Caída": "cantidad con caída interanual", "Caída i.a.": "cantidad con caída interanual",
    "Estable": "cantidad sin variación", "Sin variación": "cantidad sin variación",
}
_TOTALES_41 = {"Total", "Total A", "Total B", "Total C", "Total D", "Total G", "Total Resto", "Total (en blanco)"}
_UNIDAD_DE_ANALISIS = {"Empresas": EMPRESAS, "Destinos": "Destinos", "Productos": "Productos"}


def _difusion(unidad_de_analisis: str, corte: str, entidad: str, medida: str, unidad: str,
              nomenclatura: str = "") -> Clasificacion:
    """Una serie del indicador de difusión: el índice (medida vacía) o uno de sus conteos."""
    nombre = unir("Índice de difusión", nomenclatura or unidad_de_analisis, entidad)
    return _clasificacion(EXTERNO, DIFUSION, con_medida(nombre, medida), cobertura=unidad_de_analisis,
                          apertura=corte or nomenclatura_apertura(nomenclatura), unidad=unidad)


def nomenclatura_apertura(nomenclatura: str) -> str:
    return {"Productos a 4 dígitos del NCM": "NCM de 4 dígitos",
            "Productos a 8 dígitos del NCM": "NCM de 8 dígitos"}.get(nomenclatura, "")


def _entidad_41(texto: str) -> tuple[str, str] | None:
    """(corte, nombre) de un sector o un rango que nombra la hoja 41; None si no se reconoce."""
    if texto in _RANGOS_41:
        return POR_RANGO, _RANGOS_41[texto]
    for prefijo, sector in _SECTORES_41:
        if texto.startswith(prefijo):
            return POR_SECTOR, sector
    return None


def _difusion_41(serie: Serie) -> Clasificacion | None:
    texto = serie.descripcion
    plano_texto = plano(texto)
    # Destinos, empresas y productos: «Cantidad de X con: Y» y los índices generales.
    encontrado = re.fullmatch(r"Cantidad de (destinos|empresas|productos a [48] dígitos del NCM) con: (.+)", texto)
    if encontrado:
        sujeto, rotulo = encontrado.groups()
        if sujeto == "empresas" and rotulo == "Empresas exportadoras":
            return _empresas(TOTAL, EMPRESAS_EXPORTADORAS, cantidad=True)
        if rotulo in _CONTEOS_41:
            medida = _CONTEOS_41[rotulo]
        elif rotulo in ("Total de destinos", "Total de productos"):
            medida = "cantidad total"
        else:
            return None
        if sujeto == "destinos":
            return _difusion("Destinos", "", "", medida, "Destinos")
        if sujeto == "empresas":
            return _difusion("Empresas", TOTAL, "", medida, EMPRESAS)
        nomenclatura = sujeto[0].upper() + sujeto[1:]
        return _difusion("Productos", "", "", medida, "Productos", nomenclatura)
    if plano_texto == "indice de difusion por empresa":
        return _difusion("Empresas", TOTAL, "", "", PROPORCION)
    if plano_texto == "indice de difusion por destino":
        return _difusion("Destinos", "", "", "", PROPORCION)
    encontrado = re.fullmatch(r"Indice de difusión por producto \(NCM ([48]) dígitos\)", texto)
    if encontrado:
        return _difusion("Productos", "", "", "", PROPORCION, f"Productos a {encontrado.group(1)} dígitos del NCM")
    # Por rango: sólo el índice. Por sector de actividad: el índice y los conteos (con o sin rango).
    encontrado = re.fullmatch(r"Índice de difusión por rango de exportación (.+)", texto)
    if encontrado:
        entidad = _entidad_41(encontrado.group(1))
        return _difusion("Empresas", entidad[0], entidad[1], "", PROPORCION) if entidad else None
    encontrado = re.fullmatch(r"Índice de difusión por Sector de actividad (.+?)"
                              r"(?: (Crecimiento i\.a\.|Caída i\.a\.|Crecimiento|Caída|Estable|Total .*|Total))?", texto)
    if not encontrado:
        return None
    cuerpo, sufijo = encontrado.groups()
    if cuerpo == "Total general":
        return _empresas(TOTAL, EMPRESAS_EXPORTADORAS, cantidad=True)
    entidad = _entidad_41(cuerpo)
    if entidad is None:
        return None
    corte, nombre = entidad
    if sufijo is None:
        return _difusion("Empresas", corte, nombre, "", PROPORCION)
    if sufijo in _CONTEOS_41:
        return _difusion("Empresas", corte, nombre, _CONTEOS_41[sufijo], EMPRESAS)
    if sufijo in _TOTALES_41:     # la cantidad de empresas del corte: la misma de la hoja 39
        corte_39, nombre_39 = _nombre_empresas(rango=nombre if corte == POR_RANGO else "",
                                               sector=nombre if corte == POR_SECTOR else "")
        return _empresas(corte_39, nombre_39, cantidad=True)
    return None


# =================================================================================================
# Hoja 18. IPMP (Precios)
# =================================================================================================
def _ipmp(serie: Serie) -> Clasificacion | None:
    # El IPMP vigente es el de dólares con base 2001; el de base 1995 (en dólares y en pesos)
    # se publicó hasta 2016.
    if serie.variable == "indice_precios_materias_primas_ipmp":
        return _clasificacion(PRECIOS, IPMP, IPMP, unidad="Índice dic-2001=100")
    if serie.variable == "indice_precios_materias_primas_usd":
        return _clasificacion(PRECIOS, IPMP, IPMP, cobertura=SERIES_HISTORICAS, apertura="Base 1995",
                              unidad="Índice 1995=100")
    if serie.variable == "indice_precios_materias_primas_pesos":
        return _clasificacion(PRECIOS, IPMP, con_medida(IPMP, "en pesos"), cobertura=SERIES_HISTORICAS,
                              apertura="Base 1995", unidad="Índice 1995=100")
    return None


# =================================================================================================
# Hoja 38. Precios FOB oficiales (Precios)
# =================================================================================================
_P_CEREALES = "Cereales y derivados"
_P_OLEAGINOSAS = "Oleaginosas"
_P_ACEITES = "Aceites"
_P_SUBPRODUCTOS = "Subproductos oleaginosos"
_P_LEGUMBRES = "Legumbres"
_P_ALGODON = "Algodón"
_MAS_15 = "Más del 15 % embolsado"
_GRANEL = "A granel, hasta 15 % embolsado"
_ALUBIAS = "Porotos blancos alubias"
_ALGODON = "Algodón desmotado"

# Variable → (categoría, partes del nombre). «Los demás» sólo se conserva donde distingue.
# Las cuatro series de arroz y de cebada que la fuente rotula igual se separaron por sus valores
# (ver el informe): la cebada «no cervecera» cotiza unos 20 US$ por debajo de la cervecera.
_FOB = {
    "aceite_cacahuete_mani_granel": (_P_ACEITES, ("Aceite de maní", "A granel")),
    "aceite_girasol_granel": (_P_ACEITES, ("Aceite de girasol", "A granel")),
    "aceite_girasol_demas_refinado_granel": (_P_ACEITES, ("Aceite de girasol", "Refinado a granel")),
    "aceite_girasol_demas_refinado_tambores_mas_200_litros":
        (_P_ACEITES, ("Aceite de girasol", "Refinado en tambores de más de 200 litros")),
    "aceite_soja_granel": (_P_ACEITES, ("Aceite de soja", "A granel")),
    "algo_s_cardar_peinar_desmo_grado_c": (_P_ALGODON, (_ALGODON, "Grado C")),
    "s_cardar_peinar_desmo_grado_c_1_2": (_P_ALGODON, (_ALGODON, "Grado C 1/2")),
    "s_cardar_peinar_desmo_algo_grado_b": (_P_ALGODON, (_ALGODON, "Grado B")),
    "s_cardar_peinar_desmo_grado_d_long_27_mm_1_1_16": (_P_ALGODON, (_ALGODON, "Grado D, largo 27,0 mm")),
    "s_cardar_peinar_desmo_grado_d_1_2_long_27_mm_1_1_16":
        (_P_ALGODON, (_ALGODON, "Grado D 1/2, largo 27,0 mm")),
    "s_cardar_peinar_desmotado_grado_long_26_mm_1_1_32": (_P_ALGODON, (_ALGODON, "Grado E, largo 26,2 mm")),
    "sin_cardar_peinar_desmotado_grado_f_long_26_mm_1_1_32":
        (_P_ALGODON, (_ALGODON, "Grado F, largo 26,2 mm")),
    "arroz_descascarillado_arroz_cargo_arroz_pardo_demas_no_parbo":
        (_P_CEREALES, ("Arroz", "Descascarillado, cargo o pardo", "No parbolizado")),
    "arroz_no_parbolizado_cont_grano_quebrado_men_igual_10porc":
        (_P_CEREALES, ("Arroz", "Semiblanqueado o blanqueado, no parbolizado", "Grano quebrado hasta 10 %")),
    "arroz_puli_glas_no_parbo_cont_grano_quebrado_may_10porc":
        (_P_CEREALES, ("Arroz", "Semiblanqueado o blanqueado, no parbolizado", "Grano quebrado de más de 10 %")),
    "arroz_pulido_glasaseado_cont_gra_quebr_men_igual_10porc":
        (_P_CEREALES, ("Arroz", "Semiblanqueado o blanqueado, los demás no parbolizados",
                       "Grano quebrado hasta 10 %")),
    "arroz_pulido_glaseado_contenido_grano_quebrado_may_10porc":
        (_P_CEREALES, ("Arroz", "Semiblanqueado o blanqueado, los demás no parbolizados",
                       "Grano quebrado de más de 10 %")),
    "arvejas_enteras": (_P_LEGUMBRES, ("Arvejas", "Enteras")),
    "arvejas_partidas": (_P_LEGUMBRES, ("Arvejas", "Partidas")),
    "cacahuate_mani_confiteria_tostadero": (_P_OLEAGINOSAS, ("Maní", "Confitería y tostadero")),
    "cacahuate_mani_mani_partido": (_P_OLEAGINOSAS, ("Maní", "Partido")),
    "cacahuate_mani_demas_mas_15_porciento_embolsado": (_P_OLEAGINOSAS, ("Maní", "Los demás", _MAS_15)),
    "cacahuate_mani_demas_granel_hasta_15_porciento_embolsado":
        (_P_OLEAGINOSAS, ("Maní", "Los demás", _GRANEL)),
    "cebada_cervecera_granel_mas_15_porciento_embolsado": (_P_CEREALES, ("Cebada", "Cervecera", _MAS_15)),
    "cebada_cervecera_detalle_granel_hasta_15_porciento_embol":
        (_P_CEREALES, ("Cebada", "Cervecera", _GRANEL)),
    "cebada_cervecera_mas_15_porciento_embolsado": (_P_CEREALES, ("Cebada", "No cervecera", _MAS_15)),
    "cebada_cervecera_granel_hasta_15_porciento_embolsado":
        (_P_CEREALES, ("Cebada", "No cervecera", _GRANEL)),
    "garbanzos_enteros": (_P_LEGUMBRES, ("Garbanzos", "Enteros")),
    "garbanzos_partidos": (_P_LEGUMBRES, ("Garbanzos", "Partidos")),
    "habas_soja_demas_mas_15_porciento_embolsado": (_P_OLEAGINOSAS, ("Habas de soja", _MAS_15)),
    "habas_soja_demas_granel_hasta_15_porciento_embolsado": (_P_OLEAGINOSAS, ("Habas de soja", _GRANEL)),
    "harina_trigo": (_P_CEREALES, ("Harina de trigo",)),
    "maiz_demas_grano_mas_15_porciento_embolsado": (_P_CEREALES, ("Maíz", "Los demás en grano", _MAS_15)),
    "maiz_demas_grano_granel_hasta_15_porciento_embolsado":
        (_P_CEREALES, ("Maíz", "Los demás en grano", _GRANEL)),
    "maiz_flint_plata_mas_15_porciento_embolsado": (_P_CEREALES, ("Maíz", "Flint o plata", _MAS_15)),
    "maiz_flint_plata_granel_hasta_15_porciento_embolsado": (_P_CEREALES, ("Maíz", "Flint o plata", _GRANEL)),
    "maiz_maiz_pisingallo_mas_15_porciento_embolsado": (_P_CEREALES, ("Maíz", "Pisingallo", _MAS_15)),
    "maiz_maiz_pisingallo_granel_hasta_15_porciento_embolsado": (_P_CEREALES, ("Maíz", "Pisingallo", _GRANEL)),
    "malta_cervecera_malta_cebada_cervecera": (_P_CEREALES, ("Malta cervecera",)),
    "porotos_colorados_envases_mayores_20_kg": (_P_LEGUMBRES, ("Porotos", "Colorados")),
    "poro_negros_envases_may_20_kg": (_P_LEGUMBRES, ("Porotos", "Negros")),
    "poro_blanc_alubias_ovales_envases_may_20_kg": (_P_LEGUMBRES, (_ALUBIAS, "Ovales")),
    "poro_blanc_alubias_env_may_20_kg_hta_190_gr_cada_100_gr":
        (_P_LEGUMBRES, (_ALUBIAS, "Hasta 190 granos cada 100 g")),
    "poro_blanc_alubias_env_may_20_kg_dsd_191_hta_220_gr_c_100_gr":
        (_P_LEGUMBRES, (_ALUBIAS, "De 191 a 220 granos cada 100 g")),
    "poro_blanc_alubias_env_may_20_kg_dsd_221_gr_cada_100_gr":
        (_P_LEGUMBRES, (_ALUBIAS, "Desde 221 granos cada 100 g")),
    "semilla_girasol_unica_ind_demas_mas_15_por_embolsado":
        (_P_OLEAGINOSAS, ("Semilla de girasol para industria", _MAS_15)),
    "semilla_girasol_unica_ind_demas_granel_hta_15_porc_embol":
        (_P_OLEAGINOSAS, ("Semilla de girasol para industria", _GRANEL)),
    "semilla_girasol_unica_ind_demas_girasol_tipo_confiteria":
        (_P_OLEAGINOSAS, ("Semilla de girasol para industria", "Tipo confitería")),
    "sorgo_granifero_demas_mas_15_porciento_embolsado": (_P_CEREALES, ("Sorgo granífero", _MAS_15)),
    "sorgo_granifero_demas_granel_hasta_15_porciento_embolsado": (_P_CEREALES, ("Sorgo granífero", _GRANEL)),
    "tortas_expellers_pellets_harinas_girasol_tortas": (_P_SUBPRODUCTOS, ("Subproductos de girasol", "Tortas")),
    "tortas_expellers_pellets_harinas_girasol_harinas_tortas":
        (_P_SUBPRODUCTOS, ("Subproductos de girasol", "Harinas de tortas")),
    "tortas_expellers_pellets_harinas_girasol_pellets_integral":
        (_P_SUBPRODUCTOS, ("Subproductos de girasol", "Pellets integrales")),
    "tortas_expellers_pellets_harinas_girasol_pellets_extr":
        (_P_SUBPRODUCTOS, ("Subproductos de girasol", "Pellets de harina de extracción")),
    "tortas_expellers_pellets_harina_soja_pellets_harinas_tortas":
        (_P_SUBPRODUCTOS, ("Subproductos de soja", "Pellets de harinas de tortas")),
    "tortas_expellers_pellets_harina_soja_pellets_harina_extr":
        (_P_SUBPRODUCTOS, ("Subproductos de soja", "Pellets de harina de extracción")),
    "trigo_pan_mas_15_porciento_embolsado": (_P_CEREALES, ("Trigo pan", _MAS_15)),
    "trigo_granel_hasta_15_porciento_embolsado": (_P_CEREALES, ("Trigo pan", _GRANEL)),
}


def _precios_fob(serie: Serie) -> Clasificacion | None:
    regla = _FOB.get(serie.variable)
    if regla is None:
        return None
    categoria, partes = regla
    return _clasificacion(PRECIOS, FOB_OFICIALES, unir("Precio FOB oficial", *partes), cobertura=categoria,
                          unidad="US$ por tonelada")


# =================================================================================================
# Correcciones explícitas (ver ``tools/taxonomia/__init__.py``)
# =================================================================================================
# Hoja 41: 16 series son copias exactas de la hoja 39 del mismo libro (iguales en las 260 fechas que
# comparten; la 39 empieza en 1996 y la 41 en 1997): la cantidad total de empresas exportadoras («Total
# general» y «Empresas exportadoras», que además son iguales entre sí), el total de cada uno de los siete
# sectores y el total de cada uno de los siete rangos de exportación. Se conserva la hoja 39. Las copias
# confirman, además, la reasignación de los rangos de la 39: la 41 rotula con el rango correcto
# (p. ej. «Entre USD 5M y USD 20M Total» = 489 empresas = la columna que la 39 rotula «> USD 1 M Entre USD 1 M y USD 5 M»).
_COPIA_41 = "Copia exacta de la serie homónima de la hoja 39 (mismas 260 fechas, mismos valores); se conserva la 39, que empieza en 1996"
EXCLUIDAS = {
    **{("datos.gob.ar", id_): _COPIA_41 for id_ in (
        "362.1_CANTIDAD_ERAS__43",      # «Empresas exportadoras»: igual a «Total general» y al total de la hoja 39 (5.738 en 2018-08)
        "362.1_INDICE_DIFRAL__50",      # «Total general»
        "362.1_INDICE_DIFL_A__51", "362.1_INDICE_DIFL_B__52", "362.1_INDICE_DIFL_C__52",
        "362.1_INDICE_DIFL_D__53", "362.1_INDICE_DIFL_G__56", "362.1_INDICE_DIFSTO__54",
        "362.1_INDICE_DIFNCO__56",      # totales por sector
        "362.1_INDICE_DIFTAL__59", "362.1_INDICE_DIFTAL__52", "362.1_INDICE_DIFTAL__51",
        "362.1_INDICE_DIFTAL_4_I__52", "362.1_INDICE_DIFTAL__54", "362.1_INDICE_DIFTAL__55",
        "362.1_INDICE_DIFTALWI2__55",   # totales por rango, de menos de 0,1 M a más de 500 M
    )},
    # Hoja 38: las dos variantes de arroz «los demás no parbolizados» valen exactamente lo mismo que
    # «no parbolizado» en las 10.150 fechas en que tienen precio (y 0 en 1993-1995, cuando las otras sí
    # cotizan); se conservan las dos series completas.
    ("datos.gob.ar", "358.1_ARR_SB_B_PORC_II__59"): "Copia exacta (10.150 de 10.150 días) de «Arroz semiblanqueado o blanqueado, no parbolizado, grano quebrado hasta 10 %» (358.1_ARR_SB_B_PORC__59), que además tiene precio desde 1993",
    ("datos.gob.ar", "358.1_ARR_SB_B_PORC_IV__53"): "Copia exacta (10.150 de 10.150 días) de «Arroz semiblanqueado o blanqueado, no parbolizado, grano quebrado de más de 10 %» (358.1_ARR_SB_B_PORC__53), que además tiene precio desde 1993",
}

# Hoja 4.9.4: rotulada «Base 1993», pero el índice de términos del intercambio vale 100 y la ganancia 0 en
# 2004 (en 1993: 83,6 y -2.495), las exportaciones de 2004 en «dólares de 1993» (34.608) son las
# nominales de 2004, y los valores son los de la hoja 13 (base 2004) en 1986-2013; la API de series de
# tiempo continúa estos ID hasta 2023 con los valores de la hoja 13.
# Hoja 18: el IPMP vigente vale 100,000 en diciembre de 2001 (su promedio de 2001 es 107,4).
UNIDADES = {
    ("datos.gob.ar", "120.1_ED1_1993_0_26"): USD_2004,
    ("datos.gob.ar", "120.1_GPI_1993_0_28"): USD_2004,
    ("datos.gob.ar", "120.1_PCE_1993_0_24"): USD_2004,
    ("datos.gob.ar", "120.1_TI_1993_0_20"): INDICE_2004,
    ("datos.gob.ar", "88.1_IPMPI_1995_A_35"): "Índice dic-2001=100",
    ("datos.gob.ar", "88.2_IPMPI_1995_T_35"): "Índice dic-2001=100",
    ("datos.gob.ar", "88.3_IPMPI_1995_M_35"): "Índice dic-2001=100",
}


# =================================================================================================
# Despacho
# =================================================================================================
_HOJAS_SECTOR_EXTERNO = {
    "1. ICA": _ica,
    "2. X Rubro": _exportaciones_por_rubro,
    "3. M usos": _importaciones_por_uso,
    "10. X pyq": _indices_por_rubro,
    "11. M pyq": _indices_por_rubro,
    "12. TdI": _terminos_del_intercambio,
    "13. Poder de Compra X": _poder_de_compra,
    "17. ETI": _turismo,
    "18. IPMP": _ipmp,
    "19. X y M Intensidad tecnol.": _intensidad_tecnologica,
    "34. Embarques": _embarques,
    "35. Liquidaciones OyC CIARA-CEC": _liquidacion,
    "38. Precios FOB oficiales": _precios_fob,
    "39. Empresas exportadoras": _empresas_exportadoras,
    "40. Exportaciones por empresas": _empresas_exportadoras,
    "41. Indices de difusión": _difusion_41,
}
_HOJAS_PRECIOS = {
    "4.9.1": _indices_generales,
    "4.9.2": _indices_generales,
    "4.9.4": _poder_de_compra_1993,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == "indec-ica":
        return _indec_ica(serie)
    if serie.fuente != "datos.gob.ar":
        return None
    if serie.archivo == "sector_externo.xlsx":
        regla = _HOJAS_SECTOR_EXTERNO.get(serie.hoja)
    elif serie.archivo == "precios.xlsx":
        regla = _HOJAS_PRECIOS.get(serie.hoja)
    else:
        return None
    return regla(serie) if regla else None
