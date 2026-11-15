"""Precios mayoristas, costo de la construcción, precios implícitos, precios internacionales,
expectativas (REM y encuesta de la UTDT), canastas básicas, cigarrillos y tasas trimestrales.

Cada serie se reconoce por ``fuente``, ``archivo`` y ``hoja``; nunca por la clasificación
previa. La identidad de las series de datos.gob.ar está en ``Variable``, en ``Descripción``
o en el código del ``ID``; se usa el campo más confiable de cada hoja (varios títulos y
descripciones de la fuente tienen errores que se verificaron contra los datos).

Árbol (Tema, indicador, cobertura o versión, apertura):

    Precios > Precios mayoristas > IPIM | IPIB | IPP > sector de la clasificación
    Precios > Precios mayoristas > Series históricas > IPIM 1993=100 | IPIB 1993=100 | IPP 1993=100 > sector
    Precios > ICC
    Precios > Precios implícitos del PIB > Sectores de actividad | Oferta y demanda globales | Series históricas (base 1993)
    Precios > Precios internacionales de productos básicos > Índices | Productos > categoría
    Precios > Expectativas > REM > variable | Encuesta de expectativas de inflación
    Precios > Canastas básicas
    Precios > Precios de cigarrillos
    Sector externo > Índices de precios y cantidades del comercio exterior
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, limpiar, oracion, unir

PRECIOS = "Precios"

# --- Unidades que la fuente no informa ---------------------------------------------------------
INDICE_DIC_2015 = "Índice dic-2015=100"
INDICE_1993 = "Índice 1993=100"
INDICE_2004 = "Índice 2004=100"
INDICE_2005 = "Índice 2005=100"
PORCENTAJE = "%"
# Las variaciones de las tablas 4.3 a 4.6 y la anual de 4.13.1 vienen como fracción (0,03 = 3 %).
PROPORCION = "Proporción (1 = 100%)"

# --- Medidas ---------------------------------------------------------------------------------
VAR_MENSUAL = "var. % mensual"
VAR_TRIMESTRAL = "var. % trimestral"
VAR_INTERANUAL = "var. % interanual"
VAR_ACUMULADA_TRIMESTRE = "var. % acumulada en el trimestre"

# =================================================================================================
# Precios mayoristas (SIPM del INDEC)
# =================================================================================================
MAYORISTAS = "Precios mayoristas"
HISTORICAS = "Series históricas"

# Grupos de apertura de los tres índices (los mismos en la serie vigente y en las históricas).
AGREGADOS = "Nivel general y agregados"
PRIMARIOS = "Productos primarios"
ALIMENTOS = "Alimentos, bebidas y tabaco"
TEXTILES = "Textiles, prendas y cuero"
MADERA_PAPEL = "Madera, papel e impresiones"
QUIMICA = "Combustibles, química, caucho y plástico"
MINERALES_METALES = "Minerales no metálicos y metales"
MAQUINARIA = "Maquinaria, equipos y vehículos"
OTRAS_MANUFACTURAS = "Muebles y otras manufacturas"

_IMPORTADOS = "Importados"

# Códigos de agregación del SIPM que no son parte de la clasificación por divisiones:
# nivel general, origen nacional/importado, grandes grupos (1 y 2) y origen del producto (A a E).
_CODIGOS_AGREGADOS = {"NG", "N", "I", "1", "2", "A", "B", "C", "D", "E"}

# División de la clasificación (dos primeros dígitos del código) → grupo.
_GRUPO_POR_DIVISION = {
    "15": ALIMENTOS, "16": ALIMENTOS,
    "17": TEXTILES, "18": TEXTILES, "19": TEXTILES,
    "20": MADERA_PAPEL, "21": MADERA_PAPEL, "22": MADERA_PAPEL,
    "23": QUIMICA, "24": QUIMICA, "25": QUIMICA,
    "26": MINERALES_METALES, "27": MINERALES_METALES, "28": MINERALES_METALES,
    "29": MAQUINARIA, "31": MAQUINARIA, "32": MAQUINARIA, "33": MAQUINARIA, "34": MAQUINARIA,
    "35": MAQUINARIA,
    "36": OTRAS_MANUFACTURAS,
}

# Nombres que el INDEC repite en dos niveles de la clasificación: se aclara el más amplio o el
# residual para que dos series de una misma hoja no queden con el mismo nombre.
_NOMBRE_POR_CODIGO = {
    "241": "Sustancias químicas básicas, abonos y plásticos",   # 241 = 2411 + 2412 + 2413
    "2429": "Otros productos químicos no clasificados en otra parte",
    "3599": "Otros medios de transporte no clasificados en otra parte",
}

_N_C_E_P = re.compile(r"\s+n\.c\.e\.p\.?$")
_NO_CLASIFICADOS = " no clasificados en otra parte"


def _item_sipm(texto: str) -> str:
    """Nombre del ítem en minúsculas con tildes; «n.c.e.p.» se escribe completo."""
    item = oracion(texto)
    return _N_C_E_P.sub(_NO_CLASIFICADOS, item)


def _grupo_codigo_sipm(codigo: str) -> str | None:
    if codigo in _CODIGOS_AGREGADOS:
        return AGREGADOS
    if not codigo.isdigit():
        return None
    if codigo[0] == "0" or codigo in {"11", "14"}:
        return PRIMARIOS
    return _GRUPO_POR_DIVISION.get(codigo[:2])


def _mayorista_vigente(serie: Serie) -> Clasificacion | None:
    """Series de ``indec-sipm`` (IPIM, IPIB e IPP; base 1993, referencia dic-2015=100)."""
    indice = serie.hoja
    if indice not in ("IPIM", "IPIB", "IPP"):
        return None
    codigo = serie.variable
    grupo = _grupo_codigo_sipm(codigo)
    if grupo is None:
        return None
    prefijo = f"{codigo} - "
    if codigo in _NOMBRE_POR_CODIGO:
        item = _NOMBRE_POR_CODIGO[codigo]
    elif serie.columna.startswith(prefijo):
        item = _item_sipm(serie.columna[len(prefijo):])
    else:
        return None
    return Clasificacion(PRECIOS, MAYORISTAS, unir(indice, item), cobertura=indice, apertura=grupo,
                         unidad=INDICE_DIC_2015)


# Tablas «4 dígitos» de datos.gob.ar con el nivel general vigente (dic-2015=100).
_NIVEL_GENERAL_VIGENTE = {
    "4.3.1 IPIM 4 dígitos": "IPIM",
    "4.4.1 IPIB 4 dígitos": "IPIB",
    "4.5.1 IPP 4 dígitos": "IPP",
}


def _nivel_general_vigente(serie: Serie) -> Clasificacion | None:
    indice = _NIVEL_GENERAL_VIGENTE[serie.hoja]
    if serie.variable != "nivel_general":
        return None
    return Clasificacion(PRECIOS, MAYORISTAS, unir(indice, "Nivel general"), cobertura=indice,
                         apertura=AGREGADOS, unidad=INDICE_DIC_2015)


# --- Series históricas: tablas agregadas 4.3, 4.4 y 4.5 (base 1993, 1993=100) ---------------------

# Código del ID (segundo tramo) → ítem. «I1» es el prefijo de las series del INDEC; el ID de
# productos importados es «IPI» y sus variaciones «IPIVM» o «I1PIVM».
_AGREGADO_HISTORICO = re.compile(r"^(?:I1)?(NG|TPN|PNP|PP|PNMEE|PMEE|PRNI|PRPMEE|PI|IPI)(VA|VM)?$")
_ITEM_AGREGADO_HISTORICO = {
    "NG": "Nivel general",
    "TPN": "Productos nacionales",
    "PNP": "Primarios", "PP": "Primarios",
    "PNMEE": "Industria manufacturera y energía eléctrica",
    "PMEE": "Industria manufacturera y energía eléctrica",
    "PI": "Productos importados", "IPI": "Productos importados",
    "PRNI": "Precios relativos nacionales/importados",
    "PRPMEE": "Precios relativos primarios/manufacturados y energía eléctrica",
}
_INDICE_POR_HOJA_HISTORICA = {"4.3 IPIM": "IPIM", "4.4 IPIB": "IPIB", "4.5 IPP": "IPP"}


def _apertura_historica(indice: str, grupo: str) -> str:
    """El IPP histórico sólo tiene agregados: se omite el grupo para no dejar un nivel de un solo hijo."""
    return "" if indice == "IPP" else grupo


def _tramo_id(serie: Serie) -> str:
    partes = serie.id.split("_")
    return partes[1] if len(partes) > 1 else ""


def _medida_variacion(sufijo: str | None, frecuencia: str) -> str | None:
    """«VA» es la variación contra el mismo período del año anterior y «VM» la del período
    anterior: mensual en las series mensuales y trimestral en las trimestrales (la fuente
    la llama «mensual» en ambas, pero en las trimestrales los datos son trimestrales)."""
    if sufijo is None:
        return ""
    if sufijo == "VA":
        return VAR_INTERANUAL
    if frecuencia == "M":
        return VAR_MENSUAL
    if frecuencia == "T":
        return VAR_TRIMESTRAL
    return None


def _unidad_variacion_fraccion(medida: str) -> str:
    return PROPORCION if medida else INDICE_1993


def _mayorista_historico_agregado(serie: Serie) -> Clasificacion | None:
    indice = _INDICE_POR_HOJA_HISTORICA[serie.hoja]
    encontrado = _AGREGADO_HISTORICO.fullmatch(_tramo_id(serie))
    if not encontrado:
        return None
    item = _ITEM_AGREGADO_HISTORICO[encontrado.group(1)]
    medida = _medida_variacion(encontrado.group(2), serie.frecuencia)
    if medida is None:
        return None
    return Clasificacion(PRECIOS, MAYORISTAS, con_medida(unir(indice, item), medida),
                         cobertura=HISTORICAS, apertura=f"{indice} 1993=100",
                         medida=_apertura_historica(indice, AGREGADOS), unidad=_unidad_variacion_fraccion(medida))


# --- Series históricas: tablas de cuatro dígitos 4.11 (IPIM) y 4.12 (IPIB) --------------------------

_SECTOR_HISTORICO = {
    PRIMARIOS: (
        "Cereales y oleaginosas", "Hortalizas y legumbres", "Frutas", "Frutas y cafés",
        "Ganado vacuno y leche", "Ganado porcino y productos de granja",
        "Petróleo crudo y gas natural", "Piedras, arenas y arcillas",
        "Gomas naturales y otros productos de la silvicultura", "Cauchos naturales",
        "Productos minerales para la fabricación de abonos y sustancias químicas",
    ),
    AGREGADOS: ("Productos pesqueros", "Energía eléctrica"),
    ALIMENTOS: (
        "Productos cárnicos", "Conservas de pescados", "Conservas de frutas, hortalizas y legumbres",
        "Aceites y grasas vegetales", "Productos lácteos", "Harinas", "Productos derivados del almidón",
        "Alimentos balanceados", "Productos de panadería", "Azúcar", "Productos de chocolate y golosinas",
        "Productos farináceos", "Otros productos alimenticios no clasificados en otra parte",
        "Alimentos diversos no clasificados en otra parte",
        "Alcohol etílico y bebidas alcohólicas destiladas", "Bebidas alcohólicas destiladas",
        "Vinos y sidra", "Cervezas", "Bebidas no alcohólicas", "Tabaco",
    ),
    TEXTILES: (
        "Fibras e hilados manufacturados", "Fibras, hilados y tejidos textiles",
        "Tejidos y artículos de punto", "Ropa de cama y mantelería", "Prendas de materiales textiles",
        "Cueros curtidos", "Valijas y artículos de marroquinería", "Calzado",
    ),
    MADERA_PAPEL: (
        "Maderas aserradas", "Tableros y paneles de madera", "Carpintería de madera", "Papeles",
        "Papel y productos de papel", "Envases de papel y cartón", "Otros artículos de papel y cartón",
        "Diarios y revistas", "Artículos de librería", "Grabaciones y reproducciones",
    ),
    QUIMICA: (
        "Productos refinados del petróleo", "Sustancias químicas básicas", "Abonos y fertilizantes",
        "Sustancias plásticas y elastómeros", "Insecticidas y plaguicidas",
        "Pinturas, barnices, enduídos y tintas de imprenta", "Productos farmacéuticos y veterinarios",
        "Jabones y detergentes", "Otros productos químicos no clasificados en otra parte",
        "Cubiertas de caucho", "Otros productos de caucho", "Productos de plástico excepto muebles",
    ),
    MINERALES_METALES: (
        "Vidrio y productos de vidrio", "Productos de cerámica no refractaria para uso no estructural",
        "Productos de cerámica refractaria",
        "Productos de arcilla y cerámica no refractaria para uso estructural", "Cemento y cal",
        "Artículos de hormigón, de cemento y de yeso",
        "Otros artículos de minerales no metalíferos no clasificados en otra parte",
        "Hierro y acero en formas básicas", "Minerales no ferrosos en formas básicas",
        "Productos fundidos de metales", "Productos metálicos para uso estructural",
        "Generadores de vapor", "Herramientas de mano y artículos de ferretería",
        "Otros productos metálicos no clasificados en otra parte",
    ),
    MAQUINARIA: (
        "Motores a explosión excepto para automotores", "Bombas y compresores", "Rodamientos",
        "Equipos de elevación y manipulación", "Otra maquinaria de uso general",
        "Máquinas de uso general y sus piezas y partes", "Máquinas agrícolas", "Máquinas herramientas",
        "Máquinas para la industria extractiva y obras de construcción",
        "Máquinas para la elaboración o procesamiento de alimentos y bebidas",
        "Máquinas para lavaderos industriales",
        "Máquinas de uso especial en la industria manufacturera y sus accesorios",
        "Armas y municiones", "Aparatos de uso doméstico", "Máquinas de oficina e informática",
        "Motores, generadores y transformadores eléctricos",
        "Aparatos de distribución y control eléctrico", "Conductores eléctricos",
        "Acumuladores eléctricos", "Equipos de iluminación",
        "Otros equipos eléctricos no clasificados en otra parte", "Componentes electrónicos",
        "Equipos de televisión y video", "Aparatos de grabación y reproducción de sonidos",
        "Equipos para medicina", "Instrumentos de medición de servicios domiciliarios",
        "Automotores y sus motores", "Carrocerías y remolques", "Repuestos para automotores",
        "Motocicletas", "Otros medios de transporte no clasificados en otra parte",
    ),
    OTRAS_MANUFACTURAS: (
        "Muebles y colchones", "Otros productos manufacturados no clasificados en otra parte",
    ),
}
_GRUPO_DEL_ITEM_HISTORICO = {item: grupo for grupo, items in _SECTOR_HISTORICO.items() for item in items}

# Nombres de las tablas de cuatro dígitos que corresponden a un ítem con otro nombre oficial
# (misma serie del SIPM: mismo código de la clasificación).
_SINONIMOS_HISTORICOS = {
    "Tabacos": "Tabaco",
    "Calzados": "Calzado",
    "Productos refinados de petróleo": "Productos refinados del petróleo",
    "Otros productos químicos": "Otros productos químicos no clasificados en otra parte",
    "Otros medios de transporte": "Otros medios de transporte no clasificados en otra parte",
    "Hierros y aceros en formas básicas": "Hierro y acero en formas básicas",
    "Papeles y productos de papel": "Papel y productos de papel",   # división 21 y sus tres clases
}

# Las tablas de cuatro dígitos traen, para once productos, dos series con el mismo nombre: la
# del producto nacional y la del importado (en la clasificación del SIPM el código 7xxx es el
# del producto importado: 2010/7010 maderas, 2411/7411 sustancias químicas básicas, 2412/7412
# abonos, 2699/7699 otros minerales no metalíferos). El nombre y las etiquetas «(ii)» de la
# fuente no son confiables (varias están cruzadas entre hojas y frecuencias); lo que sí es
# parejo es el código del ID: el que termina en «A» es la serie del producto importado, y lo
# confirman los datos (cae sobre la devaluación de 2002 como el índice de importados, el doble
# que el nacional, y es mucho más volátil). La única excepción es el hierro y acero, que no
# lleva «A» pero tiene la misma firma en los datos: es la serie con nombre en singular.
_ACRONIMOS_IMPORTADOS = {
    "AFA", "ADCEA", "IPA", "MAA", "MHA", "MNFFBA", "OAMNMA", "OPQA", "PFVA", "SPEA", "SQBA",
}
_TABLAS_CUATRO_DIGITOS = {"4.11": "IPIM", "4.12": "IPIB"}


def _item_cuatro_digitos(descripcion: str) -> str:
    texto = re.split(r"\s+índice\b", descripcion, maxsplit=1)[0]
    texto = re.sub(r"\s*\((?:ii|\d{4})\)", "", texto, flags=re.IGNORECASE)   # «(ii)», «(7412)»
    texto = re.sub(r"\s+II$", "", texto)
    texto = " ".join(texto.split()).strip(" ;,")
    texto = limpiar(_N_C_E_P.sub(_NO_CLASIFICADOS, texto))
    return _SINONIMOS_HISTORICOS.get(texto, texto)


def _es_importado(serie: Serie) -> bool:
    if _tramo_id(serie) in _ACRONIMOS_IMPORTADOS:
        return True
    return serie.variable.startswith("hierro_acero_formas_basicas")


def _mayorista_historico_cuatro_digitos(serie: Serie) -> Clasificacion | None:
    indice = _TABLAS_CUATRO_DIGITOS[serie.hoja]
    item = _item_cuatro_digitos(serie.descripcion)
    grupo = _GRUPO_DEL_ITEM_HISTORICO.get(item)
    if grupo is None:
        return None
    nombre = unir(indice, item, _IMPORTADOS if _es_importado(serie) else "")
    return Clasificacion(PRECIOS, MAYORISTAS, nombre, cobertura=HISTORICAS,
                         apertura=f"{indice} 1993=100", medida=grupo, unidad=INDICE_1993)


# =================================================================================================
# ICC
# =================================================================================================
_ICC = re.compile(r"^I1(NG|GG|MOM|MO|M)(VA|VM)?$")
_ITEM_ICC = {
    "NG": "Nivel general", "GG": "Gastos generales", "MO": "Mano de obra", "M": "Materiales",
    "MOM": "Precios relativos mano de obra/materiales",
}


def _icc(serie: Serie) -> Clasificacion | None:
    encontrado = _ICC.fullmatch(_tramo_id(serie))
    if not encontrado:
        return None
    medida = _medida_variacion(encontrado.group(2), serie.frecuencia)
    if medida is None:
        return None
    return Clasificacion(PRECIOS, "ICC", con_medida(unir("ICC", _ITEM_ICC[encontrado.group(1)]), medida),
                         unidad=_unidad_variacion_fraccion(medida))


# =================================================================================================
# Precios implícitos del PIB
# =================================================================================================
IMPLICITOS = "Precios implícitos del PIB"
_POR_SECTOR = "Valor agregado por sector"
_OFERTA_DEMANDA = "Oferta y demanda globales"

# 4.7.1: deflactores del valor agregado bruto por sector (base 2004), por ``Variable``.
_SECTORES_2004 = {
    "ipi_agricultura_ganaderia_caza_silvicultura": "Agricultura, ganadería, caza y silvicultura",
    "ipi_pesca": "Pesca",
    "ipi_explotacion_minas_canteras": "Explotación de minas y canteras",
    "ipi_industria_manufacturera": "Industria manufacturera",
    "ipi_suministro_electricidad_gas_agua": "Suministro de electricidad, gas y agua",
    "ipi_construccion": "Construcción",
    "ipi_comercio_mayorista_minorista_reparaciones": "Comercio mayorista y minorista y reparaciones",
    "ipi_hoteles_restaurantes": "Hoteles y restaurantes",
    "ipi_transporte_almacenamiento_comunicaciones": "Transporte, almacenamiento y comunicaciones",
    "ipi_intermediacion_financiera": "Intermediación financiera",
    "ipi_actividades_inmobiliarias_empresariales_alquiler":
        "Actividades inmobiliarias, empresariales y de alquiler",
    "ipi_administracion_publica_defensa_seguridad_social":
        "Administración pública y defensa; planes de seguridad social de afiliación obligatoria",
    "ipi_ensenianza": "Enseñanza",
    "ipi_servicios_sociales_salud": "Servicios sociales y de salud",
    "ipi_otras_actividades_ser_sociales_personales":
        "Otras actividades de servicios comunitarias, sociales y personales",
    "ipi_hogares_privados_servicio_domestico": "Hogares privados con servicio doméstico",
    "ipi_sectores_productores_bienes": "Sectores productores de bienes",
    "ipi_sectores_productores_servicios": "Sectores productores de servicios",
    "ipi_valor_agregado_bruto_precios_basicos": "Valor agregado bruto a precios básicos",
    "ipi_impuestos_productos_netos_subsidios": "Impuestos a los productos netos de subsidios",
    "ipi_impuesto_valor_agregado": "Impuesto al valor agregado",
    "ipi_derechos_importacion": "Derechos de importación",
    "ipi_producto_interno_bruto_precios_mercado": "PIB",
}

# 4.7.2: oferta y demanda globales por componente (base 2004), por ``Variable``.
_COMPONENTES_2004 = {
    "ipi_oferta_global_pib": "PIB",
    "ipi_oferta_global_importacion": "Importaciones",
    "ipi_oferta_global_total": "Oferta global",
    "ipi_manda_global_consumo_privado": "Consumo privado",
    "ipi_demanda_global_consumo_publico": "Consumo público",
    "ipi_demanda_global_exportacion": "Exportaciones",
    "ipi_demanda_global_total": "Demanda global",
    "ipi_demanda_global_ibif_total": "Formación bruta de capital fijo",
    "ipi_demanda_global_ibif_construccion": "Inversión en construcciones",
    "ipi_demanda_global_ibif_otras_construcciones": "Inversión en otras construcciones",
    "ipi_demanda_global_ibif_equipo_durable_produccion_total": "Inversión en equipos durables de producción",
    "ipi_demanda_global_ibif_equipo_durable_produccion_nacional":
        "Inversión en equipos durables de producción · Nacionales",
    "ipi_demanda_global_ibif_equipo_durable_produccion_importado":
        "Inversión en equipos durables de producción · Importados",
    "ipi_dg_ibif_equipo_durable_prod_maquinaria_equipo_total": "Inversión en maquinaria y equipo",
    "ipi_dg_ibif_equipo_durable_prod_maquinaria_equipo_nacional": "Inversión en maquinaria y equipo · Nacionales",
    "ipi_dg_ibif_equipo_durable_prod_maquinaria_equipo_importado": "Inversión en maquinaria y equipo · Importados",
    "ipi_dg_ibif_equipo_durable_prod_material_transporte_total": "Inversión en material de transporte",
    "ipi_dg_ibif_equipo_durable_prod_material_transporte_nacional":
        "Inversión en material de transporte · Nacionales",
    "ipi_dg_ibif_equipo_durable_prod_material_transporte_imp":
        "Inversión en material de transporte · Importados",
    "ipi_demanda_global_ibif_otros_activos_fijos": "Inversión en otros activos fijos",
}

# 4.13: base 1993 (discontinuada), por el código del ID. Es la única versión histórica, así que las
# series cuelgan directamente de «Series históricas» (sin un nivel «Base 1993» de un solo hijo).
# En la tabla del PIB las descripciones y los nombres de variable de la variación trimestral y la
# interanual están cruzados; el código del ID sí es correcto (IPCVA es la interanual, IPCVT la
# trimestral: se verificó contra el nivel).
_PIB_1993 = {"IPC": ("PIB", ""), "IPCVA": ("PIB", VAR_INTERANUAL), "IPCVT": ("PIB", VAR_TRIMESTRAL)}
_SECTORES_1993 = {
    "IPB": "Sectores productores de bienes",
    "IPS": "Sectores productores de servicios",
    "IPBS": "Sectores productores de bienes y de servicios",
}


def _implicitos_2004(serie: Serie, diccionario: dict[str, str], apertura: str) -> Clasificacion | None:
    item = diccionario.get(serie.variable)
    if item is None:
        return None
    return Clasificacion(PRECIOS, IMPLICITOS, unir("Precios implícitos", item), cobertura=apertura,
                         unidad=INDICE_2004)


def _implicitos_1993(serie: Serie) -> Clasificacion | None:
    codigo = _tramo_id(serie)
    if serie.hoja == "4.13.1":
        if codigo not in _PIB_1993:
            return None
        item, medida = _PIB_1993[codigo]
        # La variación interanual anual viene como fracción; las trimestrales, en puntos porcentuales.
        if not medida:
            unidad = INDICE_1993
        else:
            unidad = PROPORCION if serie.frecuencia == "A" else PORCENTAJE
    else:
        if codigo not in _SECTORES_1993:
            return None
        item, medida, unidad = _SECTORES_1993[codigo], "", INDICE_1993
    return Clasificacion(PRECIOS, IMPLICITOS, con_medida(unir("Precios implícitos", item), medida),
                         cobertura=HISTORICAS, unidad=unidad)


# =================================================================================================
# Precios internacionales de productos básicos (4.10)
# =================================================================================================
COMMODITIES = "Precios internacionales de productos básicos"
_INDICES_COMMODITIES = "Índices"
_PRODUCTOS_COMMODITIES = "Productos"
_NOMBRE_INDICE_COMMODITIES = "Índice de precios de productos básicos"

C_ALIMENTOS = "Alimentos"
C_BEBIDAS = "Bebidas"
C_AGROPECUARIOS = "Agropecuarios no comestibles"
C_METALES = "Metales y minerales"
C_ENERGIA = "Energía"

_TONELADA = "US$ por tonelada"
_CENTAVOS_LIBRA = "Centavos de US$ por libra"
_BTU = "US$ por millón de BTU"

# Variable → (nombre, cobertura o mercado, categoría, unidad). La unidad coincide con la que
# publica la descripción de cada serie (salvo uranio, que la publica sin punto).
_PRODUCTOS = {
    "aceite_colza": ("Aceite de colza", "", C_ALIMENTOS, _TONELADA),
    "aceite_girasol": ("Aceite de girasol", "", C_ALIMENTOS, _TONELADA),
    "aceite_oliva": ("Aceite de oliva", "", C_ALIMENTOS, _TONELADA),
    "aceite_palma": ("Aceite de palma", "", C_ALIMENTOS, _TONELADA),
    "aceite_soja": ("Aceite de soja", "", C_ALIMENTOS, _TONELADA),
    "arroz": ("Arroz", "", C_ALIMENTOS, _TONELADA),
    "azucar_eeuu": ("Azúcar", "Estados Unidos", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "azucar_europa": ("Azúcar", "Europa", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "azucar_mercado_libre": ("Azúcar", "Mercado libre", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "banana": ("Banana", "", C_ALIMENTOS, _TONELADA),
    "carne_bovina": ("Carne bovina", "", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "cebada": ("Cebada", "", C_ALIMENTOS, _TONELADA),
    "cerdo": ("Cerdo", "", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "cordero": ("Cordero", "", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "harina_pescado": ("Harina de pescado", "", C_ALIMENTOS, _TONELADA),
    "harina_soja": ("Harina de soja", "", C_ALIMENTOS, _TONELADA),
    "langostino": ("Langostino", "", C_ALIMENTOS, "US$ por kg"),
    "maiz": ("Maíz", "", C_ALIMENTOS, _TONELADA),
    "mani": ("Maní", "", C_ALIMENTOS, _TONELADA),
    "naranja": ("Naranja", "", C_ALIMENTOS, _TONELADA),
    "pollo": ("Pollo", "", C_ALIMENTOS, _CENTAVOS_LIBRA),
    "poroto_soja": ("Poroto de soja", "", C_ALIMENTOS, _TONELADA),
    "salmon": ("Salmón", "", C_ALIMENTOS, "US$ por kg"),
    "trigo": ("Trigo", "", C_ALIMENTOS, _TONELADA),
    "cacao": ("Cacao", "", C_BEBIDAS, _TONELADA),
    "cafe_arabica": ("Café arábica", "", C_BEBIDAS, _CENTAVOS_LIBRA),
    "cafe_robusta": ("Café robusta", "", C_BEBIDAS, _CENTAVOS_LIBRA),
    "te": ("Té", "", C_BEBIDAS, "Centavos de US$ por kg"),
    "algodon": ("Algodón", "", C_AGROPECUARIOS, _CENTAVOS_LIBRA),
    "caucho": ("Caucho", "", C_AGROPECUARIOS, _CENTAVOS_LIBRA),
    "cuero": ("Cuero", "", C_AGROPECUARIOS, _CENTAVOS_LIBRA),
    "lana_fina": ("Lana fina", "", C_AGROPECUARIOS, "Centavos de US$ por kg"),
    "lana_gruesa": ("Lana gruesa", "", C_AGROPECUARIOS, "Centavos de US$ por kg"),
    "madera_blanda_aserrada": ("Madera blanda aserrada", "", C_AGROPECUARIOS, "US$ por m³"),
    "madera_dura_aserrada": ("Madera dura aserrada", "", C_AGROPECUARIOS, "US$ por m³"),
    "tronco_madera_blanda": ("Tronco de madera blanda", "", C_AGROPECUARIOS, "US$ por m³"),
    "tronco_madera_dura": ("Tronco de madera dura", "", C_AGROPECUARIOS, "US$ por m³"),
    "aluminio": ("Aluminio", "", C_METALES, _TONELADA),
    "cobre": ("Cobre", "", C_METALES, _TONELADA),
    "estanio": ("Estaño", "", C_METALES, _TONELADA),
    "mineral_hierro": ("Mineral de hierro", "", C_METALES, _TONELADA),
    "niquel": ("Níquel", "", C_METALES, _TONELADA),
    "plomo": ("Plomo", "", C_METALES, _TONELADA),
    "uranio": ("Uranio", "", C_METALES, "US$ por libra"),
    "zinc": ("Zinc", "", C_METALES, _TONELADA),
    "carbon_australia": ("Carbón", "Australia", C_ENERGIA, _TONELADA),
    "carbon_sudafrica": ("Carbón", "Sudáfrica", C_ENERGIA, _TONELADA),
    "gas_natural_eeuu": ("Gas natural", "Estados Unidos", C_ENERGIA, _BTU),
    "gas_natural_rusia": ("Gas natural", "Rusia", C_ENERGIA, _BTU),
    "gas_natural_licuado": ("Gas natural licuado", "", C_ENERGIA, _BTU),
    "petroleo_brent": ("Petróleo", "Brent", C_ENERGIA, "US$ por barril"),
    "petroleo_dubai": ("Petróleo", "Dubai", C_ENERGIA, "US$ por barril"),
    "petroleo_wti": ("Petróleo", "WTI", C_ENERGIA, "US$ por barril"),
    "petroleo_general": ("Petróleo", "Promedio", C_ENERGIA, "US$ por barril"),
}

_INDICES = {
    "general_commodities": "General",
    "no_energeticos": "No energéticos",
    "alimentos_bebidas": "Alimentos y bebidas",
    "alimentos": "Alimentos",
    "bebidas": "Bebidas",
    "agropecuarios_no_comestibles": "Agropecuarios no comestibles",
    "insumos_industriales": "Insumos industriales",
    "metales": "Metales",
    "energia": "Energía",
    "petroleo": "Petróleo",
}


def _commodities(serie: Serie) -> Clasificacion | None:
    if serie.variable in _INDICES:
        return Clasificacion(PRECIOS, COMMODITIES, unir(_NOMBRE_INDICE_COMMODITIES, _INDICES[serie.variable]),
                             cobertura=_INDICES_COMMODITIES, unidad=INDICE_2005)
    producto = _PRODUCTOS.get(serie.variable)
    if producto is None:
        return None
    nombre, mercado, categoria, unidad = producto
    return Clasificacion(PRECIOS, COMMODITIES, unir("Precio internacional", nombre, mercado),
                         cobertura=_PRODUCTOS_COMMODITIES, apertura=categoria, unidad=unidad)


# =================================================================================================
# Expectativas: REM y encuesta de expectativas de inflación
# =================================================================================================
EXPECTATIVAS = "Expectativas"
REM = "REM"

# Prefijo de la variable → (nombre en el REM, grupo, unidad). Los datos mostraron que varias
# series que la fuente llama «variación» son en realidad niveles: el tipo de cambio ($ por
# US$ entre 15 y 2.600), la tasa de interés (fracción), el resultado primario y las
# exportaciones e importaciones (la fuente dice «miles de millones de dólares» pero los
# valores, 4.000 por mes y 88.000 por año, están en millones).
_REM_VARIABLES = {
    "ipc": ("IPC", "IPC", PROPORCION),
    "ipc_nucleo": ("IPC núcleo", "IPC núcleo", PROPORCION),
    "tcn": ("Tipo de cambio nominal", "Tipo de cambio nominal", "Pesos por US$"),
    "tasa_politica": ("Tasa de interés", "Tasa de interés", PROPORCION),
    "pib": ("PIB", "PIB", PROPORCION),
    "resultado_primario_spnf": ("Resultado primario del sector público no financiero",
                                "Resultado primario del sector público no financiero",
                                "Miles de millones de pesos"),
    "desocupacion": ("Tasa de desocupación abierta", "Tasa de desocupación abierta", PROPORCION),
    "expo": ("Exportaciones", "Exportaciones", "Millones de US$"),
    "impo": ("Importaciones", "Importaciones", "Millones de US$"),
}
_REM_PREFIJOS = ("tasa_politica", "resultado_primario_spnf", "desocupacion", "ipc", "tcn", "pib", "expo", "impo")
_ESTADISTICA = "mediana"


def _mes(paso: str | None) -> str:
    return "mes t" if not paso else f"mes t+{paso}"


def _trimestre(paso: str | None) -> str:
    if paso is None:
        return "trimestre t"
    return "trimestre t-1" if paso == "menos_1" else f"trimestre t+{paso}"


def _horizonte_rem(clave: str, resto: str) -> tuple[str, str] | None:
    """(horizonte, medida) de la parte final de la variable; None si no se reconoce.

    ``t`` es el mes (o trimestre) del relevamiento; la fuente escribe «t 1» por «t+1». Cada
    variable sólo admite los horizontes que el REM le pregunta a los analistas.
    """
    es_precio = clave in ("ipc", "ipc_nucleo")
    encontrado = re.fullmatch(r"var_ia_(\d{4})", resto)
    if encontrado and clave in ("ipc", "ipc_nucleo", "pib"):
        return f"año {encontrado.group(1)}", VAR_INTERANUAL
    if encontrado and clave in ("tcn", "tasa_politica", "resultado_primario_spnf"):
        return f"año {encontrado.group(1)}", ""          # niveles esperados para el año
    encontrado = re.fullmatch(r"var_ia_t_(12|24)", resto)
    if encontrado and es_precio:
        return _mes(encontrado.group(1)), VAR_INTERANUAL
    if encontrado and clave in ("tcn", "tasa_politica") and encontrado.group(1) == "12":
        return _mes("12"), ""
    encontrado = re.fullmatch(r"var_mensual_t(?:_(\d))?", resto)
    if encontrado and es_precio:
        return _mes(encontrado.group(1)), VAR_MENSUAL
    if encontrado and clave in ("tcn", "tasa_politica") and encontrado.group(1):
        return _mes(encontrado.group(1)), ""
    encontrado = re.fullmatch(r"var_trimestral_t(?:_(menos_1|\d))?", resto)
    if encontrado and clave == "pib":
        return _trimestre(encontrado.group(1)), f"desestacionalizado, {VAR_TRIMESTRAL}"
    encontrado = re.fullmatch(r"4t_(\d{4})", resto)
    if encontrado and clave == "desocupacion":
        return f"4.º trimestre {encontrado.group(1)}", ""
    encontrado = re.fullmatch(r"t(?:_(menos_1|\d))?", resto)
    if encontrado and clave == "desocupacion":
        return _trimestre(encontrado.group(1)), ""
    encontrado = re.fullmatch(r"anual_(\d{4})", resto)
    if encontrado and clave in ("expo", "impo"):
        return f"año {encontrado.group(1)}", ""
    encontrado = re.fullmatch(r"mensual_t(?:_(\d))?", resto)
    if encontrado and clave in ("expo", "impo"):
        return _mes(encontrado.group(1)), ""
    return None


def _rem(serie: Serie) -> Clasificacion | None:
    variable = serie.variable.replace("__", "_")
    for prefijo in _REM_PREFIJOS:
        if variable.startswith(f"rem_{prefijo}_"):
            resto = variable[len(f"rem_{prefijo}_"):]
            break
    else:
        return None
    clave, cobertura = prefijo, ""
    if prefijo == "ipc":
        encontrado = re.fullmatch(r"(nac|gba)_(nucleo_)?(.+)", resto)
        if not encontrado:
            return None
        cobertura = "GBA" if encontrado.group(1) == "gba" else ""
        clave = "ipc_nucleo" if encontrado.group(2) else "ipc"
        resto = encontrado.group(3)
    horizonte = _horizonte_rem(clave, resto)
    if horizonte is None:
        return None
    etiqueta, medida = horizonte
    nombre, grupo, unidad = _REM_VARIABLES[clave]
    medidas = ", ".join(parte for parte in (_ESTADISTICA, medida) if parte)
    return Clasificacion(PRECIOS, EXPECTATIVAS, con_medida(unir(REM, nombre, cobertura, etiqueta), medidas),
                         cobertura=REM, apertura=grupo, unidad=unidad)


def _expectativa_inflacion_utdt(serie: Serie) -> Clasificacion | None:
    estadistica = {"expectativa_inflacion_mediana": "mediana", "expectativa_inflacion_promedio": "promedio"}
    if serie.variable not in estadistica:
        return None
    return Clasificacion(PRECIOS, EXPECTATIVAS,
                         con_medida(unir("Expectativas de inflación", "12 meses"), estadistica[serie.variable]),
                         cobertura="Encuesta de expectativas de inflación", unidad=PORCENTAJE)


# =================================================================================================
# Canastas básicas, cigarrillos, comercio exterior y tasas trimestrales
# =================================================================================================
_CANASTAS = {
    "canasta_basica_alimentaria": ("Canasta básica alimentaria · Adulto equivalente", "Pesos corrientes"),
    "canasta_basica_total": ("Canasta básica total · Adulto equivalente", "Pesos corrientes"),
    # La línea es la canasta por 3,09 (adultos equivalentes del hogar de referencia), todos los meses.
    "linea_indigencia": ("Línea de indigencia · Hogar de referencia", "Pesos corrientes"),
    "linea_pobreza": ("Línea de pobreza · Hogar de referencia", "Pesos corrientes"),
    "inversa_coeficiente_engel": ("Inversa del coeficiente de Engel", "Cociente CBT/CBA"),
}


def _canastas(serie: Serie) -> Clasificacion | None:
    canasta = _CANASTAS.get(serie.variable)
    if canasta is None:
        return None
    return Clasificacion(PRECIOS, "Canastas básicas", canasta[0], unidad=canasta[1])


_CIGARRILLOS = {
    "precio_inferior": "inferior",
    "precio_superior": "superior",
    "promedio_ponderado": "promedio ponderado",
}


def _cigarrillos(serie: Serie) -> Clasificacion | None:
    medida = _CIGARRILLOS.get(serie.variable)
    if medida is None:
        return None
    return Clasificacion(PRECIOS, "Precios de cigarrillos",
                         con_medida(unir("Precio de cigarrillos", "Paquete de 20 unidades"), medida),
                         unidad="Pesos")


_COMERCIO_EXTERIOR = {
    "Índice de precios de exportación. 2004=100": "Índice de precios de exportación",
    "Índice de precio de importación de bienes 2004=100": "Índice de precios de importación",
    "Índice de Términos del Intercambio. 2004=100": "Términos del intercambio",
}


def _comercio_exterior(serie: Serie) -> Clasificacion | None:
    nombre = _COMERCIO_EXTERIOR.get(limpiar(serie.variable) if serie.variable else limpiar(serie.nombre))
    if nombre is None:
        return None
    return Clasificacion("Sector externo", "Índices de precios y cantidades del comercio exterior", nombre,
                         unidad=INDICE_2004)


# 4.19: tasas de variación acumuladas de los meses de cada trimestre (en puntos porcentuales).
_MEDIDA_TRIMESTRAL = VAR_ACUMULADA_TRIMESTRE


def _tasas_trimestrales(serie: Serie) -> Clasificacion | None:
    v = serie.variable
    if v in ("ipim", "ipibm", "ipbp"):
        indice = {"ipim": "IPIM", "ipibm": "IPIB", "ipbp": "IPP"}[v]
        return Clasificacion(PRECIOS, MAYORISTAS,
                             con_medida(unir(indice, "Nivel general"), _MEDIDA_TRIMESTRAL),
                             cobertura=HISTORICAS, apertura=f"{indice} 1993=100",
                             medida=_apertura_historica(indice, AGREGADOS), unidad=PORCENTAJE)
    if v == "ic_construccion":
        return Clasificacion(PRECIOS, "ICC", con_medida(unir("ICC", "Nivel general"), _MEDIDA_TRIMESTRAL),
                             unidad=PORCENTAJE)
    if v == "ipc":
        return Clasificacion(PRECIOS, "IPC", con_medida(unir("IPC", "Nivel general"), _MEDIDA_TRIMESTRAL),
                             cobertura=HISTORICAS, apertura="Tasas trimestrales de variación",
                             unidad=PORCENTAJE)
    if v == "ip_combinado":
        return Clasificacion(PRECIOS, "Índice de precios combinado",
                             con_medida(unir("Índice de precios combinado", "IPC-IPIM"), _MEDIDA_TRIMESTRAL),
                             unidad=PORCENTAJE)
    return None


# =================================================================================================
# Despacho
# =================================================================================================
_HOJAS_PRECIOS = {
    "4.3 IPIM": _mayorista_historico_agregado,
    "4.4 IPIB": _mayorista_historico_agregado,
    "4.5 IPP": _mayorista_historico_agregado,
    "4.3.1 IPIM 4 dígitos": _nivel_general_vigente,
    "4.4.1 IPIB 4 dígitos": _nivel_general_vigente,
    "4.5.1 IPP 4 dígitos": _nivel_general_vigente,
    "4.11": _mayorista_historico_cuatro_digitos,
    "4.12": _mayorista_historico_cuatro_digitos,
    "4.6 ICC": _icc,
    "4.7.1": lambda s: _implicitos_2004(s, _SECTORES_2004, _POR_SECTOR),
    "4.7.2": lambda s: _implicitos_2004(s, _COMPONENTES_2004, _OFERTA_DEMANDA),
    "4.13.1": _implicitos_1993,
    "4.13.2": _implicitos_1993,
    "4.10": _commodities,
    "4.19": _tasas_trimestrales,
    "4.30": _rem,
    "4.31": _expectativa_inflacion_utdt,
    "4.32": _cigarrillos,
    "4.9.3": _comercio_exterior,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == "indec-sipm":
        return _mayorista_vigente(serie)
    if serie.fuente != "datos.gob.ar":
        return None
    if serie.archivo == "precios.xlsx":
        regla = _HOJAS_PRECIOS.get(serie.hoja)
        return regla(serie) if regla else None
    if serie.archivo == "empleo_ingresos.xlsx" and serie.hoja == "CBA y CBT 2016":
        return _canastas(serie)
    return None
