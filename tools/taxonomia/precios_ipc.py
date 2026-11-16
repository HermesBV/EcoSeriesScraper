"""Reglas de taxonomía del IPC: INDEC (vigente e histórico), Estados Unidos y precursores.

Ubicación (Tema «Precios», indicador «IPC»):

    IPC
    ├── Nacional · GBA · Pampeana · Noreste · Noroeste · Cuyo · Patagonia      (cobertura)
    │     └── Nivel general · Divisiones · Categorías · Bienes y servicios
    │         · Principales aperturas (sólo GBA) · Precios promedio              (apertura)
    ├── Provincias
    │     └── Ciudad Autónoma de Buenos Aires · Córdoba · Mendoza · Neuquén · San Luis
    │         · Santa Fe · Tucumán · Chaco (Gran Resistencia)                    (provincia)
    ├── Estados Unidos
    └── Series históricas
          └── «<cobertura> base <base>»                                         (versión)
                └── Nivel general · Capítulos · Categorías · Bienes y servicios
                    · Principales aperturas                                      (apertura)

Criterios que explican las decisiones (ver ``docs/taxonomia.md``):

* El IPC GBA del INDEC (cuadros 4.2.x de datos.gob.ar y hoja «IPC GBA Base dic 2016» del libro del
  INDEC) y la región GBA del IPC nacional (cuadros 4.1.x y libro nacional) miden lo mismo con la
  misma base (dic-2016=100): los valores coinciden mes a mes. Por eso comparten grupo («GBA») y
  nombres. Los cuadros 4.2.x traen rótulos viejos en su descripción («Indumentaria»,
  «Esparcimiento», «Servicios básicos y combustibles para la vivienda» para la división de
  conservación del hogar…); la identidad de cada serie se fijó comparando sus valores con la hoja
  del INDEC, que publica los nombres oficiales, y los nombres de acá son los oficiales.
* Divisiones (COICOP) de la base dic-2016 con el nombre oficial del INDEC; las bases viejas
  conservan sus «capítulos» con la denominación que le corresponde a cada base.
* Las series que la fuente publica como proporción (0,0159 = 1,59 %) llevan esa unidad: el INDEC,
  en cambio, publica porcentajes.
* Los cuadros 4.2.x y 4.1.4, 4.1.5 y 4.1.8 figuran como CERRADA en datos.gob.ar (la tabla dejó de
  actualizarse), pero la base sigue vigente: no son una versión vieja y no van a «Series históricas».
* Cuadro 4.2.4: cada columna empalma por posición dos conceptos. Hasta dic-2016 trae el que dice su
  rótulo (coincide con la hoja «IPC GBA Base dic 2016» del INDEC); desde ene-2017 trae la fila de igual
  posición del libro de aperturas del INDEC, que es otra. Sólo las 12 primeras posiciones (nivel general,
  alimentos y bebidas no alcohólicas y los diez rubros de alimentos y bebidas hasta el café) coinciden en
  todo el período y se publican; las otras 26 (por cada frecuencia) están en ``EXCLUIDAS`` con su
  evidencia. Los nombres de ese cuadro son los del rótulo; el contenido real se identificó con los valores.
* «Series históricas»: la versión se escribe «cobertura base período»; el nivel 5 es la apertura
  (Nivel general, Capítulos, Categorías, Bienes y servicios, Principales aperturas). La base 1988
  tiene más de 150 aperturas y las parte en alimentos y bebidas / otros capítulos.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import SEPARADOR, con_medida, plano

TEMA = "Precios"
INDICADOR = "IPC"

# --- Vocabulario controlado de grupos -----------------------------------------------------
NIVEL_GENERAL = "Nivel general"
DIVISIONES = "Divisiones"
CAPITULOS = "Capítulos"
CATEGORIAS = "Categorías"
BIENES_Y_SERVICIOS = "Bienes y servicios"
PRINCIPALES_APERTURAS = "Principales aperturas"
PRECIOS_PROMEDIO = "Precios promedio"
NACIONAL = "Nacional"
PROVINCIAS = "Provincias"
ESTADOS_UNIDOS = "Estados Unidos"
SERIES_HISTORICAS = "Series históricas"

# --- Medidas (van entre paréntesis en el nombre) ---------------------------------------------
VAR_MENSUAL = "var. % mensual"
VAR_INTERANUAL = "var. % interanual"
VAR_ACUMULADA = "var. % acumulada"
INCIDENCIA = "incidencia"
INCIDENCIA_ACUMULADA = "incidencia acumulada"
DESESTACIONALIZADO = "desestacionalizado"

# --- Unidades que completan lo que la fuente no informa ---------------------------------------
INDICE_2016 = "Índice dic-2016=100"
PROPORCION = "Proporción (1 = 100%)"
PROPORCION_INCIDENCIA = "Proporción (1 = 100 p. p.)"
PORCENTAJE = "%"
PESOS = "Pesos"
RELATIVO = "Cociente entre índices (×100)"

# --- Regiones -------------------------------------------------------------------------------------
# clave de la variable → nombre de la región (None: total nacional)
REGIONES = {
    "nacional": None, "gba": "GBA", "pampeana": "Pampeana", "nea": "Noreste", "noa": "Noroeste",
    "cuyo": "Cuyo", "patagonia": "Patagonia",
}
_CLAVE_REGION = "|".join(REGIONES)

# Etiquetas de región del libro del INDEC.
REGIONES_INDEC = {
    "Total nacional": None, "Región GBA": "GBA", "Región Pampeana": "Pampeana",
    "Región Noreste": "Noreste", "Región Noroeste": "Noroeste", "Región Cuyo": "Cuyo",
    "Región Patagonia": "Patagonia",
}

# --- Divisiones de la base dic-2016 (nombres oficiales del INDEC, COICOP) --------------------------
ALIMENTOS = "Alimentos y bebidas no alcohólicas"
BEBIDAS_ALCOHOLICAS = "Bebidas alcohólicas y tabaco"
PRENDAS = "Prendas de vestir y calzado"
VIVIENDA = "Vivienda, agua, electricidad, gas y otros combustibles"
EQUIPAMIENTO = "Equipamiento y mantenimiento del hogar"
SALUD = "Salud"
TRANSPORTE = "Transporte"
COMUNICACION = "Comunicación"
RECREACION = "Recreación y cultura"
EDUCACION = "Educación"
RESTAURANTES = "Restaurantes y hoteles"
BIENES_SERVICIOS_VARIOS = "Bienes y servicios varios"

# Variantes de rótulo y de variable de las fuentes → división. La clave se arma con ``_clave``.
_DIVISION_POR_CLAVE = {
    "alimentos_bebidas_no_alcoholicas": ALIMENTOS,
    "bebidas_alcoholicas_tabaco": BEBIDAS_ALCOHOLICAS,
    "prendas_vestir_calzado": PRENDAS,
    "vivienda_agua_electricidad_gas_otros_combustibles": VIVIENDA,
    "vivienda_agua_electricidad_otros_combustibles": VIVIENDA,
    "vivienda_agua_electricidad_combustibles": VIVIENDA,
    "vivienda_agua_elec_otros_combustibles": VIVIENDA,
    "vivienda_agua_electr_otros_combustibles": VIVIENDA,
    "vivienda_agua_electr_otros_combust": VIVIENDA,
    "vivienda_agua_elect_otros_combustibles": VIVIENDA,
    "equipamiento_mantenimiento_hogar": EQUIPAMIENTO,
    "equipamiento_mantenimientos_hogar": EQUIPAMIENTO,
    "salud": SALUD,
    "transporte": TRANSPORTE,
    "comunicacion": COMUNICACION,
    "comunicaciones": COMUNICACION,
    "recreacion_cultura": RECREACION,
    "educacion": EDUCACION,
    "restaurantes_hoteles": RESTAURANTES,
    "bienes_servicios_varios": BIENES_SERVICIOS_VARIOS,
    "otros_bienes_servicios": BIENES_SERVICIOS_VARIOS,
}

_ARTICULOS = {"y", "de", "del", "la", "el", "los", "las"}


def _clave(texto: str) -> str:
    """Forma comparable de un rótulo o de una variable: sin tildes, sin artículos ni «y»."""
    palabras = re.sub(r"[^a-z0-9]+", " ", plano(texto)).split()
    return "_".join(p for p in palabras if p not in _ARTICULOS)


def _division(texto: str) -> str | None:
    return _DIVISION_POR_CLAVE.get(_clave(texto))


# --- Armado de resultados ----------------------------------------------------------------------------
def _unir(*partes: str) -> str:
    """Como ``texto.unir``, pero conserva el punto final de un ítem («…golosinas, etc.»)."""
    return SEPARADOR.join(parte for parte in (" ".join(p.split()) for p in partes) if parte)


def _resultado(item: str, cobertura: str = "", apertura: str = "", subgrupo: str = "", *,
               en_nombre: str = "", medida: str = "", unidad: str = "",
               prefijo: str = INDICADOR) -> Clasificacion:
    """``cobertura``/``apertura``/``subgrupo`` son los niveles 3, 4 y 5 del árbol; ``en_nombre``
    es la cobertura que se escribe en el nombre (vacía para el total nacional)."""
    return Clasificacion(
        tema=TEMA, indicador=INDICADOR, nombre=con_medida(_unir(prefijo, item, en_nombre), medida),
        cobertura=cobertura, apertura=apertura, medida=subgrupo, unidad=unidad,
    )


def _regional(item: str, region: str | None, grupo: str, *, medida: str = "", unidad: str = "",
              prefijo: str = INDICADOR) -> Clasificacion:
    """Serie del IPC vigente: el total nacional o una región (GBA incluida)."""
    return _resultado(item, region or NACIONAL, grupo, en_nombre=region or "", medida=medida,
                      unidad=unidad, prefijo=prefijo)


def _unidad_medida(medida: str) -> str:
    return PROPORCION if medida else INDICE_2016


# ===================================================================================================
# IPC vigente, base dic-2016 (cuadros 4.1.x de datos.gob.ar)
# ===================================================================================================
_SUFIJOS_NG = {
    "": "", "_tasa_variacion_mensual": VAR_MENSUAL,
    "_tasa_variacion_acumulada_dic_ano_anterior": VAR_ACUMULADA,
}


def _ipc_ng(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"ipc_ng_({_CLAVE_REGION})(_tasa_variacion_mensual|_tasa_variacion_acumulada_dic_ano_anterior)?",
                     serie.variable)
    if not m:
        return None
    medida = _SUFIJOS_NG[m.group(2) or ""]
    return _regional("Nivel general", REGIONES[m.group(1)], NIVEL_GENERAL, medida=medida,
                     unidad=_unidad_medida(medida))


def _ipc_capitulos(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"ipc_(.+)_({_CLAVE_REGION})", serie.variable)
    division = _division(m.group(1)) if m else None
    if not division:
        return None
    return _regional(division, REGIONES[m.group(2)], DIVISIONES, unidad=INDICE_2016)


def _ipc_bienes_servicios(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"ipc_(bienes|servicios)_({_CLAVE_REGION})", serie.variable)
    if not m:
        return None
    return _regional(m.group(1).capitalize(), REGIONES[m.group(2)], BIENES_Y_SERVICIOS, unidad=INDICE_2016)


def _ipc_incidencia_capitulos(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"({_CLAVE_REGION})_(.+)_(mensual|acumulada)", serie.variable)
    division = _division(m.group(2)) if m else None
    if not division:
        return None
    medida = INCIDENCIA if m.group(3) == "mensual" else INCIDENCIA_ACUMULADA
    return _regional(division, REGIONES[m.group(1)], DIVISIONES, medida=medida, unidad=PROPORCION_INCIDENCIA)


def _ipc_incidencia_bienes_servicios(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"({_CLAVE_REGION})_(bienes|servicios)", serie.variable)
    if not m:
        return None
    return _regional(m.group(2).capitalize(), REGIONES[m.group(1)], BIENES_Y_SERVICIOS, medida=INCIDENCIA,
                     unidad=PROPORCION_INCIDENCIA)


_CATEGORIAS = {
    "nivel_general": (NIVEL_GENERAL, "Nivel general"), "nucleo": (CATEGORIAS, "Núcleo"),
    "estacionales": (CATEGORIAS, "Estacionales"), "regulados": (CATEGORIAS, "Regulados"),
}
# Variación mensual nacional por categoría (no trae región en la variable).
_CATEGORIAS_NACIONALES = {"ipc_nucleo": "nucleo", "estacionales": "estacionales", "ipc_regulados": "regulados"}


def _ipc_categorias(serie: Serie) -> Clasificacion | None:
    if serie.variable in _CATEGORIAS_NACIONALES:
        grupo, item = _CATEGORIAS[_CATEGORIAS_NACIONALES[serie.variable]]
        return _regional(item, None, grupo, medida=VAR_MENSUAL, unidad=PROPORCION)
    m = re.fullmatch(rf"ipc_(nivel_general|nucleo|estacionales|regulados)_({_CLAVE_REGION})", serie.variable)
    if not m:
        return None
    grupo, item = _CATEGORIAS[m.group(1)]
    return _regional(item, REGIONES[m.group(2)], grupo, unidad=INDICE_2016)


def _ipc_incidencia_categorias(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"({_CLAVE_REGION})_(nivel_general|ipc_nucleo|estacionales|regulados)_(mensual|acumulada)",
                     serie.variable)
    if not m:
        return None
    grupo, item = _CATEGORIAS[m.group(2).replace("ipc_", "")]
    medida = INCIDENCIA if m.group(3) == "mensual" else INCIDENCIA_ACUMULADA
    return _regional(item, REGIONES[m.group(1)], grupo, medida=medida, unidad=PROPORCION_INCIDENCIA)


# Productos de la canasta (cuadro 4.1.8): clave de variable → (producto, unidad en pesos).
_PRODUCTOS_CANASTA = {
    "aceite_de_girasol_1_litro_medio": ("Aceite de girasol", "Pesos por litro"),
    "arroz_blanco_simple_kg": ("Arroz blanco simple", "Pesos por kg"),
    "azucar_kg": ("Azúcar", "Pesos por kg"),
    "carne_picada_comun_kg": ("Carne picada común", "Pesos por kg"),
    "detergente_liquido_750_cc": ("Detergente líquido", "Pesos por 750 cm³"),
    "fideos_secos_tipo_guisero_500_gr": ("Fideos secos tipo guisero", "Pesos por 500 g"),
    "harina_de_trigo_comun_000_kg": ("Harina de trigo común 000", "Pesos por kg"),
    "huevos_de_gallina_docena": ("Huevos de gallina", "Pesos por docena"),
    "jabon_de_tocador_125_gr": ("Jabón de tocador", "Pesos por 125 g"),
    "lavandina_1000_cc": ("Lavandina", "Pesos por litro"),
    "leche_fresca_entera_sachet_litro": ("Leche fresca entera en sachet", "Pesos por litro"),
    "pan_frances_kg": ("Pan francés tipo flauta", "Pesos por kg"),
    "papa_kg": ("Papa", "Pesos por kg"),
    "pollo_entero_kg": ("Pollo entero", "Pesos por kg"),
}


def _precio(producto: str, region: str | None, unidad: str) -> Clasificacion:
    return _resultado(producto, region or NACIONAL, PRECIOS_PROMEDIO, en_nombre=region or "",
                      unidad=unidad, prefijo="Precio promedio")


def _ipc_canasta(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(rf"({_CLAVE_REGION})_(.+)", serie.variable)
    if not m or m.group(2) not in _PRODUCTOS_CANASTA:
        return None
    producto, unidad = _PRODUCTOS_CANASTA[m.group(2)]
    return _precio(producto, REGIONES[m.group(1)], unidad)


# ===================================================================================================
# IPC GBA, base dic-2016 (cuadros 4.2.x de datos.gob.ar)
# ===================================================================================================
def _gba(item: str, grupo: str, unidad: str = INDICE_2016) -> Clasificacion:
    return _regional(item, "GBA", grupo, unidad=unidad)


def _raiz_gba(serie: Serie) -> str | None:
    """Variable sin el prefijo de frecuencia: ipc_2016_<x> (mensual) o ipc_gba_<x> (trimestral)."""
    m = re.fullmatch(r"ipc_(?:2016|gba)_(.+)", serie.variable)
    return m.group(1) if m else None


_GBA_CAPITULOS = {
    "alimentos_bebidas": ALIMENTOS, "atencion_medica_salud": SALUD, "bebidas_alcoholicas": BEBIDAS_ALCOHOLICAS,
    "bienes_servicios_varios": BIENES_SERVICIOS_VARIOS, "equipamiento_del_hogar": EQUIPAMIENTO,
    "educacion": EDUCACION, "informacion_comunicacion": COMUNICACION, "indumentaria": PRENDAS,
    "restauranes_hoteles": RESTAURANTES, "recreacion_cultura": RECREACION, "transporte": TRANSPORTE,
    "vivienda": VIVIENDA,
}


def _ipc_gba_capitulos(serie: Serie) -> Clasificacion | None:
    raiz = _raiz_gba(serie)
    if raiz == "nivel_general":
        return _gba("Nivel general", NIVEL_GENERAL)
    if raiz in _GBA_CAPITULOS:
        return _gba(_GBA_CAPITULOS[raiz], DIVISIONES)
    return None


def _ipc_gba_bienes_servicios(serie: Serie) -> Clasificacion | None:
    raiz = _raiz_gba(serie)
    if raiz == "nivel_general":
        return _gba("Nivel general", NIVEL_GENERAL)
    if raiz in ("bienes", "servicios"):
        return _gba(raiz.capitalize(), BIENES_Y_SERVICIOS)
    return None


def _ipc_gba_categorias(serie: Serie) -> Clasificacion | None:
    raiz = _raiz_gba(serie)
    if raiz == "nivgeneral":
        return _gba("Nivel general", NIVEL_GENERAL)
    if raiz in ("estacionales", "nucleo", "regulados"):
        grupo, item = _CATEGORIAS[raiz]
        return _gba(item, grupo)
    return None


# Cuadro 4.2.4 (principales aperturas): variable → nombre oficial. Los rótulos de la descripción
# no son confiables (p. ej. «Servicios básicos y combustibles para la vivienda» es en realidad
# «Bienes y servicios para la conservación del hogar»); cada serie se identificó comparando sus
# valores con la hoja «IPC GBA Base dic 2016» del libro del INDEC.
_GBA_APERTURAS = {
    # Nivel general y divisiones
    "nive_genera": ("Nivel general", NIVEL_GENERAL),
    "aimentos_bebidas_no_acohoicas": (ALIMENTOS, DIVISIONES),
    "bebidas_acoholicas_tabaco": (BEBIDAS_ALCOHOLICAS, DIVISIONES),
    "prendas_vestir_cazado": (PRENDAS, DIVISIONES),
    "vivienda_agua_electricidad_otros_combustibes": (VIVIENDA, DIVISIONES),
    "equipamiento_mantenimiento_hogar": (EQUIPAMIENTO, DIVISIONES),
    "salud": (SALUD, DIVISIONES),
    "transporte": (TRANSPORTE, DIVISIONES),
    "informacion_comunicacion": (COMUNICACION, DIVISIONES),
    "recreacion_cultura": (RECREACION, DIVISIONES),
    "educacion": (EDUCACION, DIVISIONES),
    "restaurantes_hoteles": (RESTAURANTES, DIVISIONES),
    "bienes_servicios_varios": (BIENES_SERVICIOS_VARIOS, DIVISIONES),
    # Principales aperturas
    "aimentos": ("Alimentos", PRINCIPALES_APERTURAS),
    "pan_cereaes": ("Pan y cereales", PRINCIPALES_APERTURAS),
    "carnes_derivados": ("Carnes y derivados", PRINCIPALES_APERTURAS),
    "eche_productos_acteos_huevos": ("Leche, productos lácteos y huevos", PRINCIPALES_APERTURAS),
    "aceites_grasas_manteca": ("Aceites, grasas y manteca", PRINCIPALES_APERTURAS),
    "frutas": ("Frutas", PRINCIPALES_APERTURAS),
    "verduras_tubercuos_egumbres": ("Verduras, tubérculos y legumbres", PRINCIPALES_APERTURAS),
    "azucar_duces_chocote_goosinas_etc": ("Azúcar, dulces, chocolate, golosinas, etc.", PRINCIPALES_APERTURAS),
    "cafe_te_yerba_cacao": ("Café, té, yerba y cacao", PRINCIPALES_APERTURAS),
    "bebidas_no_acohoicas": ("Bebidas no alcohólicas", PRINCIPALES_APERTURAS),
    "bebidas_acoholicas": ("Bebidas alcohólicas", PRINCIPALES_APERTURAS),
    "tabaco": ("Tabaco", PRINCIPALES_APERTURAS),
    "prendas_vestir_materiales": ("Prendas de vestir y materiales", PRINCIPALES_APERTURAS),
    "cazado": ("Calzado", PRINCIPALES_APERTURAS),
    "aquiler_vivienda": ("Alquiler de la vivienda y gastos conexos", PRINCIPALES_APERTURAS),
    "mantenimiento_reparacion_vivienda": ("Mantenimiento y reparación de la vivienda", PRINCIPALES_APERTURAS),
    "electricidad_gas_otros_combustibes": ("Electricidad, gas y otros combustibles", PRINCIPALES_APERTURAS),
    "bienes_servicios_para_conservacion_hogar": ("Bienes y servicios para la conservación del hogar", PRINCIPALES_APERTURAS),
    "productos_medicinales_artefactos_equipos_salud": ("Productos medicinales, artefactos y equipos para la salud", PRINCIPALES_APERTURAS),
    "adquisicion_vehicuos": ("Adquisición de vehículos", PRINCIPALES_APERTURAS),
    "funcionamiento_equipos_transporte_persona": ("Funcionamiento de equipos de transporte personal", PRINCIPALES_APERTURAS),
    "transporte_pubico": ("Transporte público", PRINCIPALES_APERTURAS),
    "equipos_audiovisuaes_fotograficos_procesamiento_inf": ("Equipos audiovisuales, fotográficos y de procesamiento de la información", PRINCIPALES_APERTURAS),
    "periodicos_diarios_revistas_ibros_articuos_papeeria": ("Periódicos, diarios, revistas, libros y artículos de papelería", PRINCIPALES_APERTURAS),
    "cuidado_persona": ("Cuidado personal", PRINCIPALES_APERTURAS),
}


def _ipc_gba_aperturas(serie: Serie) -> Clasificacion | None:
    datos = _GBA_APERTURAS.get(_raiz_gba(serie) or "")
    return _gba(*datos) if datos else None


# Precios promedio del cuadro 4.2.5: variable → producto. Los 14 productos que también publica el
# cuadro 4.1.8 (misma serie, valores idénticos) traen la presentación; el resto, sólo «Pesos».
_PRODUCTOS_GBA = {
    "aceite_girasol": "Aceite de girasol", "aceite_mezcla": "Aceite mezcla", "agua_sin_gas": "Agua sin gas",
    "algodon": "Algodón", "arroz_blanco_simple": "Arroz blanco simple",
    "arvejas_secas_remojadas": "Arvejas secas remojadas", "asado": "Asado", "azucar": "Azúcar",
    "banana": "Banana", "batata": "Batata", "cafe_molido": "Café molido",
    "carne_picada_comun": "Carne picada común", "cebolla": "Cebolla", "cerveza_botella": "Cerveza en botella",
    "champu": "Champú", "cuadril": "Cuadril", "desodorante": "Desodorante",
    "detergente_liquido": "Detergente líquido", "dulce_leche": "Dulce de leche",
    "fideos_secos": "Fideos secos tipo guisero", "filet_merluza_fresco": "Filet de merluza fresco",
    "galletitas_agua_envasadas": "Galletitas de agua envasadas",
    "galletitas_dulces_envasadas": "Galletitas dulces envasadas", "gaseosa_base_cola": "Gaseosa base cola",
    "hamburguesas_congeladas": "Hamburguesas congeladas", "harina_trigo_comun": "Harina de trigo común 000",
    "huevos_gallina": "Huevos de gallina", "jabon_pan": "Jabón en pan",
    "jabon_polvo_ropa": "Jabón en polvo para ropa", "jabon_tocador": "Jabón de tocador",
    "jamon_cocido": "Jamón cocido", "lavandina": "Lavandina",
    "leche_fresca_entera_sachet": "Leche fresca entera en sachet", "leche_polvo_entera": "Leche en polvo entera",
    "lechuga": "Lechuga", "limon": "Limón", "manteca": "Manteca", "manzana_deliciosa": "Manzana deliciosa",
    "nalga": "Nalga", "naranja": "Naranja", "paleta": "Paleta", "pan_frances_flauta": "Pan francés tipo flauta",
    "pan_mesa": "Pan de mesa", "paniales_descartables": "Pañales descartables", "papa": "Papa",
    "pollo_entero": "Pollo entero", "polvo_flan": "Polvo para flan", "queso_cremoso": "Queso cremoso",
    "queso_pate_grass": "Queso pategrás", "queso_sardo": "Queso sardo", "sal_fina": "Sal fina",
    "salame": "Salame", "salchicha_viena": "Salchicha tipo Viena", "salchichon": "Salchichón",
    "tomate_entero_conserva": "Tomate entero en conserva", "tomate_redondo": "Tomate redondo",
    "vino_comun": "Vino común", "yerba_mate": "Yerba mate", "yogur_firme": "Yogur firme", "zapallo": "Zapallo",
}
_UNIDAD_PRODUCTO = {producto: unidad for producto, unidad in _PRODUCTOS_CANASTA.values()}


def _ipc_gba_precios(serie: Serie) -> Clasificacion | None:
    producto = _PRODUCTOS_GBA.get(_raiz_gba(serie) or "")
    if not producto:
        return None
    return _precio(producto, "GBA", _UNIDAD_PRODUCTO.get(producto, PESOS))


# ===================================================================================================
# Libro del INDEC (latest.xls)
# ===================================================================================================
_ETIQUETA_INDEC = re.compile(r"(Total nacional|Región [A-Za-zé]+) - (.+)")
_CATEGORIAS_INDEC = {"estacional": "Estacionales", "estacionales": "Estacionales", "nucleo": "Núcleo",
                     "regulados": "Regulados"}
_MEDIDA_HOJA_NACIONAL = {
    "Índices IPC Cobertura Nacional": "",
    "Variación mensual IPC Nacional": VAR_MENSUAL,
    "Var. interanual IPC Nacional": VAR_INTERANUAL,
}


def _item_actual(rotulo: str) -> tuple[str, str] | None:
    """(ítem, grupo) de un rótulo del INDEC de la base dic-2016."""
    clave = _clave(rotulo)
    if clave == "nivel_general":
        return "Nivel general", NIVEL_GENERAL
    if clave in ("bienes", "servicios"):
        return clave.capitalize(), BIENES_Y_SERVICIOS
    if clave in _CATEGORIAS_INDEC:
        return _CATEGORIAS_INDEC[clave], CATEGORIAS
    division = _DIVISION_POR_CLAVE.get(clave)
    return (division, DIVISIONES) if division else None


def _indec_nacional(serie: Serie) -> Clasificacion | None:
    medida = _MEDIDA_HOJA_NACIONAL[serie.hoja]
    m = _ETIQUETA_INDEC.fullmatch(serie.nombre)
    if not m or m.group(1) not in REGIONES_INDEC:
        return None
    item = _item_actual(m.group(2))
    if not item:
        return None
    return _regional(item[0], REGIONES_INDEC[m.group(1)], item[1], medida=medida,
                     unidad=PORCENTAJE if medida else INDICE_2016)


# IPC GBA base dic-2016 del INDEC (abr-dic 2016): ítems de principales aperturas con su nombre oficial.
_APERTURAS_GBA_INDEC = {
    "Alimentos", "Aceites, grasas y manteca", "Adquisición de vehículos",
    "Alquiler de la vivienda y gastos conexos", "Azúcar, dulces, chocolate, golosinas, etc.",
    "Bebidas alcohólicas", "Bebidas no alcohólicas", "Bienes y servicios para la conservación del hogar",
    "Café, té, yerba y cacao", "Calzado", "Carnes y derivados", "Cuidado personal",
    "Electricidad, gas y otros combustibles",
    "Equipos audiovisuales, fotográficos y de procesamiento de la información", "Frutas",
    "Funcionamiento de equipos de transporte personal", "Leche, productos lácteos y huevos",
    "Mantenimiento y reparación de la vivienda", "Pan y cereales",
    "Periódicos, diarios, revistas, libros y artículos de papelería", "Prendas de vestir y materiales",
    "Productos medicinales, artefactos y equipos para la salud", "Tabaco", "Transporte público",
    "Verduras, tubérculos y legumbres",
}


def _indec_gba_dic_2016(serie: Serie) -> Clasificacion | None:
    item = _item_actual(serie.nombre)
    if item:
        return _gba(*item)
    nombre = serie.nombre[:1].upper() + serie.nombre[1:]   # un rótulo viene en minúscula inicial
    if nombre in _APERTURAS_GBA_INDEC:
        return _gba(nombre, PRINCIPALES_APERTURAS)
    return None


# ===================================================================================================
# Series históricas
# ===================================================================================================
def _historica(version: str, item: str, grupo: str, *, cobertura: str = "", medida: str = "",
               unidad: str = "") -> Clasificacion:
    """Versión vieja del índice: nivel 3 «Series históricas», nivel 4 la versión, nivel 5 la apertura."""
    return _resultado(item, SERIES_HISTORICAS, version, grupo, en_nombre=cobertura, medida=medida,
                      unidad=unidad)


# --- Capítulos de las bases viejas: nombres por base ---------------------------------------------
# IPC-GBA de 2008 y IPC-NU de 2013: los nueve capítulos con sus nombres del INDEC.
_CAP_ALIMENTOS = "Alimentos y bebidas"
_CAP_INDUMENTARIA = "Indumentaria"
_CAP_VIVIENDA = "Vivienda y servicios básicos"
_CAP_EQUIPAMIENTO = "Equipamiento y mantenimiento del hogar"
_CAP_SALUD = "Atención médica y gastos para la salud"
_CAP_TRANSPORTE = "Transporte y comunicaciones"
_CAP_ESPARCIMIENTO = "Esparcimiento"
_CAP_EDUCACION = "Educación"
_CAP_OTROS = "Otros bienes y servicios"
_CAP_VARIOS = "Bienes y servicios varios"   # nombre del último capítulo en las bases 1988, 1999 y 2003

# --- Versión: GBA base abr-2016 (libro del INDEC, abr-2016 a may-2017) -------------------------------
_V_GBA_2016 = "GBA base abr-2016"
_INDICE_2016_ABR = "Índice abr-2016=100"
_CAPITULOS_GBA_2016 = {
    "Alimentos y Bebidas": _CAP_ALIMENTOS, "Indumentaria": _CAP_INDUMENTARIA,
    "Vivienda y servicios básicos": _CAP_VIVIENDA, "Equipamiento y mantenimiento del hogar": _CAP_EQUIPAMIENTO,
    "Atención médica y gastos para la salud": _CAP_SALUD, "Transporte y comunicaciones": _CAP_TRANSPORTE,
    "Esparcimiento": _CAP_ESPARCIMIENTO, "Educación": _CAP_EDUCACION, "Otros bienes y servicios": _CAP_OTROS,
}
_CORRECCIONES_GBA_2016 = {
    # el rótulo del libro trae una coma de más
    "Equipos, conexiones, y servicios de audio, televisión y computación":
        "Equipos, conexiones y servicios de audio, televisión y computación",
}


def _indec_gba_2016(serie: Serie) -> Clasificacion | None:
    rotulo = serie.nombre
    if rotulo == "Nivel general":
        return _historica(_V_GBA_2016, "Nivel general", NIVEL_GENERAL, cobertura="GBA", unidad=_INDICE_2016_ABR)
    if rotulo in _CAPITULOS_GBA_2016:
        return _historica(_V_GBA_2016, _CAPITULOS_GBA_2016[rotulo], CAPITULOS, cobertura="GBA", unidad=_INDICE_2016_ABR)
    if rotulo in ("Bienes", "Servicios"):
        return _historica(_V_GBA_2016, rotulo, BIENES_Y_SERVICIOS, cobertura="GBA", unidad=_INDICE_2016_ABR)
    if rotulo in ("Estacionales", "Núcleo", "Regulados"):
        return _historica(_V_GBA_2016, rotulo, CATEGORIAS, cobertura="GBA", unidad=_INDICE_2016_ABR)
    return _historica(_V_GBA_2016, _CORRECCIONES_GBA_2016.get(rotulo, rotulo), PRINCIPALES_APERTURAS,
                      cobertura="GBA", unidad=_INDICE_2016_ABR)


# --- Versión: Nacional urbano base oct-2013 a sep-2014 (cuadros 4.15.x) -----------------------------
_V_NU = "Nacional urbano base oct-2013 a sep-2014"
_INDICE_NU = "Índice oct-2013 a sep-2014=100"
_CAPITULOS_NU_15_1 = {
    "ipc_nu_alimentos_bebidas": _CAP_ALIMENTOS, "ipc_nu_atencion_medica_salud": _CAP_SALUD,
    "ipc_nu_bienes_servicios_varios": _CAP_OTROS, "ipc_nu_equipamiento_del_hogar": _CAP_EQUIPAMIENTO,
    "ipc_nu_educacion": _CAP_EDUCACION, "ipc_nu_esparcimiento": _CAP_ESPARCIMIENTO,
    "ipc_nu_indumentaria": _CAP_INDUMENTARIA, "ipc_nu_transporte_comunicaciones": _CAP_TRANSPORTE,
    "ipc_nu_vivienda": _CAP_VIVIENDA,
}


def _ipc_nu_capitulos(serie: Serie) -> Clasificacion | None:
    if serie.variable == "ipc_nu_nivel_general":
        return _historica(_V_NU, "Nivel general", NIVEL_GENERAL, unidad=_INDICE_NU)
    if serie.variable in _CAPITULOS_NU_15_1:
        return _historica(_V_NU, _CAPITULOS_NU_15_1[serie.variable], CAPITULOS, unidad=_INDICE_NU)
    return None


# Cuadros 4.15.2 y 4.16.4 (principales aperturas de las bases 2013 y abr-2008; mismos rótulos).
_APERTURAS_2008 = {
    "alimentos_bebidas": "Alimentos y bebidas",
    "alimentos_consumir_hogar": "Alimentos para consumir en el hogar",
    "aceites_grasas": "Aceites y grasas",
    "azucar_miel_dulces_cacao": "Azúcar, miel, dulces y cacao",
    "atencion_medica_gastos_salud": "Atención médica y gastos para la salud",
    "alquiler_vivienda": "Alquiler de la vivienda",
    "bebidas_alcoholicas": "Bebidas alcohólicas",
    "bebidas_infusiones_para_consumir_hogar": "Bebidas e infusiones para consumir en el hogar",
    "bebidas_no_alcoholicas": "Bebidas no alcohólicas",
    "carne_ave": "Carne de ave",
    "carne_vacuna": "Carne vacuna",
    "comunicaciones": "Comunicaciones",
    "carnes": "Carnes",
    "calzado": "Calzado",
    "diarios_revistas_libros": "Diarios, revistas y libros",
    "equipamiento_mantenimiento_hogar": "Equipamiento y mantenimiento del hogar",
    "esparcimiento": "Esparcimiento",
    "educacion": "Educación",
    "fiambres_embutidos_conservas": "Fiambres, embutidos y conservas",
    "frutas": "Frutas",
    "indumentaria": "Indumentaria",
    "nivel_general_indice": "Nivel general",
    "otros_bienes_servicios_varios": "Otros bienes y servicios",
    "otras_carnes_rojas_frescas": "Otras carnes rojas frescas",
    "otros_servicios_esparcimiento": "Otros servicios de esparcimiento",
    "productos_lacteos_huevos": "Productos lácteos y huevos",
    "pescados_mariscos_frescos_congelados": "Pescados y mariscos frescos y congelados",
    "productos_panificacion_cereales_pastas": "Productos de panificación, cereales y pastas",
    "ropa_exterior": "Ropa exterior",
    "ropa_interior": "Ropa interior",
    "ropa": "Ropa",
    "servicios_basicos_combustibles_para_vivienda": "Servicios básicos y combustibles para la vivienda",
    "servicios_educativos": "Servicios educativos",
    "transporte_comunicaciones": "Transporte y comunicaciones",
    "textos_utiles_escolares": "Textos y útiles escolares",
    "transporte": "Transporte",
    "turismo": "Turismo",
    "vivienda_servicios_basicos": "Vivienda y servicios básicos",
    "verduras": "Verduras",
    "atencion_medica_gastos_para_salud": "Atención médica y gastos para la salud",
    "alquiler_la_vivienda": "Alquiler de la vivienda",
    "bebidas_infusiones_consumo_hogar": "Bebidas e infusiones para consumir en el hogar",
}
_CAPITULOS_APERTURAS_2008 = {
    "alimentos_bebidas", "atencion_medica_gastos_salud", "atencion_medica_gastos_para_salud", "educacion",
    "equipamiento_mantenimiento_hogar", "esparcimiento", "indumentaria", "otros_bienes_servicios_varios",
    "transporte_comunicaciones", "vivienda_servicios_basicos",
}


def _aperturas_2008(serie: Serie, version: str, unidad: str, cobertura: str) -> Clasificacion | None:
    nombre = _APERTURAS_2008.get(serie.variable)
    if nombre is None:
        return None
    if serie.variable == "nivel_general_indice":
        return _historica(version, "Nivel general", NIVEL_GENERAL, cobertura=cobertura, unidad=unidad)
    if serie.variable in _CAPITULOS_APERTURAS_2008:
        return _historica(version, nombre, CAPITULOS, cobertura=cobertura, unidad=unidad)
    return _historica(version, nombre, PRINCIPALES_APERTURAS, cobertura=cobertura, unidad=unidad)


def _ipc_nu_aperturas(serie: Serie) -> Clasificacion | None:
    return _aperturas_2008(serie, _V_NU, _INDICE_NU, "")


# --- Versión: GBA base abr-2008 (cuadros 4.16.x) ------------------------------------------------------
_V_GBA_2008 = "GBA base abr-2008"
_INDICE_2008 = "Índice abr-2008=100"
_CAPITULOS_2008 = {
    "ipc_alimentos_bebidas": _CAP_ALIMENTOS, "ipc_atencion_medica_salud": _CAP_SALUD,
    "ipc_bienes_servicios_varios": _CAP_OTROS, "ipc_equipamiento_hogar": _CAP_EQUIPAMIENTO,
    "ipc_educacion": _CAP_EDUCACION, "ipc_esparcimiento": _CAP_ESPARCIMIENTO,
    "ipc_indumentaria": _CAP_INDUMENTARIA, "ipc_transporte_comunicaciones": _CAP_TRANSPORTE,
    "ipc_vivienda": _CAP_VIVIENDA,
}


def _ipc_gba_2008_bienes_servicios(serie: Serie) -> Clasificacion | None:
    m = re.fullmatch(r"ipc_(nivel_general|bienes_servicios|bienes|servicios)(_t|_md|_m)?", serie.variable)
    if not m:
        return None
    base = m.group(1)
    variacion = m.group(2) == "_md"
    medida = VAR_MENSUAL if variacion else ""
    unidad = PORCENTAJE if variacion else (RELATIVO if base == "bienes_servicios" else _INDICE_2008)
    if base == "nivel_general":
        return _historica(_V_GBA_2008, "Nivel general", NIVEL_GENERAL, cobertura="GBA", medida=medida, unidad=unidad)
    item = "Precio relativo servicios/bienes" if base == "bienes_servicios" else base.capitalize()
    return _historica(_V_GBA_2008, item, BIENES_Y_SERVICIOS, cobertura="GBA", medida=medida, unidad=unidad)


def _ipc_gba_2008_capitulos(serie: Serie) -> Clasificacion | None:
    if serie.variable == "ipc_nivel_general":
        return _historica(_V_GBA_2008, "Nivel general", NIVEL_GENERAL, cobertura="GBA", unidad=_INDICE_2008)
    if serie.variable in _CAPITULOS_2008:
        return _historica(_V_GBA_2008, _CAPITULOS_2008[serie.variable], CAPITULOS, cobertura="GBA",
                          unidad=_INDICE_2008)
    return None


_CATEGORIAS_2008 = {
    "ipc_nivel_general": ("Nivel general", NIVEL_GENERAL),
    "ipc_estacionales": ("Estacionales", CATEGORIAS), "ipc_regulados": ("Regulados", CATEGORIAS),
    "ipc_resto": ("Resto", CATEGORIAS),
    "precios_relativos_estacionales_ipc_resto": ("Precio relativo estacionales/resto", CATEGORIAS),
    "precios_relativos_regulados_ipc_resto": ("Precio relativo regulados/resto", CATEGORIAS),
}


def _ipc_gba_2008_categorias(serie: Serie) -> Clasificacion | None:
    datos = _CATEGORIAS_2008.get(serie.variable)
    if not datos:
        return None
    unidad = RELATIVO if serie.variable.startswith("precios_relativos") else _INDICE_2008
    return _historica(_V_GBA_2008, datos[0], datos[1], cobertura="GBA", unidad=unidad)


def _ipc_gba_2008_aperturas(serie: Serie) -> Clasificacion | None:
    return _aperturas_2008(serie, _V_GBA_2008, _INDICE_2008, "GBA")


# --- Versión: Nacional base 2003 (cuadro 4.17) ----------------------------------------------------------
_V_NACIONAL_2003 = "Nacional base 2003"
_CAPITULOS_2003 = {
    "ipc_2003_alimentos_bebidas": _CAP_ALIMENTOS, "ipc_2003_atencion_medica_salud": _CAP_SALUD,
    "ipc_2003_bienes_servicios_varios": _CAP_VARIOS, "ipc_2003_equipamiento_del_hogar": _CAP_EQUIPAMIENTO,
    "ipc_2003_educacion": _CAP_EDUCACION, "ipc_2003_esparcimiento": _CAP_ESPARCIMIENTO,
    "ipc_2003_indumentaria": _CAP_INDUMENTARIA, "ipc_2003_transporte_comunicaciones": _CAP_TRANSPORTE,
    "ipc_2003_vivienda": _CAP_VIVIENDA,
}


def _ipc_nacional_2003(serie: Serie) -> Clasificacion | None:
    if serie.variable == "ipc_2003_nivel_general":
        return _historica(_V_NACIONAL_2003, "Nivel general", NIVEL_GENERAL, unidad="Índice 2003=100")
    if serie.variable in _CAPITULOS_2003:
        return _historica(_V_NACIONAL_2003, _CAPITULOS_2003[serie.variable], CAPITULOS, unidad="Índice 2003=100")
    return None


# --- Versión: GBA base 1999 (cuadros 4.18 y 4.22) --------------------------------------------------------
_V_GBA_1999 = "GBA base 1999"
_INDICE_1999 = "Índice 1999=100"
_APERTURAS_1999 = {
    "alimentos_bebidas": "Alimentos y bebidas",
    "alimentos_consumir_hogar": "Alimentos para consumir en el hogar",
    "aves_frescas_congeladas": "Aves frescas y congeladas",
    "aceites_grasas": "Aceites y grasas",
    "aceite_mezcla": "Aceite mezcla",
    "aceite_puro": "Aceite puro",
    "arroz": "Arroz",
    "aceites": "Aceites",
    "carne_ave": "Carne de ave",
    "cortes_delanteros_carne_vacuna_fresca": "Cortes delanteros de carne vacuna fresca",
    "conservas_pescado": "Conservas de pescado",
    "cortes_traseros_carne_vacuna_fresca": "Cortes traseros de carne vacuna fresca",
    "carne_vacuna": "Carne vacuna",
    "carnes": "Carnes",
    "embutidos_frescos": "Embutidos frescos",
    "frutas_citricas": "Frutas cítricas",
    "fiambres_conservas": "Fiambres y conservas",
    "fiambres_embutidos_conservas": "Fiambres, embutidos y conservas",
    "frutas_frescas": "Frutas frescas",
    "fideos_pastas_masas_crudas": "Fideos, pastas y masas crudas",
    "frutas_secas_deshidratadas_conserva": "Frutas secas, deshidratadas y en conserva",
    "fideos_secos": "Fideos secos",
    "frutas": "Frutas",
    "facturas": "Facturas",
    "galletitas_dulces": "Galletitas dulces",
    "harina_arroz_otros_cereales": "Harina, arroz y otros cereales",
    "harina_trigo": "Harina de trigo",
    "huevos": "Huevos",
    "helados": "Helados",
    "leche_fluida": "Leche fluida",
    "leche_polvo": "Leche en polvo",
    "leche": "Leche",
    "menudencias_achuras_frescas_vacunas": "Menudencias y achuras frescas vacunas",
    "manteca_crema": "Manteca y crema",
    "merluza_fresca_congelada": "Merluza fresca y congelada",
    "margarinas_grasas": "Margarinas y grasas",
    "manzana": "Manzana",
    "nivel_general_indice": "Nivel general",
    "otras_carnes_rojas_frescas": "Otras carnes rojas frescas",
    "otros_cereales": "Otros cereales",
    "otras_frutas": "Otras frutas",
    "otras_galletas_galletitas": "Otras galletas y galletitas",
    "otras_harinas": "Otras harinas",
    "otros_productos_lacteos": "Otros productos lácteos",
    "otros_pescados_mariscos_frescos_congelados": "Otros pescados y mariscos frescos y congelados",
    "otras_verduras_frescas_congeladas": "Otras verduras frescas y congeladas",
    "pan_envasado": "Pan envasado",
    "pan_fresco": "Pan fresco",
    "pastas_frescas": "Pastas frescas",
    "productos_lacteos_huevos": "Productos lácteos y huevos",
    "pescados_mariscos_frescos_congelados": "Pescados y mariscos frescos y congelados",
    "papa_otras_raices_feculentas": "Papa y otras raíces feculentas",
    "productos_panificacion_cereales_pastas": "Productos de panificación, cereales y pastas",
    "productos_pasteleria": "Productos de pastelería",
    "productos_panificacion": "Productos de panificación",
    "quesos_blandos_untables": "Quesos blandos y untables",
    "quesos_duros": "Quesos duros",
    "quesos_semiduros": "Quesos semiduros",
    "quesos": "Quesos",
    "semipreparados_base_ave": "Semipreparados en base de ave",
    "semipreparados_base_carne_vacuna": "Semipreparados en base a carne vacuna",
    "semipreparados_base_pastas": "Semipreparados en base a pastas",
    "tomate_conserva": "Tomate en conserva",
    "tapas_masa": "Tapas de masa",
    "verduras_frescas_congeladas": "Verduras frescas y congeladas",
    "verduras_secas_conserva": "Verduras secas y en conserva",
    "verduras": "Verduras",
    "yogur_postres_lacteos": "Yogur y postres lácteos",
}
_CAPITULOS_1999 = {   # cuadro 4.22
    "alimentos_bebidas": _CAP_ALIMENTOS, "atencion_medica_salud": _CAP_SALUD,
    "bienes_servicios_varios": _CAP_VARIOS, "educacion": _CAP_EDUCACION,
    "equipamiento_mantenimiento_hogar": _CAP_EQUIPAMIENTO, "esparcimiento": _CAP_ESPARCIMIENTO,
    "indumentaria": _CAP_INDUMENTARIA, "transporte_comunicaciones": _CAP_TRANSPORTE,
    "vivienda_servicios_basicos": _CAP_VIVIENDA,
}


def _ipc_gba_1999_aperturas(serie: Serie) -> Clasificacion | None:
    nombre = _APERTURAS_1999.get(serie.variable)
    if nombre is None:
        return None
    if serie.variable == "nivel_general_indice":
        return _historica(_V_GBA_1999, "Nivel general", NIVEL_GENERAL, cobertura="GBA", unidad=_INDICE_1999)
    if serie.variable == "alimentos_bebidas":
        return _historica(_V_GBA_1999, nombre, CAPITULOS, cobertura="GBA", unidad=_INDICE_1999)
    return _historica(_V_GBA_1999, nombre, PRINCIPALES_APERTURAS, cobertura="GBA", unidad=_INDICE_1999)


def _ipc_gba_1999_historico(serie: Serie) -> Clasificacion | None:
    if serie.variable == "nivel_general":
        return _historica(_V_GBA_1999, "Nivel general", NIVEL_GENERAL, cobertura="GBA", unidad=_INDICE_1999)
    if serie.variable in _CAPITULOS_1999:
        return _historica(_V_GBA_1999, _CAPITULOS_1999[serie.variable], CAPITULOS, cobertura="GBA",
                          unidad=_INDICE_1999)
    return None


# --- Versión: GBA base 1988 (cuadro 4.20) -----------------------------------------------------------------
_V_GBA_1988 = "GBA base 1988"
_INDICE_1988 = "Índice 1988=100"
_APERTURAS_ALIMENTOS_1988 = "Principales aperturas de alimentos y bebidas"
_APERTURAS_OTROS_1988 = "Principales aperturas de otros capítulos"
_CAPITULOS_1988 = {
    "alimentos_bebidas": _CAP_ALIMENTOS, "indumentaria": _CAP_INDUMENTARIA, "vivienda": "Vivienda",
    "equipamiento_funcionamiento_del_hogar": "Equipamiento y funcionamiento del hogar", "salud": "Salud",
    "transporte_comunicaciones": _CAP_TRANSPORTE, "esparcimiento": _CAP_ESPARCIMIENTO,
    "educacion": _CAP_EDUCACION, "bienes_servicios_varios": _CAP_VARIOS,
}
# Aperturas que pertenecen al capítulo «Alimentos y bebidas».
_ALIMENTOS_1988 = frozenset({
    "azucar", "azucar_dulces_cacao", "alimentos_semipreparados", "alimentos_consumidos_fuera_del_hogar",
    "alimentos_preparados_listos_para_consumir", "almuerzo", "aderezos", "achuras_menudencias_frescas",
    "aceites_grasas", "aceite_mezcla", "aceite_puro", "arroz", "bebidas_alcoholicas", "bebidas_no_alcoholicas",
    "cafe", "cereales_derivados", "comidas_preparadas", "conservas_preparados_pescado", "carnes", "cerveza",
    "carne_ave_fresca", "carne_porcina_ovina_fresca", "carne_vacuna_fresca_cortes_traseros",
    "carne_vacuna_fresca_cortes_delanteros", "condimentos", "caldos_sopas_concentrados", "cacao_sus_derivados",
    "dulces", "empanadas_pizzas", "embutidos_conservas_preparados_carne", "frutas", "frutas_frescas",
    "frutas_secas_en_conserva", "fiambres", "fideos_secos", "facturas", "galletitas_dulces",
    "galletitas_saladas", "gaseosas", "golosinas", "helados", "harinas_trigo", "huevos", "infusiones",
    "jugos_refrescos", "lecheproductos_lacteos_huevos", "leche_en_polvo", "leche_fresca",
    "margarina_otras_grasas", "manteca_crema", "masas_precocidas_tapas", "otras_bebidas_alcoholicas",
    "otras_harinas", "pescados_mariscos_frescos", "pastas_frescas", "pan_envasado", "pan_fresco",
    "polvos_para_postres", "productos_pasteleria", "quesos_blandos_untables", "quesos_duros",
    "quesos_semiduros", "refrigerio", "sal_especias", "soda_agua_mineral", "te", "vino",
    "verdurastuberculos_legumbres", "verduras_legumbres_secas_en_conserva",
    "verduras_tuberculos_legumbres_frescas", "yerba_mate", "yoghurt_postres_lacteos",
})
_APERTURAS_1988 = {
    "azucar": "Azúcar",
    "azucar_dulces_cacao": "Azúcar, dulces y cacao",
    "aparatos_radio_tv": "Aparatos de radio y TV",
    "articulos_tocador": "Artículos de tocador",
    "articulos_belleza": "Artículos de belleza",
    "articulos_descartables": "Artículos descartables",
    "articulos_tocador_descartables": "Artículos de tocador descartables",
    "articulos_para_la_higiene_el_cuidado_personal": "Artículos para la higiene y el cuidado personal",
    "articulos_para_mantenimiento": "Artículos para mantenimiento",
    "articulos_para_limpieza_mantenimiento": "Artículos para limpieza y mantenimiento",
    "articulos_merceria": "Artículos de mercería",
    "articulos_ferreteria": "Artículos de ferretería",
    "articulos_marroquineria": "Artículos de marroquinería",
    "articulos_joyeria_relojeria": "Artículos de joyería y relojería",
    "articulos_deportivos": "Artículos deportivos",
    "alimentos_semipreparados": "Alimentos semipreparados",
    "alimentos_consumidos_fuera_del_hogar": "Alimentos consumidos fuera del hogar",
    "alimentos_preparados_listos_para_consumir": "Alimentos preparados listos para consumir",
    "almuerzo": "Almuerzo",
    "aderezos": "Aderezos",
    "artefactos_domesticos": "Artefactos domésticos",
    "artefactos_a_gas": "Artefactos a gas",
    "alquiler": "Alquiler",
    "alquiler_la_vivienda": "Alquiler de la vivienda",
    "automoviles": "Automóviles",
    "achuras_menudencias_frescas": "Achuras y menudencias frescas",
    "accesorios_terapeuticos": "Accesorios terapéuticos",
    "accesorios_para_vestir": "Accesorios para vestir",
    "aceites_grasas": "Aceites y grasas",
    "aceite_mezcla": "Aceite mezcla",
    "aceite_puro": "Aceite puro",
    "arroz": "Arroz",
    "blanco_manteleria": "Blanco y mantelería",
    "bebidas_alcoholicas": "Bebidas alcohólicas",
    "bebidas_no_alcoholicas": "Bebidas no alcohólicas",
    "bateria_cocina_cubiertos": "Batería de cocina y cubiertos",
    "calzado": "Calzado",
    "calzado_deportivo_para_hombre_mujer": "Calzado deportivo para hombre y mujer",
    "calzado_deportivo_para_ninio": "Calzado deportivo para niños",
    "calzado_para_hombre": "Calzado para hombre",
    "calzado_para_mujer": "Calzado para mujer",
    "calzado_para_ninio": "Calzado para niños",
    "cafe": "Café",
    "cereales_derivados": "Cereales y derivados",
    "comidas_preparadas": "Comidas preparadas",
    "confeccion_reparacion": "Confección y reparación",
    "correo": "Correo",
    "cubiertas_repuestos_reparaciones": "Cubiertas, repuestos y reparaciones",
    "conservas_preparados_pescado": "Conservas y preparados de pescado",
    "carnes": "Carnes",
    "cerveza": "Cerveza",
    "carne_ave_fresca": "Carne de ave fresca",
    "carne_porcina_ovina_fresca": "Carne porcina y ovina fresca",
    "carne_vacuna_fresca_cortes_traseros": "Carne vacuna fresca, cortes traseros",
    "carne_vacuna_fresca_cortes_delanteros": "Carne vacuna fresca, cortes delanteros",
    "condimentos": "Condimentos",
    "cortinas_alfombras_accesorios": "Cortinas, alfombras y accesorios",
    "comunicaciones": "Comunicaciones",
    "caldos_sopas_concentrados": "Caldos y sopas concentrados",
    "cacao_sus_derivados": "Cacao y sus derivados",
    "cigarrillos_tabaco": "Cigarrillos y tabaco",
    "cines_teatros": "Cines y teatros",
    "cristaleria_loza": "Cristalería y loza",
    "combustibles_lubricantes": "Combustibles y lubricantes",
    "club_espectaculos_deportivos": "Club y espectáculos deportivos",
    "dulces": "Dulces",
    "diarios": "Diarios",
    "detergentes_desinfectantes": "Detergentes y desinfectantes",
    "educacion_formal": "Educación formal",
    "elementos_para_audio_video_fotografia": "Elementos para audio, video y fotografía",
    "elementos_para_iluminacion": "Elementos para iluminación",
    "elementos_para_primeros_auxilios": "Elementos para primeros auxilios",
    "elementos_para_iluminacion_ferreteria": "Elementos para iluminación y ferretería",
    "entretenimientos": "Entretenimientos",
    "empanadas_pizzas": "Empanadas y pizzas",
    "equipos_audio_video_fotografia": "Equipos de audio, video y fotografía",
    "equipos_elementos_serv_de_audio_video": "Equipos, elementos y servicios de audio y video",
    "embutidos_conservas_preparados_carne": "Embutidos, conservas y preparados de carne",
    "electricidad": "Electricidad",
    "electricidad_serv_sanitarios_gas_otros": "Electricidad, servicios sanitarios, gas y otros",
    "electrodomesticos": "Electrodomésticos",
    "frutas": "Frutas",
    "frutas_frescas": "Frutas frescas",
    "frutas_secas_en_conserva": "Frutas secas y en conserva",
    "fiambres": "Fiambres",
    "funcionamiento_mantenimiento_de_vehiculos": "Funcionamiento y mantenimiento de vehículos",
    "fideos_secos": "Fideos secos",
    "facturas": "Facturas",
    "galletitas_dulces": "Galletitas dulces",
    "galletitas_saladas": "Galletitas saladas",
    "gaseosas": "Gaseosas",
    "golosinas": "Golosinas",
    "helados": "Helados",
    "hilados_para_tejer": "Hilados para tejer",
    "hotel_excursiones": "Hotel y excursiones",
    "harinas_trigo": "Harinas de trigo",
    "huevos": "Huevos",
    "infusiones": "Infusiones",
    "jabones_limpieza": "Jabones de limpieza",
    "juegos_juguetes_rodados": "Juegos, juguetes y rodados",
    "jugos_refrescos": "Jugos y refrescos",
    "lecheproductos_lacteos_huevos": "Leche, productos lácteos y huevos",
    "leche_en_polvo": "Leche en polvo",
    "leche_fresca": "Leche fresca",
    "limpieza_ropa": "Limpieza de ropa",
    "libros_diarios_revistas": "Libros, diarios y revistas",
    "libros_no_escolares": "Libros no escolares",
    "libros_texto": "Libros de texto",
    "margarina_otras_grasas": "Margarina y otras grasas",
    "medicamentos": "Medicamentos",
    "medicos_odontologos": "Médicos y odontólogos",
    "manteca_crema": "Manteca y crema",
    "menaje": "Menaje",
    "muebles_accesorios_401": "Muebles y accesorios, grupo",
    "muebles_accesorios_40101": "Muebles y accesorios, subgrupo",
    "materiales_mano_obra": "Materiales y mano de obra",
    "masas_precocidas_tapas": "Masas precocidas y tapas",
    "omnibus_corta_distancia": "Ómnibus de corta distancia",
    "otras_bebidas_alcoholicas": "Otras bebidas alcohólicas",
    "otras_harinas": "Otras harinas",
    "otros_medios_transporte": "Otros medios de transporte",
    "otros_servicios": "Otros servicios",
    "otros_servicios_educativos": "Otros servicios educativos",
    "pescados_mariscos_frescos": "Pescados y mariscos frescos",
    "pastas_frescas": "Pastas frescas",
    "pan_envasado": "Pan envasado",
    "pan_fresco": "Pan fresco",
    "polvos_para_postres": "Polvos para postres",
    "plantas_animales_domesticos": "Plantas y animales domésticos",
    "productos_farmaceuticos_accesorios_terapeuticos": "Productos farmacéuticos y accesorios terapéuticos",
    "productos_pasteleria": "Productos de pastelería",
    "quesos_blandos_untables": "Quesos blandos y untables",
    "quesos_duros": "Quesos duros",
    "quesos_semiduros": "Quesos semiduros",
    "refrigerio": "Refrigerio",
    "reparacion_aparatos_electricos": "Reparación de aparatos eléctricos",
    "reparacion_mantenimiento_de_la_vivienda": "Reparación y mantenimiento de la vivienda",
    "revistas": "Revistas",
    "ropa_exterior_para_hombre": "Ropa exterior para hombre",
    "ropa_exterior": "Ropa exterior",
    "ropa_exterior_para_mujer": "Ropa exterior para mujer",
    "ropa_exterior_para_ninio": "Ropa exterior para niños",
    "ropa_interior_para_hombre": "Ropa interior para hombre",
    "ropa_interior": "Ropa interior",
    "ropa_interior_para_mujer": "Ropa interior para mujer",
    "ropa_interior_para_ninio": "Ropa interior para niños",
    "sal_especias": "Sal y especias",
    "servicios_peluqueria_para_el_cuidado": "Servicios de peluquería y para el cuidado",
    "servicios_para_audio_video_fotografia": "Servicios para audio, video y fotografía",
    "servicios_para_el_hogar": "Servicios para el hogar",
    "servicios_varios": "Servicios varios",
    "servicios_sanitarios_gas_otros_combustibles": "Servicios sanitarios, gas y otros combustibles",
    "servicios_para_la_salud": "Servicios para la salud",
    "servicios_para_el_cuidado_personal": "Servicios para el cuidado personal",
    "servicios_de_esparcimiento": "Servicios de esparcimiento",
    "servicios_prepagos_auxiliares": "Servicios prepagos y auxiliares",
    "servicios_para_la_indumentaria": "Servicios para la indumentaria",
    "servicios_educativos": "Servicios educativos",
    "servicio_domestico_otros": "Servicio doméstico y otros",
    "seguro_estacionamiento_otros_servicios": "Seguro, estacionamiento y otros servicios",
    "soda_agua_mineral": "Soda y agua mineral",
    "tabaco": "Tabaco",
    "telefono": "Teléfono",
    "textiles_accesorios_para_decoracion": "Textiles y accesorios para decoración",
    "textos_utiles_escolares": "Textos y útiles escolares",
    "telas": "Telas",
    "turismo": "Turismo",
    "transporte_publico_de_pasajeros": "Transporte público de pasajeros",
    "transporte": "Transporte",
    "telas_hilados_articulos_de_merceria": "Telas, hilados y artículos de mercería",
    "te": "Té",
    "tren_corta_distancia": "Tren de corta distancia",
    "utiles_escolares": "Útiles escolares",
    "utensilios_limpieza": "Utensilios de limpieza",
    "vehiculos_para_transporte_personal": "Vehículos para transporte personal",
    "vino": "Vino",
    "verdurastuberculos_legumbres": "Verduras, tubérculos y legumbres",
    "verduras_legumbres_secas_en_conserva": "Verduras y legumbres secas y en conserva",
    "verduras_tuberculos_legumbres_frescas": "Verduras, tubérculos y legumbres frescas",
    "yerba_mate": "Yerba mate",
    "yoghurt_postres_lacteos": "Yogur y postres lácteos",
}


def _ipc_gba_1988(serie: Serie) -> Clasificacion | None:
    if serie.variable == "nivel_general":
        return _historica(_V_GBA_1988, "Nivel general", NIVEL_GENERAL, cobertura="GBA", unidad=_INDICE_1988)
    if serie.variable in _CAPITULOS_1988:
        return _historica(_V_GBA_1988, _CAPITULOS_1988[serie.variable], CAPITULOS, cobertura="GBA",
                          unidad=_INDICE_1988)
    nombre = _APERTURAS_1988.get(serie.variable)
    if nombre is None:
        return None
    grupo = _APERTURAS_ALIMENTOS_1988 if serie.variable in _ALIMENTOS_1988 else _APERTURAS_OTROS_1988
    return _historica(_V_GBA_1988, nombre, grupo, cobertura="GBA", unidad=_INDICE_1988)


# --- Versión: GBA base 1974 (cuadro 4.21) -----------------------------------------------------------------
_V_GBA_1974 = "GBA base 1974"
_INDICE_1974 = "Índice 1974=100"
_CAPITULOS_1974 = {
    "alimentos_bebidas": _CAP_ALIMENTOS, "indumentaria": _CAP_INDUMENTARIA,
    "vivienda_combustible_electricidad": "Vivienda, combustible y electricidad",
    "equipamiento_funcionamiento_hogar": "Equipamiento y funcionamiento del hogar",
    "atencion_medica_gastos_para_salud": _CAP_SALUD, "transporte_comunicaciones": _CAP_TRANSPORTE,
    "esparcimiento_educacion": "Esparcimiento y educación", "bienes_servicios_varios_8": _CAP_VARIOS,
}
_APERTURAS_1974 = {
    "azucar_dulce_golosinas_cacao_derivados": "Azúcar, dulces, golosinas, cacao y derivados",
    "aparatos_accesorios_terapeuticos": "Aparatos y accesorios terapéuticos",
    "articulos_textiles_para_hogar": "Artículos textiles para el hogar",
    "articulos_servicios_cuidado_personal": "Artículos y servicios para el cuidado personal",
    "alimentos_preparados_semipreparados": "Alimentos preparados y semipreparados",
    "artefactos": "Artefactos",
    "aceite_grasas": "Aceites y grasas",
    "bebidas": "Bebidas",
    "bebidas_alcoholicas": "Bebidas alcohólicas",
    "bebidas_no_alcoholicas": "Bebidas no alcohólicas",
    "bienes_80201": "Bienes para el cuidado personal",
    "servicios_80202": "Servicios para el cuidado personal",
    "bienes_servicios_varios_406": "Bienes y servicios varios del hogar",
    "bienes_40601": "Bienes varios del hogar",
    "servicios_40602": "Servicios varios del hogar",
    "bienes_varios_no_incluidos_anteriormente": "Bienes varios no incluidos anteriormente",
    "calzado_20103": "Calzado para hombre",
    "calzado_20203": "Calzado para mujer",
    "calzado_20303": "Calzado para niños",
    "cereales_derivados": "Cereales y derivados",
    "comida": "Comida",
    "comidas_tomadas_fuera_hogar": "Comidas tomadas fuera del hogar",
    "carnes_embutidos_fiambres": "Carnes, embutidos y fiambres",
    "carnes_frescas": "Carnes frescas",
    "comunicaciones": "Comunicaciones",
    "combustible": "Combustible",
    "combustible_electricidad": "Combustible y electricidad",
    "cafe_te_yerba_mate_otras_infusiones": "Café, té, yerba mate y otras infusiones",
    "elementos_primeros_auxilios": "Elementos para primeros auxilios",
    "equipos_accesorios_reparaciones": "Equipos, accesorios y sus reparaciones",
    "embutidos_fiambres_carnes_secas": "Embutidos y fiambres, carnes secas",
    "electricidad": "Electricidad",
    "frutas_frescas_secas_conserva": "Frutas frescas, secas y en conserva",
    "frutas_verduras_legumbres": "Frutas, verduras y legumbres",
    "funcionamiento_vehiculos_transporte_personal": "Funcionamiento de los vehículos para transporte personal",
    "fideos_demas_pastas_alimenticias": "Fideos y demás pastas alimenticias",
    "gastos_de_transporte": "Gastos de transporte",
    "gastos_vivienda": "Gastos por la vivienda",
    "herramientas_otros": "Herramientas y otros",
    "harina_arroz": "Harina y arroz",
    "indumentaria_calzado_hombre": "Indumentaria y calzado para hombre",
    "indumentaria_calzado_ninio_13_anios": "Indumentaria y calzado para niños hasta 13 años",
    "indumentaria_calzado_mujer": "Indumentaria y calzado para mujer",
    "legumbres_frescas_secas_conserva": "Legumbres frescas, secas y en conserva",
    "libros_periodicos_revistas_articulos_libreria": "Libros, periódicos, revistas y artículos de librería",
    "muebles_accesorios": "Muebles y accesorios",
    "nafta_aceite_grasa": "Nafta, aceite y grasa",
    "otros_gastos": "Otros gastos",
    "otros_servicios_no_incluidos_anteriormente": "Otros servicios no incluidos anteriormente",
    "pescados_mariscos_frescos_conserva": "Pescados y mariscos, frescos y en conserva",
    "partes_diversas_accesorios_reparaciones": "Partes diversas, accesorios y reparaciones",
    "productos_medicinales_farmaceuticos": "Productos medicinales y farmacéuticos",
    "productos_medicinales_farmaceuticos_accesorios_terapeuticos": "Productos medicinales, farmacéuticos y accesorios terapéuticos",
    "productos_panaderia": "Productos de panadería",
    "productos_lacteos_huevos": "Productos lácteos y huevos",
    "refrigerio": "Refrigerio",
    "ropa_exterior_accesorios_20102": "Ropa exterior y accesorios para hombre",
    "ropa_exterior_accesorios_20202": "Ropa exterior y accesorios para mujer",
    "ropa_exterior_accesorios_20302": "Ropa exterior y accesorios para niños",
    "ropa_interior_20101": "Ropa interior para hombre",
    "ropa_interior": "Ropa interior",
    "ropa_interior_20201": "Ropa interior para mujer",
    "servicios_domesticos_para_hogar": "Servicios domésticos para el hogar",
    "servicios_prestados_medicos_auxiliar": "Servicios prestados por médicos y auxiliares",
    "servicios_culturales_esparcimiento": "Servicios culturales y de esparcimiento",
    "servicios_educativos": "Servicios educativos",
    "tabaco": "Tabaco",
    "telas_accesorios_servicios_varios": "Telas, accesorios y servicios varios",
    "utensilios_accesorios_hogar": "Utensilios y accesorios para el hogar",
    "utensilios": "Utensilios",
    "vacaciones_turismo": "Vacaciones y turismo",
    "vehiculos_transporte_personal": "Vehículos para el transporte personal",
    "vinagres_especias_salsas_condimentos_afines": "Vinagres, especias, salsas, condimentos y afines",
    "vajilla": "Vajilla",
    "verduras_frescas_secas_conserva": "Verduras frescas, secas y en conserva",
}


def _ipc_gba_1974(serie: Serie) -> Clasificacion | None:
    if serie.variable == "nivel_general":
        return _historica(_V_GBA_1974, "Nivel general", NIVEL_GENERAL, cobertura="GBA", unidad=_INDICE_1974)
    if serie.variable in _CAPITULOS_1974:
        return _historica(_V_GBA_1974, _CAPITULOS_1974[serie.variable], CAPITULOS, cobertura="GBA",
                          unidad=_INDICE_1974)
    nombre = _APERTURAS_1974.get(serie.variable)
    if nombre is None:
        return None
    return _historica(_V_GBA_1974, nombre, PRINCIPALES_APERTURAS, cobertura="GBA", unidad=_INDICE_1974)


# --- Versión: Costo de vida en la Capital Federal, base 1960 (empleo_ingresos.xlsx) ------------------------
_V_COSTO_VIDA = "Costo de vida Capital Federal base 1960"
_COSTO_VIDA = {   # variable → (ítem, grupo, medida)
    "nivel_general": ("Nivel general", NIVEL_GENERAL, ""),
    "nivel_general_corregido_variacion_estacional": ("Nivel general", NIVEL_GENERAL, DESESTACIONALIZADO),
    "nivel_general_excluidos_alimentos_frescos_serv_aloj":
        ("Nivel general excluidos alimentos frescos, servicios y alojamiento", NIVEL_GENERAL, ""),
    "alimentacion": ("Alimentación", CAPITULOS, ""),
    "alojamiento": ("Alojamiento", CAPITULOS, ""),
    "gastos_generales": ("Gastos generales", CAPITULOS, ""),
    "indumentaria": ("Indumentaria", CAPITULOS, ""),
    "menaje": ("Menaje", CAPITULOS, ""),
}


def _costo_vida(serie: Serie) -> Clasificacion | None:
    datos = _COSTO_VIDA.get(serie.variable)
    if not datos:
        return None
    item, grupo, medida = datos
    return _historica(_V_COSTO_VIDA, item, grupo, cobertura="Capital Federal", medida=medida,
                      unidad="Índice 1960=100")


# ===================================================================================================
# Provincias
# ===================================================================================================
_CHACO = "Chaco (Gran Resistencia)"
_CABA = "Ciudad Autónoma de Buenos Aires"


def _provincial(provincia: str, item: str, unidad: str) -> Clasificacion:
    return _resultado(item, PROVINCIAS, provincia, en_nombre=provincia, unidad=unidad)


# Nivel general de las provincias (cuadros 4.24 a 4.29, 4.35 y 4.36). La base sale del título del cuadro,
# salvo en los que se verificó con los datos (ver ``_PROVINCIAS``).
_NG = "Nivel general"
_DIV_CABA = {
    "alimentos_bebidas_no_alcoholicas": ALIMENTOS, "bebidas_alcoholicas_tabaco": BEBIDAS_ALCOHOLICAS,
    "bienes_servicios_varios": BIENES_SERVICIOS_VARIOS, "comunicaciones": COMUNICACION, "educacion": EDUCACION,
    "equipamiento_mantenimiento_hogar": EQUIPAMIENTO, "estacionales": "Estacionales",
    "nivel_general": _NG, "prendas_vestir_calzado": PRENDAS, "recreacion_cultura": RECREACION,
    "regulados": "Regulados", "restaurantes_hoteles": RESTAURANTES, "resto": "Resto", "salud": SALUD,
    "transporte": TRANSPORTE, "vivienda_agua_electricidad_otros_combustibles": VIVIENDA,
    # Divisiones nuevas de la base 2021 (COICOP 2018)
    "cuidado_personal_proteccion_social_otros": "Cuidado personal, protección social y otros",
    "informacion_comunicacion": "Información y comunicación",
    "seguros_servicios_financieros": "Seguros y servicios financieros",
}
_DIV_CORDOBA = {
    "alimentos_bebidas": "Alimentos y bebidas", "bienes_servicios_varios": "Bienes y servicios varios",
    "ensenianza": "Enseñanza", "equipamiento_mantenimiento_hogar": EQUIPAMIENTO,
    "esparcimiento": "Esparcimiento", "indumentaria_calzado": "Indumentaria y calzado",
    "nivel_general": _NG, "propiedades_combustibles_agua_electricidad": "Propiedades, combustibles, agua y electricidad",
    "salud": SALUD, "transporte_comunicaciones": "Transporte y comunicaciones",
}
# Mendoza y San Luis publican los mismos diez rubros, con la clasificación vieja del INDEC.
_DIV_MENDOZA = {
    "alimentos_bebidas": "Alimentos y bebidas", "atencion_medica_gastos_salud": _CAP_SALUD,
    "educacion": _CAP_EDUCACION, "equipamiento_mantenimiento_hogar": _CAP_EQUIPAMIENTO,
    "esparcimiento": _CAP_ESPARCIMIENTO, "indumentaria": _CAP_INDUMENTARIA, "nivel_general": _NG,
    "otros_bienes_servicios_varios": "Otros bienes y servicios varios",
    "transporte_comunicaciones": _CAP_TRANSPORTE, "vivienda_servicios_basicos": _CAP_VIVIENDA,
}
_DIV_SANTA_FE = {
    "alimentos_bebidas": "Alimentos y bebidas", "educacion": _CAP_EDUCACION, "equipamiento_hogar": "Equipamiento del hogar",
    "esparcimiento": _CAP_ESPARCIMIENTO, "indumentaria": _CAP_INDUMENTARIA, "nivel_general": _NG,
    "otros": "Otros bienes y servicios", "salud": SALUD, "transporte": TRANSPORTE, "vivienda": "Vivienda",
}
_DIV_CHACO = {
    "ipc_chaco_indumentaria_y_calzado": "Indumentaria y calzado",
    "ipc_chaco_propiedad_combustible_agua_y_electricidad": "Propiedad, combustible, agua y electricidad",
    "ipc_chaco_alimentos_y_bebidas": "Alimentos y bebidas",
    "ipc_chaco_equipamiento_y_mantenimiento_del_hogar": EQUIPAMIENTO,
    "ipc_chaco_bienes_y_servicios_varios": "Bienes y servicios varios",
    "ipc_chaco_atencion_de_la_salud": "Atención de la salud",
    "ipc_chaco_transporte_y_comunicaciones": "Transporte y comunicaciones",
    "ipc_chaco_ng": _NG, "ipc_chaco_ensenanza": "Enseñanza", "ipc_chaco_esparcimiento_y_cultura": "Esparcimiento y cultura",
}
# hoja → (versión histórica o None, provincia, rótulos, unidad)
_PROVINCIAS = {
    "4.23": (f"{_CABA} base jul-2011 a jun-2012", _CABA, _DIV_CABA, "Índice jul-2011 a jun-2012=100"),
    "4.24": (None, "Córdoba", _DIV_CORDOBA, "Índice 2014=100"),
    "4.25": (None, "Mendoza", _DIV_MENDOZA, "Índice 2010=100"),
    "4.26": (None, "Neuquén", {"nivel_general": _NG}, "Índice ene-2007=100"),
    "4.27": (None, "San Luis", _DIV_MENDOZA, "Índice 2003=100"),
    "4.28": (None, "Santa Fe", _DIV_SANTA_FE, "Índice 2014=100"),
    # El título del cuadro 4.29 dice «base 2004», pero sus valores coinciden con los del 4.36 (dic-2017=100).
    "4.29": (None, "Tucumán", {"nivel_general": _NG}, "Índice dic-2017=100"),
    "4.35 Chaco serie histórica": (None, _CHACO, {"ipc_chaco_historico_ng": _NG}, "Índice sep-2010=100"),
    "4.36 Tucumán": (None, "Tucumán", {"nivel_general": _NG}, "Índice dic-2017=100"),
    "4.37 CABA": (None, _CABA, _DIV_CABA, "Índice 2021=100"),
    "4.38 Chaco": (None, _CHACO, _DIV_CHACO, "Índice sep-2010=100"),
}


def _ipc_provincia(serie: Serie) -> Clasificacion | None:
    version, provincia, rotulos, unidad = _PROVINCIAS[serie.hoja]
    item = rotulos.get(serie.variable)
    if item is None:
        return None
    if version:   # versión vieja de un índice provincial: va a «Series históricas»
        return _resultado(item, SERIES_HISTORICAS, version, en_nombre=provincia, unidad=unidad)
    return _provincial(provincia, item, unidad)


# ===================================================================================================
# Estados Unidos
# ===================================================================================================
def _fred(serie: Serie) -> Clasificacion | None:
    return _resultado("Nivel general", ESTADOS_UNIDOS, en_nombre="Estados Unidos", medida=DESESTACIONALIZADO)


# ===================================================================================================
# Series excluidas
# ===================================================================================================
# Cuadro 4.2.4 de datos.gob.ar (IPC GBA, principales aperturas), auditado el 15/11/2026.
#
# Cada columna (38 mensuales y 38 trimestrales) trae dos conceptos empalmados por posición:
#   * abr-2016 a dic-2016: el concepto que dice su rótulo. Los valores coinciden al decimal con la hoja
#     «IPC GBA Base dic 2016» del libro del INDEC (dic-2016 = 100 en todas).
#   * ene-2017 en adelante: la fila número N del libro de aperturas del INDEC
#     (https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls, región GBA, índices), donde N
#     es el lugar de la columna en la lista de 38 (nivel general, divisiones y aperturas, en el orden del
#     INDEC). Coinciden 110 a 114 de 114 meses mensuales (y 37 a 39 de 39 trimestres) con una tolerancia de
#     0,002 puntos de índice, y las variaciones de ene a may-2017 con la hoja «IPC GBA Abril 2016-Mayo 2017»
#     del INDEC (diferencia 0,00 p. p. en las series que cambian de concepto). Las 12 primeras
#     posiciones (nivel general, alimentos y bebidas no alcohólicas, alimentos, pan y cereales, carnes,
#     leche, aceites, frutas, verduras, azúcar, bebidas no alcohólicas y café) coinciden con su rótulo en
#     todo el período y se conservan; las otras 26 por frecuencia no se pueden renombrar (dos conceptos en
#     una misma columna). Los datos de 2016 de esas 26 siguen en la hoja «IPC GBA Base dic 2016» del
#     INDEC (indec-ipc); desde 2017 las divisiones están en 4.1.2 y 4.2.1 y en el libro nacional. Las
#     aperturas finas de GBA desde 2017 no están en el inventario (el libro de aperturas no se descarga).
#     Meses sueltos fuera de patrón (2020-07; 2023-03, en que muchas columnas traen la fila siguiente) son
#     atípicos de la fuente y no se tocan.
def _empalme_424(rotulo_2016: str, contenido_2017: str, puesto: int) -> str:
    return (f"Empalme de dos conceptos: abr-dic 2016 «{rotulo_2016}» y desde ene-2017 «{contenido_2017}» "
            f"(fila {puesto} del libro de aperturas del INDEC)")


EXCLUIDAS = {
    # mensuales
    ("datos.gob.ar", "104.1_I2BAT_2016_M_34"): _empalme_424("Bebidas alcohólicas y tabaco", "Aguas minerales, bebidas gaseosas y jugos", 13),
    ("datos.gob.ar", "104.1_I2BA_2016_M_27"): _empalme_424("Bebidas alcohólicas", "Bebidas alcohólicas y tabaco", 14),
    ("datos.gob.ar", "104.1_I2BA_2016_M_15"): _empalme_424("Tabaco", "Bebidas alcohólicas", 15),
    ("datos.gob.ar", "104.1_I2ABCFH_2016_M_30"): _empalme_424("Prendas de vestir y calzado", "Tabaco", 16),
    ("datos.gob.ar", "104.1_I2I_2016_M_34"): _empalme_424("Prendas de vestir y materiales", "Prendas de vestir y calzado", 17),
    ("datos.gob.ar", "104.1_I2R_2016_M_15"): _empalme_424("Calzado", "Prendas de vestir y materiales", 18),
    ("datos.gob.ar", "104.1_I2RI_2016_M_53"): _empalme_424("Vivienda, agua, electricidad, gas y otros combustibles", "Calzado", 19),
    ("datos.gob.ar", "104.1_I2RE_2016_M_25"): _empalme_424("Alquiler de la vivienda y gastos conexos", "Vivienda, agua, electricidad, gas y otros combustibles", 20),
    ("datos.gob.ar", "104.1_I2C_2016_M_42"): _empalme_424("Mantenimiento y reparación de la vivienda", "Alquiler de la vivienda y gastos conexos", 21),
    ("datos.gob.ar", "104.1_I2VSB_2016_M_43"): _empalme_424("Electricidad, gas y otros combustibles", "Alquiler de la vivienda", 22),
    ("datos.gob.ar", "104.1_I2AV_2016_M_41"): _empalme_424("Equipamiento y mantenimiento del hogar", "Mantenimiento y reparación de la vivienda", 23),
    ("datos.gob.ar", "104.1_I2SBCV_2016_M_49"): _empalme_424("Bienes y servicios para la conservación del hogar", "Electricidad, gas y otros combustibles", 24),
    ("datos.gob.ar", "104.1_I2RGCV_2016_M_14"): _empalme_424("Salud", "Equipamiento y mantenimiento del hogar", 25),
    ("datos.gob.ar", "104.1_I2EMH_2016_M_55"): _empalme_424("Productos medicinales, artefactos y equipos para la salud", "Bienes y servicios para la conservación del hogar", 26),
    ("datos.gob.ar", "104.1_I2EH_2016_M_19"): _empalme_424("Transporte", "Salud", 27),
    ("datos.gob.ar", "104.1_I2MH_2016_M_29"): _empalme_424("Adquisición de vehículos", "Productos medicinales, artefactos y equipos para la salud", 28),
    ("datos.gob.ar", "104.1_I2PUL_2016_M_50"): _empalme_424("Funcionamiento de equipos de transporte personal", "Gastos de prepagas", 29),
    ("datos.gob.ar", "104.1_I2SH_2016_M_26"): _empalme_424("Transporte público", "Transporte", 30),
    ("datos.gob.ar", "104.1_I2AMGS_2016_M_33"): _empalme_424("Comunicación", "Adquisición de vehículos", 31),
    ("datos.gob.ar", "104.1_I2PMAT_2016_M_27"): _empalme_424("Recreación y cultura", "Funcionamiento de equipos de transporte personal", 32),
    ("datos.gob.ar", "104.1_I2SS_2016_M_60"): _empalme_424("Equipos audiovisuales, fotográficos y de procesamiento de la información", "Combustibles y lubricantes para vehículos de uso del hogar", 33),
    ("datos.gob.ar", "104.1_I2TC_2016_M_60"): _empalme_424("Periódicos, diarios, revistas, libros y artículos de papelería", "Transporte público", 34),
    ("datos.gob.ar", "104.1_I2T_2016_M_18"): _empalme_424("Educación", "Comunicación", 35),
    ("datos.gob.ar", "104.1_I2TPP_2016_M_29"): _empalme_424("Restaurantes y hoteles", "Servicios de telefonía e internet", 36),
    ("datos.gob.ar", "104.1_I2AV_2016_M_32"): _empalme_424("Bienes y servicios varios", "Recreación y cultura", 37),
    ("datos.gob.ar", "104.1_I2FMV_2016_M_24"): _empalme_424("Cuidado personal", "Equipos audiovisuales, fotográficos y de procesamiento de la información", 38),
    # trimestrales
    ("datos.gob.ar", "104.2_I2016_BACO_2016_T_34"): _empalme_424("Bebidas alcohólicas y tabaco", "Aguas minerales, bebidas gaseosas y jugos", 13),
    ("datos.gob.ar", "104.2_I2016_BCAS_2016_T_27"): _empalme_424("Bebidas alcohólicas", "Bebidas alcohólicas y tabaco", 14),
    ("datos.gob.ar", "104.2_I2016_TACO_2016_T_15"): _empalme_424("Tabaco", "Bebidas alcohólicas", 15),
    ("datos.gob.ar", "104.2_I2016_PADO_2016_T_30"): _empalme_424("Prendas de vestir y calzado", "Tabaco", 16),
    ("datos.gob.ar", "104.2_I2016_PLES_2016_T_34"): _empalme_424("Prendas de vestir y materiales", "Prendas de vestir y calzado", 17),
    ("datos.gob.ar", "104.2_I2016_CADO_2016_T_15"): _empalme_424("Calzado", "Prendas de vestir y materiales", 18),
    ("datos.gob.ar", "104.2_I2016_VBES_2016_T_53"): _empalme_424("Vivienda, agua, electricidad, gas y otros combustibles", "Calzado", 19),
    ("datos.gob.ar", "104.2_I2016_ANDA_2016_T_25"): _empalme_424("Alquiler de la vivienda y gastos conexos", "Vivienda, agua, electricidad, gas y otros combustibles", 20),
    ("datos.gob.ar", "104.2_I2016_MNDA_2016_T_42"): _empalme_424("Mantenimiento y reparación de la vivienda", "Alquiler de la vivienda y gastos conexos", 21),
    ("datos.gob.ar", "104.2_I2016_EBES_2016_T_43"): _empalme_424("Electricidad, gas y otros combustibles", "Alquiler de la vivienda", 22),
    ("datos.gob.ar", "104.2_I2016_EGAR_2016_T_41"): _empalme_424("Equipamiento y mantenimiento del hogar", "Mantenimiento y reparación de la vivienda", 23),
    ("datos.gob.ar", "104.2_I2016_BGAR_2016_T_49"): _empalme_424("Bienes y servicios para la conservación del hogar", "Electricidad, gas y otros combustibles", 24),
    ("datos.gob.ar", "104.2_I2016_SLUD_2016_T_14"): _empalme_424("Salud", "Equipamiento y mantenimiento del hogar", 25),
    ("datos.gob.ar", "104.2_I2016_PLUD_2016_T_55"): _empalme_424("Productos medicinales, artefactos y equipos para la salud", "Bienes y servicios para la conservación del hogar", 26),
    ("datos.gob.ar", "104.2_I2016_TRTE_2016_T_19"): _empalme_424("Transporte", "Salud", 27),
    ("datos.gob.ar", "104.2_I2016_AUOS_2016_T_29"): _empalme_424("Adquisición de vehículos", "Productos medicinales, artefactos y equipos para la salud", 28),
    ("datos.gob.ar", "104.2_I2016_FONA_2016_T_50"): _empalme_424("Funcionamiento de equipos de transporte personal", "Gastos de prepagas", 29),
    ("datos.gob.ar", "104.2_I2016_TICO_2016_T_26"): _empalme_424("Transporte público", "Transporte", 30),
    ("datos.gob.ar", "104.2_I2016_IION_2016_T_33"): _empalme_424("Comunicación", "Adquisición de vehículos", 31),
    ("datos.gob.ar", "104.2_I2016_RURA_2016_T_27"): _empalme_424("Recreación y cultura", "Funcionamiento de equipos de transporte personal", 32),
    ("datos.gob.ar", "104.2_I2016_EINF_2016_T_60"): _empalme_424("Equipos audiovisuales, fotográficos y de procesamiento de la información", "Combustibles y lubricantes para vehículos de uso del hogar", 33),
    ("datos.gob.ar", "104.2_I2016_PRIA_2016_T_60"): _empalme_424("Periódicos, diarios, revistas, libros y artículos de papelería", "Transporte público", 34),
    ("datos.gob.ar", "104.2_I2016_EION_2016_T_18"): _empalme_424("Educación", "Comunicación", 35),
    ("datos.gob.ar", "104.2_I2016_RLES_2016_T_29"): _empalme_424("Restaurantes y hoteles", "Servicios de telefonía e internet", 36),
    ("datos.gob.ar", "104.2_I2016_BIOS_2016_T_32"): _empalme_424("Bienes y servicios varios", "Recreación y cultura", 37),
    ("datos.gob.ar", "104.2_I2016_CONA_2016_T_24"): _empalme_424("Cuidado personal", "Equipos audiovisuales, fotográficos y de procesamiento de la información", 38),
}


# ===================================================================================================
# Despacho
# ===================================================================================================
_HOJAS_PRECIOS = {
    "4.1.1 IPC NG": _ipc_ng,
    "4.1.2 IPC Capitulos": _ipc_capitulos,
    "4.1.3 IPC Bs Ss": _ipc_bienes_servicios,
    "4.1.4 IPC Incidencia Cap": _ipc_incidencia_capitulos,
    "4.1.5 IPC Incidencia Bs Ss": _ipc_incidencia_bienes_servicios,
    "4.1.6 IPC Categorias": _ipc_categorias,
    "4.1.7 Incidencia Cat": _ipc_incidencia_categorias,
    "4.1.8 IPC precios canasta": _ipc_canasta,
    "4.2.1": _ipc_gba_capitulos,
    "4.2.2": _ipc_gba_bienes_servicios,
    "4.2.3": _ipc_gba_categorias,
    "4.2.4": _ipc_gba_aperturas,
    "4.2.5": _ipc_gba_precios,
    "4.15.1": _ipc_nu_capitulos,
    "4.15.2": _ipc_nu_aperturas,
    "4.16.1": _ipc_gba_2008_bienes_servicios,
    "4.16.2": _ipc_gba_2008_capitulos,
    "4.16.3": _ipc_gba_2008_categorias,
    "4.16.4": _ipc_gba_2008_aperturas,
    "4.17": _ipc_nacional_2003,
    "4.18": _ipc_gba_1999_aperturas,
    "4.20": _ipc_gba_1988,
    "4.21": _ipc_gba_1974,
    "4.22": _ipc_gba_1999_historico,
    **{hoja: _ipc_provincia for hoja in _PROVINCIAS},
}
_HOJAS_INDEC = {
    "IPC GBA Abril 2016-Mayo 2017": _indec_gba_2016,
    "IPC GBA Base dic 2016": _indec_gba_dic_2016,
    **{hoja: _indec_nacional for hoja in _MEDIDA_HOJA_NACIONAL},
}


def clasificar(serie: Serie) -> Clasificacion | None:
    """Clasificación de una serie del IPC; None si la serie no es del IPC."""
    if serie.fuente == "fred-cpiaucsl":
        return _fred(serie)
    if serie.fuente == "indec-ipc":
        regla = _HOJAS_INDEC.get(serie.hoja)
        return regla(serie) if regla else None
    if serie.fuente != "datos.gob.ar":
        return None
    if serie.archivo == "precios.xlsx":
        regla = _HOJAS_PRECIOS.get(serie.hoja)
        return regla(serie) if regla else None
    if serie.archivo == "empleo_ingresos.xlsx" and serie.hoja == "CostoNivelVida60-75":
        return _costo_vida(serie)
    return None
