"""Reglas de servicios, comercio, agro, energía y encuestas de confianza de ``actividad.xlsx``.

Fuente ``datos.gob.ar``, archivo ``actividad.xlsx``. Todo va al tema «Actividad».

    Hoja                                   Indicador (nivel 2)
    1.10, 1.10.1                           Servicios públicos
    1.11, 1.11.1                           Ventas en supermercados (el mismo árbol que ``actividad_supermercados``)
    1.11.2                                 Ventas en autoservicios mayoristas
    1.12, 1.12.1                           Ventas en centros de compras
    1.19                                   Producción agrícola
    1.20                                   Faena pecuaria
    1.21                                   Captura pesquera
    1.22                                   Industria automotriz
    1.23                                   Industria siderúrgica
    1.24                                   Petróleo y derivados
    1.25                                   Gas natural
    1.26                                   Hidrocarburos por empresa
    1.27                                   Pozos y metros perforados
    1.28                                   Electricidad
    1.29                                   Biocombustibles
    1.30                                   Confianza del consumidor
    1.31                                   Ocupación hotelera
    1.32                                   Confianza en el gobierno

Decisiones de diseño:

* La energía no se junta bajo un indicador «Energía»: petróleo y derivados, gas, electricidad,
  biocombustibles, pozos y producción por empresa son operativos distintos (la regla 1 del árbol
  agrupa lo que mide el mismo fenómeno), y un único nodo mezclaría m3, GWh, MW y toneladas.
* Las hojas viejas (1.10, 1.11, 1.12) van a «Series históricas», con la versión como nivel 4.
* Las series se reconocen por ``Variable`` (o ``Descripción`` cuando la variable cambia de una
  frecuencia a otra): ``Nombre fuente`` es el título del cuadro y se repite en todas las series.
* Las unidades salen de la descripción de cada serie y se contrastaron con los datos (los
  metadatos del catálogo traen errores: p. ej. dicen «miles de pesos» para ventas que están en
  millones, o ``2004=100`` para series físicas).
"""

from __future__ import annotations

from . import Clasificacion, Serie
from .actividad_supermercados import (
    CONSTANTES, INDICADOR as SUPERMERCADOS, INDICE as INDICE_2017, POR_CANAL, POR_GRUPO,
    POR_MEDIO_DE_PAGO, TOTAL,
)
from .texto import con_medida, plano, unir

FUENTE = "datos.gob.ar"
ARCHIVO = "actividad.xlsx"
TEMA = "Actividad"

SERIES_HISTORICAS = "Series históricas"
POR_REGION = "Por región"
POR_TIPO_DE_USUARIO = "Por tipo de usuario"
POR_DISTRIBUIDORA = "Por distribuidora"
NIVEL_GENERAL = "Nivel general"

DESESTACIONALIZADO = "desestacionalizado"
TENDENCIA_CICLO = "tendencia-ciclo"

MILES_DE_PESOS = "Miles de pesos"
MILLONES_DE_PESOS = "Millones de pesos"


def _clave(texto: str) -> str:
    """Variable o texto sin guiones bajos, tildes ni mayúsculas, para buscar en las tablas."""
    return plano(texto).replace("_", "").replace(" ", "")


def _tabla(definiciones: dict) -> dict:
    return {_clave(clave): valor for clave, valor in definiciones.items()}


def _c(indicador: str, nombre: str, cobertura: str = "", apertura: str = "",
       medida: tuple[str, ...] | str = (), unidad: str = "") -> Clasificacion:
    """Clasificación de una serie; ``nombre`` ya lleva el indicador y la apertura."""
    if isinstance(medida, str):
        medida = (medida,) if medida else ()
    return Clasificacion(tema=TEMA, indicador=indicador, nombre=con_medida(nombre, ", ".join(medida)),
                         cobertura=cobertura, apertura=apertura, unidad=unidad)


# ---------------------------------------------------------------------------------------------
# Servicios públicos (1.10 y 1.10.1)
# ---------------------------------------------------------------------------------------------

SERVICIOS_PUBLICOS = "Servicios públicos"
INDICADOR_SINTETICO = "Indicador sintético"
POR_SERVICIO = "Por servicio"
BASE_1993 = "Base 1993"
BASE_1993_HASTA_2003 = "Base 1993 (hasta 2003)"

INDICE_2004 = "Índice 2004=100"

# Servicios del indicador sintético. Clave: texto de la variable sin prefijo de versión.
_SERVICIOS_INDICE_2017 = _tabla({
    "nivel_gral": None,
    "elec_gas_agua": "Electricidad, gas y agua",
    "peajes": "Peajes",
    "telef": "Telefonía",
    "transp_carga": "Transporte de carga",
    "transp_pasaj": "Transporte de pasajeros",
    "correo_postal": "Correo postal",
    "recoleccion_residuos": "Recolección de residuos",
})
_VERSIONES_INDICE_2017 = (
    ("s_original_", ()),
    ("s_desest_", (DESESTACIONALIZADO,)),
    ("s_tendencia_ciclo_", (TENDENCIA_CICLO,)),
)

# Servicios del indicador sintético de la base 1993 (sólo versión original y desestacionalizada).
_SERVICIOS_INDICE_1993 = _tabla({
    "nivel_general": None,
    "electricidad_gas_agua": "Electricidad, gas y agua",
    "peajes": "Peajes",
    "telefonia": "Telefonía",
    "transporte_cargas": "Transporte de carga",
    "transporte_pasajeros": "Transporte de pasajeros",
})

_UTEQ = "UTEq"
_VEHICULOS = "Vehículos"
_MILES_VEHICULOS = "Miles de vehículos"
_M3 = "Miles de metros cúbicos"
_MIN = "Miles de minutos"

# Series físicas de la base 2017. Variable -> (servicio, apertura, cobertura, medida, unidad).
# La apertura es el servicio tal como encabeza el cuadro; la cobertura, lo que se mide.
_FISICAS_2017 = _tabla({
    "correos_correo_postal_miles_u": ("Correo postal", "Correos", "Correo postal", "", "Miles de unidades"),
    "correos_envios_monetarios_miles_u": ("Correo postal", "Correos", "Envíos monetarios", "", "Miles de unidades"),
    "correos_telegraficos_miles_u": ("Correo postal", "Correos", "Envíos telegráficos", "", "Miles de unidades"),
    "elec_gas_agua_energia_electrica_gwh": ("Electricidad, gas y agua", "Electricidad, gas y agua",
                                            "Generación de energía eléctrica", "", "GWh"),
    "elec_gas_agua_agua_red_miles_m3": ("Electricidad, gas y agua", "Electricidad, gas y agua", "Agua de red", "", _M3),
    "elec_gas_agua_gas_natural_miles_m3": ("Electricidad, gas y agua", "Electricidad, gas y agua", "Gas natural", "", _M3),
    "peajes_accesos_caba_u": ("Peajes", "Peajes", "Accesos a la Ciudad Autónoma de Buenos Aires", "", _VEHICULOS),
    "peajes_accesos_caba_uteqs": ("Peajes", "Peajes", "Accesos a la Ciudad Autónoma de Buenos Aires", _UTEQ, _UTEQ),
    "peajes_accesos_cba_u": ("Peajes", "Peajes", "Accesos a Córdoba", "", _VEHICULOS),
    "peajes_accesos_cba_uteqs": ("Peajes", "Peajes", "Accesos a Córdoba", _UTEQ, _UTEQ),
    "peajes_autopistas_caba_u": ("Peajes", "Peajes", "Autopistas de la Ciudad Autónoma de Buenos Aires", "", _VEHICULOS),
    "peajes_autopistas_caba_uteqs": ("Peajes", "Peajes", "Autopistas de la Ciudad Autónoma de Buenos Aires", _UTEQ, _UTEQ),
    "peajes_rutas_nac_u": ("Peajes", "Peajes", "Rutas nacionales", "", _VEHICULOS),
    "peajes_rutas_nacionales_uteqs": ("Peajes", "Peajes", "Rutas nacionales", _UTEQ, _UTEQ),
    "peajes_rutas_pba_u": ("Peajes", "Peajes", "Rutas de la provincia de Buenos Aires", "", _VEHICULOS),
    "peajes_rutas_pba_uteqs": ("Peajes", "Peajes", "Rutas de la provincia de Buenos Aires", _UTEQ, _UTEQ),
    "peajes_rutas_cba_u": ("Peajes", "Peajes", "Rutas de la provincia de Córdoba", "", _VEHICULOS),
    "peajes_rutas_prov_cba_uteqs": ("Peajes", "Peajes", "Rutas de la provincia de Córdoba", _UTEQ, _UTEQ),
    "peajes_rutas_stafe_u": ("Peajes", "Peajes", "Rutas de la provincia de Santa Fe", "", _VEHICULOS),
    "peajes_rutas_prov_stafe_uteqs": ("Peajes", "Peajes", "Rutas de la provincia de Santa Fe", _UTEQ, _UTEQ),
    "residuos_residuos_solidos_ingresados_tn": ("Recolección de residuos", "Recolección de residuos",
                                                "Residuos sólidos ingresados", "", "Toneladas"),
    "serv_telef_fija_lineas_activas_miles_u": ("Telefonía", "Telefonía fija", "Líneas activas", "", "Miles de líneas"),
    "serv_telef_fija_lineas_activas_llamadas": ("Telefonía", "Telefonía fija", "Llamadas", "", "Miles de llamadas"),
    "serv_telef_fija_trafico_min_total_miles_u": ("Telefonía", "Telefonía fija", "Tráfico de minutos", "", _MIN),
    "serv_telef_fija_lineas_activas_minutos": ("Telefonía", "Telefonía fija", "Tráfico de minutos", "", _MIN),
    "serv_telef_fija_trafico_min_urbano_miles_u": ("Telefonía", "Telefonía fija", "Tráfico de minutos urbanos", "", _MIN),
    "serv_telef_fija_trafico_min_inter_urbano_miles_u": ("Telefonía", "Telefonía fija",
                                                         "Tráfico de minutos interurbanos", "", _MIN),
    "serv_telef_fija_trafico_min_inter_miles_u": ("Telefonía", "Telefonía fija",
                                                  "Tráfico de minutos internacionales", "", _MIN),
    "serv_telef_movil_lineas_activas_miles_u": ("Telefonía", "Telefonía móvil", "Líneas activas", "", "Miles de líneas"),
    "serv_telef_movil_llamadas_miles_u": ("Telefonía", "Telefonía móvil", "Llamadas", "", "Miles de llamadas"),
    "serv_telef_movil_mensajes_cortos_miles_u": ("Telefonía", "Telefonía móvil", "Mensajes cortos", "", "Miles de mensajes"),
    "serv_telef_movil_trafico_min_total_miles_u": ("Telefonía", "Telefonía móvil", "Tráfico de minutos", "", _MIN),
    "serv_telef_movil_trafico_min_urbano_miles_u": ("Telefonía", "Telefonía móvil", "Tráfico de minutos urbanos", "", _MIN),
    "serv_telef_movil_trafico_min_inter_urbano_miles_u": ("Telefonía", "Telefonía móvil",
                                                          "Tráfico de minutos interurbanos", "", _MIN),
    "serv_telef_movil_trafico_min_inter_miles_u": ("Telefonía", "Telefonía móvil",
                                                   "Tráfico de minutos internacionales", "", _MIN),
    "serv_telef_movil_datos_accesos_bamtbytes": ("Telefonía", "Telefonía móvil", "Tráfico de datos y accesos", "", "Terabytes"),
    "transp_carga_serv_ferrov_miles_tn_km": ("Transporte de carga", "Transporte de carga", "Servicio ferroviario",
                                             "toneladas-kilómetro", "Miles de toneladas-kilómetro"),
    "transp_carga_serv_aeronav_comercial_total_tn": ("Transporte de carga", "Transporte de carga",
                                                     "Servicio aéreo comercial, total", "", "Toneladas"),
    "transp_carga_serv_aeronav_comercial_cabotaje_tn": ("Transporte de carga", "Transporte de carga",
                                                        "Servicio aéreo comercial de cabotaje", "", "Toneladas"),
    "transp_carga_serv_aeronav_comercial_inter_tn": ("Transporte de carga", "Transporte de carga",
                                                     "Servicio aéreo comercial internacional", "", "Toneladas"),
    "transp_pasaj_total_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros", "Total", "", "Miles de pasajeros"),
    "transp_pasaj_subte_premetro_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros",
                                                "Subterráneo y premetro", "", "Miles de pasajeros"),
    "transp_pasaj_serv_ferrov_urbano_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros",
                                                    "Servicio ferroviario urbano", "", "Miles de pasajeros"),
    "transp_pasaj_serv_pub_auto_urbano_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros",
                                                      "Servicio público automotor urbano", "", "Miles de pasajeros"),
    "transp_pasaj_serv_ferrov_inter_urbano_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros",
                                                          "Servicio ferroviario interurbano", "", "Miles de pasajeros"),
    "transp_pasaj_serv_regular_aeronav_inter_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros",
                                                            "Servicio aéreo regular internacional", "", "Miles de pasajeros"),
    "transp_pasaj_serv_regular_aeronav_cabotaje_miles_pasaj": ("Transporte de pasajeros", "Transporte de pasajeros",
                                                               "Servicio aéreo regular de cabotaje", "", "Miles de pasajeros"),
})

# Series físicas de la base 1993. Variable -> (versión, apertura, cobertura, medida, unidad).
# Las series «desde 2002» continúan a las de la telefonía hasta 2003: mismo nombre, otra versión.
# Los cuadros dicen «miles de pasajes» en el cabotaje aéreo y en el ferrocarril interurbano, pero
# los valores (590.962 por mes; en la base 2017, 623 «miles») indican pasajes sueltos.
_FISICAS_1993 = _tabla({
    "agua_entregada_por_aguas_argentinas": (BASE_1993, "Electricidad, gas y agua",
                                            "Agua entregada por Aguas Argentinas", "", _M3),
    "consumo_gas_natural_millones": (BASE_1993, "Electricidad, gas y agua", "Gas natural", "", "Millones de metros cúbicos"),
    "generacion_energia_electrica": (BASE_1993, "Electricidad, gas y agua", "Generación de energía eléctrica", "", "GWh"),
    "cabotaje_nacional_carga_serv_aeronavegacion_comercial": (
        BASE_1993, "Transporte de carga", "Servicio aéreo comercial de cabotaje", "", "Toneladas"),
    "cabotaje_nacional_pasaj_serv_aeronavegacion_comercial": (
        BASE_1993, "Transporte de pasajeros", "Servicio aéreo regular de cabotaje", "", "Pasajes"),
    "carga_serv_ferroviario": (BASE_1993, "Transporte de carga", "Servicio ferroviario", "", "Miles de toneladas"),
    "pasaj_serv_ferroviario_interurbano": (BASE_1993, "Transporte de pasajeros",
                                           "Servicio ferroviario interurbano", "", "Pasajes"),
    "pasaj_serv_ferroviario_urbano": (BASE_1993, "Transporte de pasajeros",
                                      "Servicio ferroviario urbano", "", "Miles de pasajes"),
    "pasaj_serv_subterraneos_metrovias": (BASE_1993, "Transporte de pasajeros",
                                          "Subterráneo y premetro", "", "Miles de pasajes"),
    "pasaj_serv_subterraneos_metrovias_sa": (BASE_1993, "Transporte de pasajeros",
                                             "Subterráneo y premetro", "", "Miles de pasajes"),
    "pasaj_serv_transp_automot_metropolitano": (BASE_1993, "Transporte de pasajeros",
                                                "Servicio público automotor urbano", "", "Miles de pasajes"),
    "peaje_rutas_nacionales": (BASE_1993, "Peajes", "Rutas nacionales", "", _MILES_VEHICULOS),
    "peaje_rutas_nacionales_pba": (BASE_1993, "Peajes", "Rutas nacionales y de la provincia de Buenos Aires",
                                   _UTEQ, "Miles de UTEq"),
    "peajes_acceso_caba": (BASE_1993, "Peajes", "Accesos a la Ciudad Autónoma de Buenos Aires", "", _MILES_VEHICULOS),
    "peajes_acceso_caba_uteq": (BASE_1993, "Peajes", "Accesos a la Ciudad Autónoma de Buenos Aires",
                                _UTEQ, "Miles de UTEq"),
    "serv_telefonico_basico_lineas_servicio": (BASE_1993, "Telefonía fija", "Líneas activas", "", "Miles de líneas"),
    "serv_telefonico_basico_llamadas_urbanas": (BASE_1993, "Telefonía fija", "Llamadas urbanas", "", "Miles de llamadas"),
    "serv_telefonico_basico_llamadas_interurbanas_2002": (BASE_1993, "Telefonía fija",
                                                          "Llamadas interurbanas", "", "Miles de llamadas"),
    "serv_telefonico_basico_llamadas_interurbanas": (BASE_1993_HASTA_2003, "Telefonía fija",
                                                     "Llamadas interurbanas", "", "Miles de llamadas"),
    "serv_internacional_minutos_salida_2002": (BASE_1993, "Telefonía fija",
                                               "Minutos internacionales salientes", "", _MIN),
    "serv_telefonico_internac_minutos_salida": (BASE_1993_HASTA_2003, "Telefonía fija",
                                                "Minutos internacionales salientes", "", _MIN),
    "serv_telefonico_internac_minutos_salidos": (BASE_1993_HASTA_2003, "Telefonía fija",
                                                 "Minutos internacionales salientes", "", _MIN),
    "serv_telefonico_internac_minutos_entrados": (BASE_1993_HASTA_2003, "Telefonía fija",
                                                  "Minutos internacionales entrantes", "", _MIN),
    "serv_telefonico_movil_celular": (BASE_1993, "Telefonía móvil", "Líneas activas", "", "Miles de teléfonos"),
})


def _servicios_2017(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for prefijo, medida in _VERSIONES_INDICE_2017:
        if variable.startswith(prefijo):
            clave = _clave(variable[len(prefijo):])
            if clave not in _SERVICIOS_INDICE_2017:
                return None
            servicio = _SERVICIOS_INDICE_2017[clave]
            nombre = unir(SERVICIOS_PUBLICOS, servicio or NIVEL_GENERAL)
            if servicio is None:
                return _c(SERVICIOS_PUBLICOS, nombre, NIVEL_GENERAL, "", medida, INDICE_2004)
            return _c(SERVICIOS_PUBLICOS, nombre, POR_SERVICIO, servicio, medida, INDICE_2004)
    definicion = _FISICAS_2017.get(_clave(variable))
    if definicion is None:
        return None
    servicio, apertura, cobertura, medida, unidad = definicion
    return _c(SERVICIOS_PUBLICOS, unir(SERVICIOS_PUBLICOS, apertura, cobertura), POR_SERVICIO, servicio,
              medida, unidad)


def _servicios_1993(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for sufijo, medida in (("_base_original", ()), ("_base_desest", (DESESTACIONALIZADO,)),
                           ("_desestacionalizado", (DESESTACIONALIZADO,))):
        if variable.endswith(sufijo):
            clave = _clave(variable[: -len(sufijo)])
            if clave not in _SERVICIOS_INDICE_1993:
                return None
            servicio = _SERVICIOS_INDICE_1993[clave]
            return _c(SERVICIOS_PUBLICOS, unir(SERVICIOS_PUBLICOS, servicio or NIVEL_GENERAL),
                      SERIES_HISTORICAS, BASE_1993, medida, INDICE_2004)
    if variable == "nivel_general_desestacionalizado":
        return _c(SERVICIOS_PUBLICOS, unir(SERVICIOS_PUBLICOS, NIVEL_GENERAL), SERIES_HISTORICAS, BASE_1993,
                  (DESESTACIONALIZADO,), INDICE_2004)
    if variable == "nivel_general":
        return _c(SERVICIOS_PUBLICOS, unir(SERVICIOS_PUBLICOS, NIVEL_GENERAL), SERIES_HISTORICAS, BASE_1993,
                  (), INDICE_2004)
    definicion = _FISICAS_1993.get(_clave(variable))
    if definicion is None:
        return None
    version, apertura, cobertura, medida, unidad = definicion
    return _c(SERVICIOS_PUBLICOS, unir(SERVICIOS_PUBLICOS, apertura, cobertura), SERIES_HISTORICAS, version,
              medida, unidad)


# ---------------------------------------------------------------------------------------------
# Comercio: supermercados (1.11, 1.11.1), autoservicios mayoristas (1.11.2) y centros de compras
# ---------------------------------------------------------------------------------------------

MAYORISTAS = "Ventas en autoservicios mayoristas"
CENTROS_DE_COMPRAS = "Ventas en centros de compras"
CENTROS = "Centros de compras"   # prefijo de las series que no son ventas (cines)

_GRUPOS_ARTICULOS = {
    "almacen": "Almacén",
    "alimentos_preparados_rotiseria": "Alimentos preparados y rotisería",
    "articulos_limpieza_perfumeria": "Artículos de limpieza y perfumería",
    "bebidas": "Bebidas",
    "carnes": "Carnes",
    "electronicos_articulos_hogar": "Electrónicos y artículos para el hogar",
    "indumentaria_calzado_textiles_hogar": "Indumentaria, calzado y textiles para el hogar",
    "lacteos": "Lácteos",
    "otros": "Otros",
    "panaderia": "Panadería",
    "verduleria_fruteria": "Verdulería y frutería",
    "subtotal_ventas_alimentos_bebidas": "Alimentos y bebidas",
    "subtotal_alimentos_bebidas": "Alimentos y bebidas",
}
_GRUPOS_ARTICULOS = _tabla(_GRUPOS_ARTICULOS)

_CANALES = _tabla({"salon_ventas": "Salón de ventas", "salon_de_ventas": "Salón de ventas",
                   "canales_on_line": "Canales online"})
_MEDIOS_DE_PAGO = _tabla({"efectivo": "Efectivo", "tarjetas_debito": "Tarjetas de débito",
                          "tarjetas_credito": "Tarjetas de crédito", "otros_medios": "Otros medios"})

_ENCUESTA_NIVEL = {
    # variable -> (apertura del nombre, unidad, medida)
    "ventas_precios_corrientes": (MILLONES_DE_PESOS, ()),
    "ventas_precios_constantes": (MILLONES_DE_PESOS, (CONSTANTES,)),
    "ventas_precios_constantes_original": (INDICE_2017, (CONSTANTES,)),
    "ventas_precios_constantes_desestacionalizada": (INDICE_2017, (CONSTANTES, DESESTACIONALIZADO)),
    "ventas_precios_constantes_tendencia_ciclo": (INDICE_2017, (CONSTANTES, TENDENCIA_CICLO)),
}
_ENCUESTA_NIVEL = _tabla(_ENCUESTA_NIVEL)


def _ventas_autoservicios(serie: Serie, indicador: str, por_grupo: bool) -> Clasificacion | None:
    """Supermercados (1.11.1) y autoservicios mayoristas (1.11.2): el mismo cuadro, el mismo árbol.

    ``por_grupo``: cada grupo de artículos es una apertura. En supermercados lo es (bajo cada grupo
    cuelgan las jurisdicciones del cuadro 5 de ``actividad_supermercados``); en mayoristas no hay
    nada debajo y las series cuelgan directamente del grupo «Por grupo de artículos».
    """
    clave = _clave(serie.variable)

    def hecho(nombre_parte: str, cobertura: str, apertura: str, unidad: str, medida=()):
        return _c(indicador, unir(indicador, nombre_parte), cobertura, apertura, medida, unidad)

    if clave in _ENCUESTA_NIVEL:
        unidad, medida = _ENCUESTA_NIVEL[clave]
        return hecho(TOTAL, TOTAL, "", unidad, medida)
    # Los tres «totales por …» repiten el total de ventas (en miles de pesos) en cada cuadro.
    if clave == _clave("ventas_totales_canal_venta"):
        return hecho("Total por canal de venta", POR_CANAL, "", MILES_DE_PESOS)
    if clave == _clave("ventas_totales_medio_pago"):
        return hecho("Total por medio de pago", POR_MEDIO_DE_PAGO, "", MILES_DE_PESOS)
    if clave in (_clave("ventas_totales_grupo_articulos"), _clave("ventas_totales_grupos_articulos")):
        return hecho("Total por grupo de artículos", POR_GRUPO, TOTAL if por_grupo else "", MILES_DE_PESOS)
    if clave in _CANALES:
        return hecho(_CANALES[clave], POR_CANAL, "", MILES_DE_PESOS)
    if clave in _MEDIOS_DE_PAGO:
        return hecho(_MEDIOS_DE_PAGO[clave], POR_MEDIO_DE_PAGO, "", MILES_DE_PESOS)
    if clave in _GRUPOS_ARTICULOS:
        grupo = _GRUPOS_ARTICULOS[clave]
        return hecho(grupo, POR_GRUPO, grupo if por_grupo else "", MILES_DE_PESOS)
    return None


# Supermercados, base 1996 (1.11). Descripción -> grupo de artículos (o total). Miles de pesos.
_SUPER_1996 = (
    ("alimentos y bebidas", "Alimentos y bebidas"),
    ("articulos electronicos", "Electrónicos y artículos para el hogar"),
    ("indumentaria, calzado y textiles", "Indumentaria, calzado y textiles para el hogar"),
    ("limpieza y perfumeria", "Artículos de limpieza y perfumería"),
    ("otros articulos", "Otros"),
    ("grupos de articulos. total", TOTAL),
)


def _super_1996(serie: Serie) -> Clasificacion | None:
    descripcion = plano(serie.descripcion)
    for texto, grupo in _SUPER_1996:
        if texto in descripcion:
            return _c(SUPERMERCADOS, unir(SUPERMERCADOS, grupo), SERIES_HISTORICAS, "Base 1996", (), MILES_DE_PESOS)
    return None


_RUBROS_CENTROS = _tabla({
    "amoblamientos_decoracion_textiles_hogar": "Amoblamientos, decoración y textiles para el hogar",
    "diversion_esparcimiento": "Diversión y esparcimiento",
    "electronicos_electrodomesticos_computacion": "Electrónicos, CD, electrodomésticos y computación",
    "indumentaria_calzado_marroquineria": "Indumentaria, calzado y marroquinería",
    "jugueteria": "Juguetería",
    "libreria_papeleria": "Librería y papelería",
    "otros": "Otros",
    "patio_comidas": "Patio de comidas, alimentos y kioscos",
    "perfumeria_farmacia": "Perfumería y farmacia",
    "ropa_accesorios_deportivos": "Ropa y accesorios deportivos",
})

_CENTROS_NIVEL = _tabla({
    "ventas_totales_precios_corrientes": (TOTAL, "", MILLONES_DE_PESOS, ()),
    "ventas_totales_precios_constantes": (TOTAL, "", MILLONES_DE_PESOS, (CONSTANTES,)),
    "ventas_totales_precios_constantes_original": (TOTAL, "", INDICE_2017, (CONSTANTES,)),
    "ventas_totales_precios_constantes_desestacionalizada": (TOTAL, "", INDICE_2017,
                                                             (CONSTANTES, DESESTACIONALIZADO)),
    "ventas_totales_precios_constantes_tendencia_ciclo": (TOTAL, "", INDICE_2017, (CONSTANTES, TENDENCIA_CICLO)),
    "ventas_gba_precios_corrientes": (POR_REGION, "Gran Buenos Aires", MILLONES_DE_PESOS, ()),
    "ventas_gba_precios_constantes": (POR_REGION, "Gran Buenos Aires", MILLONES_DE_PESOS, (CONSTANTES,)),
    "ventas_resto_precios_corrientes": (POR_REGION, "Resto del país", MILLONES_DE_PESOS, ()),
    "ventas_resto_precios_constantes": (POR_REGION, "Resto del país", MILLONES_DE_PESOS, (CONSTANTES,)),
})

CINES = "Cines"
_CINES = _tabla({
    "espectadores": ("Espectadores de cine", "Personas"),
    "espectadores_sala": ("Espectadores de cine por sala", "Personas por sala"),
    "salas": ("Salas de cine", "Salas"),
})


def _centros_2017(serie: Serie) -> Clasificacion | None:
    clave = _clave(serie.variable)
    if clave in _CENTROS_NIVEL:
        grupo, region, unidad, medida = _CENTROS_NIVEL[clave]
        return _c(CENTROS_DE_COMPRAS, unir(CENTROS_DE_COMPRAS, TOTAL, region), grupo, "", medida, unidad)
    if clave == "total":
        return _c(CENTROS_DE_COMPRAS, unir(CENTROS_DE_COMPRAS, TOTAL), TOTAL, "", (), "Pesos")
    if clave in _RUBROS_CENTROS:
        return _c(CENTROS_DE_COMPRAS, unir(CENTROS_DE_COMPRAS, _RUBROS_CENTROS[clave]), POR_GRUPO, "", (), "Pesos")
    if clave in _CINES:
        nombre, unidad = _CINES[clave]
        return _c(CENTROS_DE_COMPRAS, unir(CENTROS, nombre), CINES, "", (), unidad)
    return None


# Centros de compras, encuesta anterior (1.12): descripción -> rubro. Miles de pesos.
_CENTROS_2008 = (
    ("amoblamientos", "Amoblamientos, decoración y textiles para el hogar"),
    ("diversion y esparcimiento", "Diversión y esparcimiento"),
    ("electronicos", "Electrónicos, CD, electrodomésticos y computación"),
    ("indumentaria, calzado", "Indumentaria, calzado y marroquinería"),
    ("jugueteria", "Juguetería"),
    ("libreria y papeleria", "Librería y papelería"),
    ("otros articulos", "Otros"),
    ("patio de comidas", "Patio de comidas, alimentos y kioscos"),
    ("perfumeria y farmacia", "Perfumería y farmacia"),
    ("ropa y accesorios deportivos", "Ropa y accesorios deportivos"),
    ("total de las ventas", TOTAL),
)
_CINES_2008 = (
    ("espectadores por sala", "Espectadores de cine por sala", "Personas por sala"),
    ("espectadores de cine", "Espectadores de cine", "Personas"),
    ("salas de cine", "Salas de cine", "Salas"),
)
_REGIONES_CINE = (("ciudad de buenos aires", "Ciudad Autónoma de Buenos Aires"),
                  ("gran buenos aires", "Gran Buenos Aires"))
BASE_2008 = "Base 2008"


def _centros_2008(serie: Serie) -> Clasificacion | None:
    descripcion = plano(serie.descripcion)
    for texto, nombre, unidad in _CINES_2008:
        if texto in descripcion:
            for fragmento, region in _REGIONES_CINE:
                if descripcion.endswith(fragmento):
                    return _c(CENTROS_DE_COMPRAS, unir(CENTROS, nombre, region), SERIES_HISTORICAS, BASE_2008,
                              (), unidad)
            return None
    for texto, rubro in _CENTROS_2008:
        if texto in descripcion:
            return _c(CENTROS_DE_COMPRAS, unir(CENTROS_DE_COMPRAS, rubro), SERIES_HISTORICAS, BASE_2008,
                      (), MILES_DE_PESOS)
    return None


# ---------------------------------------------------------------------------------------------
# Agro, faena pecuaria y pesca (1.19 a 1.21)
# ---------------------------------------------------------------------------------------------

PRODUCCION_AGRICOLA = "Producción agrícola"
FAENA = "Faena pecuaria"
PESCA = "Captura pesquera"

_CULTIVOS = _tabla({
    "algodon": "Algodón", "alpistes": "Alpiste", "alpiste": "Alpiste", "arroz": "Arroz", "avena": "Avena",
    "cebada_cervecera": "Cebada cervecera", "cebada_forrajera": "Cebada forrajera", "cartamo": "Cártamo",
    "centeno": "Centeno", "colza": "Colza", "girasol": "Girasol", "lino": "Lino", "maiz": "Maíz",
    "mani": "Maní", "mijo": "Mijo", "poroto": "Poroto", "sorgo_granifero": "Sorgo granífero",
    "sorgogranifero": "Sorgo granífero", "soja": "Soja", "trigo": "Trigo",
})


def _agro(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for sufijo, medida, unidad in (("_hectareas", ("superficie sembrada",), "Hectáreas"),
                                   ("_toneladadas", (), "Toneladas"),
                                   ("_toneladas", (), "Toneladas")):
        if variable.endswith(sufijo):
            cultivo = _CULTIVOS.get(_clave(variable[: -len(sufijo)]))
            if cultivo is None:
                return None
            return _c(PRODUCCION_AGRICOLA, unir(PRODUCCION_AGRICOLA, cultivo), medida=medida, unidad=unidad)
    return None


_ESPECIES = {"vacunos": "Vacunos", "porcinos": "Porcinos", "ovinos": "Ovinos", "aves": "Aves"}


def _faena(serie: Serie) -> Clasificacion | None:
    especie, _, medida = plano(serie.variable).partition("_")
    if especie not in _ESPECIES:
        return None
    nombre = unir(FAENA, _ESPECIES[especie])
    if medida == "cabezas":
        # Las aves se informan en miles de cabezas (752.562 al año; 57.103 al mes).
        return _c(FAENA, nombre, unidad="Miles de cabezas" if especie == "aves" else "Cabezas")
    if medida == "toneladas":
        return _c(FAENA, nombre, medida="toneladas", unidad="Toneladas")
    return None


_PESCA = {"total_general": "Total", "total_peces": "Peces", "total_crustaceos": "Crustáceos",
          "total_moluscos": "Moluscos"}


def _pesca(serie: Serie) -> Clasificacion | None:
    grupo = _PESCA.get(plano(serie.variable))
    if grupo is None:
        return None
    return _c(PESCA, unir(PESCA, grupo), unidad="Toneladas")


# ---------------------------------------------------------------------------------------------
# Industria automotriz y siderúrgica (1.22 y 1.23)
# ---------------------------------------------------------------------------------------------

AUTOMOTRIZ = "Industria automotriz"
SIDERURGICA = "Industria siderúrgica"
PRODUCCION = "Producción"
EXPORTACIONES = "Exportaciones"
VENTAS_MERCADO_INTERNO = "Ventas en el mercado interno"
VEHICULOS_NACIONALES = "Vehículos nacionales"
VEHICULOS_IMPORTADOS = "Vehículos importados"

_CATEGORIAS = {"automoviles": "Automóviles", "utilitarios": "Utilitarios", "categoria_b": "Categoría B",
               "resto": "Resto", "total": "Total"}


def _automotriz(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for prefijo, flujo, grupo, apertura in (
            ("produccion_", PRODUCCION, PRODUCCION, ""),
            ("exportaciones_", EXPORTACIONES, EXPORTACIONES, ""),
            ("ventas_mercado_interno_vehiculos_nacionales_", "Ventas de vehículos nacionales",
             VENTAS_MERCADO_INTERNO, VEHICULOS_NACIONALES),
            ("ventas_mercado_interno_vehiculos_importados_", "Ventas de vehículos importados",
             VENTAS_MERCADO_INTERNO, VEHICULOS_IMPORTADOS),
            ("ventas_mercado_interno_", VENTAS_MERCADO_INTERNO, VENTAS_MERCADO_INTERNO, TOTAL)):
        if variable.startswith(prefijo):
            categoria = _CATEGORIAS.get(variable[len(prefijo):])
            if categoria is None:
                return None
            return _c(AUTOMOTRIZ, unir(AUTOMOTRIZ, flujo, categoria), grupo, apertura, unidad="Unidades")
    return None


_SIDERURGIA = {
    "acero_crudo": ("Acero crudo y hierro primario", "Acero crudo"),
    "hierro_primario_total": ("Acero crudo y hierro primario", "Hierro primario"),
    "hierro_primario_arrabio": ("Acero crudo y hierro primario", "Arrabio"),
    "hierro_primario_hierro_esponja": ("Acero crudo y hierro primario", "Hierro esponja"),
    "laminados_planos_en_caliente": ("Laminados", "Laminados planos en caliente"),
    "laminados_no_planos_en_caliente": ("Laminados", "Laminados no planos en caliente"),
    "laminados_planos_en_frio": ("Laminados", "Laminados planos en frío"),
}


def _siderurgica(serie: Serie) -> Clasificacion | None:
    definicion = _SIDERURGIA.get(plano(serie.variable))
    if definicion is None:
        return None
    grupo, producto = definicion
    return _c(SIDERURGICA, unir(SIDERURGICA, PRODUCCION, producto), grupo, unidad="Miles de toneladas")


# ---------------------------------------------------------------------------------------------
# Energía (1.24 a 1.29)
# ---------------------------------------------------------------------------------------------

PETROLEO = "Petróleo y derivados"
PROCESAMIENTO = "Procesamiento en refinerías"
VENTAS_Y_DESPACHOS = "Ventas y despachos"
M3_MILES = "Miles de metros cúbicos"

_DERIVADOS = {
    "butano": "Butano", "propano": "Propano", "aerokerosene": "Aerokerosene", "mezclas_ifo": "Mezclas IFO",
    "gasoil": "Gasoil", "fuel_oil": "Fuel oil", "coque": "Coque",
    "nafta_super_mayor93_ron": "Nafta súper (>93 RON)", "nafta_mayor97_ron": "Nafta premium (>97 RON)",
    "nafta_comun_mayor83_ron": "Nafta común (>83 RON)",
}


def _petroleo(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    if variable in ("produccion_petroleo_crudo", "produccion_de_petroleo_crudo"):
        return _c(PETROLEO, unir(PETROLEO, PRODUCCION, "Petróleo crudo"), PRODUCCION, unidad=M3_MILES)
    if variable == "petroleo_crudo_procesado":
        return _c(PETROLEO, unir(PETROLEO, "Procesamiento", "Petróleo crudo"), PROCESAMIENTO, unidad=M3_MILES)
    if variable == "productos_procesados":
        return _c(PETROLEO, unir(PETROLEO, "Procesamiento", "Productos"), PROCESAMIENTO, unidad=M3_MILES)
    if variable == "despachos_asfalto":
        return _c(PETROLEO, unir(PETROLEO, "Despachos", "Asfalto"), VENTAS_Y_DESPACHOS, unidad="Toneladas")
    for prefijo, grupo in (("produccion_", PRODUCCION), ("ventas_", VENTAS_Y_DESPACHOS)):
        if variable.startswith(prefijo) and variable[len(prefijo):] in _DERIVADOS:
            nombre = unir(PETROLEO, "Producción" if grupo == PRODUCCION else "Ventas",
                          _DERIVADOS[variable[len(prefijo):]])
            return _c(PETROLEO, nombre, grupo, unidad=M3_MILES)
    return None


GAS = "Gas natural"
_GAS_USUARIOS = _tabla({
    "residencial": "Consumo residencial", "comercial": "Consumo comercial", "industria": "Consumo industrial",
    "centrales_electricas": "Consumo de centrales eléctricas", "gnc": "Consumo de GNC",
    "entes_oficiales": "Consumo de entes oficiales", "sdb": "Consumo de subdistribuidoras",
})
_GAS_DISTRIBUIDORAS = _tabla({
    "camuzzi_gas_pampeana": "Camuzzi Gas Pampeana", "camuzzi_gas_del_sur": "Camuzzi Gas del Sur",
    "distrib_gas_cuyana_ecogas": "Distribuidora de Gas Cuyana (Ecogas)",
    "distrib_gas_del_centro_ecogas": "Distribuidora de Gas del Centro (Ecogas)",
    "gasnea": "Gasnea", "gasnor": "Gasnor", "gas_natural_fenosa": "Gas Natural Fenosa",
    "litoral_gas": "Litoral Gas", "metrogas": "Metrogas", "redengas": "Redengas",
})
MILLONES_M3 = "Millones de metros cúbicos"


def _gas(serie: Serie) -> Clasificacion | None:
    clave = _clave(serie.variable)
    if clave in (_clave("produccion_gas_natural"), _clave("produccion_de_gas_natural")):
        return _c(GAS, unir(GAS, "Producción"), TOTAL, unidad=MILLONES_M3)
    if clave == "total":
        return _c(GAS, unir(GAS, "Consumo"), TOTAL, unidad=MILLONES_M3)
    if clave in _GAS_USUARIOS:
        return _c(GAS, unir(GAS, _GAS_USUARIOS[clave]), POR_TIPO_DE_USUARIO, unidad=MILLONES_M3)
    if clave in _GAS_DISTRIBUIDORAS:
        return _c(GAS, unir(GAS, "Consumo", _GAS_DISTRIBUIDORAS[clave]), POR_DISTRIBUIDORA, unidad=MILLONES_M3)
    return None


HIDROCARBUROS = "Hidrocarburos por empresa"
_EMPRESAS_PRODUCCION = _tabla({
    "chevron_argentina_srl": "Chevron Argentina",
    "enap_sipetrol_argentina_sa": "ENAP Sipetrol Argentina",
    "pan_american_energy_llc": "Pan American Energy",
    "petrobras_argentina_sa": "Petrobras Argentina",
    "pluspetrol_sa": "Pluspetrol",
    "sinopec_argentina": "Sinopec Argentina Exploration and Production",
    "sinopec_argentina_exploration": "Sinopec Argentina Exploration and Production",
    "tecpetrol_sa": "Tecpetrol",
    "total_austral_sa": "Total Austral",
    "ypf_sa": "YPF",
})
_MARCAS_VENTAS = _tabla({"shell": "Shell", "essoaxion": "Esso/Axion", "oil": "Oil", "petrobras": "Petrobras", "ypf": "YPF"})
PRODUCCION_CRUDO = "Producción de petróleo crudo"
PRODUCCION_GAS = "Producción de gas natural"
VENTAS_NAFTAS = "Ventas de naftas"
VENTAS_GASOIL = "Ventas de gasoil"


def _hidrocarburos(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for prefijo, grupo, tabla, unidad in (
            ("produccion_petroleo_crudo_", PRODUCCION_CRUDO, _EMPRESAS_PRODUCCION, "Metros cúbicos"),
            ("produccion_gas_natural_", PRODUCCION_GAS, _EMPRESAS_PRODUCCION, "Miles de metros cúbicos"),
            ("ventas_naftas_", VENTAS_NAFTAS, _MARCAS_VENTAS, "Metros cúbicos"),
            ("ventas_gasoil_", VENTAS_GASOIL, _MARCAS_VENTAS, "Metros cúbicos")):
        if variable.startswith(prefijo):
            empresa = tabla.get(_clave(variable[len(prefijo):]))
            if empresa is None:
                return None
            return _c(HIDROCARBUROS, unir(HIDROCARBUROS, grupo, empresa), grupo, unidad=unidad)
    return None


POZOS = "Pozos y metros perforados"
_TIPOS_POZO = {"avanzada": "Avanzada", "servicio": "Servicio", "explotacion": "Explotación",
               "exploracion": "Exploración", "total_general": "Total"}


def _pozos(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for prefijo, grupo, unidad in (("metros_perforados_", "Metros perforados", "Metros"),
                                   ("pozos_terminados_", "Pozos terminados", "Pozos")):
        if variable.startswith(prefijo):
            tipo = _TIPOS_POZO.get(variable[len(prefijo):])
            if tipo is None:
                return None
            return _c(POZOS, unir(POZOS, grupo, tipo), grupo, unidad=unidad)
    return None


ELECTRICIDAD = "Electricidad"
POR_TECNOLOGIA = "Por tecnología"
_DISTRIBUIDORAS_ELECTRICAS = _tabla({
    "edelap_sa": "Edelap",
    "edenor_distribuidor": "Edenor",
    "edesal_distribuidor": "Edesal",
    "edestesa_emp_dist__este": "Edestesa",
    "edesur_distribuidor": "Edesur",
    "ede_tucuman": "EDE Tucumán",
    "emp_de_energia_de_la_rioja_sa": "Empresa de Energía de La Rioja",
    "empresa_dis_estero_sa": "Empresa Distribuidora de Electricidad de Santiago del Estero",
    "empresa_jujenia_de_energia_sa": "Empresa Jujeña de Energía",
    "emp_de_energia_de_rio_negro_sa": "Empresa de Energía de Río Negro",
    "emp_dist_energ_atlantica": "Empresa Distribuidora de Energía Atlántica",
    "emp_dist_energia_de_salta": "Empresa Distribuidora de Energía de Salta",
    "emp_electric_misiones_sa": "Empresa Eléctrica de Misiones",
    "energia_de_catamarca_sa": "Energía de Catamarca",
    "energia_de_mendoza_sa": "Energía de Mendoza",
    "energia_de_entre_rios_sa": "Energía de Entre Ríos",
    "energia_san_juan_sa_exedessa": "Energía San Juan (ex Edessa)",
    "epec_distribuidor": "EPEC",
    "epen_distribuidor": "EPEN",
    "epesf_distribuidor": "EPE de Santa Fe",
    "recursos_y_energia_formosa_sa": "Recursos y Energía Formosa",
    "secheep": "SECHEEP",
    "spse_santa_cruz": "SPSE de Santa Cruz",
})
_TECNOLOGIAS = {
    "hidraulica": "Hidráulica", "hidraulica_renovable": "Hidráulica renovable", "nuclear": "Nuclear",
    "biogas": "Biogás", "turbina_a_gas": "Turbina a gas", "eolica": "Eólica", "solar": "Solar",
    "turbovapor": "Turbovapor", "motor_diesel": "Motor diésel", "ciclos_combinados": "Ciclos combinados",
}


def _electricidad(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    clave = _clave(variable)
    if variable == "demanda_total":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Demanda"), TOTAL, unidad="GWh")
    if variable == "demanda_residencial":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Demanda residencial"), POR_TIPO_DE_USUARIO, unidad="GWh")
    if variable == "comercio_e_industria":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Demanda de comercio e industria"), POR_TIPO_DE_USUARIO,
                  unidad="GWh")
    if variable == "grandes_usuarios":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Demanda de grandes usuarios"), POR_TIPO_DE_USUARIO,
                  unidad="GWh")
    if variable == "potencia_maxima":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Potencia máxima"), TOTAL, unidad="MW")
    if variable == "temperatura_promedio":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Temperatura promedio"), TOTAL, unidad="°C")
    if variable == "potencia_instalada_total":
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Potencia instalada"), TOTAL, unidad="MW")
    if variable.startswith("potencia_instalada_") and variable[len("potencia_instalada_"):] in _TECNOLOGIAS:
        tecnologia = _TECNOLOGIAS[variable[len("potencia_instalada_"):]]
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Potencia instalada", tecnologia), POR_TECNOLOGIA, unidad="MW")
    if clave in _DISTRIBUIDORAS_ELECTRICAS:
        return _c(ELECTRICIDAD, unir(ELECTRICIDAD, "Demanda", _DISTRIBUIDORAS_ELECTRICAS[clave]),
                  POR_DISTRIBUIDORA, unidad="GWh")
    return None


BIOCOMBUSTIBLES = "Biocombustibles"
BIODIESEL = "Biodiésel"
BIOETANOL = "Bioetanol"
_BIODIESEL = {
    "biodisel_produccion": ("Producción de biodiésel", "Toneladas"),
    "biodisel_ventas_al_corte": ("Ventas de biodiésel al corte", "Toneladas"),
    "biodisel_otras_ventas_al_mercado_interno": ("Otras ventas de biodiésel al mercado interno", "Toneladas"),
    "biodisel_exportaciones": ("Exportaciones de biodiésel", "Toneladas"),
}
_BIOETANOL = {
    "bioetanol_total_produccion": ("Producción de bioetanol", ""),
    "bioetanol_en_base_a_maiz_produccion": ("Producción de bioetanol", "Maíz"),
    "bioetanol_base_cania_azucar_produccion": ("Producción de bioetanol", "Caña de azúcar"),
    "bioetanol_total_ventas_corte": ("Ventas de bioetanol al corte", ""),
    "bioetanol_base_maiz_ventas_corte": ("Ventas de bioetanol al corte", "Maíz"),
    "bioetanol_base_cania_azucar_ventas_corte": ("Ventas de bioetanol al corte", "Caña de azúcar"),
}


def _biocombustibles(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    if variable in _BIODIESEL:
        nombre, unidad = _BIODIESEL[variable]
        return _c(BIOCOMBUSTIBLES, unir(BIOCOMBUSTIBLES, nombre), BIODIESEL, unidad=unidad)
    if variable in _BIOETANOL:
        nombre, materia_prima = _BIOETANOL[variable]
        return _c(BIOCOMBUSTIBLES, unir(BIOCOMBUSTIBLES, nombre, materia_prima), BIOETANOL,
                  unidad="Metros cúbicos")
    return None


# ---------------------------------------------------------------------------------------------
# Encuestas de confianza (1.30 y 1.32) y ocupación hotelera (1.31)
# ---------------------------------------------------------------------------------------------

CONFIANZA_CONSUMIDOR = "Confianza del consumidor"
CONFIANZA_GOBIERNO = "Confianza en el gobierno"
POR_SUBINDICE = "Por subíndice"
POR_COMPONENTE = "Por componente"

_ICC_SUBINDICES = {
    "desagregacion_subindices_situacion_personal": "Situación personal",
    "desagregacion_subindices_situacion_macro": "Situación macroeconómica",
    "desagregacion_subindices_bienes_durables_inmuebles": "Bienes durables e inmuebles",
}
_ICC_REGIONES = {
    "desagregacion_regiones_capital": "Ciudad Autónoma de Buenos Aires",
    "desagregacion_regiones_gba": "Gran Buenos Aires",
    "desagregacion_regiones_interior": "Interior",
}


def _confianza_consumidor(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    unidad = "Índice (0-100)"
    if variable == "icc_nacional":
        return _c(CONFIANZA_CONSUMIDOR, unir(CONFIANZA_CONSUMIDOR, NIVEL_GENERAL), NIVEL_GENERAL, unidad=unidad)
    if variable in _ICC_SUBINDICES:
        return _c(CONFIANZA_CONSUMIDOR, unir(CONFIANZA_CONSUMIDOR, _ICC_SUBINDICES[variable]), POR_SUBINDICE,
                  unidad=unidad)
    if variable in _ICC_REGIONES:
        return _c(CONFIANZA_CONSUMIDOR, unir(CONFIANZA_CONSUMIDOR, NIVEL_GENERAL, _ICC_REGIONES[variable]),
                  POR_REGION, unidad=unidad)
    return None


_ICG_COMPONENTES = {"capacidad": "Capacidad", "eficiencia": "Eficiencia", "evaluacion_general": "Evaluación general",
                    "honestidad": "Honestidad", "interes_general": "Interés general"}


def _confianza_gobierno(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    unidad = "Índice (0-5)"
    if variable == "icg_nivel_general":
        return _c(CONFIANZA_GOBIERNO, unir(CONFIANZA_GOBIERNO, NIVEL_GENERAL), NIVEL_GENERAL, unidad=unidad)
    if variable in _ICG_COMPONENTES:
        return _c(CONFIANZA_GOBIERNO, unir(CONFIANZA_GOBIERNO, _ICG_COMPONENTES[variable]), POR_COMPONENTE,
                  unidad=unidad)
    return None


OCUPACION_HOTELERA = "Ocupación hotelera"
_EOH = {
    "pernoctaciones_totales": ("Pernoctaciones", "Pernoctaciones", "", "Noches"),
    "pernoctaciones_residentes": ("Pernoctaciones", "Pernoctaciones", "Residentes", "Noches"),
    "pernoctaciones_no_residentes": ("Pernoctaciones", "Pernoctaciones", "No residentes", "Noches"),
    "viajeros_totales": ("Viajeros", "Viajeros", "", "Personas"),
    "viajeros_residentes": ("Viajeros", "Viajeros", "Residentes", "Personas"),
    "viajeros_no_residentes": ("Viajeros", "Viajeros", "No residentes", "Personas"),
}


def _eoh(serie: Serie) -> Clasificacion | None:
    definicion = _EOH.get(plano(serie.variable))
    if definicion is None:
        return None
    grupo, apertura, cobertura, unidad = definicion
    return _c(OCUPACION_HOTELERA, unir(OCUPACION_HOTELERA, apertura, cobertura), grupo, unidad=unidad)


# ---------------------------------------------------------------------------------------------

_POR_HOJA = {
    "1.10 SS PP": _servicios_1993,
    "1.10.1 SSPP 2017": _servicios_2017,
    "1.11 Super": _super_1996,
    "1.11.1 Super": lambda serie: _ventas_autoservicios(serie, SUPERMERCADOS, True),
    "1.11.2 Mayoristas": lambda serie: _ventas_autoservicios(serie, MAYORISTAS, False),
    "1.12 Centros compras": _centros_2008,
    "1.12.1 Centros compras": _centros_2017,
    "1.19 Agro": _agro,
    "1.20 Pecuarios": _faena,
    "1.21 Pesca": _pesca,
    "1.22 Historicos Automotriz": _automotriz,
    "1.23 Historicos Siderurgia": _siderurgica,
    "1.24 Petroleo": _petroleo,
    "1.25 Gas": _gas,
    "1.26 Empresas": _hidrocarburos,
    "1.27 Pozos": _pozos,
    "1.28 Electricidad": _electricidad,
    "1.29 Biocombustibles": _biocombustibles,
    "1.30 ICC - UTDT": _confianza_consumidor,
    "1.31 EOH": _eoh,
    "1.32 ICG - UTDT": _confianza_gobierno,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.archivo != ARCHIVO:
        return None
    regla = _POR_HOJA.get(serie.hoja)
    return regla(serie) if regla else None
