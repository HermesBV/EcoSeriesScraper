"""Reglas de mercados financieros y economía internacional.

Alcance (coincidencia exacta de fuente, archivo y hoja):

* ``ambito-riesgo-pais`` (``history.csv``): el EMBI de Argentina.
* ``datos.gob.ar`` ``finanzas.xlsx``: renta variable, mercado cambiario, FCI, financiamiento
  privado, renta fija histórica e índices sectoriales.
* ``datos.gob.ar`` ``internacional.xlsx``: ponderaciones del comercio, Brasil, IED, cuenta
  corriente por país, tasas de interés y bonos del Tesoro de Estados Unidos.

Árbol resultante:

    Mercados financieros
      Riesgo país                                   una serie
      Índices bursátiles > Generales | Por sector
      Dólar futuro                                  precio y posiciones abiertas por plazo
      Volumen operado en el mercado de cambios
      Fondos comunes de inversión                   patrimonio neto por tipo de fondo
      Financiamiento en el mercado de capitales > <instrumento> > <apertura>
      Títulos públicos > <tipo de bono>             cotización, duración, vida y rendimiento
    Economía internacional
      Brasil > Producto interno bruto | Actividad económica | Remuneración media real | Tipo de cambio
      Tasas de interés internacionales
      Rendimiento de los bonos del Tesoro de Estados Unidos > Nominales | Indexados a la inflación
      Inversión extranjera directa en el mundo > <continente>
      Cuenta corriente por país > <continente>
    Sector externo
      Tipo de cambio nominal                        el tipo de cambio spot de la hoja 2 (A 3500, MAE, BNA, ADR)
      Tipo de cambio real > Ponderadores            la hoja «15 Ponderaciones Comercio»

Decisiones que los datos obligaron a tomar:

* Las «Ponderaciones Comercio» son, serie por serie y fecha por fecha, las mismas que el BCRA
  publica como «Ponderadores» del ITCRM (mismos 13 socios, suman 100 y coinciden desde enero
  de 1997 hasta el último dato común), así que van al indicador del tipo de cambio real y no al
  comercio exterior.
* Varios cuadros publican porcentajes como fracciones (0,035 = 3,5%): las participaciones de
  las ON y de los fideicomisos, las tasas, el rendimiento de los bonos, la IED y la cuenta
  corriente en proporción del PIB. Esas series llevan la unidad «Proporción (1 = 100%)» y el
  nombre «(% del PIB)», «(% del total)» o «(tasa nominal anual)».
* Las participaciones por sector de las ON y por activo subyacente de los fideicomisos suman
  1 cada mes: son participaciones y no montos, aunque la descripción diga «Emisión de…».
* Los títulos públicos se identifican por su código (el año del nombre de la fuente es
  erróneo en varios: «Bonar 2025 - AA19» vence en 2019) y se agrupan por tipo de bono, que es
  lo que la fuente dice de cada uno.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

FUENTE_EMBI = "ambito-riesgo-pais"
FUENTE = "datos.gob.ar"

MERCADOS = "Mercados financieros"
INTERNACIONAL = "Economía internacional"
EXTERNO = "Sector externo"

PROPORCION = "Proporción (1 = 100%)"
DOLARES = "Dólares"
MILLONES_DOLARES = "Millones de dólares"
MILLONES_PESOS = "Millones de pesos"
PESOS_POR_DOLAR = "Pesos por US$"
PORCENTAJE = "%"
ANIOS = "Años"
CONTRATOS = "Contratos"
EMISIONES = "Emisiones"

PIB_PCT = "% del PIB"
CANTIDAD = "cantidad de emisiones"
CONSTANTES = "precios constantes"
DESESTACIONALIZADO = "desestacionalizado"


def _fila(serie: Serie, archivo: str) -> str:
    """Hoja normalizada de las series de ``datos.gob.ar`` del archivo; '' si no es de ese archivo."""
    if serie.fuente != FUENTE or serie.archivo != archivo:
        return ""
    return serie.hoja


# =================================================================================================
# Riesgo país
# =================================================================================================
RIESGO_PAIS = "Riesgo país"


def _riesgo_pais(serie: Serie) -> Clasificacion | None:
    if serie.archivo != "history.csv" or serie.hoja != "Histórico":
        return None
    return Clasificacion(tema=MERCADOS, indicador=RIESGO_PAIS, nombre=unir(RIESGO_PAIS, "Argentina"),
                         unidad="Puntos básicos")


# =================================================================================================
# Hoja 1 y hoja 6: índices bursátiles
# =================================================================================================
INDICES = "Índices bursátiles"
GENERALES = "Generales"
POR_SECTOR = "Por sector"

_INDICES_GENERALES = {
    "merval": "Merval",
    "merval_25": "Merval 25",
    "merval_arg": "Merval Argentina",
    "burcap": "Burcap",
    "bolsa_g": "Bolsa G",
}

_INDICES_SECTORIALES = {
    "ind_burs_agrop": "Agropecuario",
    "ind_burs_alim_beb": "Alimentos y bebidas",
    "ind_burs_bs_raices": "Bienes raíces",
    "ind_burs_comer": "Comercio",
    "ind_burs_const": "Construcción",
    "ind_burs_energ": "Energía",
    "ind_burs_fciero": "Financiero",
    "ind_burs_metalmec": "Metalmecánica",
    "ind_burs_min_no_met": "Minerales no metálicos",
    "ind_burs_qui_otros": "Química y otros",
    "ind_burs_side": "Siderurgia",
    "ind_burs_sspp": "Servicios públicos",
    "ind_burs_telecom": "Telecomunicaciones",
}


def _renta_variable(serie: Serie) -> Clasificacion | None:
    nombre = _INDICES_GENERALES.get(serie.variable)
    if nombre is None:
        return None
    return Clasificacion(tema=MERCADOS, indicador=INDICES, cobertura=GENERALES,
                         nombre=unir("Índice bursátil", nombre))


def _indices_sectoriales(serie: Serie) -> Clasificacion | None:
    sector = _INDICES_SECTORIALES.get(serie.variable)
    if sector is None:
        return None
    return Clasificacion(tema=MERCADOS, indicador=INDICES, cobertura=POR_SECTOR,
                         nombre=unir("Índice bursátil sectorial", sector))


# =================================================================================================
# Hoja 2: mercado cambiario
# =================================================================================================
DOLAR_FUTURO = "Dólar futuro"
VOLUMEN_CAMBIOS = "Volumen operado en el mercado de cambios"
TIPO_DE_CAMBIO = "Tipo de cambio nominal"

_PLAZOS = {"1m": "1 mes", "2m": "2 meses", "3m": "3 meses", "4m": "4 meses", "5m": "5 meses",
           "6m": "6 meses", "12m": "12 meses"}

# Tipo de cambio spot. Se escribe con el nombre que usa el indicador en el resto del árbol
# («Tipo de cambio · Oficial mayorista (A 3500)»).
_TIPOS_DE_CAMBIO = {
    "tipo_cambio_a3500": "Oficial mayorista (A 3500)",
    "tipo_cambio_mae": "Mayorista (MAE)",
    "tipo_cambio_bna_vendedor": "Banco Nación (vendedor)",
    "tipo_cambio_implicito_en_adrs": "Implícito en ADR",
}


def _mercado_cambiario(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    tipo = _TIPOS_DE_CAMBIO.get(variable)
    if tipo is not None:
        return Clasificacion(tema=EXTERNO, indicador=TIPO_DE_CAMBIO, nombre=unir("Tipo de cambio", tipo),
                             unidad=PESOS_POR_DOLAR)
    if variable == "volumen_mae":
        return Clasificacion(tema=MERCADOS, indicador=VOLUMEN_CAMBIOS,
                             nombre=unir(VOLUMEN_CAMBIOS, "MAE"))
    precio = re.fullmatch(r"futuro_rofex_usd_?(\d+m)", variable)
    if precio and precio.group(1) in _PLAZOS:
        return Clasificacion(tema=MERCADOS, indicador=DOLAR_FUTURO,
                             nombre=unir(DOLAR_FUTURO, _PLAZOS[precio.group(1)]), unidad=PESOS_POR_DOLAR)
    abierto = re.fullmatch(r"interes_abierto_(\d+m)", variable)
    if abierto and abierto.group(1) in _PLAZOS:
        return Clasificacion(tema=MERCADOS, indicador=DOLAR_FUTURO,
                             nombre=con_medida(unir(DOLAR_FUTURO, _PLAZOS[abierto.group(1)]), "interés abierto"),
                             unidad=CONTRATOS)
    return None


# =================================================================================================
# Hoja 3: fondos comunes de inversión
# =================================================================================================
FCI = "Fondos comunes de inversión"
_TIPOS_FCI = {
    "fci_patrimonio_neto_total": "Total",
    "fci_mercado_dinero": "Mercado de dinero",
    "fci_renta_fija": "Renta fija",
    "fci_renta_variable": "Renta variable",
    "fci_renta_mixta": "Renta mixta",
    "fci_retorno_total": "Retorno total",
    "fci_infraestructura": "Infraestructura",
    "fci_fondos_pymes": "PyMEs",
    "fci_cerrados": "Cerrados",
    "fci_asg": "ASG",
}


def _fci(serie: Serie) -> Clasificacion | None:
    tipo = _TIPOS_FCI.get(serie.variable)
    if tipo is None:
        return None
    # El patrimonio está en millones de pesos: el total de diciembre de 2022 es 6,96 billones.
    return Clasificacion(tema=MERCADOS, indicador=FCI, nombre=con_medida(unir(FCI, tipo), "patrimonio neto"),
                         unidad=MILLONES_PESOS)


# =================================================================================================
# Hoja 4: financiamiento en el mercado de capitales
# =================================================================================================
FINANCIAMIENTO = "Financiamiento en el mercado de capitales"

TOTAL = "Total"
ON = "Obligaciones negociables"
FF = "Fideicomisos financieros"
ACCIONES = "Acciones"
CPD = "Cheques de pago diferido"
PAGARES = "Pagarés"

POR_TAMANIO = "Por tamaño de empresa"
POR_ACTIVIDAD = "Por sector"
POR_ACTIVO = "Por activo subyacente"
TASAS = "Tasas de interés"

GRANDES = "Grandes empresas"
PYMES = "PyMEs"

# Montos emitidos (en dólares) y cantidad de emisiones: (instrumento, tamaño de empresa).
_EMISIONES = {
    "financiamiento_mercado_capitales_total_general": (TOTAL, ""),
    "financiamiento_mercado_capitales_ge_total": (TOTAL, GRANDES),
    "financiamiento_mercado_capitales_pymes_total": (TOTAL, PYMES),
    "financiamiento_mercado_capitales_ge_on_vcp": (ON, GRANDES),
    "financiamiento_mercado_capitales_pymes_on_vcp": (ON, PYMES),
    "financiamiento_mercado_capitales_ge_ff": (FF, GRANDES),
    "financiamiento_mercado_capitales_pymes_ff": (FF, PYMES),
    "financiamiento_mercado_capitales_ge_acciones": (ACCIONES, GRANDES),
    "financiamiento_mercado_capitales_pymes_acciones": (ACCIONES, PYMES),
    "financiamiento_mercado_capitales_ge_cdcp": (CPD, GRANDES),
    "financiamiento_mercado_capitales_pymes_cdcp": (CPD, PYMES),
    "financiamiento_mercado_capitales_pymes_pagares": (PAGARES, PYMES),
}
_CANTIDADES = {
    "emisiones_totales_ge_pymes": (TOTAL, ""),
    "emisiones_ge_total": (TOTAL, GRANDES),
    "emisiones_pymes_total": (TOTAL, PYMES),
    "emisiones_ge_on": (ON, GRANDES),
    "emisiones_pymes_on": (ON, PYMES),
    "emisiones_ge_ff": (FF, GRANDES),
    "emisiones_pymes_ff": (FF, PYMES),
    "emisiones_ge_acc": (ACCIONES, GRANDES),
    "emisiones_pymes_acc": (ACCIONES, PYMES),
    "emisiones_ge_cpd": (CPD, GRANDES),
    "emisiones_pymes_cpd": (CPD, PYMES),
    "emisiones_pymes_pag": (PAGARES, PYMES),
}

# Participación de cada sector en las obligaciones negociables emitidas en el mes.
_ON_POR_SECTOR = {
    "agricultura_ganaderia": "Agricultura y ganadería",
    "alimentos_bebidas": "Alimentos y bebidas",
    "act_financieras_seguros": "Finanzas y seguros",
    "actividades_inmobiliarias": "Actividades inmobiliarias",
    "bancos": "Bancos",
    "calzado_indumentaria": "Calzado e indumentaria",
    "comercio": "Comercio",
    "construccion": "Construcción",
    "distribucion_de_gas": "Distribución de gas",
    "distribucion_energia_electrica": "Distribución de energía eléctrica",
    "editoriales_imprentas": "Editoriales e imprentas",
    "equipamiento_hogar": "Equipamiento del hogar",
    "grupos_concesionarios": "Grupos concesionarios",
    "generacion_energia_electrica": "Generación de energía eléctrica",
    "holdings": "Holdings",
    "industria_metalurgica_siderurgica": "Industria metalúrgica y siderúrgica",
    "industria_petroleo_gas": "Industria del petróleo y gas",
    "industria_farmaceutica": "Industria farmacéutica",
    "industria_quimica_petroquimica": "Industria química y petroquímica",
    "industria_automotriz": "Industria automotriz",
    "manufacturas_origen_industrial": "Manufacturas de origen industrial",
    "manufacturas_origen_agropecuario": "Manufacturas de origen agropecuario",
    "manufacturas_actividades_empresariales": "Otras actividades empresariales",
    "manufacturas_papel_celulosa": "Papel y celulosa",
    "manufacturas_servicios_informaticos": "Servicios informáticos",
    "manufacturas_telecomunicaciones": "Telecomunicaciones",
    "manufacturas_tecnologia": "Tecnología",
    "manufacturas_transporte_gas": "Transporte de gas",
    "manufacturas_transporte_energia_electrica": "Transporte de energía eléctrica",
    "manufacturas_transporte_pasajeros_peajes": "Transporte de pasajeros y peajes",
}

# Participación de cada activo subyacente en los fideicomisos financieros emitidos en el mes.
_FF_POR_ACTIVO = {
    "eff_agropecuarios": "Agropecuarios",
    "eff_creditos_comerciales": "Créditos comerciales",
    "eff_estructurados": "Estructurados",
    "eff_innovacion_financiera": "Innovación financiera",
    "eff_financiacion_exportaciones": "Financiación de exportaciones",
    "eff_flujos_futuros": "Flujos futuros",
    "eff_inmobiliario_hipotecario": "Inmobiliarios e hipotecarios",
    "eff_leasing": "Leasing",
    "eff_obras_publicas": "Obras públicas",
    "eff_prendarios": "Prendarios",
    "eff_prestamos_personales_consumo": "Préstamos personales y de consumo",
    "eff_tarjetas_credito": "Tarjetas de crédito",
}

# Tasa de interés nominal anual: (instrumento, nombre).
_TASAS = {
    "tasa_interes_vdf_senior": (FF, "Deuda fiduciaria senior"),
    "tasa_interes_cpda": (CPD, "Avalados"),
    "tasa_interes_cpdp": (CPD, "Patrocinados"),
}


def _financiamiento_privado(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if variable in _EMISIONES or variable in _CANTIDADES:
        instrumento, tamanio = (_EMISIONES.get(variable) or _CANTIDADES[variable])
        cantidad = variable in _CANTIDADES
        # Total, acciones y pagarés se abren sólo por tamaño de empresa: sin nivel intermedio.
        abre_por_tamanio = instrumento in (ON, FF, CPD)
        nombre = unir(FINANCIAMIENTO, instrumento, tamanio)
        return Clasificacion(
            tema=MERCADOS, indicador=FINANCIAMIENTO, cobertura=instrumento,
            apertura=POR_TAMANIO if abre_por_tamanio else "",
            nombre=con_medida(nombre, CANTIDAD if cantidad else ""),
            unidad=EMISIONES if cantidad else DOLARES)
    if variable in _ON_POR_SECTOR:
        return Clasificacion(
            tema=MERCADOS, indicador=FINANCIAMIENTO, cobertura=ON, apertura=POR_ACTIVIDAD,
            nombre=con_medida(unir(FINANCIAMIENTO, ON, _ON_POR_SECTOR[variable]), "% del total"),
            unidad=PROPORCION)
    if variable in _FF_POR_ACTIVO:
        return Clasificacion(
            tema=MERCADOS, indicador=FINANCIAMIENTO, cobertura=FF, apertura=POR_ACTIVO,
            nombre=con_medida(unir(FINANCIAMIENTO, FF, _FF_POR_ACTIVO[variable]), "% del total"),
            unidad=PROPORCION)
    if variable in _TASAS:
        instrumento, rotulo = _TASAS[variable]
        return Clasificacion(
            tema=MERCADOS, indicador=FINANCIAMIENTO, cobertura=instrumento, apertura=TASAS,
            nombre=con_medida(unir(FINANCIAMIENTO, instrumento, rotulo), "tasa nominal anual"),
            unidad=PROPORCION)
    return None


# =================================================================================================
# Hoja 5.1: títulos públicos de deuda (Merval)
# =================================================================================================
TITULOS = "Títulos públicos"

BONAR = "Bonar y Bonad"
TESORO = "Bonos del Tesoro Nacional"
CER = "Bonos ajustables por CER"
GATILLO = "Bonos con cláusula gatillo"
DUALES = "Bonos duales"
SOBERANOS_USD = "Bonos soberanos en dólares"
CONSOLIDACION = "Bonos de consolidación y garantizados"
CUPONES_PIB = "Cupones vinculados al PIB"

# Código → (tipo de bono, cómo se llama el bono). El nombre lleva el código y no el año: el año
# que trae la fuente es erróneo en varios bonos y el código lo contiene.
_BONOS = {
    "a2e2": (BONAR, "Bonar A2E2"), "a2e3": (BONAR, "Bonar A2E3"), "a2e7": (BONAR, "Bonar A2E7"),
    "a2e8": (BONAR, "Bonar A2E8"), "aa19": (BONAR, "Bonar AA19"), "aa21": (BONAR, "Bonar AA21"),
    "aa25": (BONAR, "Bonar AA25"), "aa26": (BONAR, "Bonar AA26"), "aa37": (BONAR, "Bonar AA37"),
    "am20": (BONAR, "Bonar AM20"), "amx8": (BONAR, "Bonar AMX8"), "amx9": (BONAR, "Bonar AMX9"),
    "an18": (BONAR, "Bonar AN18"), "ao17": (BONAR, "Bonar AO17"), "ao20": (BONAR, "Bonar AO20"),
    "am18": (BONAR, "Bonad AM18"), "as17": (BONAR, "Bonad AS17"), "tv21": (BONAR, "Bonad TV21"),
    "ac17": (BONAR, "Bono de la Nación Argentina AC17"),
    "ay24": (BONAR, "Bono de la Nación Argentina AY24"),
    "pr15": (TESORO, "Bono del Tesoro Nacional PR15"), "tb21": (TESORO, "Bono del Tesoro Nacional TB21"),
    "tj20": (TESORO, "Bono del Tesoro Nacional TJ20"), "tm18": (TESORO, "Bono del Tesoro Nacional TM18"),
    "tn20": (TESORO, "Bono del Tesoro Nacional TN20"), "to21": (TESORO, "Bono del Tesoro Nacional TO21"),
    "to23": (TESORO, "Bono del Tesoro Nacional TO23"), "to26": (TESORO, "Bono del Tesoro Nacional TO26"),
    "ts18": (TESORO, "Bono del Tesoro Nacional TS18"),
    "tc20": (CER, "Boncer TC20"), "tc21": (CER, "Boncer TC21"), "tc23": (CER, "Boncer TC23"),
    "tc25p": (CER, "Boncer TC25P"), "tx21": (CER, "Boncer TX21"),
    "cuap": (CER, "Bono ajustable por CER CUAP"), "dicp": (CER, "Bono ajustable por CER DICP"),
    "parp": (CER, "Bono ajustable por CER PARP"),
    "a2m2": (GATILLO, "Bono con cláusula gatillo A2M2"), "af19": (GATILLO, "Bono con cláusula gatillo AF19"),
    "td21": (GATILLO, "Bono con cláusula gatillo TD21"),
    "a2j9": (DUALES, "Bono dual en pesos A2J9"), "a2j9d": (DUALES, "Bono dual en dólares A2J9D"),
    "af20": (DUALES, "Bono dual en pesos AF20"), "af20d": (DUALES, "Bono dual en dólares AF20D"),
    "dica": (SOBERANOS_USD, "Discount ley argentina DICA"), "dicy": (SOBERANOS_USD, "Discount DICY"),
    "diy0": (SOBERANOS_USD, "Bono soberano ley Nueva York DIY0"),
    "para": (SOBERANOS_USD, "Par PARA"), "pary": (SOBERANOS_USD, "Bono soberano en dólares PARY"),
    "aa46": (SOBERANOS_USD, "Bono soberano en dólares AA46"),
    "aa48": (SOBERANOS_USD, "Bono soberano en dólares AA48"),
    "pr13": (CONSOLIDACION, "Bono de consolidación PR13"), "nf18": (CONSOLIDACION, "Bogar NF18"),
    "no20": (CONSOLIDACION, "Bogar NO20"), "aa22": (CONSOLIDACION, "Bocan AA22"),
    "tvpa": (CUPONES_PIB, "Cupón PIB TVPA"), "tvpe": (CUPONES_PIB, "Cupón PIB TVPE"),
    "tvpp": (CUPONES_PIB, "Cupón PIB TVPP"), "tvpy": (CUPONES_PIB, "Cupón PIB TVPY"),
}

# Sufijo de la variable → (medida, unidad). La fuente escribe a veces «dur», «dm en años»…
_MEDIDAS_BONOS = {
    "cotizacion": ("cotización", ""), "cot_anios": ("cotización", ""),
    "dur": ("duración", ANIOS), "dm_en_anios": ("duración", ANIOS),
    "ppv_en_anios": ("vida promedio ponderada", ANIOS), "ppv_anios": ("vida promedio ponderada", ANIOS),
    "yield": ("rendimiento", PROPORCION), "yield_anios": ("rendimiento", PROPORCION),
}
_VARIABLE_BONO = re.compile(r"([a-z0-9]+)_(cotizacion|cot_anios|dur|dm_en_anios|ppv_en_anios|ppv_anios|yield|yield_anios)")


def _titulos_publicos(serie: Serie) -> Clasificacion | None:
    partes = _VARIABLE_BONO.fullmatch(serie.variable)
    if partes is None or partes.group(1) not in _BONOS:
        return None
    tipo, bono = _BONOS[partes.group(1)]
    medida, unidad = _MEDIDAS_BONOS[partes.group(2)]
    return Clasificacion(tema=MERCADOS, indicador=TITULOS, cobertura=tipo, nombre=con_medida(bono, medida),
                         unidad=unidad)


# =================================================================================================
# Hojas 16 a 19: Brasil
# =================================================================================================
BRASIL = "Brasil"
PIB_BRASIL = "Producto interno bruto"
ACTIVIDAD_BRASIL = "Actividad económica"
REMUNERACION = "Remuneración media real"
CAMBIO_BRASIL = "Tipo de cambio"

# Cuadro 16: PIB trimestral a precios constantes de 1995 (el PIB suma consumo, gasto público,
# inversión y exportaciones menos importaciones, y el valor del primer trimestre de 1995,
# 157 mil millones de reales, sólo cierra con precios constantes de ese año).
_PIB_BRASIL = {
    "pib_pib": "",
    "pib_agropecuario": "Agropecuario",
    "pib_industria": "Industria",
    "pib_servicios": "Servicios",
    "pib_consumo": "Consumo",
    "pib_gasto_publico": "Gasto público",
    "pib_inversion_privada": "Inversión privada",
    "pib_exportaciones": "Exportaciones",
    "pib_importaciones": "Importaciones",
}

# Cuadro 17: (nombre, medida, unidad).
_ACTIVIDAD_BRASIL = {
    "indice_actividad_ibc_br_original": ("IBC-Br", "", ""),
    "indice_actividad_ibc_br_desest": ("IBC-Br", DESESTACIONALIZADO, ""),
    "produccion_industrial_original": ("Producción industrial", "", ""),
    "produccion_industrial_desestacionalizado": ("Producción industrial", DESESTACIONALIZADO, ""),
    "produccion_automotriz_unidades": ("Producción automotriz", "", "Unidades"),
}

_REMUNERACION_BRASIL = {
    "remuneracion_media_real_ocupados": "Ocupados",
    "remuneracion_media_real_todos_los_trabajos": "Todos los trabajos",
}


def _brasil_pib(serie: Serie) -> Clasificacion | None:
    if serie.variable not in _PIB_BRASIL:
        return None
    return Clasificacion(
        tema=INTERNACIONAL, indicador=BRASIL, cobertura=PIB_BRASIL,
        nombre=con_medida(unir("PIB", _PIB_BRASIL[serie.variable], "Brasil"), CONSTANTES),
        unidad="Millones de reales de 1995")


def _brasil_actividad(serie: Serie) -> Clasificacion | None:
    if serie.variable not in _ACTIVIDAD_BRASIL:
        return None
    nombre, medida, unidad = _ACTIVIDAD_BRASIL[serie.variable]
    return Clasificacion(tema=INTERNACIONAL, indicador=BRASIL, cobertura=ACTIVIDAD_BRASIL,
                         nombre=con_medida(unir(nombre, "Brasil"), medida), unidad=unidad)


def _brasil_remuneracion(serie: Serie) -> Clasificacion | None:
    if serie.variable not in _REMUNERACION_BRASIL:
        return None
    return Clasificacion(tema=INTERNACIONAL, indicador=BRASIL, cobertura=REMUNERACION,
                         nombre=unir(REMUNERACION, _REMUNERACION_BRASIL[serie.variable], "Brasil"))


def _brasil_tipo_de_cambio(serie: Serie) -> Clasificacion | None:
    if serie.variable != "tipo_de_cambio_real":
        return None
    return Clasificacion(tema=INTERNACIONAL, indicador=BRASIL, cobertura=CAMBIO_BRASIL,
                         nombre=unir("Tipo de cambio real", "Brasil"))


# =================================================================================================
# Hoja 5: tasas de interés de referencia; hoja 6: bonos del Tesoro de Estados Unidos
# =================================================================================================
TASAS_INTERNACIONALES = "Tasas de interés internacionales"
TASA_REFERENCIA = "Tasa de interés de referencia"

_TASAS_INTERNACIONALES = {
    "ff_eeuu_tasa": "Estados Unidos (Fed funds)",
    "mro_zonaeuro_tasa": "Zona Euro (MRO)",
    "overnight_interest_rate_japon_tasa": "Japón (tasa overnight)",
    "repo_inglaterra_tasa": "Reino Unido (tasa repo)",
    "selic_brasil_tasa": "Brasil (Selic)",
}


def _tasas_internacionales(serie: Serie) -> Clasificacion | None:
    pais = _TASAS_INTERNACIONALES.get(serie.variable)
    if pais is None:
        return None
    return Clasificacion(tema=INTERNACIONAL, indicador=TASAS_INTERNACIONALES,
                         nombre=unir(TASA_REFERENCIA, pais), unidad=PORCENTAJE)


TESORO_EEUU = "Rendimiento de los bonos del Tesoro de Estados Unidos"
RENDIMIENTO_TESORO = "Rendimiento del Tesoro de Estados Unidos"
NOMINALES = "Nominales"
INDEXADOS = "Indexados a la inflación"

_PLAZO_TESORO = re.compile(r"titulos_us_(indexado_)?(\d+)_(anios|meses)")


def _bonos_eeuu(serie: Serie) -> Clasificacion | None:
    partes = _PLAZO_TESORO.fullmatch(serie.variable)
    if partes is None:
        return None
    indexado, cantidad, unidad_plazo = partes.groups()
    plazo = f"{cantidad} {'año' if cantidad == '1' else 'años'}" if unidad_plazo == "anios" else f"{cantidad} meses"
    nombre = unir(RENDIMIENTO_TESORO, plazo)
    return Clasificacion(
        tema=INTERNACIONAL, indicador=TESORO_EEUU, cobertura=INDEXADOS if indexado else NOMINALES,
        nombre=con_medida(nombre, "indexado a la inflación" if indexado else ""), unidad=PORCENTAJE)


# =================================================================================================
# Hojas 20 y 4.b: inversión extranjera directa y cuenta corriente por país
# =================================================================================================
IED = "Inversión extranjera directa en el mundo"
IED_NOMBRE = "Inversión extranjera directa"
CUENTA_CORRIENTE = "Cuenta corriente por país"
CUENTA_CORRIENTE_NOMBRE = "Cuenta corriente"

AFRICA, AMERICA, ASIA, EUROPA, OCEANIA = "África", "América", "Asia", "Europa", "Oceanía"

# Rótulo de la fuente → (nombre oficial en castellano, continente). Nombres de las Naciones
# Unidas en su forma corta; continentes según el esquema geográfico de la ONU (M49), en el que
# Chipre, Georgia, Armenia, Azerbaiyán y Turquía son de Asia y Rusia es de Europa.
_PAISES = {
    # África
    "Angola": ("Angola", AFRICA), "Argelia": ("Argelia", AFRICA), "Benín": ("Benín", AFRICA),
    "Botswana": ("Botsuana", AFRICA), "Burkina Faso": ("Burkina Faso", AFRICA),
    "Burundi": ("Burundi", AFRICA), "Cabo Verde": ("Cabo Verde", AFRICA),
    "Camerún": ("Camerún", AFRICA), "Chad": ("Chad", AFRICA), "Comoras": ("Comoras", AFRICA),
    "Congo": ("Congo", AFRICA), "Côte d'Ivoire": ("Côte d'Ivoire", AFRICA),
    "Djibouti": ("Yibuti", AFRICA), "Egipto": ("Egipto", AFRICA), "Eritrea": ("Eritrea", AFRICA),
    "Swazilandia": ("Eswatini", AFRICA), "Etiopía": ("Etiopía", AFRICA), "Gabón": ("Gabón", AFRICA),
    "Gambia": ("Gambia", AFRICA), "Ghana": ("Ghana", AFRICA), "Guinea": ("Guinea", AFRICA),
    "Guinea-Bissau": ("Guinea-Bissau", AFRICA), "Guinea Ecuatorial": ("Guinea Ecuatorial", AFRICA),
    "Kenia": ("Kenia", AFRICA), "Lesoto": ("Lesoto", AFRICA), "Liberia": ("Liberia", AFRICA),
    "Libia": ("Libia", AFRICA), "Madagascar": ("Madagascar", AFRICA), "Malawi": ("Malawi", AFRICA),
    "Mali": ("Malí", AFRICA), "Marruecos": ("Marruecos", AFRICA), "Mauricio": ("Mauricio", AFRICA),
    "Mauritania": ("Mauritania", AFRICA), "Mozambique": ("Mozambique", AFRICA),
    "Namibia": ("Namibia", AFRICA), "Níger": ("Níger", AFRICA), "Nigeria": ("Nigeria", AFRICA),
    "República Centroafricana": ("República Centroafricana", AFRICA),
    "Rep. Dem. Del Congo": ("República Democrática del Congo", AFRICA),
    "República Unida de Tanzania": ("República Unida de Tanzania", AFRICA),
    "Ruanda": ("Ruanda", AFRICA), "Santo Tomé y Príncipe": ("Santo Tomé y Príncipe", AFRICA),
    "Senegal": ("Senegal", AFRICA), "Seychelles": ("Seychelles", AFRICA),
    "Sierra Leona": ("Sierra Leona", AFRICA), "Sudáfrica": ("Sudáfrica", AFRICA),
    "Sudán del Sur": ("Sudán del Sur", AFRICA), "Túnez": ("Túnez", AFRICA),
    "Uganda": ("Uganda", AFRICA), "Zambia": ("Zambia", AFRICA), "Zimbabue": ("Zimbabue", AFRICA),
    # América
    "Anguila": ("Anguila", AMERICA), "Antigua y Barbuda": ("Antigua y Barbuda", AMERICA),
    "Antillas Holandesas": ("Antillas Neerlandesas", AMERICA), "Argentina": ("Argentina", AMERICA),
    "Aruba": ("Aruba", AMERICA), "Bahamas": ("Bahamas", AMERICA), "Barbados": ("Barbados", AMERICA),
    "Belice": ("Belice", AMERICA), "islas Bermudas": ("Bermudas", AMERICA),
    "Bolivia": ("Bolivia", AMERICA), "Brasil": ("Brasil", AMERICA), "Canadá": ("Canadá", AMERICA),
    "Chile": ("Chile", AMERICA), "Colombia": ("Colombia", AMERICA), "Costa Rica": ("Costa Rica", AMERICA),
    "Curazao": ("Curazao", AMERICA), "Dominica": ("Dominica", AMERICA), "Ecuador": ("Ecuador", AMERICA),
    "El Salvador": ("El Salvador", AMERICA), "Estados Unidos": ("Estados Unidos", AMERICA),
    "Granada": ("Granada", AMERICA), "Guatemala": ("Guatemala", AMERICA), "Guayana": ("Guyana", AMERICA),
    "Haití": ("Haití", AMERICA), "Honduras": ("Honduras", AMERICA),
    "Islas Caimán": ("Islas Caimán", AMERICA),
    "Islas Vírgenes británicas": ("Islas Vírgenes Británicas", AMERICA),
    "Jamaica": ("Jamaica", AMERICA), "Méjico": ("México", AMERICA), "Montserrat": ("Montserrat", AMERICA),
    "Nicaragua": ("Nicaragua", AMERICA), "Panamá": ("Panamá", AMERICA), "Paraguay": ("Paraguay", AMERICA),
    "Perú": ("Perú", AMERICA), "República Dominicana": ("República Dominicana", AMERICA),
    "San Cristóbal y Nieves": ("San Cristóbal y Nieves", AMERICA),
    "San Vicente y las Granadinas": ("San Vicente y las Granadinas", AMERICA),
    "Santa Lucía": ("Santa Lucía", AMERICA), "Sint Maarten": ("Sint Maarten", AMERICA),
    "Suriname": ("Suriname", AMERICA), "Trinidad y Tobago": ("Trinidad y Tobago", AMERICA),
    "Uruguay": ("Uruguay", AMERICA), "Venezuela": ("Venezuela", AMERICA),
    # Asia
    "Afganistán": ("Afganistán", ASIA), "Arabia Saudita": ("Arabia Saudita", ASIA),
    "Armenia": ("Armenia", ASIA), "Azerbaiyán": ("Azerbaiyán", ASIA), "Bahrein": ("Baréin", ASIA),
    "Bangladesh": ("Bangladés", ASIA), "Bhután": ("Bután", ASIA),
    "Brunei Darussalam": ("Brunei Darussalam", ASIA), "Camboya": ("Camboya", ASIA),
    "China": ("China", ASIA), "China, Provincia de Taiwán": ("China, provincia de Taiwán", ASIA),
    "China, RAE de Macao": ("China, RAE de Macao", ASIA),
    "China, RAE de Hong Kong": ("China, RAE de Hong Kong", ASIA), "Chipre": ("Chipre", ASIA),
    "Emiratos Árabes Unidos": ("Emiratos Árabes Unidos", ASIA),
    "Estado de Palestina": ("Estado de Palestina", ASIA), "Filipinas": ("Filipinas", ASIA),
    "Georgia": ("Georgia", ASIA), "India": ("India", ASIA), "Indonesia": ("Indonesia", ASIA),
    "Irak": ("Irak", ASIA), "Irán": ("Irán", ASIA), "Israel": ("Israel", ASIA), "Japón": ("Japón", ASIA),
    "Jordán": ("Jordania", ASIA), "Kazakhstan": ("Kazajstán", ASIA), "Kirguistán": ("Kirguistán", ASIA),
    "Katar": ("Catar", ASIA), "Kuwait": ("Kuwait", ASIA),
    "Lao": ("República Democrática Popular Lao", ASIA), "Líbano": ("Líbano", ASIA),
    "Malasia": ("Malasia", ASIA), "Maldivas": ("Maldivas", ASIA), "Mongolia": ("Mongolia", ASIA),
    "Myanmar": ("Myanmar", ASIA), "Nepal": ("Nepal", ASIA), "Omán": ("Omán", ASIA),
    "Pakistán": ("Pakistán", ASIA), "República Árabe Siria": ("República Árabe Siria", ASIA),
    "Rep. De Corea": ("República de Corea", ASIA), "Singapur": ("Singapur", ASIA),
    "Sri Lanka": ("Sri Lanka", ASIA), "Tailandia": ("Tailandia", ASIA), "Tayikistán": ("Tayikistán", ASIA),
    "Timor Oriental": ("Timor-Leste", ASIA), "Turkmenistán": ("Turkmenistán", ASIA),
    "Turquía": ("Turquía", ASIA), "Uzbekistán": ("Uzbekistán", ASIA), "Viet Nam": ("Vietnam", ASIA),
    "Yemen": ("Yemen", ASIA), "Yemen, Demócrata": ("Yemen Democrático", ASIA),
    "Yemen, República Arabe": ("República Árabe del Yemen", ASIA),
    # Europa
    "Albania": ("Albania", EUROPA), "Alemania": ("Alemania", EUROPA), "Austria": ("Austria", EUROPA),
    "Belarús": ("Belarús", EUROPA), "Bélgica": ("Bélgica", EUROPA),
    "Bosnia y Herzegovina": ("Bosnia y Herzegovina", EUROPA), "Bulgaria": ("Bulgaria", EUROPA),
    "Checoslovaquia": ("Checoslovaquia", EUROPA), "Czechia": ("Chequia", EUROPA),
    "Croacia": ("Croacia", EUROPA), "Dinamarca": ("Dinamarca", EUROPA),
    "Eslovaquia": ("Eslovaquia", EUROPA), "Eslovenia": ("Eslovenia", EUROPA), "España": ("España", EUROPA),
    "Estonia": ("Estonia", EUROPA), "Federación Rusa": ("Federación de Rusia", EUROPA),
    "Finlandia": ("Finlandia", EUROPA), "Francia": ("Francia", EUROPA), "Grecia": ("Grecia", EUROPA),
    "Hungría": ("Hungría", EUROPA), "Irlanda": ("Irlanda", EUROPA), "Islandia": ("Islandia", EUROPA),
    "Islas Feroe": ("Islas Feroe", EUROPA), "Italia": ("Italia", EUROPA), "Letonia": ("Letonia", EUROPA),
    "Lituania": ("Lituania", EUROPA), "Luxemburgo": ("Luxemburgo", EUROPA), "Malta": ("Malta", EUROPA),
    "Montenegro": ("Montenegro", EUROPA), "Noruega": ("Noruega", EUROPA),
    "Países Bajos": ("Países Bajos", EUROPA), "Polonia": ("Polonia", EUROPA),
    "Portugal": ("Portugal", EUROPA), "Reino Unido": ("Reino Unido", EUROPA),
    "República de Moldova": ("República de Moldova", EUROPA), "Rumania": ("Rumania", EUROPA),
    "Serbia": ("Serbia", EUROPA), "Serbia y Montenegro": ("Serbia y Montenegro", EUROPA),
    "Suecia": ("Suecia", EUROPA), "Suiza": ("Suiza", EUROPA), "Ucrania": ("Ucrania", EUROPA),
    "TFYR de Macedonia": ("Macedonia del Norte", EUROPA),
    # Oceanía
    "Australia": ("Australia", OCEANIA), "Fiyi": ("Fiyi", OCEANIA), "Islas Cook": ("Islas Cook", OCEANIA),
    "Islas Salomón": ("Islas Salomón", OCEANIA), "Kiribati": ("Kiribati", OCEANIA),
    "Nueva Caledonia": ("Nueva Caledonia", OCEANIA), "Nueva Zelanda": ("Nueva Zelanda", OCEANIA),
    "Papúa Nueva Guinea": ("Papúa Nueva Guinea", OCEANIA),
    "Polinesia francés": ("Polinesia Francesa", OCEANIA), "Tonga": ("Tonga", OCEANIA),
    "Tuvalu": ("Tuvalu", OCEANIA), "Vanuatu": ("Vanuatu", OCEANIA),
}

_PREFIJO_IED = "Flujos netos de inversión extranjera directa "
_PREFIJO_CUENTA = "Cuenta corriente de balance de pago "


def _por_pais(serie: Serie, prefijo: str, indicador: str, nombre: str) -> Clasificacion | None:
    descripcion = " ".join(serie.descripcion.split())
    if not descripcion.startswith(prefijo):
        return None
    pais = _PAISES.get(descripcion[len(prefijo):])
    if pais is None:
        return None
    # La medida está en el título del cuadro: «flujos monetarios» o «porcentuales / en porcentaje del producto».
    titulo = plano(serie.dataset)
    if "porcentua" in titulo or "porcentaje" in titulo:
        medida, unidad = PIB_PCT, PROPORCION
    elif "monetari" in titulo:
        medida, unidad = "", MILLONES_DOLARES
    else:
        return None
    nombre_pais, continente = pais
    return Clasificacion(tema=INTERNACIONAL, indicador=indicador, cobertura=continente,
                         nombre=con_medida(unir(nombre, nombre_pais), medida), unidad=unidad)


def _ied(serie: Serie) -> Clasificacion | None:
    return _por_pais(serie, _PREFIJO_IED, IED, IED_NOMBRE)


def _cuenta_corriente(serie: Serie) -> Clasificacion | None:
    return _por_pais(serie, _PREFIJO_CUENTA, CUENTA_CORRIENTE, CUENTA_CORRIENTE_NOMBRE)


# =================================================================================================
# Hoja 15: ponderaciones del comercio (los ponderadores del ITCRM)
# =================================================================================================
TIPO_DE_CAMBIO_REAL = "Tipo de cambio real"
PONDERADORES = "Ponderadores"

_SOCIOS = {
    "comercio_pct_brasil": "Brasil", "comercio_pct_canada": "Canadá", "comercio_pct_chile": "Chile",
    "comercio_pct_china": "China", "comercio_pct_estados_unidos": "Estados Unidos",
    "comercio_pct_india": "India", "comercio_pct_japon": "Japón", "comercio_pct_mexico": "México",
    "comercio_pct_reino_unido": "Reino Unido", "comercio_pct_suiza": "Suiza",
    "comercio_pct_uruguay": "Uruguay", "comercio_pct_vietnam": "Vietnam",
    "comercio_pct_zona_euro": "Zona Euro",
}


def _ponderaciones(serie: Serie) -> Clasificacion | None:
    socio = _SOCIOS.get(serie.variable)
    if socio is None:
        return None
    return Clasificacion(tema=EXTERNO, indicador=TIPO_DE_CAMBIO_REAL, cobertura=PONDERADORES,
                         nombre=unir("Ponderador del tipo de cambio real", socio), unidad=PORCENTAJE)


# =================================================================================================
_FINANZAS = {
    "1. Renta Variable": _renta_variable,
    "2. Mercado Cambiario": _mercado_cambiario,
    "3. FCI": _fci,
    "4. Financiamiento Privado": _financiamiento_privado,
    "5.1 Renta Fija Histórico": _titulos_publicos,
    "6. Indices Sectoriales": _indices_sectoriales,
}
_INTERNACIONAL = {
    "15 Ponderaciones Comercio": _ponderaciones,
    "16 Brasil PIB": _brasil_pib,
    "17 Bras Activid Indust Autos": _brasil_actividad,
    "18 Bras Salario real": _brasil_remuneracion,
    "19. Bras Tipo de cambio": _brasil_tipo_de_cambio,
    "20 IED": _ied,
    "4.b cuenta corriente": _cuenta_corriente,
    "5 tasa de interes": _tasas_internacionales,
    "6 bonos usa": _bonos_eeuu,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == FUENTE_EMBI:
        return _riesgo_pais(serie)
    regla = _FINANZAS.get(_fila(serie, "finanzas.xlsx")) or _INTERNACIONAL.get(_fila(serie, "internacional.xlsx"))
    return regla(serie) if regla else None
