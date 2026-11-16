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

* Las «Ponderaciones Comercio» son, socio por socio, casi idénticas a los «Ponderadores» del ITCRM
  del BCRA (mismos 13 socios, desde enero de 1997; diferencia media de 0,00 a 0,04 puntos, máxima
  0,6), así que van al indicador del tipo de cambio real con el mismo nombre. No son exactamente
  iguales: la hoja no está revisada como la del BCRA.
* Varios cuadros publican porcentajes como fracciones (0,035 = 3,5%): las participaciones de
  las ON y de los fideicomisos, las tasas, el rendimiento de los bonos, la IED y la cuenta
  corriente en proporción del PIB. Esas series llevan la unidad «Proporción (1 = 100%)» y el
  nombre «(% del PIB)», «(% del total)» o «(tasa nominal anual)».
* Las participaciones por sector de las ON y por activo subyacente de los fideicomisos suman
  1 en 102 de 176 meses (en los demás, un sector queda un mes de menos y el siguiente de más: suman
  2 entre ambos): son participaciones y no montos, aunque la descripción diga «Emisión de…».
* Los títulos públicos se identifican por su código de especie de BYMA (el año del nombre de la
  fuente es erróneo en varios: «Bonar 2025 - AA19» vence en 2019) y se agrupan por moneda y
  ajuste (dólares, vinculados al dólar, duales, pesos a tasa fija o variable, CER, cláusula gatillo,
  canje de deuda, consolidación, cupones del PIB). La cotización es el precio en pesos por cada
  100 de valor nominal (en dólares en las especies con sufijo D); «dur» es la duración modificada.
* Las IED por país son las entradas netas (inversión directa recibida) y la cuenta corriente es su
  saldo, en millones de dólares o como fracción del PIB.
* El «tipo de cambio BNA vendedor» está excluido: del 2018 al 2023 repite el mayorista del MAE.
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
MILES_DOLARES = "Miles de dólares"
INDICE_2012 = "Índice 2012=100"
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
}
# El «implícito en ADR» es el dólar contado con liquidación: sigue al dólar MEP (cociente mediano
# 1,04 en 1.280 días) y al blue (0,99), coincide con el oficial sin cepo (2016-2019: 1,00) y se
# separa de él con cepo (2012-2015: 1,35-1,55; 2020-2023: 1,7-2,0).
ADR = "Contado con liquidación (implícito en ADR)"


def _mercado_cambiario(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    tipo = _TIPOS_DE_CAMBIO.get(variable)
    if tipo is not None:
        return Clasificacion(tema=EXTERNO, indicador=TIPO_DE_CAMBIO, nombre=unir("Tipo de cambio", tipo),
                             unidad=PESOS_POR_DOLAR)
    if variable == "tipo_cambio_implicito_en_adrs":
        return Clasificacion(tema=EXTERNO, indicador=TIPO_DE_CAMBIO, cobertura="Paralelos y financieros",
                             nombre=unir("Tipo de cambio", ADR), unidad=PESOS_POR_DOLAR)
    if variable == "volumen_mae":
        # La API de series de tiempo lo declara en miles de dólares y los órdenes de magnitud
        # lo confirman: mediana de 392 millones de dólares diarios en 2018, 129 millones en 2020.
        return Clasificacion(tema=MERCADOS, indicador=VOLUMEN_CAMBIOS,
                             nombre=unir(VOLUMEN_CAMBIOS, "MAE"), unidad=MILES_DOLARES)
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
ON_VCP = "ON y valores de corto plazo"
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
        # Los montos y cantidades de ON incluyen los valores de corto plazo («ON/VCP»); la CNV
        # los informa dentro de las ON (monto de la serie × tipo de cambio ≈ monto en pesos de la CNV:
        # jul-2022, 97.028 contra 95.377 millones de pesos).
        rotulo = ON_VCP if instrumento == ON else instrumento
        nombre = unir(FINANCIAMIENTO, rotulo, tamanio)
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

USD = "Bonos en dólares"
VINCULADOS = "Bonos vinculados al dólar"
DUALES = "Bonos duales"
TASA_FIJA = "Bonos en pesos a tasa fija"
TASA_VARIABLE = "Bonos en pesos a tasa variable"
CER = "Bonos ajustables por CER"
GATILLO = "Bonos con cláusula gatillo"
CANJE = "Bonos del canje de deuda (Discount, Par y Cuasipar)"
CONSOLIDACION = "Bonos de consolidación y garantizados"
CUPONES_PIB = "Cupones vinculados al PIB"

# Código de especie de BYMA → (grupo, denominación). Se identifica cada bono por su código: la
# fuente (IAMC) trae el año equivocado en varios («Bonar 2025 - AA19» vence en 2019; «Bono del
# Tesoro Nacional 2015 - PR15» cotiza hasta 2020) y rotula mal el tipo de otros («cláusula
# gatillo» en TD21, que es de tasa dual; «LAR» en PARY, que es ley Nueva York). La denominación
# sale del nombre de la especie en el mercado (Rava/BYMA), y el año es el de vencimiento: coincide
# con el sufijo del código y con la vida promedio ponderada de la propia hoja.
# Moneda de denominación, por el cociente entre la cotización y el A 3500 (≈ 30-115 para los
# bonos en dólares, ≈ 100 para los de pesos a la par) y por el nombre del mercado.
_BONOS = {
    # Bonos en dólares (Bonar y bonos internacionales): cotizan en pesos por cada 100 de VN en dólares.
    "a2e2": (USD, "Bonar 2022 A2E2"), "a2e3": (USD, "Bonar 2023 A2E3"),
    "a2e7": (USD, "Bonar 2027 A2E7"), "a2e8": (USD, "Bonar 2028 A2E8"),
    "aa19": (USD, "Bonar 2019 AA19"), "aa21": (USD, "Bonar 2021 AA21"),
    "aa25": (USD, "Bonar 2025 AA25"), "aa26": (USD, "Bonar 2026 AA26"),
    "aa37": (USD, "Bonar 2037 AA37"), "aa46": (USD, "Bonar 2046 AA46"),
    "aa48": (USD, "Bono en dólares 2048 AA48"), "ac17": (USD, "Bono internacional 2117 AC17"),
    "an18": (USD, "Bonar 2018 AN18"), "ao20": (USD, "Bonar 2020 AO20"), "ay24": (USD, "Bonar 2024 AY24"),
    # Bonos en dólares que pagan en pesos al tipo de cambio.
    "am18": (VINCULADOS, "Bonad 2018 AM18"), "as17": (VINCULADOS, "Bonad 2017 AS17"),
    "tv21": (VINCULADOS, "Bono del Tesoro vinculado al dólar 2021 TV21"),
    # «en pesos»/«en dólares» de la fuente es la moneda de negociación, no de denominación.
    "a2j9": (DUALES, "Bono dual 2019 A2J9"), "a2j9d": (DUALES, "Bono dual 2019 A2J9D"),
    "af20": (DUALES, "Bono dual 2020 AF20"), "af20d": (DUALES, "Bono dual 2020 AF20D"),
    "tm18": (TASA_FIJA, "Bonar marzo 2018 TM18"), "ts18": (TASA_FIJA, "Bonar septiembre 2018 TS18"),
    "tj20": (TASA_FIJA, "Bono del Tesoro en pesos 2020 TJ20"),
    "tn20": (TASA_FIJA, "Bono del Tesoro en pesos 2020 TN20"),
    "to21": (TASA_FIJA, "Bonte 2021 TO21"), "to23": (TASA_FIJA, "Bonte 2023 TO23"),
    "to26": (TASA_FIJA, "Bonte 2026 TO26"),
    "am20": (TASA_VARIABLE, "Bocan 2020 AM20"), "aa22": (TASA_VARIABLE, "Bocan 2022 AA22"),
    "amx8": (TASA_VARIABLE, "Bonar 2018 en pesos AMX8"), "amx9": (TASA_VARIABLE, "Bonar 2019 en pesos AMX9"),
    "ao17": (TASA_VARIABLE, "Bonar octubre 2017 AO17"),
    "tb21": (TASA_VARIABLE, "Bono del Tesoro en pesos Badlar 2021 TB21"),
    "td21": (TASA_VARIABLE, "Bono del Tesoro en pesos a tasa dual 2021 TD21"),
    "tc20": (CER, "Boncer 2020 TC20"), "tc21": (CER, "Boncer 2021 TC21"), "tc23": (CER, "Boncer 2023 TC23"),
    "tc25p": (CER, "Boncer 2025 TC25P"), "tx21": (CER, "Boncer 2021 TX21"),
    "a2m2": (GATILLO, "Bogato 2020 A2M2"), "af19": (GATILLO, "Bogato 2019 AF19"),
    "dica": (CANJE, "Discount en dólares ley argentina 2033 DICA"),
    "dicy": (CANJE, "Discount en dólares ley Nueva York 2033 DICY"),
    "diy0": (CANJE, "Discount en dólares ley Nueva York (canje 2010) DIY0"),
    "dicp": (CANJE, "Discount en pesos ajustable por CER 2033 DICP"),
    "para": (CANJE, "Par en dólares ley argentina 2038 PARA"),
    "pary": (CANJE, "Par en dólares ley Nueva York 2038 PARY"),
    "parp": (CANJE, "Par en pesos ajustable por CER 2038 PARP"),
    "cuap": (CANJE, "Cuasipar en pesos ajustable por CER 2045 CUAP"),
    "pr13": (CONSOLIDACION, "Bono de consolidación serie 6 2024 PR13"),
    "pr15": (CONSOLIDACION, "Bono de consolidación serie 8 2022 PR15"),
    "nf18": (CONSOLIDACION, "Bogar 2018 NF18"), "no20": (CONSOLIDACION, "Bogar 2020 NO20"),
    "tvpa": (CUPONES_PIB, "Cupón PIB en dólares ley argentina TVPA"),
    "tvpe": (CUPONES_PIB, "Cupón PIB en euros TVPE"),
    "tvpp": (CUPONES_PIB, "Cupón PIB en pesos TVPP"),
    "tvpy": (CUPONES_PIB, "Cupón PIB en dólares ley Nueva York TVPY"),
}

PESOS_POR_100 = "Pesos por cada 100 de valor nominal"
DOLARES_POR_100 = "Dólares por cada 100 de valor nominal"

# Sufijo de la variable → (medida, unidad). La fuente escribe a veces «dm», «cot_anios»…
# «dur» es la duración modificada (en AA19, AO20 y AA21 coincide con la que se recalcula de los
# flujos: error medio 0,01-0,04 años contra 0,03-0,12 de la de Macaulay) y «ppv» la vida promedio
# ponderada (error medio 0,01-0,06); «yield» es la TIR efectiva como fracción (mediana 0,096).
_MEDIDAS_BONOS = {
    "cotizacion": ("cotización", PESOS_POR_100), "cot_anios": ("cotización", PESOS_POR_100),
    "dur": ("duración modificada", ANIOS), "dm_en_anios": ("duración modificada", ANIOS),
    "ppv_en_anios": ("vida promedio ponderada", ANIOS), "ppv_anios": ("vida promedio ponderada", ANIOS),
    "yield": ("rendimiento", PROPORCION), "yield_anios": ("rendimiento", PROPORCION),
}
_VARIABLE_BONO = re.compile(r"([a-z0-9]+)_(cotizacion|cot_anios|dur|dm_en_anios|ppv_en_anios|ppv_anios|yield|yield_anios)")
# Especies que cotizan en dólares (sufijo D de BYMA): A2J9D 96-105, AF20D 24-104 por cada 100 de VN.
_COTIZAN_EN_DOLARES = {"a2j9d", "af20d"}


def _titulos_publicos(serie: Serie) -> Clasificacion | None:
    partes = _VARIABLE_BONO.fullmatch(serie.variable)
    if partes is None or partes.group(1) not in _BONOS:
        return None
    grupo, bono = _BONOS[partes.group(1)]
    medida, unidad = _MEDIDAS_BONOS[partes.group(2)]
    if partes.group(1) in _COTIZAN_EN_DOLARES and medida == "cotización":
        medida, unidad = "cotización en dólares", DOLARES_POR_100
    return Clasificacion(tema=MERCADOS, indicador=TITULOS, cobertura=grupo, nombre=con_medida(bono, medida),
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
    # Base 2012=100: el promedio de 2012 es 100,08 (IBC-Br original), 100,07 (desest.), 100,0 y 99,9
    # (producción industrial); el IBC-Br coincide con las series 24363 y 24364 del BCB (diferencia
    # media 0,3) y la producción industrial es la 21859 del BCB en base 2002 por 0,858 = 100/116,55.
    "indice_actividad_ibc_br_original": ("IBC-Br", "", INDICE_2012),
    "indice_actividad_ibc_br_desest": ("IBC-Br", DESESTACIONALIZADO, INDICE_2012),
    "produccion_industrial_original": ("Producción industrial", "", INDICE_2012),
    "produccion_industrial_desestacionalizado": ("Producción industrial", DESESTACIONALIZADO, INDICE_2012),
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
    # Coincide al centésimo, fin de mes, con la tasa objetivo de la Fed (DFEDTAR hasta 2008 y límite
    # superior de DFEDTARU después: 436 de 436 meses); la tasa efectiva (FEDFUNDS) difiere 0,15 en promedio.
    "ff_eeuu_tasa": "Estados Unidos (objetivo de fondos federales)",
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
# Entradas netas de IED (inversión directa recibida, pasivos): en Argentina coincide con
# BX.KLT.DINV.CD.WD del Banco Mundial en 32 de 34 años (94%), en 69 de 74 series comparables con
# el Banco Mundial es más cercana a las entradas que a la inversión directa neta (BN.KLT.DINV.CD,
# con signo invertido: 80% contra 34% de coincidencias) y es positiva (Estados Unidos, 467.625 en 2015).
IED_NOMBRE = "Entradas netas de inversión extranjera directa"
CUENTA_CORRIENTE = "Cuenta corriente por país"
# Saldo de la cuenta corriente del balance de pagos: coincide con BN.CAB.XOKA.CD y .GD.ZS del
# Banco Mundial (Argentina 97% y 94% de los años; Brasil, % del PIB, 100%); déficit con signo negativo.
CUENTA_CORRIENTE_NOMBRE = "Saldo de la cuenta corriente"

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


# Mismo nombre que los «Ponderadores» del ITCRM del BCRA (externo_cambiario): en 339 meses, de 1997-01
# a 2025-03, la diferencia media por socio es de 0,000 a 0,040 puntos (idénticos Suiza, Reino Unido,
# Vietnam y Zona Euro; máxima 0,6 en China y Brasil por revisiones) y los 13 suman entre 99,5 y 100,4 (el BCRA, 100).
def _ponderaciones(serie: Serie) -> Clasificacion | None:
    socio = _SOCIOS.get(serie.variable)
    if socio is None:
        return None
    return Clasificacion(tema=EXTERNO, indicador=TIPO_DE_CAMBIO_REAL, cobertura=PONDERADORES,
                         nombre=unir("ITCRM", "Ponderador", socio), unidad=PORCENTAJE)


# =================================================================================================
# Series que dejan de publicarse (los datos no se tocan).
EXCLUIDAS = {
    (FUENTE, "168.1_T_CAMBIOR_D_0_0_26"):
        "«Tipo de cambio BNA (vendedor)» empalma dos conceptos: del 2018-01 al 2023 es idéntica, al "
        "centavo en todos los días, a la columna del tipo de cambio mayorista del MAE (oficial minorista "
        "del Banco Nación el 15/10/2020: 82,50; la serie: 77,48), y antes y después sí es el minorista.",
}


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
