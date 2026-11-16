"""Reglas del Informe sobre Bancos del BCRA (``dinero_bancos.xlsx`` de datos.gob.ar).

Tres hojas, todas con la misma forma: grupo de entidades × partida, una serie anual y una
mensual por cada combinación.

    8.12 Indicadores financieros   ->  Indicadores del sistema financiero
    8.13 Rentabilidad financiera   ->  Rentabilidad del sistema financiero
    8.14 Situacion Patrimonial     ->  Situación patrimonial del sistema financiero

Árbol: Moneda y sistema financiero > <uno de los tres indicadores> > <grupo de entidades> >
<bloque de partidas, sólo en la situación patrimonial>.

* Tres indicadores hermanos y no uno solo: son tres cuadros distintos del Informe (ratios,
  cuadro de resultados y balance), con unidades distintas (porcentajes y millones de pesos), y
  un indicador único superaría las 900 series. El grupo de entidades es la cobertura (nivel 3,
  seis hijos en cada indicador, siempre escritos igual); lo que queda por abrir es la partida.
* La situación patrimonial se abre además en «Activo» y «Pasivo y patrimonio neto» (nivel 4);
  los otros dos indicadores quedan en 34 y 42 series por grupo, sin apertura.

Cómo se deriva cada dimensión:

* Grupo de entidades: del prefijo de la ``Descripción``, que viene abreviado de varias maneras
  («Bancos ext priv», «Datos enti finan no banc», «Sist financ», «Bcos priv nac»); un diccionario
  explícito lo lleva a los seis nombres del Informe. Nunca se lee de ``Variable``: en 8.12 hay
  una variable sin prefijo (``liquidez_con_pases``, del sistema financiero).
* Partida de 8.12: de la ``Variable`` sin el prefijo del grupo ni el número de orden (que la
  fuente escribe de seis maneras distintas); cada variante figura en ``_INDICADORES``.
* Partida de 8.13: de la ``Variable`` sin el prefijo del grupo.
* Partida de 8.14: del resto de la ``Descripción`` («Activo. Préstamos. Sector Privado»).
* Las series anuales y las mensuales del mismo concepto llevan el mismo nombre. En 8.13 la
  anual es la suma del año y en 8.14 la anual es el saldo de diciembre.

Convenciones de nombre: ``<Partida> · <subpartida> · <grupo de entidades> (<medida>)``, con el
grupo omitido cuando es el total del sistema financiero. En 8.12 todo son porcentajes y la
medida dice sobre qué base (``% de los depósitos``, ``% del activo``…); 8.13 y 8.14 son
montos en millones de pesos y no llevan medida.
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

FUENTE = "datos.gob.ar"
ARCHIVO = "dinero_bancos.xlsx"
TEMA = "Moneda y sistema financiero"

INDICADORES = "Indicadores del sistema financiero"
RENTABILIDAD = "Rentabilidad del sistema financiero"
PATRIMONIAL = "Situación patrimonial del sistema financiero"

PORCENTAJE = "%"
MILLONES = "Millones de pesos"

# Grupos de entidades, con la grafía del Informe sobre Bancos.
SISTEMA = "Sistema financiero"
PUBLICOS = "Bancos públicos"
PRIVADOS = "Bancos privados"
NACIONALES = "Bancos privados nacionales"
EXTRANJEROS = "Bancos privados extranjeros"
NO_BANCARIAS = "Entidades financieras no bancarias"

# Prefijos de la «Descripción» (comparados sin tildes ni mayúsculas) -> grupo.
_GRUPOS = {
    "sistema financiero": SISTEMA,
    "sist financ": SISTEMA,
    "bancos publicos": PUBLICOS,
    "bancos pub": PUBLICOS,
    "bcos pub": PUBLICOS,
    "bancos privados": PRIVADOS,
    "bancos priv": PRIVADOS,
    "bcos priv": PRIVADOS,
    "bancos privados nacionales": NACIONALES,
    "bancos priv nac": NACIONALES,
    "bcos priv nac": NACIONALES,
    "bancos privados extranjeros": EXTRANJEROS,
    "bancos ext priv": EXTRANJEROS,
    "bancos priv ext": EXTRANJEROS,
    "bcos priv ext": EXTRANJEROS,
    "entidades financieras no bancarias": NO_BANCARIAS,
    "datos enti finan no banc": NO_BANCARIAS,
    "enti finan no banc": NO_BANCARIAS,
}
_PREFIJOS_GRUPO = sorted(_GRUPOS, key=len, reverse=True)


def _grupo_y_resto(descripcion: str) -> tuple[str, str] | None:
    """(grupo de entidades, resto de la descripción) por el prefijo más largo que coincida."""
    texto = plano(descripcion)
    for prefijo in _PREFIJOS_GRUPO:
        if texto == prefijo or texto.startswith(prefijo + " ") or texto.startswith(prefijo + "."):
            return _GRUPOS[prefijo], texto[len(prefijo):].lstrip(" .")
    return None


# --- 8.12 Indicadores financieros -------------------------------------------------------

# Clave de la variable (sin prefijo de grupo ni número de orden) -> (nombre, medida).
_LIQUIDEZ_BASE = "% de los depósitos"
_APR = "% de los activos ponderados por riesgo"
_APR_CREDITO = "% de los activos ponderados por riesgo de crédito"
_INDICADORES_BASE = {
    "liquidez_amplia": ("Liquidez amplia", _LIQUIDEZ_BASE),
    "liquidez_amplia_pesos": ("Liquidez amplia · En pesos", "% de los depósitos en pesos"),
    "liquidez": ("Liquidez", _LIQUIDEZ_BASE),
    "liquidez_pesos": ("Liquidez · En pesos", "% de los depósitos en pesos"),
    "liquidez_con_pases": ("Liquidez con pases", _LIQUIDEZ_BASE),
    "credito_sector_publico": ("Crédito al sector público", "% del activo"),
    "credito_sector_privado": ("Crédito al sector privado", "% del activo"),
    "irregularidad_cartera_privada": ("Irregularidad de la cartera privada", "% de la cartera privada"),
    "exposicion_patrimonial_sector_privado": (
        "Exposición patrimonial al sector privado", "% del patrimonio neto"),
    "roa_acumulado_anual": ("ROA", "% anualizado del activo neteado"),
    "roe_acumulado_anual": ("ROE", "% anualizado del patrimonio neto"),
    # Definición del Informe sobre Bancos (notas del Anexo estadístico, cuadro 1): (margen financiero +
    # resultado por servicios) / gastos de administración, acumulado anual, en %. Verificado con 8.13:
    # el cociente recalculado coincide con 8.12 en 2017-2024 (anual) y, con las partidas mensuales
    # acumuladas en el año, en 196 de 196 meses (mediana del error 0,0). Cuadro 1 del Informe de
    # dic-2011 trae 167 (2008) y 179 (2011); la hoja da 167,0 y 178,9.
    "eficiencia_acumulado_anual": (
        "Eficiencia", "margen financiero y servicios, % de los gastos de administración"),
    "integracion_capital": ("Integración de capital", _APR),
    "integracion_capital_riesgo_credito": (
        "Integración de capital · Riesgo de crédito", _APR_CREDITO),
    "integracion_capital_nivel_1": ("Integración de capital · Nivel 1", _APR),
    "integracion_capital_nivel_1_riesgo_credito": (
        "Integración de capital · Nivel 1 · Riesgo de crédito", _APR_CREDITO),
    # Informe: (integración - exigencia, con franquicias) / exigencia de capital; es el exceso, no el total.
    "posicion_capital": ("Posición de capital", "exceso sobre la exigencia, % de la exigencia"),
}
# Variantes de escritura de la fuente (abreviaturas y guiones bajos finales) -> clave.
_VARIANTES_INDICADOR = {
    "liquidezconpases": "liquidez_con_pases",
    "exposicion_pat_sector_priv": "exposicion_patrimonial_sector_privado",
    "exposicion_patrimonial_sector_priv": "exposicion_patrimonial_sector_privado",
    "integracion_cap_riesgo_cred": "integracion_capital_riesgo_credito",
    "integracion_capital_riesgo_cred": "integracion_capital_riesgo_credito",
    "integracion_cap_nivel_1": "integracion_capital_nivel_1",
    "integ_cap_nivel_1_riesgo_cred": "integracion_capital_nivel_1_riesgo_credito",
    "integracion_capital_nivel_1_riesgo_cred": "integracion_capital_nivel_1_riesgo_credito",
}
_PREFIJOS_VARIABLE = (
    "bancos_priv_extranjeros_", "bancos_priv_nacionales_", "bancos_priv_", "bancos_publicos_",
    "entidades_finan_no_banc_", "sistema_finan_",
)


def _clave_indicador(variable: str) -> str | None:
    resto = variable.lower()
    for prefijo in _PREFIJOS_VARIABLE:
        if resto.startswith(prefijo):
            resto = resto[len(prefijo):]
            break
    resto = re.sub(r"^\d+[ab]?_", "", resto).strip("_")
    resto = _VARIANTES_INDICADOR.get(resto, resto)
    return resto if resto in _INDICADORES_BASE else None


def _indicadores(serie: Serie) -> Clasificacion | None:
    encontrado = _grupo_y_resto(serie.descripcion)
    clave = _clave_indicador(serie.variable)
    if encontrado is None or clave is None:
        return None
    grupo = encontrado[0]
    partida, medida = _INDICADORES_BASE[clave]
    return Clasificacion(
        tema=TEMA, indicador=INDICADORES, cobertura=grupo,
        nombre=con_medida(unir(partida, "" if grupo == SISTEMA else grupo), medida),
        unidad=PORCENTAJE)


# --- 8.13 Rentabilidad financiera -------------------------------------------------------

_RESULTADOS = {
    "resultado_total_integrado": "Resultado total integrado",
    "resultado_total_integrado_ajustado": "Resultado total integrado ajustado",
    "resultado_total_integrado_ajust": "Resultado total integrado ajustado",
    "resultado_total": "Resultado total",
    "ori": "Otros resultados integrales",
    "margen_financ": "Margen financiero",
    "margen_financ_ingresos_por_intereses": "Margen financiero · Ingresos por intereses",
    "margen_financ_egresos_por_intereses": "Margen financiero · Egresos por intereses",
    "margen_financ_resultado_titulos_valores": "Margen financiero · Resultado por títulos valores",
    "margen_financ_ajustes_cer_cvs": "Margen financiero · Ajustes por CER y CVS",
    "margen_financ_diferencias_cotizacion": "Margen financiero · Diferencias de cotización",
    "margen_financ_primas_por_pases": "Margen financiero · Primas por pases",
    "margen_financ_otros_resultados_financs": "Margen financiero · Otros resultados financieros",
    "resultado_por_servicios": "Resultado por servicios",
    "cargos_por_incobrabilidad": "Cargos por incobrabilidad",
    "gastos_administracion": "Gastos de administración",
    "cargas_impositivas": "Cargas impositivas",
    "impuesto_ganancias": "Impuesto a las ganancias",
    "ajustes_valuacion_prestamos_sector_publico": "Ajustes de valuación de préstamos al sector público",
    "ajustes_valuacion_prestamos_al_sector_publico": "Ajustes de valuación de préstamos al sector público",
    "ajustes_valuacion_prestamos_sector_pub": "Ajustes de valuación de préstamos al sector público",
    "amortizacion_amparos": "Amortización de amparos",
    "resultado_monetario": "Resultado monetario",
    "diversos": "Resultados diversos",
}
_PREFIJOS_RESULTADOS = (
    "bancos_ext_priv_", "bancos_priv_nac_", "bancos_priv_", "bancos_pub_",
    "enti_finan_no_banc_", "sist_financ_",
)
_GRUPO_DE_RESULTADOS = {
    "bancos_ext_priv_": EXTRANJEROS, "bancos_priv_nac_": NACIONALES, "bancos_priv_": PRIVADOS,
    "bancos_pub_": PUBLICOS, "enti_finan_no_banc_": NO_BANCARIAS, "sist_financ_": SISTEMA,
}


def _rentabilidad(serie: Serie) -> Clasificacion | None:
    variable = serie.variable.lower()
    for prefijo in _PREFIJOS_RESULTADOS:
        if variable.startswith(prefijo):
            partida = _RESULTADOS.get(variable[len(prefijo):])
            if partida is None:
                return None
            grupo = _GRUPO_DE_RESULTADOS[prefijo]
            return Clasificacion(
                tema=TEMA, indicador=RENTABILIDAD, cobertura=grupo,
                nombre=unir(partida, "" if grupo == SISTEMA else grupo), unidad=MILLONES)
    return None


# --- 8.14 Situación patrimonial ---------------------------------------------------------

ACTIVO = "Activo"
PASIVO_Y_PATRIMONIO = "Pasivo y patrimonio neto"

# Resto de la descripción (sin grupo, comparado sin tildes) -> (bloque, nombre).
_BALANCE = {
    "activo neteado": (ACTIVO, "Activo neteado"),
    "activo neteado consolidado": (ACTIVO, "Activo neteado consolidado"),
    "total activo": (ACTIVO, "Activo"),
    "activo. disponibilidades": (ACTIVO, "Activo · Disponibilidades"),
    "activo. titulos publicos": (ACTIVO, "Activo · Títulos públicos"),
    "activo. titulos publicos. lebac y nobac": (ACTIVO, "Activo · Títulos públicos · Lebac y Nobac"),
    "activo. titulos publicos. lebac y nobac. tenencia por cartera propia":
        (ACTIVO, "Activo · Títulos públicos · Lebac y Nobac · Tenencia por cartera propia"),
    "activo. titulos publicos. lebac y nobac. tenencia por operaciones de pase activo":
        (ACTIVO, "Activo · Títulos públicos · Lebac y Nobac · Tenencia por operaciones de pase activo"),
    "activo. titulos publicos. otros titulos publicos": (ACTIVO, "Activo · Títulos públicos · Otros títulos públicos"),
    "activo. titulos privados": (ACTIVO, "Activo · Títulos privados"),
    "activo. prestamos": (ACTIVO, "Activo · Préstamos"),
    "activo. prestamos. sector publico": (ACTIVO, "Activo · Préstamos · Sector público"),
    "activo. prestamos. sector privado": (ACTIVO, "Activo · Préstamos · Sector privado no financiero"),
    "activo. prestamos. sector financiero": (ACTIVO, "Activo · Préstamos · Sector financiero"),
    "activo. previsiones por prestamos": (ACTIVO, "Activo · Previsiones por préstamos"),
    "activo. otros creditos por intermediacion financiera":
        (ACTIVO, "Activo · Otros créditos por intermediación financiera"),
    "activo. otros creditos por intermediacion financiera. fideicomisos sin cotizacion":
        (ACTIVO, "Activo · Otros créditos por intermediación financiera · Fideicomisos sin cotización"),
    "activo. otros creditos por intermediacion financiera. on y os":
        (ACTIVO, "Activo · Otros créditos por intermediación financiera · Obligaciones negociables y subordinadas"),
    "activo. otros creditos por intermediacion financiera. otros":
        (ACTIVO, "Activo · Otros créditos por intermediación financiera · Otros"),
    "activo. otros creditos por intermediacion financieracompensacion a recibir":
        (ACTIVO, "Activo · Otros créditos por intermediación financiera · Compensación a recibir"),
    "activo. participacion en otras sociedades": (ACTIVO, "Activo · Participación en otras sociedades"),
    "activo. bienes en locacion financiera (leasing)": (ACTIVO, "Activo · Bienes en locación financiera (leasing)"),
    "activo. filiales en el exterior": (ACTIVO, "Activo · Filiales en el exterior"),
    "activo. bienes de uso y diversos": (ACTIVO, "Activo · Bienes de uso y diversos"),
    "activo. otros activos": (ACTIVO, "Activo · Otros activos"),
    "total pasivo": (PASIVO_Y_PATRIMONIO, "Pasivo"),
    "pasivo. depositos": (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos"),
    "pasivo. depositos sector publico": (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos · Sector público"),
    "pasivo. depositos sector privado":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos · Sector privado no financiero"),
    "pasivo. depositos sector privado. cuenta corriente":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos · Sector privado no financiero · Cuenta corriente"),
    "pasivo. depositos sector privado. caja de ahorros":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos · Sector privado no financiero · Caja de ahorros"),
    "pasivo. depositos sector privado. plazo fijo":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos · Sector privado no financiero · Plazo fijo"),
    "pasivo. depositos sector privado. cedro":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Depósitos · Sector privado no financiero · CEDRO"),
    "pasivo. otras obligaciones por intermediacion financiera":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Otras obligaciones por intermediación financiera"),
    "pasivo. otras obligaciones por intermediacion financiera. obligaciones interfinancieras":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Otras obligaciones por intermediación financiera · Obligaciones interfinancieras"),
    "pasivo. otras obligaciones por intermediacion financiera. obligaciones con el bcra":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Otras obligaciones por intermediación financiera · Obligaciones con el BCRA"),
    "pasivo. otras obligaciones por intermediacion financiera. obligaciones negociables":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Otras obligaciones por intermediación financiera · Obligaciones negociables"),
    "pasivo. otras obligaciones por intermediacion financiera. lineas de prestamos del exterior":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Otras obligaciones por intermediación financiera · Líneas de préstamos del exterior"),
    "pasivo. otras obligaciones por intermediacion financiera. otros":
        (PASIVO_Y_PATRIMONIO, "Pasivo · Otras obligaciones por intermediación financiera · Otros"),
    "pasivo. obligaciones subordinadas": (PASIVO_Y_PATRIMONIO, "Pasivo · Obligaciones subordinadas"),
    "pasivo. otros pasivos": (PASIVO_Y_PATRIMONIO, "Pasivo · Otros pasivos"),
    "patrimonio neto": (PASIVO_Y_PATRIMONIO, "Patrimonio neto"),
}


def _patrimonial(serie: Serie) -> Clasificacion | None:
    encontrado = _grupo_y_resto(serie.descripcion)
    if encontrado is None:
        return None
    grupo, resto = encontrado
    definicion = _BALANCE.get(resto)
    if definicion is None:
        return None
    bloque, partida = definicion
    nombre = unir(*partida.split(" · "), "" if grupo == SISTEMA else grupo)
    return Clasificacion(
        tema=TEMA, indicador=PATRIMONIAL, cobertura=grupo, apertura=bloque,
        nombre=nombre, unidad=MILLONES)


_POR_HOJA = {
    "8.12 Indicadores financieros": _indicadores,
    "8.13 Rentabilidad financiera": _rentabilidad,
    "8.14 Situacion Patrimonial": _patrimonial,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.archivo != ARCHIVO:
        return None
    regla = _POR_HOJA.get(serie.hoja)
    return regla(serie) if regla else None
