"""Reglas de las finanzas públicas en base caja: cuenta ahorro-inversión-financiamiento (AIF)
e Informe mensual de ingresos y gastos (IMIG).

Dos indicadores del tema «Finanzas públicas»:

``Cuenta ahorro-inversión-financiamiento (base caja)``
    ``finanzas_publicas.xlsx`` de datos.gob.ar, hojas SPN, APN, TN, OD, ISS, RA, EP_PAMI_FF y Ex_CP.
    Cada hoja es un subsector del Sector Público Nacional y todas repiten las mismas partidas,
    así que hay un único analizador para las ocho.

        Indicador > <subsector institucional> > <bloque de la cuenta>          metodología vigente
        Indicador > Series históricas > <subsector> · metodología <período> > <bloque>   metodologías cerradas

    Bloques: Ingresos, Gastos, Figurativos (contribuciones y gastos figurativos), Resultados y
    Financiamiento (fuentes y aplicaciones financieras). El bloque lo fija la primera palabra de
    la partida.

``Informe mensual de ingresos y gastos (IMIG)``
    Hoja IMIG de datos.gob.ar y fuente ``mecon-imig`` (Ministerio de Economía): mismas partidas con
    los mismos nombres. Nivel 3: Ingresos, Gastos, Resultados y Partidas de ajuste; los gastos se
    abren en Corrientes, De capital y Totales.

Cómo se deriva cada dimensión
-----------------------------
* Partida (AIF): ``Descripción`` es «<partida> . Metodología <período>» con la jerarquía aplanada
  y abreviada («Gastos corrientes  intereses y otras rentas de la prop. intereses deuda externa»).
  Se normaliza (sin tildes, sin puntuación, sin la metodología ni años sueltos) y se busca en
  ``_PARTIDAS``, un diccionario explícito que lleva cada texto a su nombre con la denominación
  de la clasificación presupuestaria. Las erratas de la fuente («financieraa», «resnta»,
  «primarios sin renta») están como alias del mismo concepto.
* Subsector (AIF): la hoja de origen.
* Metodología y frecuencia: no entran en el nombre. Las tres metodologías (1993-2006, 2007-2014
  y 2017 en adelante) y las tres frecuencias (mensual, trimestral, anual) de una misma partida
  llevan el mismo nombre; los períodos no se solapan y la frecuencia y las fechas tienen su
  columna. El período sale del título de la fuente («… Valores anuales 1993-2006»): las dos
  metodologías cerradas van a «Series históricas», una por subsector y metodología, abierta
  en bloques; la vigente (un solo año en el título) queda en el subsector. Convivir en un
  mismo grupo no cabe: el SPN pasa de 150 series sólo en sus gastos corrientes (173), y con
  nueve versiones por partida lo mismo pasa en la APN (164) y en los totales por subsector.
* IMIG: la jerarquía viene en ``Nombre fuente`` («GASTOS PRIMARIOS - Prestaciones sociales -
  Pensiones no contributivas», en mecon-imig) o en ``Descripción`` («IMIG. Gastos primarios.
  Gastos corrientes primarios. Prestaciones sociales. Pensiones no contributivas», en
  datos.gob.ar). Se parte en segmentos, se unifica la jerarquía de ambas fuentes (datos.gob.ar
  intercala «Gastos corrientes primarios»; mecon-imig antepone «Gastos primarios» a las
  funciones del gasto de capital) y cada segmento se escribe con un diccionario.

Unidad: millones de pesos (los valores de las hojas AIF y de la IMIG de datos.gob.ar están en
millones: el gasto corriente del SPN de 2005 es 107.004 y el de 2023, de siete cifras más).
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import plano, unir

TEMA = "Finanzas públicas"
INDICADOR_AIF = "Cuenta ahorro-inversión-financiamiento (base caja)"
INDICADOR_IMIG = "Informe mensual de ingresos y gastos (IMIG)"
UNIDAD = "Millones de pesos"

ARCHIVO = "finanzas_publicas.xlsx"
FUENTE_DATOS = "datos.gob.ar"
FUENTE_IMIG = "mecon-imig"
HOJA_IMIG = "IMIG"

# Bloques de la cuenta (nivel 4 de la AIF, o 5 en las series históricas; nivel 3 de la IMIG).
INGRESOS = "Ingresos"
GASTOS = "Gastos"
FIGURATIVOS = "Figurativos"
RESULTADOS = "Resultados"
FINANCIAMIENTO = "Financiamiento"
AJUSTES = "Partidas de ajuste"

# Apertura de los gastos de la IMIG (nivel 4): superan las 150 series si se dejan juntos.
CORRIENTES = "Corrientes"
DE_CAPITAL = "De capital"
TOTALES = "Totales"

# Subsectores: hoja de origen -> nombre completo (nivel 3 y final del nombre de la serie).
SUBSECTORES = {
    "SPN": "Sector Público Nacional no financiero",
    # datos.gob.ar la titula «Administración Pública Nacional», pero el cuadro AIF de la Secretaría de
    # Hacienda (dic-2019 y dic-2025) la rotula «ADMINISTRACION NACIONAL» y la hoja es exactamente
    # Tesoro + Recursos afectados + Organismos descentralizados + Instituciones de la seguridad social +
    # Ex cajas provinciales (diferencia 0,0 en 2007-2026 y en 1993-2006 salvo 1997); la denominación
    # legal (Ley 24.156, art. 8) es «Administración Nacional».
    "APN": "Administración Nacional",
    "TN": "Tesoro Nacional",
    "OD": "Organismos descentralizados",
    "ISS": "Instituciones de la seguridad social",
    "RA": "Recursos afectados",
    "EP_PAMI_FF": "Empresas públicas, PAMI y fondos fiduciarios",
    "Ex_CP": "Ex cajas provinciales",
}
# El SPN es la cobertura natural de la cuenta: su nombre no lleva el subsector.
COBERTURA_NATURAL = "SPN"

HISTORICAS = "Series históricas"

# --------------------------------------------------------------------------------------------
# Partidas de la cuenta AIF
# --------------------------------------------------------------------------------------------

_IC = "Ingresos corrientes"
_GC = "Gastos corrientes"
_RC = "Recursos de capital"
_GK = "Gastos de capital"
_INT = "Rentas de la propiedad"
_CONSUMO = "Gastos de consumo y operación"
_TC_PUB = "Transferencias al sector público"
_TC_PRIV = "Transferencias al sector privado"
_PCIAS = "Provincias y CABA"
_TK = "Transferencias de capital"
_FUENTES = "Fuentes financieras"
_APLIC = "Aplicaciones financieras"
_FGS = "FGS"
_BCRA = "BCRA"
_CONTRIB = "Contribuciones figurativas"

# clave normalizada de «Descripción» -> (bloque, nombre de la partida)
_PARTIDAS: dict[str, tuple[str, str]] = {}


def _p(bloque: str, nombre: str, *claves: str) -> None:
    for clave in claves:
        _PARTIDAS[clave] = (bloque, nombre)


# Ingresos corrientes
_p(INGRESOS, _IC, "total ingresos corrientes")
_p(INGRESOS, "Ingresos tributarios", "ingresos corrientes ingresos tributarios")
_p(INGRESOS, "Ingresos no tributarios", "ingresos corrientes ingresos no tributarios")
_p(INGRESOS, unir(_IC, "Aportes y contribuciones a la seguridad social"),
   "ingresos corrientes aportes y contrib a la seg social")
_p(INGRESOS, unir(_IC, "Ingresos extrapresupuestarios"),
   "ingresos corrientes ingresos extrapresupuestarios")
_p(INGRESOS, unir(_IC, "Ingresos de operación"), "ingresos corrientes ingresosdeoperacion")
_p(INGRESOS, unir(_IC, "Otros ingresos"), "ingresos corrientes otros ingresos")
_p(INGRESOS, unir(_IC, "Rentas de la propiedad"),
   "ingresos corrientes rentas de la propiedad total rentas de la propiedad")
_p(INGRESOS, unir(_IC, "Rentas de la propiedad netas"),
   "ingresos corrientes rentas de la propiedad rentas de la propiedad netas",
   "ing corrientes rentas de la prop rentas de la propiedad netas")
_p(INGRESOS, unir(_IC, "Rentas de la propiedad", "Otras rentas de la propiedad"),
   "ingresos corrientes rentas de la propiedad otras rentas de la propiedad")
_p(INGRESOS, unir(_IC, "Rentas de la propiedad", f"Rentas percibidas del {_BCRA}"),
   "ingresos corrientes rentas de la propiedad rentas percibidas del bcra")
_p(INGRESOS, unir(_IC, "Rentas de la propiedad", f"Rentas públicas percibidas por el {_FGS}"),
   "ingresos corrientes rentas de la propiedad rentas publicas percibidas por el fgs")
_p(INGRESOS, unir(_IC, "Rentas de la propiedad", f"Rentas públicas percibidas por el {_FGS} y otros"),
   "ingresos corrientes rentas de la propiedad rentas publicas percibidas por el fgs y otros",
   "ing corrientes rentas de la prop rentas publicas percibidas por el fcg y otros")
_p(INGRESOS, unir(_IC, "Superávit operativo de empresas públicas"),
   "ingresos corrientes superavit operativo empresas pub")
_p(INGRESOS, unir(_IC, "Transferencias corrientes"), "ingresos corrientes transferencias corrientes")
_p(INGRESOS, unir(_IC, "Ventas de bienes y servicios de las administraciones públicas"),
   "ingresos corrientes ventas de bs y serv de las adm pub")
# Recursos de capital
_p(INGRESOS, _RC, "recursos de capital total recursos de capital")
_p(INGRESOS, unir(_RC, "Privatizaciones"), "recursos de capital privatizaciones")
_p(INGRESOS, unir(_RC, "Otros recursos"), "recursos de capital otros")
# Agregados de ingresos
_p(INGRESOS, "Ingresos antes de figurativos", "ingresos antes de figurativos")
_p(INGRESOS, "Ingresos después de figurativos", "ingresos despues de figurativos")
_p(INGRESOS, "Ingresos primarios antes de figurativos", "ingresos primarios antes de figurativos",
   "ingresos primario antes de figurativos",
   # Rótulo equivocado de la fuente (SPN trimestral 2007-2014): la columna es la de los primarios.
   "ingresos corrientes antes de figurativos")
_p(INGRESOS, "Ingresos primarios después de figurativos", "ingresos primarios despues de figurativos")

# Gastos corrientes
_p(GASTOS, _GC, "total gastos corrientes", "gastos corrientes", "gasto corrientes",
   "gastos corrientes 1993")
_p(GASTOS, unir(_GC, _CONSUMO, "Remuneraciones"),
   "gastos corrientes gastos de consumo y operacion remuneraciones")
_p(GASTOS, unir(_GC, _CONSUMO, "Bienes y servicios"),
   "gastos corrientes gastos de consumo y operacion bienes y servicios")
_p(GASTOS, unir(_GC, _CONSUMO, "Otros gastos"),
   "gastos corrientes gastos de consumo y operacion otros gastos")
_p(GASTOS, unir(_GC, _INT), "gastos corrientes intereses y otras rentas de la prop total rentas de la propiedad")
_p(GASTOS, unir(_GC, _INT, "Intereses de la deuda externa"),
   "gastos corrientes intereses y otras rentas de la prop intereses deuda externa")
_p(GASTOS, unir(_GC, _INT, "Intereses de la deuda interna"),
   "gastos corrientes intereses y otras rentas de la prop intereses deuda interna")
_p(GASTOS, unir(_GC, _INT, "Intereses en moneda extranjera"),
   "gastos corrientes intereses y otras rentas de la prop intereses en moneda extranjera")
_p(GASTOS, unir(_GC, _INT, "Intereses en moneda local"),
   "gastos corrientes intereses y otras rentas de la prop intereses en moneda local")
_p(GASTOS, unir(_GC, _INT, "Intereses netos"),
   "gastos corrientes intereses y otras rentas de la prop intereses netos")
_p(GASTOS, unir(_GC, _INT, "Intereses pagados dentro del sector público"),
   "gastos corrientes intereses y otras rentas de la prop intereses pagados intra sector publico")
_p(GASTOS, unir(_GC, _INT, "Otras rentas"),
   "gastos corrientes intereses y otras rentas de la prop otras rentas")
_p(GASTOS, unir(_GC, "Prestaciones de la seguridad social"),
   "gastos corrientes prestaciones de la seguridad social")
_p(GASTOS, unir(_GC, _TC_PRIV), "gastos corrientes transferencias corrientes al sector privado total sector privado")
_p(GASTOS, unir(_GC, _TC_PRIV, "Instituciones de enseñanza"),
   "gastos corrientes transferencias corrientes al sector privado instituciones de ensenanza")
_p(GASTOS, unir(_GC, _TC_PRIV, "Otras transferencias"),
   "gastos corrientes transferencias corrientes al sector privado otras")
_p(GASTOS, unir(_GC, _TC_PUB, "Universidades"),
   "gastos corrientes transferencias corrientes al sector publico universidades")
_p(GASTOS, unir(_GC, _TC_PUB, "Otras transferencias"),
   "gastos corrientes transferencias corrientes al sector publico otras")
_p(GASTOS, unir(_GC, _TC_PUB, _PCIAS),
   "gastos corrientes transferencias corrientes al sector publico provincias y caba total provincias y caba")
_p(GASTOS, unir(_GC, _TC_PUB, _PCIAS, "Recursos coparticipados"),
   "gastos corrientes transferencias corrientes al sector publico provincias y caba recursos coparticipados")
_p(GASTOS, unir(_GC, _TC_PUB, _PCIAS, "Leyes especiales"),
   "gastos corrientes transferencias corrientes al sector publico provincias y caba leyes especiales")
_p(GASTOS, unir(_GC, _TC_PUB, _PCIAS, "Garantía del acuerdo con las provincias"),
   "gastos corrientes transferencias corrientes al sector publico provincias y caba garantia acuerdo provincias")
_p(GASTOS, unir(_GC, _TC_PUB, _PCIAS, "Resto"),
   "gastos corrientes transferencias corrientes al sector publico provincias y caba resto")
_p(GASTOS, unir(_GC, "Transferencias al sector externo"),
   "gastos corrientes transferencias corrientes al sector externo")
_p(GASTOS, unir(_GC, "Déficit operativo de empresas públicas"),
   "gastos corrientes deficit operativo empresas pub")
_p(GASTOS, unir(_GC, "Gastos extrapresupuestarios"), "gastos corrientes gastos extrapresupuestarios")
_p(GASTOS, unir(_GC, "Otros gastos"), "gastos corrientes otros gastos")
_p(GASTOS, unir(_GC, "Otros gastos corrientes"), "gastos corrientes otros gastos corrientes")
# Gastos de capital
_p(GASTOS, _GK, "total gastos de capital")
_p(GASTOS, unir(_GK, "Inversión real directa"), "gastos de capital inversion real directa")
_p(GASTOS, unir(_GK, f"Inversión financiera a provincias y CABA"),
   "gastos de capital inversion financieraa provincias y caba")
_p(GASTOS, unir(_GK, "Resto"), "gastos de capital resto")
_p(GASTOS, unir(_GK, _TK, "Otras transferencias"), "gastos de capital transferencias de capital otras")
_p(GASTOS, unir(_GK, _TK, _PCIAS),
   "gastos de capital transferencias de capital a provincias y caba total provincias y caba")
_p(GASTOS, unir(_GK, _TK, _PCIAS, "Leyes especiales"),
   "gastos de capital transferencias de capital a provincias y caba leyes especiales")
_p(GASTOS, unir(_GK, _TK, _PCIAS, "Resto"),
   "gastos de capital transferencias de capital a provincias y caba resto")
# Agregados de gastos
_p(GASTOS, "Gastos antes de figurativos", "gastos antes de figurativos")
_p(GASTOS, "Gastos después de figurativos", "gastos despues de figurativos")
_p(GASTOS, "Gastos primarios antes de figurativos", "gastos primarios antes de figurativos",
   "gastos primario antes de figurativos", "gastos primariosantes de figurativos")
_p(GASTOS, "Gastos primarios después de figurativos", "gastos primarios despues de figurativos")

# Figurativos: la contraparte entre los subsectores del sector público
_p(FIGURATIVOS, "Gastos figurativos", "gastos figurativos")
_p(FIGURATIVOS, unir(_CONTRIB, "Tesoro Nacional"), "contribuciones figurativas del tesoro nacional")
_p(FIGURATIVOS, unir(_CONTRIB, "Instituciones de la seguridad social"),
   "contribuciones figurativas de instituciones de seguridad social")
_p(FIGURATIVOS, unir(_CONTRIB, "Organismos descentralizados"),
   "contribuciones figurativas de organismos descentralizados")
_p(FIGURATIVOS, unir(_CONTRIB, "Recursos afectados"), "contribuciones figurativas de recursos afectados")
# En 1993-2006 conviven las dos contrapartes («Empresas públicas y otros» y «PAMI, fondos
# fiduciarios y otros»); desde 2007 queda sólo la segunda.
_p(FIGURATIVOS, unir(_CONTRIB, "Empresas públicas y otros"), "contribuciones figurativas de empresas publicas y otros")
_p(FIGURATIVOS, unir(_CONTRIB, "PAMI, fondos fiduciarios y otros"), "contribuciones figurativas de pami fdos fiduciarios y otros")
_p(FIGURATIVOS, unir(_CONTRIB, "Ex cajas provinciales"), "contribuciones figurativas de excajas provinciales")

# Resultados
_p(RESULTADOS, "Resultado económico", "resultado economico ahorro desahorro")
_p(RESULTADOS, "Resultado primario", "superavit primario")
_p(RESULTADOS, unir("Resultado primario", "Sin privatizaciones"), "superavit primario sin privatizaciones")
# «Resultado financiero sin renta» (1993-2006) es la misma columna que «Resultado primario sin
# rentas» de las demás metodologías y frecuencias (mismo lugar en la hoja, mismos valores).
# Verificado con las 24 series: en todo el período y en los tres rótulos vale exactamente
# «Ingresos primarios antes de figurativos − Gastos primarios antes de figurativos» (residuo 0,0 en
# SPN, OD, ISS, EP_PAMI_FF y Ex_CP) y, en el SPN de dic-2025 y dic-2019, coincide con el cuadro AIF
# de la Secretaría de Hacienda (−3.346.192,2 = 11.597.104,8 − 14.943.297,0). En los subsectores
# no es «Resultado primario − rentas»: el primario (XIV) es después de figurativos y esta partida
# es antes. En el SPN los figurativos se anulan y las dos definiciones dan lo mismo.
_p(RESULTADOS, unir("Resultado primario", "Sin rentas", "Antes de figurativos"), "resultado primario sin rentas",
   "resultado primario sin renta", "resultado primario sin resnta", "resultado primarios sin renta",
   "resultado financiero sin renta", "resultados financieros sin renta")
_p(RESULTADOS, "Resultado financiero", "resultado financiero")
_p(RESULTADOS, unir("Resultado financiero", "Antes de figurativos"), "resultado financiero antes de figurativos")
_p(RESULTADOS, unir("Resultado financiero", "Sin privatizaciones"), "resultado financiero sin privatizaciones")

# Financiamiento
_p(FINANCIAMIENTO, unir(_FUENTES, "Disminución de la inversión financiera"),
   "fuentes financieras disminucion de la inversion financiera")
_p(FINANCIAMIENTO, unir(_FUENTES, "Incremento del patrimonio"), "fuentes financieras incremento del patrimonio")
_p(FINANCIAMIENTO, unir(_FUENTES, "Contribuciones figurativas para aplicaciones financieras"),
   "fuentes financieras contribuciones figurativas para aplicaciones financieras")
_p(FINANCIAMIENTO, unir(_FUENTES, "Endeudamiento externo"),
   "fuentes financieras endeudamiento publico e incremento de otros pasivos endeudamiento externo")
_p(FINANCIAMIENTO, unir(_FUENTES, "Endeudamiento interno"),
   "fuentes financieras endeudamiento publico e incremento de otros pasivos endeudamiento interno")
_p(FINANCIAMIENTO, unir(_FUENTES, "Endeudamiento en moneda local"),
   "fuentes financieras endeudamiento publico e incremento de otros pasivos endeudamiento en moneda local")
_p(FINANCIAMIENTO, unir(_FUENTES, "Endeudamiento en moneda extranjera"),
   "fuentes financieras endeudamiento publico e incremento de otros pasivos endeudamiento en moneda extranjera")
_p(FINANCIAMIENTO, unir(_FUENTES, "Incremento de otros pasivos"),
   "fuentes financieras endeudamiento publico e incremento de otros pasivos incremento otros pasivos")
_p(FINANCIAMIENTO, unir(_APLIC, "Inversión financiera"), "aplicaciones financieras inversion financiera")
_p(FINANCIAMIENTO, unir(_APLIC, "Disminución del patrimonio"), "aplicaciones financieras disminucion del patrimonio")
_p(FINANCIAMIENTO, unir(_APLIC, "Gastos figurativos para aplicaciones financieras"),
   "aplicaciones financieras gastos figurat para aplic financ")
_p(FINANCIAMIENTO, unir(_APLIC, "Amortización de deuda externa"),
   "aplicaciones financieras amort deudas y dism otros pasivos amortizacion deuda externa")
_p(FINANCIAMIENTO, unir(_APLIC, "Amortización de deuda interna"),
   "aplicaciones financieras amort deudas y dism otros pasivos amortizacion deuda interna")
_p(FINANCIAMIENTO, unir(_APLIC, "Amortización en moneda local"),
   "aplicaciones financieras amort deudas y dism otros pasivos amortizacion en moneda local")
_p(FINANCIAMIENTO, unir(_APLIC, "Amortización en moneda extranjera"),
   "aplicaciones financieras amort deudas y dism otros pasivos amortizacion en moneda extranjera")
_p(FINANCIAMIENTO, unir(_APLIC, "Disminución de otros pasivos"),
   "aplicaciones financieras amort deudas y dism otros pasivos disminucion otros pasivos")


def _clave(descripcion: str) -> str:
    """«Gastos corrientes  intereses y … prop. intereses deuda externa . Metodología 1993 - 2006»
    -> «gastos corrientes intereses y … prop intereses deuda externa»."""
    texto = plano(descripcion)
    texto = re.sub(r"\bmetodologia\b.*$", "", texto)
    texto = re.sub(r"[^a-z0-9]+", " ", texto).strip()
    return re.sub(r"(\s\d{4})+$", "", texto)


def _metodologia_cerrada(serie: Serie) -> str:
    """«1993-2006» o «2007-2014» si la serie es de una metodología que ya no se publica; si no, «».

    El período sale del título de la fuente («… Valores anuales 1993-2006»); la metodología vigente
    figura con un solo año («… Valores anuales 2017») y sigue abierta hasta hoy.
    """
    encontrado = re.search(r"\b((?:19|20)\d{2})\s*[-–]\s*((?:19|20)\d{2})\b", serie.nombre)
    return f"{encontrado.group(1)}-{encontrado.group(2)}" if encontrado else ""


def _aif(serie: Serie) -> Clasificacion | None:
    partida = _PARTIDAS.get(_clave(serie.descripcion))
    if partida is None:
        return None
    bloque, nombre = partida
    subsector = SUBSECTORES[serie.hoja]
    if serie.hoja != COBERTURA_NATURAL:
        nombre = unir(nombre, subsector)
    cerrada = _metodologia_cerrada(serie)
    if cerrada:
        return Clasificacion(tema=TEMA, indicador=INDICADOR_AIF, cobertura=HISTORICAS,
                             apertura=f"{subsector} · metodología {cerrada}", medida=bloque,
                             nombre=nombre, unidad=UNIDAD)
    return Clasificacion(tema=TEMA, indicador=INDICADOR_AIF, cobertura=subsector, apertura=bloque,
                         nombre=nombre, unidad=UNIDAD)


# --------------------------------------------------------------------------------------------
# Informe mensual de ingresos y gastos
# --------------------------------------------------------------------------------------------

# Segmento de la jerarquía (sin tildes ni mayúsculas) -> cómo se escribe.
_SEGMENTOS_IMIG = {
    "ingresos totales": "Ingresos totales",
    "ingresos tributarios": "Ingresos tributarios",
    "tributarios": "Ingresos tributarios",
    "aportes y contribuciones a la seguriad social": "Aportes y contribuciones a la seguridad social",
    "aportes y contribuciones a la seguridad social": "Aportes y contribuciones a la seguridad social",
    "contribuciones a la seguriad social": "Aportes y contribuciones a la seguridad social",
    "contribuciones a la seguridad social": "Aportes y contribuciones a la seguridad social",
    "bienes personales": "Bienes personales",
    "combustibles": "Combustibles",
    "debitos y creditos": "Débitos y créditos",
    "derechos de exportacion": "Derechos de exportación",
    "derechos de importacion": "Derechos de importación",
    "ganancias": "Ganancias",
    "impuestos internos": "Impuestos internos",
    "iva neto de reintegros": "IVA neto de reintegros",
    "resto de ingresos tributarios": "Resto de ingresos tributarios",
    "resto tributarios": "Resto de ingresos tributarios",
    "resto impositivos": "Resto de ingresos tributarios",
    "otros ingresos corrientes": "Otros ingresos corrientes",
    "ingresos no tributarios": "Ingresos no tributarios",
    "transferencias corrientes": "Transferencias corrientes",
    "resto de ingresos corrientes": "Resto de ingresos corrientes",
    "resto ingresos corrientes": "Resto de ingresos corrientes",
    "rentas de la propiedad": "Rentas de la propiedad",
    "rentas de la propiedad netas": "Rentas de la propiedad netas",
    "fgs cobradas al sector privado y publico financiero": "Rentas del FGS cobradas al sector privado y público financiero",
    "fgs cobradas al sector privado y sector publico financiero": "Rentas del FGS cobradas al sector privado y público financiero",
    "resto rentas de la propiedad": "Resto de rentas de la propiedad",
    "rentas por colocaciones de emisiones primarias": "Rentas por colocaciones de emisiones primarias",
    "ingresos de capital": "Recursos de capital",
    "gastos primarios": "Gastos primarios",
    "gastos corrientes primarios": "Gastos corrientes primarios",
    "gastos de capital": "Gastos de capital",
    "prestaciones sociales": "Prestaciones sociales",
    "jubilaciones y pensiones contributivas": "Jubilaciones y pensiones contributivas",
    "pensiones no contributivas": "Pensiones no contributivas",
    "prestaciones del inssjp": "Prestaciones del INSSJP",
    "asignaciones familiares activos pasivos y otras": "Asignaciones familiares de activos, pasivos y otras",
    "asignacion universal para la proteccion social": "Asignación Universal para Protección Social",
    "asignacion universal para proteccion social": "Asignación Universal para Protección Social",
    # Las asignaciones familiares y por hijo (hasta 2018) y las «Asignaciones (familiares y universales
    # para protección social)» de datos.gob.ar son el mismo agregado: el que después se abre en las dos.
    "asignaciones familiares y por hijo": "Asignaciones familiares y universales",
    "asignaciones familiares y universales para proteccion social": "Asignaciones familiares y universales",
    # Residuo de prestaciones sociales: no es un solo concepto. En 2018-2019 los informes lo definen
    # de tres maneras y, en los meses que se solapan, los valores difieren (2018-01: 1.549,0 contra
    # 3.630,8; 2019-01: 4.874,6 contra 5.479,6). Cada definición lleva su nombre. Las variantes
    # «Progresar, Argentina Trabaja, otros» (con la errata «Otras Programas») coinciden entre sí
    # (2017-05: 2.592,0), igual que «Progresar, Emergencia Social, otros» y «Proyectos Productivos
    # Comunitarios, otros» (2019-01 y 2019-02: diferencia 0,1).
    "otros programas progresar argentina trabaja otros": "Otros programas (Progresar, Argentina Trabaja, otros)",
    "otras programas progresar argentina trabaja otros": "Otros programas (Progresar, Argentina Trabaja, otros)",
    "otros programas progresar emergencia social otros": "Otros programas (Progresar, Emergencia Social, otros)",
    "otros programas proyectos productivos comunitarios otros": "Otros programas (Progresar, Emergencia Social, otros)",
    "otras programos sociales": "Otros programas sociales",
    "subsidios economicos": "Subsidios económicos",
    "gastos de funcionamiento": "Gastos de funcionamiento",
    "gastos de funcionamiento y otros": "Gastos de funcionamiento",
    # «Salarios» coincide valor por valor con las remuneraciones de la cuenta AIF del SPN.
    "salarios": "Remuneraciones",
    "otros gastos de funcionamiento": "Otros gastos de funcionamiento",
    "transferencias corrientes a provincias": "Transferencias corrientes a provincias",
    "desarrollo social": "Desarrollo social",
    "salud": "Salud",
    "seguridad social": "Seguridad social",
    "otras transferencias": "Otras transferencias",
    "otros gastos corrientes": "Otros gastos corrientes",
    "transferencias a universidades": "Transferencias a universidades",
    "deficit operativo de empresas publicas": "Déficit operativo de empresas públicas",
    "deficit operativo empresas publicas": "Déficit operativo de empresas públicas",
    "deficit pperativo de empresas publicas": "Déficit operativo de empresas públicas",
    "resultado operativo de empresas publicas": "Déficit operativo de empresas públicas",
    "resto": "Resto",
    "energia": "Energía",
    "transporte": "Transporte",
    "otras funciones": "Otras funciones",
    "agua potable y alcantarillado": "Agua potable y alcantarillado",
    "educacion": "Educación",
    "vivienda": "Vivienda",
    "otros": "Otros",
    "nacion": "Nación",
    "transferencias a provincias": "Transferencias a provincias",
    "fondo federal solidario": "Fondo Federal Solidario",
    # «Intereses» (2016-2019) e «Intereses Netos» (2016-2026) valen lo mismo mes a mes y los dos
    # coinciden con «Intereses netos» de la cuenta AIF del SPN (diferencia máxima 0,0 en 39 y 115
    # meses); el informe de dic-2025 lo rotula «Intereses Netos».
    "intereses": "Intereses netos",
    "intereses netos": "Intereses netos",
    "resultado primario": "Resultado primario",
    "resultado financiero": "Resultado financiero",
    "resultado financiero neto": "Resultado financiero neto",
    "resultado primario con p i p meta fmi": "Resultado primario con Programa de Inversiones Prioritarias",
    "resultado primario segun programa de facilidades extendidas": "Resultado primario según Programa de Facilidades Extendidas",
    "resultado primario segun programa facilidades extendidas": "Resultado primario según Programa de Facilidades Extendidas",
    "resultado primario excluye": "Resultado primario excluyendo rentas por emisión primaria excedentes",
    "programa de inversiones prioritarias p i p": "Programa de Inversiones Prioritarias",
    "ingresos extraordinarios": "Ingresos extraordinarios",
    "recursos extraordinarios": "Recursos extraordinarios",
    "derechos por licitacion bandas de frecuencias de 5g": "Derechos por licitación de bandas de frecuencias de 5G",
    "rentas por emision primaria que excenden limite del pfe":
        "Rentas por emisión primaria que exceden el límite del Programa de Facilidades Extendidas",
    "rentas por emision primaria que excenden limite del programa de facilidades extendidas":
        "Rentas por emisión primaria que exceden el límite del Programa de Facilidades Extendidas",
    "cumplimiento medida cautelar csjn 1864 22": "Cumplimiento de la medida cautelar de la Corte Suprema 1864/22",
    "ajustadores acuerdo fmi 1 ejecucion del gasto social abarcado y del gasto de capital con fuente externa":
        "Ajustadores del acuerdo con el FMI",
    "gasto social": "Gasto social",
    "gasto de capital con financiamiento externo": "Gasto de capital con financiamiento externo",
}

# En mecon-imig las funciones del gasto de capital cuelgan directamente de «Gastos primarios».
_FUNCIONES_DE_CAPITAL = frozenset({
    "agua potable y alcantarillado", "educacion", "energia", "transporte", "vivienda", "otros",
    "fondo federal solidario",
})


def _segmentos_imig(serie: Serie) -> list[str]:
    if serie.fuente == FUENTE_IMIG:
        texto = serie.nombre
    else:
        texto = re.sub(r"^\s*IMIG\.\s*", "", serie.descripcion)
    partes = re.split(r"\s+-\s+|\.\s+(?=[A-ZÁÉÍÓÚÑ])", texto)
    claves = (re.sub(r"[^a-z0-9]+", " ", plano(parte)).strip() for parte in partes)
    return [clave for clave in claves if clave]


def _jerarquia_imig(segmentos: list[str]) -> list[str]:
    """Unifica la jerarquía de las dos fuentes (segmentos sin tildes)."""
    if not segmentos:
        return segmentos
    if segmentos[0] == "gastos de capital":              # datos.gob.ar: «Gastos de capital. Educación. Nación»
        segmentos = ["gastos primarios", *segmentos]
    elif segmentos[0] == "gastos primarios" and len(segmentos) > 1:
        if segmentos[1] in _FUNCIONES_DE_CAPITAL:        # mecon-imig: «GASTOS PRIMARIOS - Educación - Nación»
            segmentos = [segmentos[0], "gastos de capital", *segmentos[1:]]
        elif segmentos[1] == "gastos corrientes primarios" and len(segmentos) > 2:
            segmentos = [segmentos[0], *segmentos[2:]]   # datos.gob.ar intercala el subtotal corriente
    elif segmentos[0] == "intereses":
        return segmentos
    if segmentos[:2] == ["gastos primarios", "otros gastos corrientes"] and segmentos[2:] == ["transferencias a universidades"]:
        segmentos = ["gastos primarios", "transferencias a universidades"]
    return segmentos


def _bloque_imig(segmentos: list[str]) -> tuple[str, str] | None:
    """(bloque, apertura del gasto) según la jerarquía unificada."""
    raiz = segmentos[0]
    if raiz == "ingresos totales":
        return INGRESOS, ""
    if raiz in {"intereses", "intereses netos"}:
        return GASTOS, CORRIENTES
    if raiz == "gastos primarios":
        if len(segmentos) == 1 or segmentos[1:] in (["gastos corrientes primarios"], ["gastos de capital"]):
            return GASTOS, TOTALES
        return GASTOS, DE_CAPITAL if segmentos[1] == "gastos de capital" else CORRIENTES
    if raiz.startswith("resultado"):
        return RESULTADOS, ""
    return AJUSTES, ""


def _imig(serie: Serie) -> Clasificacion | None:
    segmentos = _jerarquia_imig(_segmentos_imig(serie))
    if not segmentos or any(s not in _SEGMENTOS_IMIG for s in segmentos):
        return None
    bloque, apertura = _bloque_imig(segmentos)
    nombre = unir(*(_SEGMENTOS_IMIG[s] for s in segmentos))
    return Clasificacion(tema=TEMA, indicador=INDICADOR_IMIG, cobertura=bloque, apertura=apertura,
                         nombre=nombre, unidad=UNIDAD)



# --------------------------------------------------------------------------------------------
# Series que empalman dos o tres conceptos distintos bajo un mismo ID (no se publican; los datos no se tocan)
# --------------------------------------------------------------------------------------------

_EMPALME_PROGRAMAS = (
    "la columna vale «Otros programas (Progresar, Argentina Trabaja, otros)» hasta 2018-12, «(Progresar, Emergencia "
    "Social, otros / Proyectos Productivos Comunitarios, otros)» en 2019 y «Otros programas sociales» desde 2020-01: coincide "
    "mes a mes con las tres columnas de mecon-imig (2018-01: 1.549,0 = primera; 2019-01: 4.874,6 = segunda; "
    "2020-01: 31.111,2 = tercera) y esas definiciones difieren entre sí (2019-01: 4.874,6 contra 5.479,6)")
_EMPALME_RESTO = (
    "empalma dos alcances: hasta 2019-12 no incluye «Combustibles» y desde 2020-01 sí; en 2019 «Resto» + «Combustibles» "
    "de esta hoja coincide con el «Resto tributarios» de mecon-imig (diferencia 0,0 en los 12 meses) y mecon-imig "
    "(que desde 2019-01 sí incluye Combustibles) coincide con esta hoja desde 2020-01")

EXCLUIDAS: dict[tuple[str, str], str] = {
    ("datos.gob.ar", "452.2_OTROS_PROGMAS_0_T_15_11"): "«Otros programas» mensual: " + _EMPALME_PROGRAMAS,
    ("datos.gob.ar", "452.2_OTROS_PROGMAS_0_T_15_65"): "«Otros programas» anual: " + _EMPALME_PROGRAMAS,
    ("datos.gob.ar", "452.2_OTROS_PROGMAS_0_T_15_7"): "«Otros programas» trimestral: " + _EMPALME_PROGRAMAS,
    ("datos.gob.ar", "452.2_RESTO_TRIBIOS_0_T_17_0"): "«Resto de ingresos tributarios» mensual: " + _EMPALME_RESTO,
    ("datos.gob.ar", "452.2_RESTO_TRIBIOS_0_T_17_42"): "«Resto de ingresos tributarios» anual: " + _EMPALME_RESTO,
    ("datos.gob.ar", "452.1_RESTO_TRIBIOS_0_A_17_88"): "«Resto de ingresos tributarios» trimestral: " + _EMPALME_RESTO,
    ("mecon-imig", "ingresos-totales-tributarios-resto-tributarios"):
        "«Resto tributarios» empalma dos alcances: hasta 2018-12 sin Combustibles (el total tributario cierra con "
        "Combustibles como hijo) y desde 2019-01 con Combustibles (el total cierra con residuo exacto de −Combustibles: "
        "2019-01 −4.604, 2019-12 −6.815); el salto 2018-12 → 2019-01 (10.594 → 15.395) es el de Combustibles",
    ("mecon-imig", "gastos-primarios-otros-gastos-corrientes"):
        "«Otros Gastos Corrientes» empalma dos alcances: hasta 2018-12 incluye las transferencias a universidades "
        "(2018-12: 20.300,0 = déficit operativo 40,9 + universidades 13.841,9 + resto 6.417,2) y desde 2019-01 no las "
        "incluye (2019-01: 2.762,0 = −1.071,6 + 3.833,6); el informe de dic-2025 las lista aparte (499.381,4) y deja "
        "esta partida en 223.225,9",
}


# --------------------------------------------------------------------------------------------


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == FUENTE_IMIG:
        return _imig(serie)
    if serie.fuente != FUENTE_DATOS or serie.archivo != ARCHIVO:
        return None
    if serie.hoja == HOJA_IMIG:
        return _imig(serie)
    if serie.hoja in SUBSECTORES:
        return _aif(serie)
    return None
