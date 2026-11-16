"""Reglas del sector externo cambiario: tipo de cambio, mercado de cambios, balance cambiario y reservas.

Alcance (por ``Código fuente``, ``Archivo origen`` y ``Hoja origen``; nunca por el tema actual):

* fuentes completas: ``bcra-mc-bc``, ``bcra-itc``, ``bcra-com3500``, ``bcra-bandas``,
  ``bcra-cotizaciones``, ``bcra-irfcl``, ``iiep``, ``iiep-rin``, ``ambito-dolar-blue`` y
  ``ambito-dolar-mep``;
* ``datos.gob.ar``: ``sector_externo.xlsx`` (hojas 20 a 23), ``precios.xlsx`` (hojas 4.8.1, 4.8.2
  y 4.14) y ``dinero_bancos.xlsx`` (hojas 8.4 y 8.8).

Árbol (Tema «Sector externo», salvo los pasivos del BCRA de la hoja 8.4, que van a «Moneda y
sistema financiero» > «Balance del BCRA»):

    Tipo de cambio nominal      Oficial mayorista | Dólar blue | Dólar MEP | Bandas cambiarias |
                                Otras monedas | Índices nominales (ITCNM e ITCNB) | Series históricas
    Tipo de cambio real         Multilateral (ITCRM) | Bilaterales (ITCRB) | Ponderadores | Series históricas
    Reservas internacionales    Saldos | Composición > I.A, I.B, II y Notas del país (secciones de la planilla IRFCL)
    Balance cambiario           Cuenta corriente | Cuenta capital y financiera | Variación de reservas |
                                Ítem de memorándum | Otros
    Mercado de cambios          Resultado | Cuenta corriente | Cuenta capital y financiera | Base de operaciones

Cómo se deriva cada dimensión
-----------------------------

* ``bcra-mc-bc``: los rótulos de columna del libro vienen con celdas combinadas que el scraper
  rellenó hacia la derecha, de modo que el título del inventario arrastra restos de otras
  columnas («… - Total - Egresos - Otros pagos de Gobierno Nacional»). La identidad estable es el
  código de la fila de códigos (``bal001``, ``mlc001``, ``viajes001``, ``cap001``). Cada código se
  asigna con una tabla explícita, que se verificó contra los jerarquía real del libro (encabezados
  con celdas combinadas) y contra los valores: en el libro, cada «Total» es el subtotal del grupo
  que empieza en esa columna, y se comprobó que cada subtotal es la suma de sus componentes.
* ``datos.gob.ar`` (hojas 20 a 23): el título del cuadro se repite en todas las series; la
  identidad está en ``Variable``. Las series de la hoja 20 se llevan al mismo concepto del Balance
  Cambiario del BCRA (se verificó que coinciden en los períodos recientes).
* El ITCRM de ``bcra-itc`` y el de ``datos.gob.ar`` 4.8.2 son la misma serie (coinciden salvo
  revisiones: en los meses la diferencia máxima es 0,90 % para el ITCRM y 0,19 % para los ITCRB); el
  ITCRB con Estados Unidos del IIEP empalma con el del BCRA (idéntico en los 357 meses desde 1997).
  Los tres llevan el mismo nombre y el mismo grupo.
* Auditoría de noviembre de 2026 (fuentes bajadas de nuevo: anexo del mercado de cambios y balance
  cambiario, ITCRM/ITCNM, bandas, A 3500, planilla IRFCL de ago-2026 y API cambiaria del BCRA):
  - Anexo del BCRA: los 184 códigos se contrastaron con la jerarquía real de encabezados (celdas
    combinadas) y con identidades. En cada «Total» de la hoja «Balance Cambiario» y de «Mercado de
    Cambios» el total va ANTES de sus componentes (cada grupo empieza con su «Total»). Cierran
    exactamente (residuo < 1e-9): cc = bienes + servicios + primario + secundario; bienes =
    cobros − pagos (egresos con signo positivo en el balance y NEGATIVO en el mercado de cambios:
    allí total = ingresos + egresos); intereses = ingresos − egresos; egresos de intereses = FMI +
    otros organismos + otros + Gobierno Nacional; variación de reservas por transacciones = cuenta
    corriente + capital + financiera + concepto no informado; contable = transacciones + ajuste.
    La cuenta financiera (bal024, mlc066) NO es la suma exacta de sus componentes: entre sep-2019 y
    dic-2022 queda un residuo de −0,1 a −5,2 millones de dólares (mismo en las dos hojas), la parte
    de la apertura que el BCRA no publica; no se corrige.
  - Hojas 20 a 23 de ``sector_externo.xlsx``: mismo signo y mismo concepto que el anexo; difieren por
    revisiones (máximo 593 millones de dólares, en 2003-2012 casi todas las series; desde 2013 sólo
    unos pocos meses) y 7 series son idénticas.
  - Desde julio de 2025 el BCRA incluye los servicios digitales en «Otros servicios» del mercado de
    cambios (nota 8 del anexo); el anexo de tarjetas y viajes los mantiene dentro del total de
    consumos con tarjetas. Por eso viajes003 y viajes012 ya no son mlc016 y mlc033.

Convenciones de nombre: sin sufijo es el saldo o el valor del indicador; «(promedio)» es el
promedio del período; el signo (ingresos y egresos) va en el nombre como «Ingresos» y «Egresos».
"""

from __future__ import annotations

from dataclasses import replace

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

TEMA = "Sector externo"
TEMA_MONEDA = "Moneda y sistema financiero"

# Indicadores (nivel 2)
TC_NOMINAL = "Tipo de cambio nominal"
TC_REAL = "Tipo de cambio real"
RESERVAS = "Reservas internacionales"
BALANCE_CAMBIARIO = "Balance cambiario"
MERCADO_DE_CAMBIOS = "Mercado de cambios"
BALANCE_BCRA = "Balance del BCRA"

PESOS_POR_DOLAR = "Pesos por dólar"
MILLONES_DE_DOLARES = "Millones de dólares estadounidenses"
INDICE_ITC = "Índice 17-dic-2015=100"

HISTORICAS = "Series históricas"
PROMEDIO = "promedio"

# --------------------------------------------------------------------------------------
# Correcciones explícitas (auditoría de noviembre de 2026)
# --------------------------------------------------------------------------------------

_MC = "bcra-mc-bc"
EXCLUIDAS = {
    # No son series.
    (_MC, "Balance Cambiario|nan"):
        "Columna sin rótulo al final de la hoja: un contador (3762, 3765, 3768… de a 3 por mes, 2003-01 a"
        " 2025-04), no un agregado cambiario.",
    (_MC, "Datos Mercado de Cambios|Monto"):
        "Valor suelto de una tabla de 270.000 operaciones (anexo, sector, concepto, monto): el scraper"
        " se queda con la última fila de cada mes, que no es ningún agregado.",
    # Copias exactas de otra serie de la misma fuente (se conserva la de la hoja principal).
    (_MC, "Cta. Financiera por sector|cap001"):
        "Idéntica en los 285 meses a «Balance Cambiario|bal024» (cuenta financiera, total).",
    (_MC, "Mercado de Cambios|mlc101"):
        "Idéntica en los 285 meses a «Balance Cambiario|bal053» (ítem de memorándum), con el mismo nombre.",
    (_MC, "Anexo tarjetas y viajes|viajes010"):
        "Idéntica en los 129 meses a «Mercado de Cambios|mlc022» (ingresos por transporte de pasajeros).",
    (_MC, "Anexo tarjetas y viajes|viajes019"):
        "Idéntica en los 129 meses a «Mercado de Cambios|mlc039» (egresos por transporte de pasajeros).",
    ("bcra-com3500", "Serie de TCNPM|nan"):
        "Copia de «TCR diario y TCNPM|Tipo de Cambio Nominal Promedio Mensual» (diferencia máxima 5e-5 en"
        " 286 meses, redondeo a 4 decimales; ambas son el promedio de la A 3500).",
}

# Hoja 8.8: las series históricas se publican en la moneda de cada época, sin convertir. Cambios de
# moneda: peso ley 18.188 (1-1-1970), peso argentino (1-6-1983, = 10.000 pesos ley), austral
# (15-6-1985, = 1.000 pesos argentinos) y peso convertible (1-1-1992, = 10.000 australes). En el dato:
# el dólar libre cae de 81,13 a 8,22 el 2-6-1983 (÷10: antes estaba en miles de pesos ley, 81.130) y de
# 783,86 a 0,80 el 19-6-1985 (÷1.000); desde el 4-1-1990 (1.800 → 13,0) está en centenas de australes y
# termina en 99,9 el 1-1-1992 (9.990 australes), que empalma con 0,991 pesos del dólar estadounidense
# del 2-1-1992. El dólar financiero llega a 39.000 el 2-1-1983 (pesos ley), coherente con el libre de
# 48,83 (miles) del 3-1-1983. Ningún tramo se reescala.
_MONEDA_DE_CADA_EPOCA = "Moneda de cada época por dólar (sin convertir)"
_PESOS_LEY = "Pesos ley 18.188 por dólar"
UNIDADES = {
    # Todo el período es anterior al 1-6-1983: pesos ley.
    ("datos.gob.ar", "175.1_DR_TIPOICO_0_0_16"): _PESOS_LEY,       # tipo único 1970-71 y 1981-82
    ("datos.gob.ar", "175.1_DR_FINANTA_0_0_22"): _PESOS_LEY,       # financiero venta 1971-1983-01
    ("datos.gob.ar", "175.1_DR_FINAPRA_0_0_23"): _PESOS_LEY,       # financiero compra
    ("datos.gob.ar", "175.1_DR_FINANTA_0_0_21"): _PESOS_LEY,       # financiero especial venta 1975-76
    ("datos.gob.ar", "175.1_DR_FINAPRA_0_0_22"): _PESOS_LEY,       # financiero especial compra
    # Atraviesan cambios de moneda (y de escala).
    ("datos.gob.ar", "175.1_DR_LIBRNTA_0_0_17"): _MONEDA_DE_CADA_EPOCA,   # libre venta 1976-1992
    ("datos.gob.ar", "175.1_DR_LIBRPRA_0_0_18"): _MONEDA_DE_CADA_EPOCA,   # libre compra
    ("datos.gob.ar", "175.1_DR_OFICNTA_0_0_19"): _MONEDA_DE_CADA_EPOCA,   # oficial venta (pesos ley 1976, australes 1987-89)
    ("datos.gob.ar", "175.1_DR_OFICPRA_0_0_20"): _MONEDA_DE_CADA_EPOCA,   # oficial compra
}

# --------------------------------------------------------------------------------------
# Tipo de cambio nominal
# --------------------------------------------------------------------------------------

OFICIAL = "Oficial mayorista"
BLUE = "Dólar blue"
MEP = "Dólar MEP"
BANDAS = "Bandas cambiarias"
OTRAS_MONEDAS = "Otras monedas"

_A3500 = "Oficial mayorista (A 3500)"


def _tc(cobertura: str, *partes: str, medida: str = "", unidad: str = PESOS_POR_DOLAR) -> Clasificacion:
    return Clasificacion(
        tema=TEMA, indicador=TC_NOMINAL, cobertura=cobertura, unidad=unidad,
        nombre=con_medida(unir("Tipo de cambio", *partes), medida),
    )


def _com3500(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    if serie.hoja == "Serie de TCNPM" or "promedio mensual" in variable:
        return _tc(OFICIAL, _A3500, medida=PROMEDIO)
    if "referencia" in variable:
        return _tc(OFICIAL, _A3500)
    return None


def _bandas(serie: Serie) -> Clasificacion | None:
    variable = plano(serie.variable)
    for clave, lado in (("inferior", "Inferior"), ("superior", "Superior")):
        if clave in variable:
            return Clasificacion(
                tema=TEMA, indicador=TC_NOMINAL, cobertura=BANDAS, unidad=PESOS_POR_DOLAR,
                nombre=unir("Banda cambiaria", lado))
    return None


def _cotizaciones(serie: Serie) -> Clasificacion | None:
    # Tipo de cotización: pesos por unidad de moneda; tipo de pase: dólares por unidad de moneda.
    variable = serie.variable
    moneda = {"tipo_cotizacion_cny": ("Tipo de cotización", "Pesos por yuan"),
              "tipo_pase_cny": ("Tipo de pase", "Dólares estadounidenses por yuan")}.get(variable)
    if moneda is None:
        return None
    tipo, unidad = moneda
    return Clasificacion(
        tema=TEMA, indicador=TC_NOMINAL, cobertura=OTRAS_MONEDAS, unidad=unidad,
        nombre=unir("Tipo de cambio", "Yuan renminbi", tipo))


def _ambito(serie: Serie) -> Clasificacion | None:
    if serie.fuente == "ambito-dolar-mep":
        return _tc(MEP, "Dólar MEP")
    lado = {"ambito-dolar-blue-compra": "Compra", "ambito-dolar-blue-venta": "Venta"}.get(serie.id)
    if lado is None:
        return None
    return _tc(BLUE, "Dólar blue", lado)


# Hoja 8.8 «Tipos de cambio históricos»: el dólar de referencia A 3500 y los tipos vigentes
# antes de 2002. «Dólar estadounidense» (desde 1992, días corridos) NO es la A 3500 (promedio de la
# rueda): es el «tipo de cotización» del dólar que publica el BCRA (API de estadísticas cambiarias,
# campo tipoCotizacion: 1.198 el 14-4-2025, 1.135 el 16-4, 1.094 el 21-4; coincide en 338 de 425 días
# de 2025-26, el resto con diferencias de hasta 1 peso) y es la serie sobre la que el BCRA arma el ITCNB
# con Estados Unidos (ITCNB = esta serie / su valor del 17-12-2015 × 100, mediana de la diferencia
# 0,00 % en todos los años). Contra la A 3500: mediana 0,01-0,03 % en 2020-24, pero hasta 4-6 % en
# días sueltos (2018, 2023, 2025). Desde el 21-7-2026 la hoja repite 1.477,5 (76 días): el valor real
# de la API fue 1.483 el 22-7 y 1.520 el 5-10; por eso en oct-2026 parece diferir 3 % de la A 3500.
_TC_HISTORICOS = {
    "dolar_estadounidense": (OFICIAL, ("Dólar estadounidense", "Tipo de cotización")),
    "dolar_referencia_com_3500": (OFICIAL, (_A3500,)),
    "dolar_libre_compra": (HISTORICAS, ("Dólar libre", "Compra")),
    "dolar_libre_venta": (HISTORICAS, ("Dólar libre", "Venta")),
    "dolar_oficial_compra": (HISTORICAS, ("Dólar oficial", "Compra")),
    "dolar_oficial_venta": (HISTORICAS, ("Dólar oficial", "Venta")),
    "dolar_financiero_compra": (HISTORICAS, ("Dólar financiero", "Compra")),
    "dolar_financiero_venta": (HISTORICAS, ("Dólar financiero", "Venta")),
    "dolar_finan_esp_compra": (HISTORICAS, ("Dólar financiero especial", "Compra")),
    "dolar_finan_esp_venta": (HISTORICAS, ("Dólar financiero especial", "Venta")),
    "dolar_tipo_unico": (HISTORICAS, ("Tipo de cambio único",)),
}


def _tipos_historicos(serie: Serie) -> Clasificacion | None:
    regla = _TC_HISTORICOS.get(serie.variable)
    return _tc(regla[0], *regla[1]) if regla else None


def _promedio_anual(serie: Serie) -> Clasificacion | None:
    # Hoja 4.8.1 «Cotización peso argentino»: promedio anual (fecha 31-dic de cada año) del tipo de
    # cotización del dólar de la hoja 8.8: coincide con el promedio del año de esa serie diaria
    # (diferencia ≤ 0,03 % de 2003 a 2023; 2023: 296,701 contra 296,706) y no con el valor de fin de
    # año (13,04 contra 9,27 en 2015). Arranca en 2001 (0,9997), antes de la A 3500.
    if serie.hoja.strip() == "4.8.1":
        return _tc(OFICIAL, "Dólar estadounidense", "Tipo de cotización", medida=PROMEDIO)
    return None


# --------------------------------------------------------------------------------------
# Tipo de cambio real y nominal multilateral (ITCRM, ITCNM y bilaterales)
# --------------------------------------------------------------------------------------

MULTILATERAL_REAL = "Multilateral (ITCRM)"
BILATERALES_REALES = "Bilaterales (ITCRB)"
PONDERADORES = "Ponderadores"
# El ITCNM y los ITCNB son índices del tipo de cambio nominal (sin deflactar por precios): cuelgan de
# «Tipo de cambio nominal». Se verificó que el ITCNB con Estados Unidos es el tipo de cotización del
# dólar dividido por su valor del 17-12-2015, por 100 (mediana de la diferencia 0,00 % cada año).
INDICES_NOMINALES = "Índices nominales (ITCNM e ITCNB)"

# «Sudamérica» es el promedio simple de Brasil, Chile y Uruguay (nota del libro del BCRA: «*Promedio
# simple de valores de Brasil, Chile y Uruguay», en el ITCRB y en el ITCNB). Verificado con los valores:
# media aritmética de los tres bilaterales con diferencia 0 en los 10.881 días y en los 357 meses (la
# media geométrica difiere hasta 4-6 %).
SUDAMERICA = "Sudamérica (promedio simple de Brasil, Chile y Uruguay)"
_PAISES = {
    "brasil": "Brasil", "canada": "Canadá", "chile": "Chile", "china": "China",
    "estados-unidos": "Estados Unidos", "india": "India", "japon": "Japón", "mexico": "México",
    "reino-unido": "Reino Unido", "suiza": "Suiza", "uruguay": "Uruguay", "vietnam": "Vietnam",
    "zona-euro": "Zona Euro", "sudamerica": SUDAMERICA,
}
_ITCNM = "indice-de-tipo-de-cambio-nominal-multilateral-itcnm"


def _itc(cobertura: str, *partes: str, unidad: str = INDICE_ITC,
         indicador: str = TC_REAL) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=indicador, cobertura=cobertura,
                         nombre=unir(*partes), unidad=unidad)


def _itc_bcra(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if serie.hoja == "Ponderadores":
        pais = _PAISES.get(variable)
        return _itc(PONDERADORES, "ITCRM", "Ponderador", pais, unidad="%") if pais else None
    if variable == "itcrm":
        return _itc(MULTILATERAL_REAL, "ITCRM")
    if variable == _ITCNM:
        return _itc(INDICES_NOMINALES, "ITCNM", indicador=TC_NOMINAL)
    for prefijo, sigla, grupo, indicador in (("itcrb-", "ITCRB", BILATERALES_REALES, TC_REAL),
                                             ("itcnb-", "ITCNB", INDICES_NOMINALES, TC_NOMINAL)):
        if variable.startswith(prefijo):
            pais = _PAISES.get(variable[len(prefijo):])
            return _itc(grupo, sigla, pais, indicador=indicador) if pais else None
    return None


def _itcrm_datos(serie: Serie) -> Clasificacion | None:
    """Hoja 4.8.2 de precios.xlsx: el ITCRM y los ITCRB del BCRA (base 17-dic-2015), de datos.gob.ar."""
    clave = serie.variable.removeprefix("tipo_cambio_real_")
    if clave == "multilateral_actual":
        return _itc(MULTILATERAL_REAL, "ITCRM")
    pais = _PAISES.get(clave.replace("_", "-"))
    return _itc(BILATERALES_REALES, "ITCRB", pais) if pais else None


def _itcrm_historico(serie: Serie) -> Clasificacion | None:
    """Hoja 4.14: el ITCRM discontinuado, base diciembre de 2001."""
    if serie.variable == "tipo_cambio_real_multilateral":
        return _itc(HISTORICAS, "ITCRM", unidad="Índice dic-2001=100")
    return None


def _itcrb_iiep(serie: Serie) -> Clasificacion | None:
    if serie.id == "itcrb-eeuu-empalmado-importacion-m":
        return _itc(BILATERALES_REALES, "ITCRB", "Estados Unidos")
    return None


# --------------------------------------------------------------------------------------
# Reservas internacionales
# --------------------------------------------------------------------------------------

SALDOS = "Saldos"
COMPOSICION = "Composición"
SECCION_IA = "I.A Activos de reserva oficiales"
SECCION_IB = "I.B Otros activos en moneda extranjera"
SECCION_II = "II Drenajes netos predeterminados"
NOTAS_DEL_PAIS = "Notas del país"


def _reservas(cobertura: str, apertura: str, *partes: str, medida: str = "",
              unidad: str = MILLONES_DE_DOLARES) -> Clasificacion:
    return Clasificacion(
        tema=TEMA, indicador=RESERVAS, cobertura=cobertura, apertura=apertura, unidad=unidad,
        nombre=con_medida(unir("Reservas internacionales", *partes), medida))


_BRUTAS = "Brutas"
_DRENAJES = "Drenajes predeterminados"
_CUSTODIA = "Custodia"
_FORWARDS = "Posiciones cortas en forwards, futuros y swaps"

# Verificado contra la planilla PDF del BCRA de agosto de 2026 (temp0826.pdf): las 22 series coinciden
# al centavo, con sus secciones (I.A, I.A.1.a/b.i-iii, I.A.2 a 5, I.B, II.1, II.2 por vencimiento, II.3,
# total II) y sus signos: los activos son positivos y los flujos netos predeterminados (sección II)
# negativos, como en la planilla (II = II.1 + II.2 + II.3 exacto; II.2 = hasta 1 mes + 1 a 3 meses + 3 a
# 12 meses). Las «Reservas brutas» del IRFCL y del IIEP son idénticas (313 meses, diferencia ≤ 0,005 por
# redondeo). Problema de la fuente, sin corregir: de 2000-09 a 2006-06 «Divisas convertibles» (I.A.1)
# repite «Valores» (I.A.1.a) en lugar del total (69 meses; desde 2006-07 I.A = I.A.1 + 2 + 3 + 4 + 5).
# variable -> (cobertura, apertura, partes, medida, unidad)
_IRFCL = {
    "irfcl_ia_activos_reserva": (SALDOS, "", (_BRUTAS,)),
    "irfcl_ia1_divisas": (COMPOSICION, SECCION_IA, (_BRUTAS, "Divisas convertibles")),
    "irfcl_ia1a_valores": (COMPOSICION, SECCION_IA, (_BRUTAS, "Valores en divisas")),
    "irfcl_ia1b_bancos_centrales_bpi_fmi":
        (COMPOSICION, SECCION_IA, (_BRUTAS, "Depósitos en otros bancos centrales, BPI y FMI")),
    "irfcl_ia1b_bancos_residentes":
        (COMPOSICION, SECCION_IA, (_BRUTAS, "Depósitos en bancos con casa matriz en el país")),
    "irfcl_ia1b_bancos_exterior":
        (COMPOSICION, SECCION_IA, (_BRUTAS, "Depósitos en bancos con casa matriz en el exterior")),
    "irfcl_ia2_posicion_fmi": (COMPOSICION, SECCION_IA, (_BRUTAS, "Posición de reserva en el FMI")),
    "irfcl_ia3_deg": (COMPOSICION, SECCION_IA, (_BRUTAS, "Derechos especiales de giro")),
    "irfcl_ia4_oro": (COMPOSICION, SECCION_IA, (_BRUTAS, "Oro monetario")),
    "irfcl_ia5_otros": (COMPOSICION, SECCION_IA, (_BRUTAS, "Otros activos de reserva")),
    "irfcl_ib_otros_activos_me":
        (COMPOSICION, SECCION_IB, ("Otros activos en moneda extranjera no incluidos en reservas",)),
    "irfcl_ii_total": (COMPOSICION, SECCION_II, (_DRENAJES,)),
    "irfcl_ii1_prestamos_valores_depositos":
        (COMPOSICION, SECCION_II, (_DRENAJES, "Préstamos, valores y depósitos en moneda extranjera")),
    "irfcl_ii2_forwards_swaps": (COMPOSICION, SECCION_II, (_DRENAJES, _FORWARDS)),
    "irfcl_ii2_hasta_1_mes": (COMPOSICION, SECCION_II, (_DRENAJES, _FORWARDS, "Hasta 1 mes")),
    "irfcl_ii2_1_a_3_meses": (COMPOSICION, SECCION_II, (_DRENAJES, _FORWARDS, "De 1 a 3 meses")),
    "irfcl_ii2_3_a_12_meses": (COMPOSICION, SECCION_II, (_DRENAJES, _FORWARDS, "De 3 a 12 meses")),
    "irfcl_ii3_otros": (COMPOSICION, SECCION_II, (_DRENAJES, "Otros")),
    # Notas del país (c) sobre II.1, con la denominación de la nota del BCRA («Cheques Cancelatorios en
    # Otras Monedas y Certificados de Depósitos para la Inversión», «Depósitos en Otras Monedas»,
    # «Depósitos del Gobierno Nacional (en otras monedas)» y «Otras Obligaciones en Moneda Extranjera»).
    # Son las partidas de custodia: la serie «Custodia» del IIEP es la suma de estas tres (idéntica desde
    # 2012-05 salvo 2014-11 y 2016-08, ajustes de los autores; ago-2026: 16,12 + 2.779,07 + 13.601,18 = 16.396,37). La cuarta
    # partida de la nota (35,36 en ago-2026) no está en el inventario.
    "irfcl_nota_cheques_cdi":
        (COMPOSICION, NOTAS_DEL_PAIS,
         (_CUSTODIA, "Cheques cancelatorios y certificados de depósito para la inversión")),
    "irfcl_nota_depositos_gobierno":
        (COMPOSICION, NOTAS_DEL_PAIS, (_CUSTODIA, "Depósitos del Gobierno Nacional en otras monedas")),
    "irfcl_nota_depositos_otras_monedas":
        (COMPOSICION, NOTAS_DEL_PAIS, (_CUSTODIA, "Depósitos en otras monedas")),
}


def _irfcl(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    if variable == "irfcl_ia4_oro_onzas":
        return _reservas(COMPOSICION, SECCION_IA, _BRUTAS, "Oro monetario", medida="volumen",
                         unidad="Millones de onzas troy finas")
    regla = _IRFCL.get(variable)
    return _reservas(regla[0], regla[1], *regla[2]) if regla else None


_IIEP_RIN = {
    "rin_brutas": (_BRUTAS,),
    "rin_netas": ("Netas",),
    "rin_netas_planilla": ("Netas según planilla IRFCL",),
    "rin_liquidas": ("Líquidas",),
    "rin_propias": ("Propias",),
    "rin_propias_liquidas": ("Propias líquidas",),
    "rin_prestadas": ("Prestadas",),
    "rin_custodia": (_CUSTODIA,),
    "rin_rin_eff": ("Netas del acuerdo con el FMI (RIN-EFF)",),
}


def _iiep_rin(serie: Serie) -> Clasificacion | None:
    partes = _IIEP_RIN.get(serie.variable)
    return _reservas(SALDOS, "", *partes) if partes else None


# --------------------------------------------------------------------------------------
# Hoja 8.4 de dinero_bancos.xlsx: reservas y pasivos del BCRA
# --------------------------------------------------------------------------------------

MILLONES_DE_PESOS = "Millones de pesos"
PASIVOS = "Pasivo"
PASES = "Pases"
ACTIVOS = "Activo"


def _balance_bcra(cobertura: str, *partes: str, unidad: str = "") -> Clasificacion:
    # Todo el balance del BCRA de la hoja 8.4 está en millones de pesos (la fuente informa la unidad sólo
    # en algunas series). En particular «Letras en dólares» (título: «en millones de dólares») son los
    # dólares de las letras valuados en pesos: el monto en dólares (valor ÷ tipo de cambio de valuación)
    # es constante entre revisiones (4.889,686408 los 11 días del 20 al 30-9-2026; 13.016,01 el
    # 31-12-2025) y el valor (7,4 billones en sep-2026) es del orden de los pasivos monetarios en pesos
    # (67 billones), no de las reservas en dólares (46.092 millones).
    return Clasificacion(tema=TEMA_MONEDA, indicador=BALANCE_BCRA, cobertura=cobertura,
                         nombre=unir("Balance del BCRA", *partes), unidad=unidad)


# variable -> clasificación; las de reservas y tipo de cambio se resuelven aparte.
_PASIVOS_84 = {
    "pasivos_monetarios": _balance_bcra(PASIVOS, "Pasivos monetarios", unidad=MILLONES_DE_PESOS),
    "pasivos_monetarios_letras_pesos":
        _balance_bcra(PASIVOS, "Pasivos monetarios", "Letras", unidad=MILLONES_DE_PESOS),
    "pasivos_monetarios_letras_dolares":
        _balance_bcra(PASIVOS, "Pasivos monetarios", "Letras en dólares", unidad=MILLONES_DE_PESOS),
    "letras_liquidez_saldo_pesos": _balance_bcra(PASIVOS, "Letras de liquidez", unidad=MILLONES_DE_PESOS),
    "otros_depositos_gobierno":
        _balance_bcra(PASIVOS, "Otros depósitos del Gobierno", unidad=MILLONES_DE_PESOS),
    "posicion_neta_pases": _balance_bcra(PASES, "Pases", "Posición neta", unidad=MILLONES_DE_PESOS),
    # Pasivos − activos = posición neta (diferencia máxima 1,0 en 8.673 días): millones de pesos.
    "pos_neta_det_des_pases_pasivos":
        _balance_bcra(PASES, "Pases", "Posición neta de pases pasivos", unidad=MILLONES_DE_PESOS),
    "pos_neta_det_des_pases_activos":
        _balance_bcra(PASES, "Pases", "Posición neta de pases activos", unidad=MILLONES_DE_PESOS),
    "redescuentos_adelantos_iliquidez":
        _balance_bcra(ACTIVOS, "Redescuentos y adelantos por iliquidez", unidad=MILLONES_DE_PESOS),
}


def _reservas_y_pasivos(serie: Serie) -> Clasificacion | None:
    variable = serie.variable
    # En esta hoja la serie mensual es el promedio de la diaria (se verificó con los valores).
    mensual = serie.frecuencia == "M"
    if variable == "reservas_internacionales_dolares":
        return _reservas(SALDOS, "", _BRUTAS, medida=PROMEDIO if mensual else "")
    if variable == "tipo_cambio_valuacion":
        # Es la A 3500 desde 2005 (coincide en 8.200 de 8.673 días; desde 2005 sólo difieren 22 días, de
        # 2003 a 2004 casi todos, hasta 3,3 %), por eso lleva su nombre propio y no el de la A 3500.
        return _tc(OFICIAL, "De valuación de las reservas", medida=PROMEDIO if mensual else "")
    pasivo = _PASIVOS_84.get(variable)
    if pasivo is not None and mensual:
        return replace(pasivo, nombre=con_medida(pasivo.nombre, PROMEDIO))
    return pasivo


# --------------------------------------------------------------------------------------
# Balance cambiario y mercado de cambios
# --------------------------------------------------------------------------------------

CUENTA_CORRIENTE = "Cuenta corriente"
CUENTA_CAPITAL_Y_FINANCIERA = "Cuenta capital y financiera"
VARIACION_DE_RESERVAS = "Variación de reservas"
MEMORANDUM = "Ítem de memorándum"
OTROS = "Otros"
RESULTADO = "Resultado"
BASE_DE_OPERACIONES = "Base de operaciones"

TOTAL = "Total"
BIENES = "Bienes"
SERVICIOS = "Servicios"
INGRESO_PRIMARIO = "Ingreso primario"
INGRESO_SECUNDARIO = "Ingreso secundario"
CUENTA_DE_CAPITAL = "Cuenta de capital"
CUENTA_FINANCIERA = "Cuenta financiera"
POR_MODALIDAD = "Por modalidad de pago"
POR_SECTOR = "Por sector"
POR_TIPO_DE_SERVICIO = "Por tipo de servicio"
VIAJES_Y_TARJETAS = "Viajes y tarjetas"

INGRESOS = "Ingresos"
EGRESOS = "Egresos"
COBROS = "Cobros de exportaciones"
PAGOS = "Pagos de importaciones"

# Una entrada es (cobertura, apertura, segunda apertura, partes del nombre).
_Lugar = tuple[str, str, str, tuple[str, ...]]


def _lugar(indicador: str, lugar: _Lugar) -> Clasificacion:
    cobertura, apertura, segunda, partes = lugar
    return Clasificacion(
        tema=TEMA, indicador=indicador, cobertura=cobertura, apertura=apertura, medida=segunda,
        nombre=unir(indicador, *partes), unidad=MILLONES_DE_DOLARES)


# Tipos de servicio con la misma redacción en el mercado de cambios y en el balance cambiario.
_TARJETAS = "Consumos con tarjetas y viajes"
_TIPOS_DE_SERVICIO = (
    _TARJETAS, "Servicios empresariales, profesionales y técnicos",
    "Servicios de comunicaciones, telecomunicaciones", "Cargos por el uso de la propiedad intelectual",
    "Servicios financieros", "Fletes", "Servicios de transporte de pasajeros", "Otros servicios de transporte",
    "Servicios de seguros", "Servicios relacionados con el comercio", "Servicios de construcción",
    "Servicios de arrendamiento operativo", "Servicios de información e informática",
    "Servicios personales, culturales y recreativos", "Servicios de gobierno", "Otros servicios",
)

# Sectores del Balance Cambiario por sector (hoja 22), con la grafía del BCRA.
_SECTORES = {
    "alimentos_bebidas_tabaco": "Alimentos, bebidas y tabaco",
    "comercio": "Comercio",
    "prod_minerales_no_metalicos": "Productos minerales no metálicos (cementos, cerámicos y otros)",
    "industria_textil_curtidos": "Industria textil y curtidos",
    "otros_industria_manufacturera": "Otros de la industria manufacturera",
    "otros_sector_privado_no_financiero": "Otros del sector privado no financiero",
    "agua": "Agua",
    "entidades_financieras_cambiarias": "Entidades financieras y cambiarias",
    "agric_ganaderia_otras_activid_primarias": "Agricultura, ganadería y otras actividades primarias",
    "informatica": "Informática",
    "sector_publico": "Sector público",
    "industria_quimica_caucho_plastico": "Industria química, caucho y plástico",
    "construccion": "Construcción",
    "metales_comunes_elaboracion": "Metales comunes y elaboración",
    "petroleo": "Petróleo",
    "gastronomia": "Gastronomía",
    "comunicaciones": "Comunicaciones",
    "industria_papel_ediciones_impresiones": "Industria de papel, ediciones e impresiones",
    "entretenimiento": "Entretenimiento",
    "gas_extraccion_transporte_distribucion": "Gas (extracción, transporte y distribución)",
    "electr_generacion_transporte_distribucion": "Electricidad (generación, transporte y distribución)",
    "maquinarias_equipos": "Maquinarias y equipos",
    "mineria": "Minería",
    "turismo_hoteleria": "Turismo y hotelería",
    "industria_automotriz": "Industria automotriz",
    "seguros": "Seguros",
    "oleaginosos_cerealeros": "Oleaginosos y cerealeros",
    "transporte": "Transporte",
}


def _financiera(*partes: str) -> _Lugar:
    return (CUENTA_CAPITAL_Y_FINANCIERA, CUENTA_FINANCIERA, "", partes)


def _tabla_balance() -> dict[int, _Lugar]:
    cc = CUENTA_CORRIENTE
    bal: dict[int, _Lugar] = {
        1: (cc, TOTAL, "", (cc,)),
        2: (cc, BIENES, TOTAL, (BIENES,)),
        3: (cc, BIENES, TOTAL, (BIENES, COBROS)),
        4: (cc, BIENES, TOTAL, (BIENES, PAGOS)),
        5: (cc, SERVICIOS, TOTAL, (SERVICIOS,)),
        6: (cc, SERVICIOS, TOTAL, (SERVICIOS, INGRESOS)),
        7: (cc, SERVICIOS, TOTAL, (SERVICIOS, EGRESOS)),
        8: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO,)),
        9: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses")),
        10: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses", INGRESOS)),
        11: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses", EGRESOS)),
        12: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses", EGRESOS, "Pagos al FMI")),
        13: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses", EGRESOS,
                                        "Pagos a otros organismos internacionales y bilaterales")),
        14: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses", EGRESOS, "Otros pagos de intereses")),
        15: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Intereses", EGRESOS,
                                        "Otros pagos de intereses del Gobierno Nacional")),
        16: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Utilidades y dividendos y otras rentas")),
        17: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Utilidades y dividendos y otras rentas", INGRESOS)),
        18: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, "Utilidades y dividendos y otras rentas", EGRESOS)),
        19: (cc, INGRESO_SECUNDARIO, "", (INGRESO_SECUNDARIO,)),
        20: (cc, INGRESO_SECUNDARIO, "", (INGRESO_SECUNDARIO, INGRESOS)),
        21: (cc, INGRESO_SECUNDARIO, "", (INGRESO_SECUNDARIO, EGRESOS)),
        23: (CUENTA_CAPITAL_Y_FINANCIERA, CUENTA_DE_CAPITAL, "", ("Cuenta capital",)),
        24: _financiera("Cuenta financiera"),
        25: _financiera("Inversión directa de no residentes"),
        26: _financiera("Inversión directa de no residentes", INGRESOS),
        27: _financiera("Inversión directa de no residentes", EGRESOS),
        28: _financiera("Inversión de portafolio de no residentes"),
        29: _financiera("Inversión de portafolio de no residentes", INGRESOS),
        30: _financiera("Inversión de portafolio de no residentes", EGRESOS),
        31: _financiera("Préstamos financieros, títulos de deuda y líneas de crédito"),
        32: _financiera("Préstamos financieros, títulos de deuda y líneas de crédito", INGRESOS),
        33: _financiera("Préstamos financieros, títulos de deuda y líneas de crédito", EGRESOS),
        34: _financiera("Operaciones con el FMI"),
        35: _financiera("Operaciones con el FMI", INGRESOS),
        36: _financiera("Operaciones con el FMI", EGRESOS),
        37: _financiera("Préstamos de otros organismos internacionales y bilaterales"),
        38: _financiera("Préstamos de otros organismos internacionales y bilaterales", INGRESOS),
        39: _financiera("Préstamos de otros organismos internacionales y bilaterales", EGRESOS),
        40: _financiera(_FAE_SPNF),
        41: _financiera(_FAE_SPNF, INGRESOS),
        42: _financiera(_FAE_SPNF, EGRESOS),
        43: _financiera("Operaciones de canje por transferencias con el exterior"),
        44: _financiera("Formación de activos externos del sector financiero (PGC)"),
        45: _financiera("Formación de activos externos del sector público"),
        46: _financiera("Compra-venta de títulos valores"),
        47: _financiera("Otras operaciones del sector público"),
        48: _financiera(_OTROS_MOVIMIENTOS),
        49: (VARIACION_DE_RESERVAS, "", "", ("Concepto no informado por el cliente",)),
        50: (VARIACION_DE_RESERVAS, "", "", ("Variación de reservas internacionales", "Por transacciones")),
        51: (VARIACION_DE_RESERVAS, "", "", ("Variación de reservas internacionales", "Contable")),
        52: (VARIACION_DE_RESERVAS, "", "", ("Variación de reservas internacionales",
                                              "Ajuste por tipo de pase y valuación")),
        53: (MEMORANDUM, "", "", (_TARJETAS_EXTERIOR,)),
    }
    return bal


_FAE_SPNF = "Formación de activos externos del sector privado no financiero"
_OTROS_MOVIMIENTOS = "Otros movimientos netos de la cuenta financiera"
_TARJETAS_EXTERIOR = "Pago del saldo en moneda extranjera por uso de tarjetas en el exterior"
_BALANCE = _tabla_balance()

# Cuenta financiera por sector (hoja «Cta. Financiera por sector»). Su «Otros movimientos netos» es el
# resto de la apertura por sector y no coincide con el de la apertura por instrumento (bal048).
_CAP = {
    1: _BALANCE[24],
    2: _financiera("Cuenta financiera", "Sector privado no financiero"),
    3: _financiera("Cuenta financiera", "Sector financiero"),
    4: _financiera("Cuenta financiera", "Gobierno general y BCRA"),
    5: _financiera("Cuenta financiera", "Otros movimientos netos"),
}

# Hoja 20 de sector_externo.xlsx: variable -> código del Balance Cambiario del BCRA.
_HOJA_20 = {
    "ajuste_reservas_internac_tipo_pase_valuacion": 52,
    "concepto_no_informado_cliente_neto": 49,
    "cuenta_corriente_cambiaria_total_servicios": 5,
    "cuenta_corriente_cambiaria_ingresos_servicios": 6,
    "cuenta_corriente_cambiaria_egresos_servicios": 7,
    "cuenta_corriente_cambiaria_total_bienes": 2,
    "cuenta_corriente_cambiaria_pagos_importaciones_bienes": 4,
    "c_corriente_cobros_expo_bienes": 3,
    "cuenta_corriente_cambiaria_egreso_secundario": 21,
    "cuenta_corriente_cambiaria_ingreso_secundario": 20,
    "cuenta_corriente_cambiaria_total_ingreso_secundario": 19,
    "cuenta_corriente_cambiaria_total_ingreso_primario": 8,
    "cuenta_corriente_cambiaria_ingreso_primario_total_intereses": 9,
    "c_cap_financ_total_cuenta_financ": 24,
    "c_cap_financ_cuenta_financ_total_operac_con_fmi": 34,
    "c_cap_financ_cuenta_financ_egresos_operac_con_fmi": 36,
    "c_cap_financ_cuenta_financ_ingresos_operac_con_fmi": 35,
    "c_cap_financ_c_financ_form_activos_ext_sect_financ_pgc": 44,
    "c_cap_financ_c_financ_form_activos_ext_sect_publico": 45,
    "c_cap_financ_c_financ_compra_venta_titulos_valores": 46,
    "c_cap_financ_cuenta_capital_cambiaria": 23,
    "c_cap_financ_cuenta_financ_otros_movimientos_netos": 48,
    "c_cap_financ_c_financ_otras_operac_sect_publico_neto": 47,
    "c_corriente_ingreso_primariopagos_intereses_al_fmi": 12,
    "c_corriente_ingreso_primario_egresos_intereses": 11,
    "c_corriente_ingreso_primario_ingresos_intereses": 10,
    "c_corriente_ingreso_primariootros_pagos_intereses": 14,
    "c_corr_ingr_prim_pag_intereses_otros_org_int": 13,
    "c_corr_ingr_prim_otros_pagos_intereses_gobierno_nacional": 15,
    "c_corr_ingr_prim_total_utilidades_dividendos_otras_rentas": 16,
    "c_corr_ingr_prim_ingres_utilidades_dividendos_otras_rentas": 17,
    "c_corr_ingr_prim_egres_utilidades_dividendos_otras_rentas": 18,
    "c_k_financ_c_financ_total_inv_directa_no_residentes": 25,
    "c_k_financ_c_financ_ingres_inv_directa_no_residentes": 26,
    "c_k_financ_c_financ_egres_inv_directa_no_residentes": 27,
    "c_k_finc_c_finc_operac_canje_transf_con_ext": 43,
    "c_k_finc_c_finc_total_form_activos_ext_sect_priv_no_finc": 40,
    "c_k_finc_c_finc_ingres_form_activos_ext_sect_priv_no_finc": 41,
    "c_k_finc_c_finc_egres_form_activos_ext_sect_priv_no_finc": 42,
    "c_k_finc_c_finc_total_prest_otros_org_int_otros_bilat": 37,
    "c_k_finc_c_finc_ingres_prest_otros_org_int_otros_bilat": 38,
    "c_k_finc_c_finc_egres_prest_otros_org_int_otros_bilat": 39,
    "c_k_finc_c_finc_total_prest_fincs_tit_deud_lin_cred": 31,
    "c_k_finc_c_finc_ingres_prest_fincs_tit_deud_lin_cred": 32,
    "c_k_finc_c_finc_egres_prest_fincs_tit_deud_lin_cred": 33,
    "c_k_finc_c_finc_total_inv_portafolio_no_residentes": 28,
    "c_k_finc_c_finc_ingres_inv_portafolio_no_residentes": 29,
    "c_k_finc_c_finc_egres_inv_portafolio_no_residentes": 30,
    "total_cuenta_corriente_cambiaria": 1,
    "variacion_contable_reservas_internacionales": 51,
    "variacion_reservas_internacionales_transacciones": 50,
}
_TOTAL_CAPITAL_Y_FINANCIERA: _Lugar = (
    CUENTA_CAPITAL_Y_FINANCIERA, TOTAL, "", ("Cuenta capital y financiera",))

# Hoja 21: cobros y pagos por modalidad de pago.
_HOJA_21 = {
    "total_bienes": _BALANCE[2],
    "cobros_exportaciones_bienes": _BALANCE[3],
    "pagos_importaciones_bienes": _BALANCE[4],
}
# «Cobros de exportaciones» es la denominación del BCRA de la primera modalidad (código de concepto B01,
# 101 «Cobros de exportaciones»; encabezado del anexo y título «Bienes Cobros de exportaciones» de la hoja
# 21). Es el resto de los cobros que no son anticipados ni financiados: las cinco modalidades suman el
# total sin residuo.
_MODALIDADES_COBRO = {
    "bienes_cobros_exportaciones": "Cobros de exportaciones",
    "bienes_cobros_anticipados_exportaciones": "Cobros anticipados",
    "bienes_cobros_financiamientos_locales_exportaciones": "Financiaciones locales",
    "bienes_cobros_prefinanciaciones_expo_del_exterrior": "Prefinanciaciones del exterior",
    "bienes_cobros_otros_ingresos_bienes": "Otros ingresos por bienes",
}
_MODALIDADES_PAGO = {
    "bienes_pagos_anticipados_importaciones": "Pagos anticipados",
    "bienes_pagos_a_la_vista_importaciones": "Pagos a la vista",
    "bienes_pagos_diferidos_impo_otros_egresos_bienes": "Pagos diferidos y otros egresos por bienes",
}

# Hoja 23: cobros y pagos por tipo de servicio. variable -> (sentido, índice en _TIPOS_DE_SERVICIO).
_SERVICIOS_23 = {
    "cobro_servicios_turismo_viajes": (INGRESOS, 0), "pago_servicios_turismo_viajes": (EGRESOS, 0),
    "cobro_servicios_empresariales_profesionales_tecnicos": (INGRESOS, 1),
    "pago_servicios_empresariales_profesionales_tecnicos": (EGRESOS, 1),
    "cobro_servicios_comunicaciones_telecomunicaciones": (INGRESOS, 2),
    "pago_servicios_comunicaciones_telecomunicaciones": (EGRESOS, 2),
    "cobr_serv_cargos_uso_la_prop_intelect": (INGRESOS, 3), "pag_serv_cargos_uso_prop_intelec": (EGRESOS, 3),
    "cobros_servicios_financieros": (INGRESOS, 4), "pagos_servicios_financieros": (EGRESOS, 4),
    "cobro_servicios_fletes": (INGRESOS, 5), "pago_servicios_fletes": (EGRESOS, 5),
    "cobro_servicios_transporte_pasajeros": (INGRESOS, 6), "pago_servicios_transporte_pasajeros": (EGRESOS, 6),
    "cobro_otros_servicios_transporte": (INGRESOS, 7), "pago_otros_servicios_transporte": (EGRESOS, 7),
    "cobro_servicios_seguros": (INGRESOS, 8), "pago_servicios_seguros": (EGRESOS, 8),
    "cobro_servicios_relacionados_con_comercio": (INGRESOS, 9),
    "pago_servicios_relacionados_con_comercio": (EGRESOS, 9),
    "cobro_servicios_construccion": (INGRESOS, 10), "pago_servicios_construccion": (EGRESOS, 10),
    "cobro_servicios_arrendamiento_operativo": (INGRESOS, 11),
    "pago_servicios_arrendamiento_operativo": (EGRESOS, 11),
    "cobro_servicios_informacion_informatica": (INGRESOS, 12),
    "pago_servicios_informacion_informatica": (EGRESOS, 12),
    "cobro_servicios_personales_culturales_recreativos": (INGRESOS, 13),
    "pago_servicios_personales_culturales_recreativos": (EGRESOS, 13),
    "cobro_servicios_gobierno": (INGRESOS, 14), "pago_servicios_gobierno": (EGRESOS, 14),
    "otros_cobros_servicios": (INGRESOS, 15), "otros_pagos_servicios": (EGRESOS, 15),
}
_SERVICIOS_23_TOTALES = {
    "total_servicios": _BALANCE[5],
    "servicios_total_ingresos_servicios": _BALANCE[6],
    "total_egresos_servicios": _BALANCE[7],
}


def _balance_datos(serie: Serie) -> Clasificacion | None:
    """Hojas 20 a 23 de sector_externo.xlsx, al mismo grupo y con los mismos nombres que el BCRA."""
    variable = serie.variable
    hoja = serie.hoja
    lugar: _Lugar | None = None
    if hoja == "20.Balance cambiario":
        if variable == "total_cuenta_capital_financiera_cambiaria":
            lugar = _TOTAL_CAPITAL_Y_FINANCIERA
        elif variable in _HOJA_20:
            lugar = _BALANCE[_HOJA_20[variable]]
    elif hoja == "21.Bienes por modalidad de pago":
        if variable in _HOJA_21:
            lugar = _HOJA_21[variable]
        elif variable in _MODALIDADES_COBRO:
            lugar = (CUENTA_CORRIENTE, BIENES, POR_MODALIDAD, (BIENES, COBROS, _MODALIDADES_COBRO[variable]))
        elif variable in _MODALIDADES_PAGO:
            lugar = (CUENTA_CORRIENTE, BIENES, POR_MODALIDAD, (BIENES, PAGOS, _MODALIDADES_PAGO[variable]))
    elif hoja == "22.Bienes por sector":
        totales = {"total_bienes": _BALANCE[2], "bienes_cobros_totales_exportaciones": _BALANCE[3],
                   "pagos_importaciones_bienes": _BALANCE[4]}
        if variable in totales:
            lugar = totales[variable]
        else:
            for prefijo, concepto in (("bienes_cobros_", COBROS), ("bienes_cobr_", COBROS),
                                      ("bienes_pagos_", PAGOS), ("bienes_pag_", PAGOS)):
                if variable.startswith(prefijo) and variable[len(prefijo):] in _SECTORES:
                    sector = _SECTORES[variable[len(prefijo):]]
                    lugar = (CUENTA_CORRIENTE, BIENES, POR_SECTOR, (BIENES, concepto, sector))
                    break
    elif hoja == "23.Servicios por tipo":
        if variable in _SERVICIOS_23_TOTALES:
            lugar = _SERVICIOS_23_TOTALES[variable]
        elif variable in _SERVICIOS_23:
            sentido, indice = _SERVICIOS_23[variable]
            lugar = (CUENTA_CORRIENTE, SERVICIOS, POR_TIPO_DE_SERVICIO,
                     (SERVICIOS, sentido, _TIPOS_DE_SERVICIO[indice]))
    return _lugar(BALANCE_CAMBIARIO, lugar) if lugar else None


# --- Mercado de Cambios (hoja «Mercado de Cambios» del anexo del BCRA) ---------------------

def _tabla_mercado() -> dict[int, _Lugar]:
    cc = CUENTA_CORRIENTE
    mlc: dict[int, _Lugar] = {
        1: (RESULTADO, "", "", (RESULTADO,)),
        2: (cc, TOTAL, "", (cc,)),
        3: (cc, BIENES, TOTAL, (BIENES,)),
        4: (cc, BIENES, TOTAL, (BIENES, COBROS)),
        10: (cc, BIENES, TOTAL, (BIENES, PAGOS)),
        14: (cc, SERVICIOS, TOTAL, (SERVICIOS,)),
        15: (cc, SERVICIOS, TOTAL, (SERVICIOS, INGRESOS)),
        32: (cc, SERVICIOS, TOTAL, (SERVICIOS, EGRESOS)),
        49: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO,)),
        50: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, INGRESOS)),
        55: (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, EGRESOS)),
        60: (cc, INGRESO_SECUNDARIO, "", (INGRESO_SECUNDARIO,)),
        61: (cc, INGRESO_SECUNDARIO, "", (INGRESO_SECUNDARIO, INGRESOS)),
        62: (cc, INGRESO_SECUNDARIO, "", (INGRESO_SECUNDARIO, EGRESOS)),
        63: (CUENTA_CAPITAL_Y_FINANCIERA, CUENTA_DE_CAPITAL, "", ("Cuenta capital",)),
        64: (CUENTA_CAPITAL_Y_FINANCIERA, CUENTA_DE_CAPITAL, "", ("Cuenta capital", INGRESOS)),
        65: (CUENTA_CAPITAL_Y_FINANCIERA, CUENTA_DE_CAPITAL, "", ("Cuenta capital", EGRESOS)),
        66: _financiera("Cuenta financiera"),
        96: _financiera("Operaciones de canje por transferencias con el exterior"),
        97: _financiera("Compra-venta de títulos valores por entidades financieras"),
        98: _financiera("Suscripción primaria de valores en moneda extranjera por entidades financieras"),
        99: _financiera("Otros movimientos de la cuenta financiera"),
        100: (RESULTADO, "", "", ("Concepto no informado por el cliente",)),
    }
    modalidades = {
        5: (COBROS, "Cobros de exportaciones"), 6: (COBROS, "Cobros anticipados"),
        7: (COBROS, "Financiaciones locales"), 8: (COBROS, "Prefinanciaciones del exterior"),
        9: (COBROS, "Otros ingresos por bienes"),
        11: (PAGOS, "Pagos anticipados"), 12: (PAGOS, "Pagos a la vista"),
        13: (PAGOS, "Pagos diferidos y otros egresos por bienes"),
    }
    for codigo, (concepto, modalidad) in modalidades.items():
        mlc[codigo] = (cc, BIENES, POR_MODALIDAD, (BIENES, concepto, modalidad))
    for sentido, primero in ((INGRESOS, 16), (EGRESOS, 33)):
        for desplazamiento, tipo in enumerate(_TIPOS_DE_SERVICIO):
            mlc[primero + desplazamiento] = (
                cc, SERVICIOS, POR_TIPO_DE_SERVICIO, (SERVICIOS, sentido, tipo))
    for codigo, rubro in ((51, "Intereses"), (52, "Utilidades y dividendos"), (53, "Remuneraciones"),
                          (54, "Otras rentas")):
        mlc[codigo] = (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, INGRESOS, rubro))
        mlc[codigo + 5] = (cc, INGRESO_PRIMARIO, "", (INGRESO_PRIMARIO, EGRESOS, rubro))
    prestamos = "Préstamos, deuda y líneas de crédito"
    cuentas = (
        (67, "Inversión directa y de portafolio de no residentes",
         ("Inversión directa", "Compra de inmuebles", "Inversión de portafolio")),
        (76, prestamos, ("Organismos internacionales y agencias oficiales",
                         "Otros préstamos del exterior y títulos de deuda", "Líneas de crédito",
                         "Financiaciones locales")),
        (87, _FAE_SPNF, ("Billetes", "Inversión directa", "Otras inversiones")),
    )
    for primero, concepto, rubros in cuentas:
        # total del concepto, luego ingresos con sus rubros y egresos con los suyos
        mlc[primero] = _financiera(concepto)
        mlc[primero + 1] = _financiera(concepto, INGRESOS)
        for desplazamiento, rubro in enumerate(rubros):
            mlc[primero + 2 + desplazamiento] = _financiera(concepto, INGRESOS, rubro)
        egresos = primero + 2 + len(rubros)
        mlc[egresos] = _financiera(concepto, EGRESOS)
        for desplazamiento, rubro in enumerate(rubros):
            mlc[egresos + 1 + desplazamiento] = _financiera(concepto, EGRESOS, rubro)
    return mlc


_MERCADO = _tabla_mercado()

# Anexo de tarjetas y viajes. El transporte de pasajeros (viajes010 y viajes019) es idéntico al del
# mercado de cambios (mlc022 y mlc039) y se excluye por duplicado. En cambio los totales de consumos con
# tarjetas y viajes (viajes003 y viajes012) coinciden con mlc016 y mlc033 hasta junio de 2025 y desde
# julio de 2025 difieren exactamente en los servicios digitales (viajes005 y viajes014: 145,8 en jul-2025,
# 244,3 en ene-2026), que el mercado de cambios computa en «Otros servicios» (nota 8 del anexo): son
# series distintas y llevan otro nombre.
_TARJETAS_PASAJES = "Consumos con tarjetas, viajes y transporte de pasajeros"
_CONSUMOS_CON_DIGITALES = "Consumos con tarjetas y viajes, incluidos los servicios digitales"


def _viajes(*partes: str) -> _Lugar:
    return (CUENTA_CORRIENTE, SERVICIOS, VIAJES_Y_TARJETAS, (VIAJES_Y_TARJETAS, *partes))


_TARJETAS_VIAJES = "Tarjetas, incluye viajes"
_VIAJES = {
    1: _viajes(),
    2: _viajes(INGRESOS),
    3: _viajes(INGRESOS, _CONSUMOS_CON_DIGITALES),
    4: _viajes(INGRESOS, _TARJETAS_VIAJES),
    5: _viajes(INGRESOS, _TARJETAS_VIAJES, "Servicios digitales"),
    6: _viajes(INGRESOS, _TARJETAS_VIAJES, "Otros consumos de bienes y servicios"),
    7: _viajes(INGRESOS, "Otros viajes"),
    8: _viajes(INGRESOS, "Otros viajes", "Operadores turísticos y otras transferencias"),
    9: _viajes(INGRESOS, "Otros viajes", "Billetes de no residentes"),
    10: _MERCADO[22],
    11: _viajes(EGRESOS),
    12: _viajes(EGRESOS, _CONSUMOS_CON_DIGITALES),
    13: _viajes(EGRESOS, _TARJETAS_VIAJES),
    14: _viajes(EGRESOS, _TARJETAS_VIAJES, "Servicios digitales"),
    15: _viajes(EGRESOS, _TARJETAS_VIAJES, "Otros consumos de bienes y servicios"),
    16: _viajes(EGRESOS, "Otros viajes"),
    17: _viajes(EGRESOS, "Otros viajes", "Operadores turísticos y otras transferencias"),
    18: _viajes(EGRESOS, "Otros viajes", "Billetes de no residentes"),
    19: _MERCADO[39],
    21: _viajes("Bienes por servicios postales", "Exportaciones FOB"),
    22: _viajes("Bienes por servicios postales", "Importaciones CIF"),
    24: _viajes("Estimación de viajes y pasajes", INGRESOS),
    25: _viajes("Estimación de viajes y pasajes", EGRESOS),
    26: _viajes("Estimación de viajes y pasajes", "Resultado neto"),
}

# Hoja «Datos Mercado de Cambios»: una sola serie, «Monto», sin más rótulo.
_MONTO_DE_OPERACIONES = "Monto de la base de datos de operaciones"


def _codigo(serie: Serie) -> str:
    return serie.id.rsplit("|", 1)[-1]


def _numero(codigo: str, prefijo: str) -> int | None:
    resto = codigo.removeprefix(prefijo)
    return int(resto) if codigo.startswith(prefijo) and resto.isdigit() else None


def _mercado_cambios_bcra(serie: Serie) -> Clasificacion | None:
    """Libro «anexo estadístico del mercado de cambios y balance cambiario» del BCRA."""
    codigo = _codigo(serie)
    hoja = serie.hoja
    if hoja == "Balance Cambiario":
        numero = _numero(codigo, "bal")
        if numero in _BALANCE:
            return _lugar(BALANCE_CAMBIARIO, _BALANCE[numero])
        if codigo == "nan":
            # Columna sin rótulo al final de la hoja: un contador auxiliar (3762, 3765, 3768…).
            return Clasificacion(tema=TEMA, indicador=BALANCE_CAMBIARIO, cobertura=OTROS,
                                 nombre=unir("Balance cambiario", "Columna auxiliar sin rótulo"))
        return None
    if hoja == "Cta. Financiera por sector":
        numero = _numero(codigo, "cap")
        return _lugar(BALANCE_CAMBIARIO, _CAP[numero]) if numero in _CAP else None
    if hoja == "Mercado de Cambios":
        numero = _numero(codigo, "mlc")
        if numero == 101:
            return _lugar(BALANCE_CAMBIARIO, _BALANCE[53])
        return _lugar(MERCADO_DE_CAMBIOS, _MERCADO[numero]) if numero in _MERCADO else None
    if hoja == "Anexo tarjetas y viajes":
        numero = _numero(codigo, "viajes")
        return _lugar(MERCADO_DE_CAMBIOS, _VIAJES[numero]) if numero in _VIAJES else None
    if hoja == "Datos Mercado de Cambios" and codigo == "Monto":
        return Clasificacion(tema=TEMA, indicador=MERCADO_DE_CAMBIOS, cobertura=BASE_DE_OPERACIONES,
                             nombre=unir("Mercado de cambios", _MONTO_DE_OPERACIONES))
    return None


# --------------------------------------------------------------------------------------
# Despacho
# --------------------------------------------------------------------------------------

def _datos_gob_ar(serie: Serie) -> Clasificacion | None:
    clave = (serie.archivo, serie.hoja)
    if clave[0] == "sector_externo.xlsx" and clave[1] in (
            "20.Balance cambiario", "21.Bienes por modalidad de pago", "22.Bienes por sector",
            "23.Servicios por tipo"):
        return _balance_datos(serie)
    if clave == ("precios.xlsx", "4.8.1"):
        return _promedio_anual(serie)
    if clave == ("precios.xlsx", "4.8.2"):
        return _itcrm_datos(serie)
    if clave == ("precios.xlsx", "4.14"):
        return _itcrm_historico(serie)
    if clave == ("dinero_bancos.xlsx", "8.4 Res. Int. y Pasivos BCRA"):
        return _reservas_y_pasivos(serie)
    if clave == ("dinero_bancos.xlsx", "8.8. Tipos de cambio historicos"):
        return _tipos_historicos(serie)
    return None


_POR_FUENTE = {
    "bcra-mc-bc": _mercado_cambios_bcra,
    "bcra-itc": _itc_bcra,
    "bcra-com3500": _com3500,
    "bcra-bandas": _bandas,
    "bcra-cotizaciones": _cotizaciones,
    "bcra-irfcl": _irfcl,
    "iiep": _itcrb_iiep,
    "iiep-rin": _iiep_rin,
    "ambito-dolar-blue": _ambito,
    "ambito-dolar-mep": _ambito,
    "datos.gob.ar": _datos_gob_ar,
}


def clasificar(serie: Serie) -> Clasificacion | None:
    regla = _POR_FUENTE.get(serie.fuente)
    return regla(serie) if regla else None
