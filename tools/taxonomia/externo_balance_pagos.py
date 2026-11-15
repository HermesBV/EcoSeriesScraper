"""Reglas de las cuentas internacionales: balanza de pagos, posición de inversión
internacional (PII), deuda externa e inversión extranjera directa (IED) del BCRA.

Alcance (coincidencia exacta con ``Código fuente``, ``Archivo origen`` y ``Hoja origen``):

* ``indec-cin``: los 26 cuadros de las cuentas internacionales del INDEC (1 a 24, 29 y 30).
* ``datos.gob.ar`` / ``sector_externo.xlsx``: hojas 14, 14.a, 15.a, 16, 16.a y 24 a 33.

Tema «Sector externo». Cuatro indicadores:

    Balanza de pagos                    MBP6 (cuadros 1 a 14 del INDEC y hoja 14.a); MBP5 en «Series históricas»
    Posición de inversión internacional cuadros 15 a 22 del INDEC y hoja 16.a; base 1991 en «Series históricas»
    Deuda externa                       cuadros 23, 24, 29 y 30 del INDEC, hoja 15.a y deuda externa privada (hoja 33)
    Inversión extranjera directa (BCRA) hojas 24 a 32: stock, flujos y renta, total, por sector y por país

Árbol de la balanza de pagos (sigue el MBP6)::

    Resumen                         agregados de cada cuenta, cuenta financiera por sector, errores y omisiones
    Cuenta corriente > Bienes | Servicios (un grupo por tipo de servicio) | Ingreso primario | Ingreso secundario
    Cuenta de capital > Activos no financieros no producidos | Transferencias de capital
    Cuenta financiera > Inversión directa | de cartera | Derivados financieros | Otra inversión (Neto, Activos,
                        Pasivos) | Activos de reserva
    Series históricas > Balanza de pagos MBP5

Árbol de la PII (nombres con el prefijo «PII»): ``Posición neta``, ``Activos`` y ``Pasivos`` (con un grupo por
categoría funcional y «Total», que reúne los totales, los totales por sector y por moneda), ``Factores de variación``
(cuadro 20, con un grupo por lado) y ``Series históricas > Base 1991``. El valor de mercado es el de la PII por
omisión; el valor nominal lleva «(valor nominal)». En la deuda externa pasa al revés: el valor nominal es el habitual
y el de mercado lleva «(valor de mercado)». «Saldo final» del cuadro 20 es la posición misma y se llama como ella.

Árbol de la deuda externa: ``Total economía`` (un grupo por sector residente, con «Total» y el subtotal «Otros
sectores») y ``Sector privado`` (deuda externa privada del BCRA, hoja 33).

Árbol de la IED (nombres con el prefijo «IED»): ``Stock``, ``Flujos`` y ``Renta``, cada uno con ``Total`` (incluye la
apertura por instrumento y los totales de las hojas por sector y por país), ``Por sector de actividad`` y
``Por país de origen``. Los cuadros de renta por sector y por país suman sólo la renta de participaciones de capital
(su total coincide con ese subtotal de la hoja 26, que además trae intereses) y así se llaman.

Cómo se deriva cada dimensión
-----------------------------
Los cuadros del INDEC no traen el encabezado jerárquico: la serie sólo conserva el rótulo de su columna, y
cuando un rótulo se repite (Crédito, Débito, Activos, Pasivos, un sector) el lector de planillas lo antepuso con
el rótulo anterior, que a veces es del bloque vecino («Pasivos - 3.2 Inversión de cartera - Activos» es el
activo de cartera). Por eso la identidad de cada columna de los cuadros 1 a 13, 15 y 18 se fijó por su posición en la
planilla y se verificó con identidades contables y con los valores (créditos menos débitos igual al saldo,
activos menos pasivos igual al neto, y cada cuadro contra el cuadro 14 y los de PII entre sí: 895 pares de series de
distintas hojas con el mismo nombre coinciden en sus últimos 12 períodos). Las tablas ``_CUADRO_N`` guardan ese
resultado, rótulo por rótulo. Los cuadros 14, 19 y 22 traen además la clave SDMX en el ID (sector, contraparte,
partida, categoría funcional, instrumento, plazo), que identifica la serie sin ambigüedad y se usa en lugar del
rótulo. Los cuadros 16, 17, 20, 21, 23, 24 y 29 son sistemáticos y se interpretan por sus partes.

Hojas de datos.gob.ar: la 14 (MBP5) y la 14.a, la 15.a (deuda a valor nominal, desactualizada: termina en 2024T2 y
sus valores no recogen las revisiones posteriores), la 16.a (PII a valor de mercado) y las hojas de IED se
identifican por ``Variable`` o ``ID``. Los rótulos de la hoja 16.a están corridos en los cuadros por sector
(«activos del gobierno general» trae los activos de otra inversión de las sociedades captadoras de depósitos), así que
cada serie se identificó cotejando sus valores con los cuadros 15 y 16 del INDEC, que coinciden al decimal; las
series anuales se aparearon con las trimestrales por su nombre.

Convenciones de nombre
----------------------
* Balanza de pagos: sin sufijo es el saldo; «(crédito)» y «(débito)» son los movimientos brutos (en bienes
  y servicios, exportaciones e importaciones). En la cuenta financiera sin sufijo es el neto de la categoría
  y «Activos» / «Pasivos» (adquisición neta de activos / emisión neta de pasivos) van en la ruta.
* Para que ningún nombre pase de 120 caracteres: «Participaciones de capital y en fondos de inversión» se escribe
  «Participaciones de capital»; las sociedades captadoras de depósitos, excepto el banco central, «Sociedades
  captadoras de depósitos» (el banco central es su hermano en la clasificación sectorial); las sociedades
  financieras, no financieras, hogares e ISFLSH del ingreso secundario, «Sociedades, hogares e ISFLSH»; algunos
  rubros de servicios y de IED se abrevian (`_CORTO`, `_SECTORES_IED`). Los nombres de la renta de la inversión omiten
  «Ingreso primario» (todo lo que lleva «Renta de la inversión» ya es ingreso primario).
* Las series históricas (MBP5 y PII base 1991) se llaman como su equivalente vigente cuando miden lo mismo.
"""

from __future__ import annotations

from . import Clasificacion, Serie
from .texto import con_medida, plano, unir

TEMA = "Sector externo"
UNIDAD = "Millones de dólares"
INDICADOR_BP = "Balanza de pagos"
INDICADOR_PII = "Posición de inversión internacional"
INDICADOR_DEUDA = "Deuda externa"
INDICADOR_IED = "Inversión extranjera directa (BCRA)"

CIN = "indec-cin"
DATOS = "datos.gob.ar"
LIBRO = "sector_externo.xlsx"

# ── Sectores institucionales (MBP6) ────────────────────────────────────────────────────────────
BC = "Banco central"
SCD = "Sociedades captadoras de depósitos"
GG = "Gobierno general"
OS = "Otros sectores"
SOC = "Sociedades, hogares e ISFLSH"
SECTOR_SDMX = {"S121": BC, "S122": SCD, "S13": GG, "S1Z": OS}

PARTICIPACIONES = "Participaciones de capital"
CRED_ANT = "Créditos y anticipos comerciales"
CUENTAS_COBRAR_PAGAR = "Otras cuentas por cobrar/por pagar"
DEG_ASIGNACIONES = "Asignaciones de derechos especiales de giro"
SIN_REINVERSION = "Participaciones de capital sin utilidades reinvertidas"

CRED = "crédito"
DEB = "débito"

# Formas abreviadas para que ningún nombre pase de 120 caracteres; se aplican sólo al armar el nombre.
_CORTO = {
    "Servicios de seguros y pensiones": "Seguros y pensiones",
    "Servicios que se cobran explícitamente y otros servicios financieros": "Cargos explícitos y otros servicios financieros",
    "Servicios de intermediación financiera medidos indirectamente (SIFMI)": "Intermediación financiera medida indirectamente",
    "Servicios profesionales y de consultoría en administración de empresas": "Servicios profesionales y de consultoría",
    "Servicios de pensiones y garantías normalizadas": "Pensiones y garantías normalizadas",
    "Servicios personales, culturales y recreativos": "Personales, culturales y recreativos",
    "Otros servicios personales, culturales y recreativos": "Otros",
    "Servicios de telecomunicaciones, informática e información": "Telecomunicaciones, informática e información",
    "Servicios de telecomunicaciones": "Telecomunicaciones",
    "Servicios informáticos": "Informáticos",
    "Servicios de información": "Información",
    "Servicios relacionados con el turismo en viajes y transporte de pasajeros": "Turismo en viajes y transporte de pasajeros",
    "Servicios de publicidad, investigación de mercado y encuestas de opinión pública": "Publicidad, investigación de mercado y encuestas",
    "Servicios de tratamiento de residuos y descontaminación, servicios agrícolas y mineros":
        "Tratamiento de residuos, descontaminación y servicios agrícolas y mineros",
    "Dividendos de inversionistas directos en empresas de inversión directa": "Dividendos de inversionistas directos",
    "Bienes adquiridos en virtud de compraventa": "Bienes adquiridos",
    "Bienes vendidos en virtud de compraventa": "Bienes vendidos",
    "Crédito y préstamos del FMI (distintos de reservas)": "Crédito y préstamos del FMI",
    "Ajuste por corrección de fletes de importación (CIF/FOB)": "Ajuste CIF/FOB por fletes",
    "Ajuste por corrección de seguros de importación (CIF/FOB)": "Ajuste CIF/FOB por seguros",
    "Ajuste por exportaciones netas de bienes en compraventa": "Ajuste por compraventa",
    "Impuestos corrientes sobre la renta, el patrimonio, etc.": "Impuestos corrientes",
    "Impuestos corrientes sobre el ingreso, el patrimonio, etc.": "Impuestos corrientes",
    "Seguros, pensiones y mecanismos normalizados de garantía": "Seguros, pensiones y garantías",
    "Otras cuentas por cobrar/por pagar": "Otras cuentas por cobrar/pagar",
}


def _corto(partes) -> tuple:
    return tuple(_CORTO.get(parte, parte) for parte in partes)

# Categorías funcionales
IED_ = "Inversión directa"
CART = "Inversión de cartera"
DERIV = "Derivados financieros"
OTRA = "Otra inversión"
RESERVA = "Activos de reserva"
TOTAL = "Total"

NETO = "Neto"
ACT = "Activos"
PAS = "Pasivos"


# ── Conceptos ──────────────────────────────────────────────────────────────────────────────────
# Un concepto es (área, ruta, flujo):
#   RES  resumen:            ruta = (nombre,)
#   CC   cuenta corriente:   ruta = (Bienes | Servicios | Ingreso primario | Ingreso secundario, …)
#   KA   cuenta de capital:  ruta = (grupo, …)
#   CF   cuenta financiera:  ruta = (categoría funcional, lado, detalle…), lado = Neto | Activos | Pasivos
# y flujo "", «crédito» o «débito».  Un cuarto elemento opcional da la ruta del nombre cuando difiere de la del árbol.

def _cc(*ruta, f="", nombre=None):
    return ("CC", ruta, f, nombre)


def _res(*ruta, f=""):
    return ("RES", ruta, f, None)


def _ka(*ruta, f=""):
    return ("KA", ruta, f, None)


def _cf(categoria, lado, *detalle):
    return ("CF", (categoria, lado, *detalle), "", None)


def _bp(concepto) -> Clasificacion:
    area, ruta, flujo, nombre = concepto
    partes = nombre if nombre else ruta
    if area == "RES":
        cobertura, apertura, medida = "Resumen", "", ""
    elif area == "CC":
        cobertura, apertura = "Cuenta corriente", ruta[0]
        medida = ruta[1] if ruta[0] == "Servicios" else ""
        if ruta[0] == "Ingreso primario" and not nombre:
            partes = ruta[1:]       # «Remuneración de empleados», «Renta de la inversión…»: ya son ingreso primario
    elif area == "KA":
        cobertura, apertura, medida = "Cuenta de capital", ruta[0], ""
    else:  # CF
        categoria, lado = ruta[0], ruta[1]
        cobertura, apertura = "Cuenta financiera", categoria
        medida = lado if categoria != RESERVA else ""
        partes = (categoria, "" if lado == NETO or categoria == RESERVA else lado, *ruta[2:])
    return Clasificacion(tema=TEMA, indicador=INDICADOR_BP, cobertura=cobertura, apertura=apertura, medida=medida,
                         nombre=con_medida(unir(INDICADOR_BP, *_corto(partes)), flujo), unidad=UNIDAD)


# ── Cuadro 14 (y 14.a): clave SDMX → concepto ──────────────────────────────────────────────────
# Clave: sector.sector contraparte.partida.categoría funcional.instrumento.plazo; el asiento (B, C, D)
# da el flujo. Los servicios llevan como segundo elemento de la ruta el tipo de servicio (un grupo del árbol).
_T_MANUFACTURA = "Servicios de manufactura sobre insumos físicos pertenecientes a otros"
_T_MANTENIMIENTO = "Mantenimiento y reparaciones no incluidos en otra parte"
_T_SEGUROS = "Servicios de seguros y pensiones"
_T_FINANCIEROS = "Servicios financieros"
_T_PROPIEDAD = "Cargos por el uso de la propiedad intelectual"
_T_TELECOM = "Servicios de telecomunicaciones, informática e información"
_T_EMPRESARIALES = "Otros servicios empresariales"
_T_PERSONALES = "Servicios personales, culturales y recreativos"
_T_GOBIERNO = "Bienes y servicios del gobierno no incluidos en otra parte"
TIPOS_SERVICIOS = (
    _T_MANUFACTURA, _T_MANTENIMIENTO, "Transporte", "Viajes", "Construcción", _T_SEGUROS, _T_FINANCIEROS,
    _T_PROPIEDAD, _T_TELECOM, _T_EMPRESARIALES, _T_PERSONALES, _T_GOBIERNO,
)
INFORMATIVAS = "Partidas informativas"


def _sv(*detalle, nombre=None):
    return _cc("Servicios", *detalle, nombre=nombre)


_IP = "Ingreso primario"
_R_DIR = "Renta de la inversión directa"
_R_CART = "Renta de la inversión de cartera"
_R_OTRA = "Renta de otra inversión"
_IS = "Ingreso secundario"
_TC = "Transferencias de capital"
_RESTO_EMP = "Resto de otros servicios empresariales"

_CLAVES_CUENTAS = {
    # Resumen
    "S1.S1.CA._Z._Z._Z": _res("Cuenta corriente"),
    "S1.S1.GS._Z._Z._Z": _res("Bienes y servicios"),
    "S1.S1.G._Z._Z._Z": _res("Bienes"),
    "S1.S1.S._Z._Z._Z": _res("Servicios"),
    "S1.S1.IN1._Z._Z._Z": _res(_IP),
    "S1.S1.IN2._Z._Z._Z": _res(_IS),
    "S1.S1.KA._Z._Z._Z": _res("Cuenta de capital"),
    "S1.S1.CKA._Z._Z._Z": _res("Préstamo neto / endeudamiento neto"),
    "S1.S1.EO._Z._Z._Z": _res("Errores y omisiones netos"),
    # Bienes
    "S1.S1.G1._Z._Z._Z": _cc("Bienes", "Mercancías generales"),
    "S1.S1.G1Z._Z._Z._Z": _cc("Bienes", "Mercancías generales", "Reexportación"),
    "S1.S1.G2._Z._Z._Z": _cc("Bienes", "Exportaciones netas de bienes en compraventa"),
    "S1.S1.G21._Z._Z._Z": _cc("Bienes", "Exportaciones netas de bienes en compraventa",
                              "Bienes adquiridos en virtud de compraventa"),
    "S1.S1.G22._Z._Z._Z": _cc("Bienes", "Exportaciones netas de bienes en compraventa",
                              "Bienes vendidos en virtud de compraventa"),
    "S1.S1N.G3._Z._Z._Z": _cc("Bienes", "Oro no monetario"),
    # Servicios
    "S1.S1.SA._Z._Z._Z": _sv(_T_MANUFACTURA),
    "S1.S1.SB._Z._Z._Z": _sv(_T_MANTENIMIENTO),
    "S1.S1.SC._Z._Z._Z": _sv("Transporte"),
    "S1.S1.SCA._Z._Z._Z": _sv("Transporte", "Pasajeros"),
    "S1.S1.SCB._Z._Z._Z": _sv("Transporte", "Fletes"),
    "S1.S1.SCC._Z._Z._Z": _sv("Transporte", "Otros"),
    "S1.S1.SC1._Z._Z._Z": _sv("Transporte", "Marítimo"),
    "S1.S1.SC11._Z._Z._Z": _sv("Transporte", "Marítimo", "Pasajeros"),
    "S1.S1.SC12._Z._Z._Z": _sv("Transporte", "Marítimo", "Fletes"),
    "S1.S1.SC13._Z._Z._Z": _sv("Transporte", "Marítimo", "Otros"),
    "S1.S1.SC2._Z._Z._Z": _sv("Transporte", "Aéreo"),
    "S1.S1.SC21._Z._Z._Z": _sv("Transporte", "Aéreo", "Pasajeros"),
    "S1.S1.SC22._Z._Z._Z": _sv("Transporte", "Aéreo", "Fletes"),
    "S1.S1.SC23._Z._Z._Z": _sv("Transporte", "Aéreo", "Otros"),
    "S1.S1.SC3._Z._Z._Z": _sv("Transporte", "Otras modalidades"),
    "S1.S1.SC31._Z._Z._Z": _sv("Transporte", "Otras modalidades", "Pasajeros"),
    "S1.S1.SC32._Z._Z._Z": _sv("Transporte", "Otras modalidades", "Fletes"),
    "S1.S1.SC33._Z._Z._Z": _sv("Transporte", "Otras modalidades", "Otros"),
    "S1.S1.SC4._Z._Z._Z": _sv("Transporte", "Servicios postales y de mensajería"),
    "S1.S1.SD._Z._Z._Z": _sv("Viajes"),
    "S1.S1.SDA._Z._Z._Z": _sv("Viajes", "De negocios"),
    "S1.S1.SDB._Z._Z._Z": _sv("Viajes", "Personales"),
    "S1.S1.SE._Z._Z._Z": _sv("Construcción"),
    "S1.S1.SE1._Z._Z._Z": _sv("Construcción", "Construcción en el extranjero"),
    "S1.S1.SE2._Z._Z._Z": _sv("Construcción", "Construcción en la economía declarante"),
    "S1.S1.SF._Z._Z._Z": _sv(_T_SEGUROS),
    "S1.S1.SF1._Z._Z._Z": _sv(_T_SEGUROS, "Seguros directos"),
    "S1.S1.SF2._Z._Z._Z": _sv(_T_SEGUROS, "Reaseguros"),
    "S1.S1.SF3._Z._Z._Z": _sv(_T_SEGUROS, "Servicios auxiliares de los seguros"),
    "S1.S1.SF4._Z._Z._Z": _sv(_T_SEGUROS, "Servicios de pensiones y garantías normalizadas"),
    "S1.S1.SG._Z._Z._Z": _sv(_T_FINANCIEROS),
    "S1.S1.SG1._Z._Z._Z": _sv(_T_FINANCIEROS, "Servicios que se cobran explícitamente y otros servicios financieros"),
    "S1.S1.SG2._Z._Z._Z": _sv(_T_FINANCIEROS, "Servicios de intermediación financiera medidos indirectamente (SIFMI)"),
    "S1.S1.SH._Z._Z._Z": _sv(_T_PROPIEDAD),
    "S1.S1.SI._Z._Z._Z": _sv(_T_TELECOM),
    "S1.S1.SI1._Z._Z._Z": _sv(_T_TELECOM, "Servicios de telecomunicaciones"),
    "S1.S1.SI2._Z._Z._Z": _sv(_T_TELECOM, "Servicios informáticos"),
    "S1.S1.SI3._Z._Z._Z": _sv(_T_TELECOM, "Servicios de información"),
    "S1.S1.SJ._Z._Z._Z": _sv(_T_EMPRESARIALES),
    "S1.S1.SJ1._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios de investigación y desarrollo"),
    "S1.S1.SJ2._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios profesionales y de consultoría en administración de empresas"),
    "S1.S1.SJ21._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios profesionales y de consultoría en administración de empresas",
                               "Servicios jurídicos, contables y de consultoría de gerencia",
                               nombre=("Servicios", "Servicios jurídicos, contables y de consultoría de gerencia")),
    "S1.S1.SJ22._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios profesionales y de consultoría en administración de empresas",
                               "Servicios de publicidad, investigación de mercado y encuestas de opinión pública",
                               nombre=("Servicios", "Servicios de publicidad, investigación de mercado y encuestas de opinión pública")),
    "S1.S1.SJ3._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios técnicos relacionados con el comercio y otros servicios empresariales",
                              nombre=("Servicios", "Servicios técnicos relacionados con el comercio y otros servicios empresariales")),
    "S1.S1.SJ31._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios técnicos relacionados con el comercio y otros servicios empresariales",
                               "Servicios arquitectónicos, de ingeniería y otros servicios técnicos",
                               nombre=("Servicios", "Servicios arquitectónicos, de ingeniería y otros servicios técnicos")),
    "S1.S1.SJ32._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios técnicos relacionados con el comercio y otros servicios empresariales",
                               "Servicios de tratamiento de residuos y descontaminación, servicios agrícolas y mineros",
                               nombre=("Servicios", "Servicios de tratamiento de residuos y descontaminación, servicios agrícolas y mineros")),
    "S1.S1.SJ33._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios técnicos relacionados con el comercio y otros servicios empresariales",
                               "Servicios de arrendamiento operativo",
                               nombre=("Servicios", "Servicios de arrendamiento operativo")),
    "S1.S1.SJ34._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios técnicos relacionados con el comercio y otros servicios empresariales",
                               "Servicios relacionados con el comercio",
                               nombre=("Servicios", "Servicios relacionados con el comercio")),
    "S1.S1.SJ35._Z._Z._Z": _sv(_T_EMPRESARIALES, "Servicios técnicos relacionados con el comercio y otros servicios empresariales",
                               _RESTO_EMP, nombre=("Servicios", _RESTO_EMP)),
    "S1.S1.SK._Z._Z._Z": _sv(_T_PERSONALES),
    "S1.S1.SK1._Z._Z._Z": _sv(_T_PERSONALES, "Servicios audiovisuales y conexos"),
    "S1.S1.SK2._Z._Z._Z": _sv(_T_PERSONALES, "Otros servicios personales, culturales y recreativos"),
    "S1.S1.SL._Z._Z._Z": _sv(_T_GOBIERNO),
    "S1.S1.SDz._Z._Z._Z": _sv(INFORMATIVAS, "Servicios relacionados con el turismo en viajes y transporte de pasajeros"),
    # Ingreso primario
    "S1.S1.D1._Z._Z._Z": _cc(_IP, "Remuneración de empleados"),
    "S1.S1.D4P._T.F._Z": _cc(_IP, "Renta de la inversión"),
    "S1.S1.D4P.D.F._Z": _cc(_IP, _R_DIR),
    "S1.S1.D4S.D.F5._Z": _cc(_IP, _R_DIR, "Renta de participaciones de capital"),
    "S1.S1.D42S.D.F5._Z": _cc(_IP, _R_DIR, "Dividendos y retiros de ingresos de cuasisociedades"),
    "S1.S1.D42S.D1.F5._Z": _cc(_IP, _R_DIR, "Dividendos y retiros de ingresos de cuasisociedades",
                               "Inversionista directo en empresas de inversión directa",
                               nombre=(_R_DIR, "Dividendos de inversionistas directos en empresas de inversión directa")),
    "S1.S1.D43S.D.F5._Z": _cc(_IP, _R_DIR, "Utilidades reinvertidas"),
    "S1.S1.D4Q.D.FL._Z": _cc(_IP, _R_DIR, "Intereses"),
    "S1.S1.D4P.P.F._Z": _cc(_IP, _R_CART),
    "S1.S1.D4S.P.F5._Z": _cc(_IP, _R_CART, "Renta de participaciones de capital"),
    "S1.S1.D42.P.F51._Z": _cc(_IP, _R_CART, "Renta de participaciones de capital", "Dividendos",
                              nombre=(_R_CART, "Dividendos de participaciones de capital")),
    "S1.S1.D41.P.F3.T": _cc(_IP, _R_CART, "Intereses"),
    "S1.S1.D41.P.F3.L": _cc(_IP, _R_CART, "Intereses", "Intereses a largo plazo",
                            nombre=(_R_CART, "Intereses a largo plazo")),
    "S1.S1.D4P.O.F._Z": _cc(_IP, _R_OTRA),
    "S1.S1.D41.O.FLA.T": _cc(_IP, _R_OTRA, "Intereses"),
    "S1.S1.D41G.O.FLA.T": _cc(_IP, _R_OTRA, "Intereses excluidos los SIFMI"),
    "S121.S1.D4P.R.F._Z": _cc(_IP, "Renta de activos de reserva"),
    # Ingreso secundario
    "S13.S1.IN2._Z._Z._Z": _cc(_IS, GG),
    "S13.S1.D5._Z._Z._Z": _cc(_IS, GG, "Impuestos corrientes sobre la renta, el patrimonio, etc."),
    "S13.S1.D61._Z._Z._Z": _cc(_IS, GG, "Contribuciones sociales"),
    "S13.S1.D62._Z._Z._Z": _cc(_IS, GG, "Prestaciones sociales"),
    "S13.S1.D74._Z._Z._Z": _cc(_IS, GG, "Cooperación internacional corriente"),
    "S13.S1.D75._Z._Z._Z": _cc(_IS, GG, "Transferencias corrientes diversas"),
    "S1W.S1.IN2._Z._Z._Z": _cc(_IS, SOC),
    "S1W.S1.D752._Z._Z._Z": _cc(_IS, SOC, "Transferencias personales"),
    "S1W.S1.IN22._Z._Z._Z": _cc(_IS, SOC, "Otras transferencias corrientes"),
    "S1W.S1.D5._Z._Z._Z": _cc(_IS, SOC, "Impuestos corrientes sobre el ingreso, el patrimonio, etc."),
    "S1W.S1.D61._Z._Z._Z": _cc(_IS, SOC, "Contribuciones sociales"),
    "S1W.S1.D62._Z._Z._Z": _cc(_IS, SOC, "Prestaciones sociales"),
    "S1W.S1.D71._Z._Z._Z": _cc(_IS, SOC, "Primas netas de seguros no de vida"),
    "S1W.S1.D72._Z._Z._Z": _cc(_IS, SOC, "Indemnizaciones de seguros no de vida"),
    "S1W.S1.D74._Z._Z._Z": _cc(_IS, SOC, "Cooperación internacional corriente"),
    "S1W.S1.D75._Z._Z._Z": _cc(_IS, SOC, "Transferencias corrientes diversas"),
    # Cuenta de capital
    "S1.S1.NP._Z._Z._Z": _ka("Activos no financieros no producidos"),
    "S1.S1.D9._Z._Z._Z": _ka(_TC),
    "S13.S1.D9._Z._Z._Z": _ka(_TC, GG),
    "S13.S1.D99A._Z._Z._Z": _ka(_TC, GG, "Condonación de la deuda"),
    "S13.S1.D9A._Z._Z._Z": _ka(_TC, GG, "Otras transferencias de capital"),
    "S1W.S1.D9._Z._Z._Z": _ka(_TC, SOC),
    "S1W.S1.D99A._Z._Z._Z": _ka(_TC, SOC, "Condonación de la deuda"),
    "S1W.S1.D9A._Z._Z._Z": _ka(_TC, SOC, "Otras transferencias de capital"),
}
_FLUJO_ASIENTO = {"B": "", "N": "", "C": CRED, "D": DEB}

# Partidas financieras (cuenta financiera y PII): categoría funcional (campo 9) e instrumento (campo 10).
_CATEGORIA = {"D": IED_, "P": CART, "F": DERIV, "O": OTRA, "R": RESERVA, "_T": TOTAL}
_INSTRUMENTO = {
    ("D", "F"): (), ("D", "F5"): (PARTICIPACIONES,),
    ("D", "F5A"): (SIN_REINVERSION,),
    ("D", "F5B"): ("Utilidades reinvertidas",), ("D", "FL"): ("Instrumentos de deuda",),
    ("P", "F"): (), ("P", "F5"): (PARTICIPACIONES,), ("P", "F3"): ("Títulos de deuda",),
    ("F", "F7"): (),
    ("O", "F"): (), ("O", "F519"): ("Otras participaciones de capital",), ("O", "F2"): ("Moneda y depósitos",),
    ("O", "F4"): ("Préstamos",), ("O", "F6"): ("Seguros, pensiones y mecanismos normalizados de garantía",),
    ("O", "F81"): (CRED_ANT,), ("O", "F89"): (CUENTAS_COBRAR_PAGAR,), ("O", "F12"): (DEG_ASIGNACIONES,),
    ("R", "F"): (), ("R", "F11"): ("Oro monetario",), ("R", "F12"): ("Derechos especiales de giro",),
    ("R", "FK"): ("Posición de reserva en el FMI",), ("R", "FR2"): ("Otros activos de reserva",),
    ("R", "F2"): ("Otros activos de reserva", "Moneda y depósitos"),
    ("R", "FR1"): ("Otros activos de reserva", "Títulos"),
    ("R", "F71"): ("Otros activos de reserva", "Derivados financieros"),
    ("R", "FR41"): ("Otros activos de reserva", "Otros derechos sobre activos"),
    ("_T", "F"): (),
}
_LADO_ASIENTO = {"A": ACT, "L": PAS, "N": NETO}


def _partida_financiera(campos: list[str]):
    """(categoría, detalle) de una clave SDMX de la cuenta financiera o de la PII, o None si no se conoce."""
    contraparte, sector, categoria, instrumento, plazo = campos[3], campos[4], campos[9], campos[10], campos[11]
    base = _INSTRUMENTO.get((categoria, instrumento))
    if base is None or categoria not in _CATEGORIA:
        return None
    detalle = list(base)
    if categoria != "R" and sector in SECTOR_SDMX:
        detalle.append(SECTOR_SDMX[sector])
    if categoria == "O" and instrumento == "F4":
        if contraparte == "1C" and sector == "S121":
            detalle.append("Crédito y préstamos del FMI (distintos de reservas)")
        elif sector == "S121" and plazo in ("S", "L"):
            detalle.append("Otros pasivos a corto plazo" if plazo == "S" else "Otros pasivos a largo plazo")
        elif sector == "S122" and plazo in ("S", "L"):
            detalle.append("Corto plazo" if plazo == "S" else "Largo plazo")
    return _CATEGORIA[categoria], tuple(detalle)

_CUADRO_1 = {
    '1. Cuenta corriente': _res("Cuenta corriente"),
    '1.A Bienes y servicios': _res("Bienes y servicios"),
    '1.A.a Bienes': _res("Bienes"),
    'Crédito (Exportaciones)': _res("Bienes", f=CRED),
    'Débito (Importaciones)': _res("Bienes", f=DEB),
    '1.A.b Servicios': _res("Servicios"),
    '1.A.b Servicios - Crédito (Exportaciones)': _res("Servicios", f=CRED),
    '1.A.b Servicios - Débito (Importaciones)': _res("Servicios", f=DEB),
    '1.B Ingreso primario': _res(_IP),
    '1.B.1 Remuneración de empleados': _cc(_IP, "Remuneración de empleados"),
    'Crédito': _cc(_IP, "Remuneración de empleados", f=CRED),
    'Débito': _cc(_IP, "Remuneración de empleados", f=DEB),
    '1.B.2 Renta de la inversión': _cc(_IP, "Renta de la inversión"),
    '1.B.2.1 Inversión directa': _cc(_IP, _R_DIR),
    'En el extranjero': _cc(_IP, _R_DIR, f=CRED),
    'En Argentina': _cc(_IP, _R_DIR, f=DEB),
    '1.B.2.2 Inversión de cartera': _cc(_IP, _R_CART),
    'Débito - 1.B.2.2 Inversión de cartera - Crédito': _cc(_IP, _R_CART, f=CRED),
    'Crédito - 1.B.2.2 Inversión de cartera - Débito': _cc(_IP, _R_CART, f=DEB),
    '1.B.2.3 Otra inversión': _cc(_IP, _R_OTRA),
    'Débito - 1.B.2.3 Otra inversión - Crédito': _cc(_IP, _R_OTRA, f=CRED),
    'Crédito - 1.B.2.3 Otra inversión - Débito': _cc(_IP, _R_OTRA, f=DEB),
    '1.B.2.4 Activos de reserva': _cc(_IP, "Renta de activos de reserva", f=CRED),
    '1.C. Ingreso secundario': _res(_IS),
    'Débito - 1.C. Ingreso secundario - Crédito': _res(_IS, f=CRED),
    'Crédito - 1.C. Ingreso secundario - Débito': _res(_IS, f=DEB),
    '2. Cuenta de capital': _res("Cuenta de capital"),
    'Préstamo neto (+) / Endeudamiento neto (-)': _res("Préstamo neto / endeudamiento neto"),
    '3. Cuenta financiera': _res("Cuenta financiera"),
    '3.1 Inversión directa': _cf(IED_, NETO),
    'Activos': _cf(IED_, ACT),
    'Pasivos': _cf(IED_, PAS),
    '3.2 Inversión de cartera': _cf(CART, NETO),
    'Pasivos - 3.2 Inversión de cartera - Activos': _cf(CART, ACT),
    'Activos - 3.2 Inversión de cartera - Pasivos': _cf(CART, PAS),
    '3.3 Instrumentos financieros derivados': _cf(DERIV, NETO),
    'Pasivos - 3.3 Instrumentos financieros derivados - Activos': _cf(DERIV, ACT),
    'Activos - 3.3 Instrumentos financieros derivados - Pasivos': _cf(DERIV, PAS),
    '3.4 Otra inversión': _cf(OTRA, NETO),
    'Pasivos - 3.4 Otra inversión - Activos': _cf(OTRA, ACT),
    'Activos - 3.4 Otra inversión - Pasivos': _cf(OTRA, PAS),
    '3.5 Activos de reserva': _cf(RESERVA, ""),
    'Errores y omisiones netos': _res("Errores y omisiones netos"),
}

_CUADRO_2 = {
    '1. Cuenta corriente': _res("Cuenta corriente"),
    '1.A Bienes y servicios': _res("Bienes y servicios"),
    '1.A.a Bienes': _res("Bienes"),
    'Crédito (Exportaciones)': _res("Bienes", f=CRED),
    'Débito (Importaciones)': _res("Bienes", f=DEB),
    '1.A.b Servicios': _res("Servicios"),
    '1.A.b Servicios - Crédito (Exportaciones)': _res("Servicios", f=CRED),
    '1.A.b Servicios - Débito (Importaciones)': _res("Servicios", f=DEB),
    '1.B Ingreso primario': _res(_IP),
    '1.B.1 Remuneración de empleados': _cc(_IP, "Remuneración de empleados"),
    'Crédito': _cc(_IP, "Remuneración de empleados", f=CRED),
    'Débito': _cc(_IP, "Remuneración de empleados", f=DEB),
    '1.B.2 Renta de la inversión': _cc(_IP, "Renta de la inversión"),
    '1.B.2.1 Inversión directa': _cc(_IP, _R_DIR),
    'En el extranjero': _cc(_IP, _R_DIR, f=CRED),
    'Renta procedente de participaciones de capital y participaciones en fondos de inversión': _cc(_IP, _R_DIR, "Renta de participaciones de capital", f=CRED),
    'Dividendos y retiros de ingresos de cuasisociedades': _cc(_IP, _R_DIR, "Dividendos y retiros de ingresos de cuasisociedades", f=CRED),
    'Utilidades reinvertidas': _cc(_IP, _R_DIR, "Utilidades reinvertidas", f=CRED),
    'En Argentina': _cc(_IP, _R_DIR, f=DEB),
    'Débito - 1.B.2.1 Inversión directa - Renta procedente de participaciones de capital y participaciones en fondos de inversión': _cc(_IP, _R_DIR, "Renta de participaciones de capital", f=DEB),
    'Débito - 1.B.2.1 Inversión directa - Dividendos y retiros de ingresos de cuasisociedades': _cc(_IP, _R_DIR, "Dividendos y retiros de ingresos de cuasisociedades", f=DEB),
    'Débito - 1.B.2.1 Inversión directa - Utilidades reinvertidas': _cc(_IP, _R_DIR, "Utilidades reinvertidas", f=DEB),
    'Intereses': _cc(_IP, _R_DIR, "Intereses", f=DEB),
    '1.B.2.2 Inversión de cartera': _cc(_IP, _R_CART),
    'Débito - 1.B.2.2 Inversión de cartera - Crédito': _cc(_IP, _R_CART, f=CRED),
    'Renta de la inversión en participaciones de capital y participaciones en fondos de inversión': _cc(_IP, _R_CART, "Renta de participaciones de capital", f=CRED),
    'Crédito - 1.B.2.2 Inversión de cartera - Intereses': _cc(_IP, _R_CART, "Intereses", f=CRED),
    'Crédito - 1.B.2.2 Inversión de cartera - Débito': _cc(_IP, _R_CART, f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera - Renta de la inversión en participaciones de capital y participaciones en fondos de inversión': _cc(_IP, _R_CART, "Renta de participaciones de capital", f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera - Intereses': _cc(_IP, _R_CART, "Intereses", f=DEB),
    '1.B.2.3 Otra inversión': _cc(_IP, _R_OTRA),
    'Débito - 1.B.2.3 Otra inversión - Crédito': _cc(_IP, _R_OTRA, f=CRED),
    'Crédito - 1.B.2.3 Otra inversión - Débito': _cc(_IP, _R_OTRA, f=DEB),
    '1.B.2.4 Activos de reserva': _cc(_IP, "Renta de activos de reserva", f=CRED),
    '1.C. Ingreso secundario': _res(_IS),
    'Débito - 1.C. Ingreso secundario - Crédito': _res(_IS, f=CRED),
    'Crédito - 1.C. Ingreso secundario - Débito': _res(_IS, f=DEB),
    '2. Cuenta de capital': _res("Cuenta de capital"),
    'Préstamo neto (+) / Endeudamiento neto (-)': _res("Préstamo neto / endeudamiento neto"),
    '3. Cuenta financiera': _res("Cuenta financiera"),
    'S121. Banco central': _res("Cuenta financiera", BC),
    'Activos de Reserva': _res("Cuenta financiera", BC, ACT),
    'Pasivos': _res("Cuenta financiera", BC, PAS),
    'S122. Sociedades captadoras de depósito, excepto el banco central': _res("Cuenta financiera", SCD),
    'Activos': _res("Cuenta financiera", SCD, ACT),
    'Activos - 3. Cuenta financiera - S122. Sociedades captadoras de depósito, excepto el banco central - Pasivos': _res("Cuenta financiera", SCD, PAS),
    'S13. Gobierno general': _res("Cuenta financiera", GG),
    'Pasivos - 3. Cuenta financiera - S13. Gobierno general - Activos': _res("Cuenta financiera", GG, ACT),
    'Activos - 3. Cuenta financiera - S13. Gobierno general - Pasivos': _res("Cuenta financiera", GG, PAS),
    'S1Z. Otros sectores': _res("Cuenta financiera", OS),
    'Pasivos - 3. Cuenta financiera - S1Z. Otros sectores - Activos': _res("Cuenta financiera", OS, ACT),
    'Activos - 3. Cuenta financiera - S1Z. Otros sectores - Pasivos': _res("Cuenta financiera", OS, PAS),
    'Errores y omisiones netos': _res("Errores y omisiones netos"),
}

_CUADRO_3 = {
    'Saldo del comercio de bienes': _res("Bienes"),
    '1.A.a.1 Mercancías generales según la balanza de pagos': _cc("Bienes", "Mercancías generales"),
    '1.A.a.2 Exportaciones netas de bienes de compraventa': _cc("Bienes", "Exportaciones netas de bienes en compraventa"),
    '1.A.a.3 Oro no monetario': _cc("Bienes", "Oro no monetario"),
    'Ingresos (Exportaciones)': _res("Bienes", f=CRED),
    'Mercancías generales en lo que se refiere a la balanza de pagos': _cc("Bienes", "Mercancías generales", f=CRED),
    'Exportaciones netas de bienes mediante compraventa (crédito)': _cc("Bienes", "Exportaciones netas de bienes en compraventa", f=CRED),
    'Bienes adquiridos en virtud de compraventa (crédito negativo)': _cc("Bienes", "Exportaciones netas de bienes en compraventa", "Bienes adquiridos en virtud de compraventa", f=CRED),
    'Bienes vendidos en virtud de compraventa': _cc("Bienes", "Exportaciones netas de bienes en compraventa", "Bienes vendidos en virtud de compraventa", f=CRED),
    'Oro no monetario': _cc("Bienes", "Oro no monetario", f=CRED),
    'Egresos (Importaciones) (1)': _res("Bienes", f=DEB),
    '1.A.a.3 Oro no monetario - Mercancías generales en lo que se refiere a la balanza de pagos': _cc("Bienes", "Mercancías generales", f=DEB),
    '1.A.a.3 Oro no monetario - Exportaciones netas de bienes mediante compraventa (crédito)': _cc("Bienes", "Exportaciones netas de bienes en compraventa", f=DEB),
    '1.A.a.3 Oro no monetario - Oro no monetario': _cc("Bienes", "Oro no monetario", f=DEB),
    'Estadísticas del comercio de bienes en "Intercambio Comercial Argentino"': _cc("Bienes", "Conciliación con el comercio exterior", "Estadísticas del comercio de bienes", f=CRED),
    'Ajuste por exportaciones netas de bienes en compraventa': _cc("Bienes", "Conciliación con el comercio exterior", "Ajuste por exportaciones netas de bienes en compraventa", f=CRED),
    'Bienes totales según la balanza de pagos': _cc("Bienes", "Conciliación con el comercio exterior", "Bienes totales según la balanza de pagos", f=CRED),
    '1.A.a.3 Oro no monetario - Estadísticas del comercio de bienes en "Intercambio Comercial Argentino"': _cc("Bienes", "Conciliación con el comercio exterior", "Estadísticas del comercio de bienes", f=DEB),
    'Ajuste por corrección fletes de importación (CIF/FOB)': _cc("Bienes", "Conciliación con el comercio exterior", "Ajuste por corrección de fletes de importación (CIF/FOB)", f=DEB),
    'Ajuste por corrección seguros de importación (CIF/FOB)': _cc("Bienes", "Conciliación con el comercio exterior", "Ajuste por corrección de seguros de importación (CIF/FOB)", f=DEB),
    '1.A.a.3 Oro no monetario - Bienes totales según la balanza de pagos': _cc("Bienes", "Conciliación con el comercio exterior", "Bienes totales según la balanza de pagos", f=DEB),
}

_CUADRO_4 = {
    'Saldo del comercio de servicios': _res("Servicios"),
    '1.A.b.1 Servicios de manufactura sobre insumos físicos pertenecientes a otros': _cc("Servicios", "Servicios de manufactura sobre insumos físicos pertenecientes a otros"),
    '1.A.b.2 Servicios de mantenimiento y reparaciones n.i.o.p.': _cc("Servicios", "Mantenimiento y reparaciones no incluidos en otra parte"),
    '1.A.b.3 Transporte': _cc("Servicios", "Transporte"),
    '1.A.b.4 Viajes': _cc("Servicios", "Viajes"),
    '1.A.b.5 Construcción': _cc("Servicios", "Construcción"),
    '1.A.b.6 Servicios de seguros y pensiones': _cc("Servicios", "Servicios de seguros y pensiones"),
    '1.A.b.7 Servicios financieros': _cc("Servicios", "Servicios financieros"),
    '1.A.b.8 Cargos por el uso de la propiedad intelectual n.i.o.p.': _cc("Servicios", "Cargos por el uso de la propiedad intelectual"),
    '1.A.b.9 Servicios de telecomunicaciones, informática y de información': _cc("Servicios", "Servicios de telecomunicaciones, informática e información"),
    '1.A.b.10 Otros servicios empresariales': _cc("Servicios", "Otros servicios empresariales"),
    '1.A.b.11 Servicios personales, culturales y recreativos': _cc("Servicios", "Servicios personales, culturales y recreativos"),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p.': _cc("Servicios", "Bienes y servicios del gobierno no incluidos en otra parte"),
    'INGRESOS SERVICIOS': _res("Servicios", f=CRED),
    'Servicios de manufactura sobre insumos físicos pertenecientes a otros': _cc("Servicios", "Servicios de manufactura sobre insumos físicos pertenecientes a otros", f=CRED),
    'Servicios de mantenimiento y reparaciones n.i.o.p.': _cc("Servicios", "Mantenimiento y reparaciones no incluidos en otra parte", f=CRED),
    'Transporte': _cc("Servicios", "Transporte", f=CRED),
    'Viajes': _cc("Servicios", "Viajes", f=CRED),
    'Construcción': _cc("Servicios", "Construcción", f=CRED),
    'Servicios de seguros y pensiones': _cc("Servicios", "Servicios de seguros y pensiones", f=CRED),
    'Servicios financieros': _cc("Servicios", "Servicios financieros", f=CRED),
    'Cargos por el uso de la propiedad intelectual n.i.o.p.': _cc("Servicios", "Cargos por el uso de la propiedad intelectual", f=CRED),
    'Servicios de telecomunicaciones, informática y de información': _cc("Servicios", "Servicios de telecomunicaciones, informática e información", f=CRED),
    'Otros servicios empresariales': _cc("Servicios", "Otros servicios empresariales", f=CRED),
    'Servicios personales, culturales y recreativos': _cc("Servicios", "Servicios personales, culturales y recreativos", f=CRED),
    'Bienes y servicios del gobierno, n.i.o.p.': _cc("Servicios", "Bienes y servicios del gobierno no incluidos en otra parte", f=CRED),
    'EGRESOS SERVICIOS': _res("Servicios", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Servicios de manufactura sobre insumos físicos pertenecientes a otros': _cc("Servicios", "Servicios de manufactura sobre insumos físicos pertenecientes a otros", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Servicios de mantenimiento y reparaciones n.i.o.p.': _cc("Servicios", "Mantenimiento y reparaciones no incluidos en otra parte", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Transporte': _cc("Servicios", "Transporte", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Viajes': _cc("Servicios", "Viajes", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Construcción': _cc("Servicios", "Construcción", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Servicios de seguros y pensiones': _cc("Servicios", "Servicios de seguros y pensiones", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Servicios financieros': _cc("Servicios", "Servicios financieros", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Cargos por el uso de la propiedad intelectual n.i.o.p.': _cc("Servicios", "Cargos por el uso de la propiedad intelectual", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Servicios de telecomunicaciones, informática y de información': _cc("Servicios", "Servicios de telecomunicaciones, informática e información", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Otros servicios empresariales': _cc("Servicios", "Otros servicios empresariales", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Servicios personales, culturales y recreativos': _cc("Servicios", "Servicios personales, culturales y recreativos", f=DEB),
    '1.A.b.12 Bienes y servicios del gobierno, n.i.o.p. - Bienes y servicios del gobierno, n.i.o.p.': _cc("Servicios", "Bienes y servicios del gobierno no incluidos en otra parte", f=DEB),
}

_CUADRO_5 = {
    '1.B Ingreso primario': _res(_IP),
    'Crédito': _res(_IP, f=CRED),
    '1.B.1 Remuneración de empleados': _cc(_IP, "Remuneración de empleados", f=CRED),
    '1.B.2 Renta de la inversión': _cc(_IP, "Renta de la inversión", f=CRED),
    '1.B.2.1 Inversión directa': _cc(_IP, _R_DIR, f=CRED),
    'S122. Sociedades captadoras de depósitos, excepto el banco central': _cc(_IP, _R_DIR, SCD, f=CRED),
    'S1Z. Otros sectores': _cc(_IP, _R_DIR, OS, f=CRED),
    '1.B.2.2 Inversión de cartera': _cc(_IP, _R_CART, f=CRED),
    'S121. Banco central': _cc(_IP, _R_CART, BC, f=CRED),
    'Crédito - 1.B.2.2 Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central': _cc(_IP, _R_CART, SCD, f=CRED),
    'S13. Gobierno general': _cc(_IP, _R_CART, GG, f=CRED),
    'Crédito - 1.B.2.2 Inversión de cartera - S1Z. Otros sectores': _cc(_IP, _R_CART, OS, f=CRED),
    '1.B.2.3 Otra inversión': _cc(_IP, _R_OTRA, f=CRED),
    'Crédito - 1.B.2.3 Otra inversión - S121. Banco central': _cc(_IP, _R_OTRA, BC, f=CRED),
    'Crédito - 1.B.2.3 Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central': _cc(_IP, _R_OTRA, SCD, f=CRED),
    'Crédito - 1.B.2.3 Otra inversión - S13. Gobierno general': _cc(_IP, _R_OTRA, GG, f=CRED),
    'Crédito - 1.B.2.3 Otra inversión - S1Z. Otros sectores': _cc(_IP, _R_OTRA, OS, f=CRED),
    '1.B.2.4 Activos de reserva': _cc(_IP, "Renta de activos de reserva", f=CRED),
    'Débito': _res(_IP, f=DEB),
    'Débito - 1.B.1 Remuneración de empleados': _cc(_IP, "Remuneración de empleados", f=DEB),
    'Débito - 1.B.2 Renta de la inversión': _cc(_IP, "Renta de la inversión", f=DEB),
    'Débito - 1.B.2.1 Inversión directa': _cc(_IP, _R_DIR, f=DEB),
    'Débito - 1.B.2.1 Inversión directa - S122. Sociedades captadoras de depósitos, excepto el banco central': _cc(_IP, _R_DIR, SCD, f=DEB),
    'Débito - 1.B.2.1 Inversión directa - S1Z. Otros sectores': _cc(_IP, _R_DIR, OS, f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera': _cc(_IP, _R_CART, f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera - S121. Banco central': _cc(_IP, _R_CART, BC, f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central': _cc(_IP, _R_CART, SCD, f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera - S13. Gobierno general': _cc(_IP, _R_CART, GG, f=DEB),
    'Débito - 1.B.2.2 Inversión de cartera - S1Z. Otros sectores': _cc(_IP, _R_CART, OS, f=DEB),
    'Débito - 1.B.2.3 Otra inversión': _cc(_IP, _R_OTRA, f=DEB),
    'Débito - 1.B.2.3 Otra inversión - S121. Banco central': _cc(_IP, _R_OTRA, BC, f=DEB),
    'Débito - 1.B.2.3 Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central': _cc(_IP, _R_OTRA, SCD, f=DEB),
    'Débito - 1.B.2.3 Otra inversión - S13. Gobierno general': _cc(_IP, _R_OTRA, GG, f=DEB),
    'Débito - 1.B.2.3 Otra inversión - S1Z. Otros sectores': _cc(_IP, _R_OTRA, OS, f=DEB),
}

_CUADRO_6 = {
    '3.1 Inversión directa': _cf(IED_, NETO),
    'Adquisición neta de activos financieros': _cf(IED_, ACT),
    '3.1.1 Participaciones de capital y participaciones en fondos de inversión': _cf(IED_, ACT, PARTICIPACIONES),
    'S122. Sociedades captadoras de depósitos, excepto el banco central': _cf(IED_, ACT, PARTICIPACIONES, SCD),
    'S1Z. Otros sectores': _cf(IED_, ACT, PARTICIPACIONES, OS),
    '3.1.2 Instrumentos de deuda': _cf(IED_, ACT, "Instrumentos de deuda"),
    'Adquisición neta de activos financieros - 3.1.2 Instrumentos de deuda - S122. Sociedades captadoras de depósitos, excepto el banco central': _cf(IED_, ACT, "Instrumentos de deuda", SCD),
    'Adquisición neta de activos financieros - 3.1.2 Instrumentos de deuda - S1Z. Otros sectores': _cf(IED_, ACT, "Instrumentos de deuda", OS),
    'Emisión neta de pasivos': _cf(IED_, PAS),
    'Emisión neta de pasivos - 3.1.1 Participaciones de capital y participaciones en fondos de inversión': _cf(IED_, PAS, PARTICIPACIONES),
    'Emisión neta de pasivos - 3.1.1 Participaciones de capital y participaciones en fondos de inversión - S122. Sociedades captadoras de depósitos, excepto el banco central': _cf(IED_, PAS, PARTICIPACIONES, SCD),
    'Emisión neta de pasivos - 3.1.1 Participaciones de capital y participaciones en fondos de inversión - S1Z. Otros sectores': _cf(IED_, PAS, PARTICIPACIONES, OS),
    'Emisión neta de pasivos - 3.1.2 Instrumentos de deuda': _cf(IED_, PAS, "Instrumentos de deuda"),
    'Emisión neta de pasivos - 3.1.2 Instrumentos de deuda - S122. Sociedades captadoras de depósitos, excepto el banco central': _cf(IED_, PAS, "Instrumentos de deuda", SCD),
    'Emisión neta de pasivos - 3.1.2 Instrumentos de deuda - S1Z. Otros sectores': _cf(IED_, PAS, "Instrumentos de deuda", OS),
}

_CUADRO_7 = {
    '3.2 Inversión de cartera': _cf(CART, NETO),
    'Adquisición neta de activos financieros': _cf(CART, ACT),
    '3.2.1 Participaciones de capital y participaciones en fondos de inversión': _cf(CART, ACT, PARTICIPACIONES),
    '3.2.1.1 Banco central (S121)': _cf(CART, ACT, PARTICIPACIONES, BC),
    '3.2.1.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(CART, ACT, PARTICIPACIONES, SCD),
    '3.2.1.3 Gobierno general (S13)': _cf(CART, ACT, PARTICIPACIONES, GG),
    '3.2.1.4 Otros sectores (S1Z)': _cf(CART, ACT, PARTICIPACIONES, OS),
    '3.2.2 Títulos de deuda': _cf(CART, ACT, "Títulos de deuda"),
    '3.2.2.1 Banco central (S121)': _cf(CART, ACT, "Títulos de deuda", BC),
    '3.2.2.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(CART, ACT, "Títulos de deuda", SCD),
    '3.2.2.3 Gobierno general (S13)': _cf(CART, ACT, "Títulos de deuda", GG),
    '3.2.2.4 Otros sectores (S1Z)': _cf(CART, ACT, "Títulos de deuda", OS),
    'Emisión neta de pasivos': _cf(CART, PAS),
    'Emisión neta de pasivos - 3.2.1 Participaciones de capital y participaciones en fondos de inversión': _cf(CART, PAS, PARTICIPACIONES),
    'Emisión neta de pasivos - 3.2.1.1 Banco central (S121)': _cf(CART, PAS, PARTICIPACIONES, BC),
    'Emisión neta de pasivos - 3.2.1.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(CART, PAS, PARTICIPACIONES, SCD),
    'Emisión neta de pasivos - 3.2.1.3 Gobierno general (S13)': _cf(CART, PAS, PARTICIPACIONES, GG),
    'Emisión neta de pasivos - 3.2.1.4 Otros sectores (S1Z)': _cf(CART, PAS, PARTICIPACIONES, OS),
    'Emisión neta de pasivos - 3.2.2 Títulos de deuda': _cf(CART, PAS, "Títulos de deuda"),
    'Emisión neta de pasivos - 3.2.2.1 Banco central (S121)': _cf(CART, PAS, "Títulos de deuda", BC),
    'Emisión neta de pasivos - 3.2.2.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(CART, PAS, "Títulos de deuda", SCD),
    'Emisión neta de pasivos - 3.2.2.3 Gobierno general (S13)': _cf(CART, PAS, "Títulos de deuda", GG),
    'Emisión neta de pasivos - 3.2.2.4 Otros sectores (S1Z)': _cf(CART, PAS, "Títulos de deuda", OS),
    '3.3 Derivados financieros netos': _cf(DERIV, NETO),
    '3.3.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(DERIV, NETO, SCD),
    '3.3.3 Gobierno general (S13)': _cf(DERIV, NETO, GG),
    '3.3.4 Otros sectores (S1Z)': _cf(DERIV, NETO, OS),
}

_CUADRO_8 = {
    '3.4 Otra inversión': _cf(OTRA, NETO),
    'Adquisición neta de activos financieros': _cf(OTRA, ACT),
    '3.4.1 Otras participaciones de capital': _cf(OTRA, ACT, "Otras participaciones de capital"),
    '3.4.2 Moneda y depósitos': _cf(OTRA, ACT, "Moneda y depósitos"),
    '3.4.2.1 Banco central (S121)': _cf(OTRA, ACT, "Moneda y depósitos", BC),
    '3.4.2.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, ACT, "Moneda y depósitos", SCD),
    '3.4.2.3 Gobierno general (S13)': _cf(OTRA, ACT, "Moneda y depósitos", GG),
    '3.4.2.4 Otros sectores (S1Z)': _cf(OTRA, ACT, "Moneda y depósitos", OS),
    '3.4.3 Préstamos': _cf(OTRA, ACT, "Préstamos"),
    '3.4.3.1 Banco central (S121)': _cf(OTRA, ACT, "Préstamos", BC),
    '3.4.3.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, ACT, "Préstamos", SCD),
    '3.4.3.3 Gobierno general (S13)': _cf(OTRA, ACT, "Préstamos", GG),
    '3.4.3.4 Otros sectores (S1Z)': _cf(OTRA, ACT, "Préstamos", OS),
    '3.4.4 Seguros, pensiones y mecanismos normalizados de garantía': _cf(OTRA, ACT, "Seguros, pensiones y mecanismos normalizados de garantía"),
    '3.4.4.1 Banco central (S121)': _cf(OTRA, ACT, "Seguros, pensiones y mecanismos normalizados de garantía", BC),
    '3.4.4.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, ACT, "Seguros, pensiones y mecanismos normalizados de garantía", SCD),
    '3.4.4.3 Gobierno general (S13)': _cf(OTRA, ACT, "Seguros, pensiones y mecanismos normalizados de garantía", GG),
    '3.4.4.4 Otros sectores (S1Z)': _cf(OTRA, ACT, "Seguros, pensiones y mecanismos normalizados de garantía", OS),
    '3.4.5 Créditos y anticipos comerciales': _cf(OTRA, ACT, CRED_ANT),
    '3.4.5.1 Banco central (S121)': _cf(OTRA, ACT, CRED_ANT, BC),
    '3.4.5.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, ACT, CRED_ANT, SCD),
    '3.4.5.3 Gobierno general (S13)': _cf(OTRA, ACT, CRED_ANT, GG),
    '3.4.5.4 Otros sectores (S1Z)': _cf(OTRA, ACT, CRED_ANT, OS),
    '3.4.6 Otras cuentas por cobrar/por pagar': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR),
    '3.4.6.1 Banco central (S121)': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR, BC),
    '3.4.6.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR, SCD),
    '3.4.6.3 Gobierno general (S13)': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR, GG),
    '3.4.6.4 Otros sectores (S1Z)': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR, OS),
    'Emisión neta de pasivos': _cf(OTRA, PAS),
    'Emisión neta de pasivos - 3.4.1 Otras participaciones de capital': _cf(OTRA, PAS, "Otras participaciones de capital"),
    'Emisión neta de pasivos - 3.4.2 Moneda y depósitos': _cf(OTRA, PAS, "Moneda y depósitos"),
    'Emisión neta de pasivos - 3.4.2.1 Banco central (S121)': _cf(OTRA, PAS, "Moneda y depósitos", BC),
    'Emisión neta de pasivos - 3.4.2.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, PAS, "Moneda y depósitos", SCD),
    'Emisión neta de pasivos - 3.4.2.3 Gobierno general (S13)': _cf(OTRA, PAS, "Moneda y depósitos", GG),
    'Emisión neta de pasivos - 3.4.2.4 Otros sectores (S1Z)': _cf(OTRA, PAS, "Moneda y depósitos", OS),
    'Emisión neta de pasivos - 3.4.3 Préstamos': _cf(OTRA, PAS, "Préstamos"),
    'Emisión neta de pasivos - 3.4.3.1 Banco central (S121)': _cf(OTRA, PAS, "Préstamos", BC),
    'Emisión neta de pasivos - 3.4.3.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, PAS, "Préstamos", SCD),
    'Emisión neta de pasivos - 3.4.3.3 Gobierno general (S13)': _cf(OTRA, PAS, "Préstamos", GG),
    'Emisión neta de pasivos - 3.4.3.4 Otros sectores (S1Z)': _cf(OTRA, PAS, "Préstamos", OS),
    'Emisión neta de pasivos - 3.4.4 Seguros, pensiones y mecanismos normalizados de garantía': _cf(OTRA, PAS, "Seguros, pensiones y mecanismos normalizados de garantía"),
    'Emisión neta de pasivos - 3.4.4.1 Banco central (S121)': _cf(OTRA, PAS, "Seguros, pensiones y mecanismos normalizados de garantía", BC),
    'Emisión neta de pasivos - 3.4.4.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, PAS, "Seguros, pensiones y mecanismos normalizados de garantía", SCD),
    'Emisión neta de pasivos - 3.4.4.3 Gobierno general (S13)': _cf(OTRA, PAS, "Seguros, pensiones y mecanismos normalizados de garantía", GG),
    'Emisión neta de pasivos - 3.4.4.4 Otros sectores (S1Z)': _cf(OTRA, PAS, "Seguros, pensiones y mecanismos normalizados de garantía", OS),
    'Emisión neta de pasivos - 3.4.5 Créditos y anticipos comerciales': _cf(OTRA, PAS, CRED_ANT),
    'Emisión neta de pasivos - 3.4.5.1 Banco central (S121)': _cf(OTRA, PAS, CRED_ANT, BC),
    'Emisión neta de pasivos - 3.4.5.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, PAS, CRED_ANT, SCD),
    'Emisión neta de pasivos - 3.4.5.3 Gobierno general (S13)': _cf(OTRA, PAS, CRED_ANT, GG),
    'Emisión neta de pasivos - 3.4.5.4 Otros sectores (S1Z)': _cf(OTRA, PAS, CRED_ANT, OS),
    'Emisión neta de pasivos - 3.4.6 Otras cuentas por cobrar/por pagar': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR),
    'Emisión neta de pasivos - 3.4.6.1 Banco central (S121)': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, BC),
    'Emisión neta de pasivos - 3.4.6.2 Sociedades captadoras de depósitos, excepto el banco central (S122)': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, SCD),
    'Emisión neta de pasivos - 3.4.6.3 Gobierno general (S13)': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, GG),
    'Emisión neta de pasivos - 3.4.6.4 Otros sectores (S1Z)': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, OS),
    '3.4.7 Derechos especiales de giro (emisión neta de pasivos)': _cf(OTRA, PAS, DEG_ASIGNACIONES),
}

_CUADRO_9 = {
    '3.5 Activos de reserva': _cf(RESERVA, ""),
    '3.5.1 Oro monetario': _cf(RESERVA, "", "Oro monetario"),
    '3.5.1.1 Oro en lingotes': _cf(RESERVA, "", "Oro monetario", "Oro en lingotes"),
    '3.5.1.2 Cuentas de oro no asignadas': _cf(RESERVA, "", "Oro monetario", "Cuentas de oro no asignadas"),
    '3.5.2 Derechos especiales de giro': _cf(RESERVA, "", "Derechos especiales de giro"),
    '3.5.3 Posición de reserva en el FMI': _cf(RESERVA, "", "Posición de reserva en el FMI"),
    '3.5.4 Otros activos de reserva': _cf(RESERVA, "", "Otros activos de reserva"),
    '3.5.4.1 Moneda y depósitos': _cf(RESERVA, "", "Otros activos de reserva", "Moneda y depósitos"),
    '3.5.4.2 Títulos-valores': _cf(RESERVA, "", "Otros activos de reserva", "Títulos"),
    '3.5.4.3 Derivados financieros': _cf(RESERVA, "", "Otros activos de reserva", DERIV),
    '3.5.4.4 Otros activos': _cf(RESERVA, "", "Otros activos de reserva", "Otros derechos sobre activos"),
}

_CUADRO_10 = {
    'S121. Total Banco central': _res("Cuenta financiera", BC),
    'Activos': _res("Cuenta financiera", BC, ACT),
    '3.2 Inversión de cartera': _cf(CART, ACT, BC),
    '3.3 Instrumentos financieros derivados': _cf(DERIV, ACT, BC),
    '3.4 Otra inversión': _cf(OTRA, ACT, BC),
    '3.5 Activos de reserva': _cf(RESERVA, ""),
    '3.5.1 Oro monetario': _cf(RESERVA, "", "Oro monetario"),
    '3.5.1.1 Oro en lingotes': _cf(RESERVA, "", "Oro monetario", "Oro en lingotes"),
    '3.5.1.2 Cuentas de oro no asignadas': _cf(RESERVA, "", "Oro monetario", "Cuentas de oro no asignadas"),
    '3.5.2 Derechos especiales de giro': _cf(RESERVA, "", "Derechos especiales de giro"),
    '3.5.3 Posición de reserva en el FMI': _cf(RESERVA, "", "Posición de reserva en el FMI"),
    '3.5.4 Otros activos de reserva': _cf(RESERVA, "", "Otros activos de reserva"),
    'Pasivos': _res("Cuenta financiera", BC, PAS),
    'Pasivos - 3.2 Inversión de cartera': _cf(CART, PAS, BC),
    'Pasivos - 3.3 Instrumentos financieros derivados': _cf(DERIV, PAS, BC),
    'Pasivos - 3.4 Otra inversión': _cf(OTRA, PAS, BC),
    'Moneda y depósitos': _cf(OTRA, PAS, "Moneda y depósitos", BC),
    'Préstamos': _cf(OTRA, PAS, "Préstamos", BC),
    'Créditos y anticipos comerciales': _cf(OTRA, PAS, CRED_ANT, BC),
    'Otras cuentas por pagar': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, BC),
    'Derechos especiales de giro (emisión neta de pasivos)': _cf(OTRA, PAS, DEG_ASIGNACIONES),
}

_CUADRO_11 = {
    'S13. Total Gobierno general': _res("Cuenta financiera", GG),
    'Activos': _res("Cuenta financiera", GG, ACT),
    '3.2 Inversión de cartera': _cf(CART, ACT, GG),
    '3.3 Instrumentos financieros derivados': _cf(DERIV, ACT, GG),
    '3.4 Otra inversión': _cf(OTRA, ACT, GG),
    'Otras participaciones de capital': _cf(OTRA, ACT, "Otras participaciones de capital", GG),
    'Moneda y depósitos': _cf(OTRA, ACT, "Moneda y depósitos", GG),
    'Préstamos': _cf(OTRA, ACT, "Préstamos", GG),
    'Créditos y anticipos comerciales': _cf(OTRA, ACT, CRED_ANT, GG),
    'Pasivos': _res("Cuenta financiera", GG, PAS),
    'Pasivos - 3.2 Inversión de cartera': _cf(CART, PAS, GG),
    'Pasivos - 3.3 Instrumentos financieros derivados': _cf(DERIV, PAS, GG),
    'Pasivos - 3.4 Otra inversión': _cf(OTRA, PAS, GG),
    'Pasivos - 3.4 Otra inversión - Moneda y depósitos': _cf(OTRA, PAS, "Moneda y depósitos", GG),
    'Pasivos - 3.4 Otra inversión - Préstamos': _cf(OTRA, PAS, "Préstamos", GG),
    'Pasivos - 3.4 Otra inversión - Créditos y anticipos comerciales': _cf(OTRA, PAS, CRED_ANT, GG),
    'Otras cuentas por pagar': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, GG),
}

_CUADRO_12 = {
    'S122. Sociedades captadoras de depósitos, excepto el banco central': _res("Cuenta financiera", SCD),
    'Adquisición neta de activos financieros': _res("Cuenta financiera", SCD, ACT),
    'Inversión directa': _cf(IED_, ACT, SCD),
    'Inversión de cartera': _cf(CART, ACT, SCD),
    'Participaciones de capital y en fondos de inversión': _cf(CART, ACT, PARTICIPACIONES, SCD),
    'Títulos de deuda': _cf(CART, ACT, "Títulos de deuda", SCD),
    'Derivados financieros': _cf(DERIV, ACT, SCD),
    'Otra inversión': _cf(OTRA, ACT, SCD),
    'Moneda y depósitos': _cf(OTRA, ACT, "Moneda y depósitos", SCD),
    'Préstamos': _cf(OTRA, ACT, "Préstamos", SCD),
    'Seguros, pensiones y mecanismos normalizados de garantía': _cf(OTRA, ACT, "Seguros, pensiones y mecanismos normalizados de garantía", SCD),
    'Créditos y anticipos comerciales': _cf(OTRA, ACT, CRED_ANT, SCD),
    'Otras cuentas por cobrar/por pagar': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR, SCD),
    'Emisión neta de pasivos': _res("Cuenta financiera", SCD, PAS),
    'Emisión neta de pasivos - S122. Sociedades captadoras de depósitos, excepto el banco central - Inversión directa': _cf(IED_, PAS, SCD),
    'Emisión neta de pasivos - Inversión directa - S122. Sociedades captadoras de depósitos, excepto el banco central - Inversión de cartera': _cf(CART, PAS, SCD),
    'Emisión neta de pasivos - Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central - Participaciones de capital y en fondos de inversión': _cf(CART, PAS, PARTICIPACIONES, SCD),
    'Emisión neta de pasivos - Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central - Títulos de deuda': _cf(CART, PAS, "Títulos de deuda", SCD),
    'Emisión neta de pasivos - Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central - Derivados financieros': _cf(DERIV, PAS, SCD),
    'Emisión neta de pasivos - Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central - Otra inversión': _cf(OTRA, PAS, SCD),
    'Emisión neta de pasivos - Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central - Moneda y depósitos': _cf(OTRA, PAS, "Moneda y depósitos", SCD),
    'Emisión neta de pasivos - Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central - Préstamos': _cf(OTRA, PAS, "Préstamos", SCD),
    'Emisión neta de pasivos - Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central - Seguros, pensiones y mecanismos normalizados de garantía': _cf(OTRA, PAS, "Seguros, pensiones y mecanismos normalizados de garantía", SCD),
    'Emisión neta de pasivos - Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central - Créditos y anticipos comerciales': _cf(OTRA, PAS, CRED_ANT, SCD),
    'Emisión neta de pasivos - Otra inversión - S122. Sociedades captadoras de depósitos, excepto el banco central - Otras cuentas por cobrar/por pagar': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, SCD),
}

_CUADRO_13 = {
    'S1Z. Otros sectores': _res("Cuenta financiera", OS),
    'Adquisición neta de activos financieros': _res("Cuenta financiera", OS, ACT),
    'Inversión directa': _cf(IED_, ACT, OS),
    'Participaciones de capital distintas de reinversión de utilidades': _cf(IED_, ACT, SIN_REINVERSION, OS),
    'Reinversión de utilidades': _cf(IED_, ACT, "Utilidades reinvertidas", OS),
    'Inversión de cartera': _cf(CART, ACT, OS),
    'Participaciones de capital y en fondos de inversión': _cf(CART, ACT, PARTICIPACIONES, OS),
    'Instrumentos de deuda': _cf(CART, ACT, "Títulos de deuda", OS),
    'Otra inversión': _cf(OTRA, ACT, OS),
    'Moneda y depósitos': _cf(OTRA, ACT, "Moneda y depósitos", OS),
    'Créditos y anticipos comerciales': _cf(OTRA, ACT, CRED_ANT, OS),
    'Otras cuentas por cobrar/por pagar': _cf(OTRA, ACT, CUENTAS_COBRAR_PAGAR, OS),
    'Emisión neta de pasivos': _res("Cuenta financiera", OS, PAS),
    'Emisión neta de pasivos - S1Z. Otros sectores - Inversión directa': _cf(IED_, PAS, OS),
    'Emisión neta de pasivos - Inversión directa - S1Z. Otros sectores - Participaciones de capital distintas de reinversión de utilidades': _cf(IED_, PAS, SIN_REINVERSION, OS),
    'Emisión neta de pasivos - Inversión directa - S1Z. Otros sectores - Reinversión de utilidades': _cf(IED_, PAS, "Utilidades reinvertidas", OS),
    'Emisión neta de pasivos - Inversión directa - S1Z. Otros sectores - Instrumentos de deuda': _cf(IED_, PAS, "Instrumentos de deuda", OS),
    'Emisión neta de pasivos - Inversión directa - S1Z. Otros sectores - Inversión de cartera': _cf(CART, PAS, OS),
    'Emisión neta de pasivos - Inversión de cartera - S1Z. Otros sectores - Participaciones de capital y en fondos de inversión': _cf(CART, PAS, PARTICIPACIONES, OS),
    'Emisión neta de pasivos - Inversión de cartera - S1Z. Otros sectores - Instrumentos de deuda': _cf(CART, PAS, "Títulos de deuda", OS),
    'Emisión neta de pasivos - Inversión de cartera - S1Z. Otros sectores - Otra inversión': _cf(OTRA, PAS, OS),
    'Préstamos': _cf(OTRA, PAS, "Préstamos", OS),
    'Emisión neta de pasivos - Otra inversión - S1Z. Otros sectores - Créditos y anticipos comerciales': _cf(OTRA, PAS, CRED_ANT, OS),
    'Emisión neta de pasivos - Otra inversión - S1Z. Otros sectores - Otras cuentas por cobrar/por pagar': _cf(OTRA, PAS, CUENTAS_COBRAR_PAGAR, OS),
}


# ── Posición de inversión internacional ────────────────────────────────────────────────────────
PII = "PII"
POSICION_NETA = "Posición neta"
FACTORES = "Factores de variación"
A_VALOR_NOMINAL = "valor nominal"


def _pii(lado, categoria, detalle=(), nominal=False, factor=""):
    """Concepto de la PII: lado (Posición neta, Activos, Pasivos), categoría funcional (o Total) y detalle."""
    return (lado, categoria, tuple(detalle), nominal, factor)


def _pii_clasificacion(concepto) -> Clasificacion:
    lado, categoria, detalle, nominal, factor = concepto
    partes = [PII]
    if factor and factor != "saldo final":
        partes.append(FACTORES)
    partes += [lado, "" if categoria == TOTAL else categoria, *detalle]
    medida = ", ".join(m for m in (A_VALOR_NOMINAL if nominal else "", factor if factor != "saldo final" else "") if m)
    if factor and factor != "saldo final":
        cobertura, apertura = FACTORES, lado
    else:
        cobertura = lado
        apertura = "" if lado == POSICION_NETA else categoria
    return Clasificacion(tema=TEMA, indicador=INDICADOR_PII, cobertura=cobertura, apertura=apertura,
                         nombre=con_medida(unir(*_corto(partes)), medida), unidad=UNIDAD)


_LADO_PII = {"A": ACT, "L": PAS, "N": POSICION_NETA}


def _pii_sdmx(serie: Serie, nominal: bool) -> Clasificacion | None:
    """Cuadros 19 y 22: la clave SDMX del ID da lado, categoría funcional, instrumento y sector."""
    campos = serie.id.split("|")[-1].split(".")
    if len(campos) != 16 or campos[8] != "FA":
        return None
    lado = _LADO_PII.get(campos[7])
    partida = _partida_financiera(campos)
    if lado is None or partida is None:
        return None
    categoria, detalle = partida
    if categoria == RESERVA:
        lado = ACT
    if categoria == OTRA and lado == PAS and detalle == (CUENTAS_COBRAR_PAGAR,):
        # El total de «Otras cuentas por pagar» de estos cuadros incluye las asignaciones de DEG (que además se
        # publican aparte): 1.469,8 + 6.880,1 = 8.349,8, mientras que los sectores y el cuadro 15 suman 1.469,8.
        detalle = ("Otras cuentas por pagar, incluidas asignaciones de DEG",)
    return _pii_clasificacion(_pii(lado, categoria, detalle, nominal))


# Cuadros 16 y 21 (por sector institucional): «<lado> - <sector> - <categoría funcional>».
_SECTOR_PII = {"S121": BC, "S122": SCD, "S13": GG, "S1Z": OS}
_CATEGORIA_PII = {"1": IED_, "2": CART, "3": DERIV, "4": OTRA, "5": RESERVA}


def _lado_de(rotulo: str) -> str | None:
    clave = plano(rotulo)
    if clave.startswith("b90") or clave.startswith("posicion de inversion internacional neta"):
        return POSICION_NETA
    if clave.startswith("a. activos"):
        return ACT
    if clave.startswith("p. pasivos"):
        return PAS
    return None


def _pii_sectores(serie: Serie, nominal: bool) -> Clasificacion | None:
    partes = [p.strip() for p in serie.columna.split(" - ")]
    lado = _lado_de(partes[0])
    if lado is None:
        return None
    sector = categoria = None
    for parte in partes[1:]:
        if parte[:4] in _SECTOR_PII or parte[:3] in _SECTOR_PII:
            sector = _SECTOR_PII.get(parte.split(".")[0])
        elif parte[:2].rstrip(".") in _CATEGORIA_PII:
            categoria = _CATEGORIA_PII[parte[:1]]
        else:
            return None
    if sector is None:
        return _pii_clasificacion(_pii(lado, TOTAL, (), nominal)) if len(partes) == 1 else None
    if categoria is None:
        return _pii_clasificacion(_pii(lado, TOTAL, (sector,), nominal))
    if categoria == RESERVA:
        return _pii_clasificacion(_pii(lado, RESERVA, (), nominal))
    return _pii_clasificacion(_pii(lado, categoria, (sector,), nominal))


# Cuadro 17 (por moneda): «<lado> - [<categoría>] - [<instrumento>] - [<moneda>]».
_MONEDAS = {
    "dolar": "En dólares", "euro": "En euros", "yen": "En yenes", "libra esterlina": "En libras esterlinas",
    "renminbi": "En renminbi", "moneda nacional": "En moneda nacional", "otras monedas": "En otras monedas",
    "canasta de monedas del deg (derechos especiales de giro)": "En la canasta de monedas del DEG",
    "monedas distintas del deg": "En monedas distintas del DEG",
}


def _pii_monedas(serie: Serie) -> Clasificacion | None:
    partes = [p.strip() for p in serie.columna.split(" - ")]
    lado = _lado_de(partes[0])
    if lado is None:
        return None
    categoria, detalle = TOTAL, []
    for parte in partes[1:]:
        clave = plano(parte)
        clave = clave[:-4].strip() if clave.endswith(" (1)") else clave
        if clave in _MONEDAS:
            detalle.append(_MONEDAS[clave])
        elif parte[:2].rstrip(".") in _CATEGORIA_PII:
            categoria = _CATEGORIA_PII[parte[:1]]
        elif clave == "participaciones de capital":
            detalle.append(PARTICIPACIONES)
        elif clave == "instrumentos de deuda":
            detalle.append("Instrumentos de deuda" if categoria == IED_ else "Títulos de deuda")
        else:
            return None
    return _pii_clasificacion(_pii(lado, categoria, detalle))


# Cuadro 20 (factores de variación): «<lado> - [<categoría romana> - <a. instrumento>] - <factor>».
_ROMANO = {"I": IED_, "II": CART, "III": DERIV, "IV": OTRA, "V": RESERVA}
_FACTORES_PII = {
    "saldo inicial": "saldo inicial", "transacciones financieras": "transacciones financieras",
    "revalorizaciones durante el periodo": "revalorizaciones", "variaciones de precio": "efecto precio",
    "variaciones de tipo de cambio": "efecto tipo de cambio",
    "otras variaciones de volumen": "otras variaciones de volumen", "saldo final": "saldo final",
}


def _pii_factores(serie: Serie) -> Clasificacion | None:
    partes = [p.strip() for p in serie.columna.split(" - ")]
    lado = _lado_de(partes[0])
    if lado is None or len(partes) < 2:
        return None
    factor = _FACTORES_PII.get(plano(partes[-1]))
    if factor is None and partes[-1] == "0.0":
        factor = "columna sin rótulo"   # columna espuria del cuadro: ceros en un tramo de la serie
    if factor is None:
        return None
    categoria, detalle = TOTAL, []
    for parte in partes[1:-1]:
        clave = plano(parte)
        if parte.split(".")[0] in _ROMANO and not parte.startswith("a."):
            categoria = _ROMANO[parte.split(".")[0]]
        elif clave == "a. participaciones de capital":
            detalle.append(PARTICIPACIONES)
        elif clave == "a. instrumentos de deuda":
            detalle.append("Instrumentos de deuda" if categoria == IED_ else "Títulos de deuda")
        else:
            return None
    return _pii_clasificacion(_pii(lado, categoria, detalle, False, factor))


# Hoja 16.a de datos.gob.ar (PII a valor de mercado, anual y trimestral): ID → (lado, categoría, detalle).
# Los rótulos del libro de datos.gob.ar de los cuadros por sector están corridos (p. ej. «activos del gobierno
# general» trae los activos de otra inversión de las sociedades captadoras de depósitos), así que cada serie se
# identificó cotejando sus valores con los cuadros 15 y 16 del INDEC, que coinciden al decimal.

_CUADRO_15 = {
    'B90. POSICIÓN DE INVERSIÓN INTERNACIONAL NETA (A-P)': _pii(POSICION_NETA, TOTAL),
    'A. ACTIVOS': _pii(ACT, TOTAL),
    '1. Inversión directa': _pii(ACT, IED_),
    '1.1 Participaciones en el capital y utilidades reinvertidas': _pii(ACT, IED_, (PARTICIPACIONES,)),
    '1.2 Instrumentos de deuda': _pii(ACT, IED_, ("Instrumentos de deuda",)),
    '2. Inversión de cartera': _pii(ACT, CART),
    '2.1 Participación de capital y participaciones en fondos de inversión': _pii(ACT, CART, (PARTICIPACIONES,)),
    '2.2 Títulos de deuda': _pii(ACT, CART, ("Títulos de deuda",)),
    '3. Derivados financieros': _pii(ACT, DERIV),
    '4. Otra Inversión': _pii(ACT, OTRA),
    '4.1 Otras participaciones de capital': _pii(ACT, OTRA, ("Otras participaciones de capital",)),
    '4.2 Moneda y depósitos': _pii(ACT, OTRA, ("Moneda y depósitos",)),
    '4.3 Préstamos': _pii(ACT, OTRA, ("Préstamos",)),
    '4.4 Seguros, pensiones y mecanismos normalizados de garantía': _pii(ACT, OTRA, ("Seguros, pensiones y mecanismos normalizados de garantía",)),
    '4.5 Créditos y anticipos comerciales': _pii(ACT, OTRA, (CRED_ANT,)),
    '4.6 Otras cuentas por cobrar': _pii(ACT, OTRA, (CUENTAS_COBRAR_PAGAR,)),
    '5. Activos de reserva': _pii(ACT, RESERVA),
    '5.1 Oro monetario': _pii(ACT, RESERVA, ("Oro monetario",)),
    '5.2 Derechos especiales de giro': _pii(ACT, RESERVA, ("Derechos especiales de giro",)),
    '5.3 Posición de reserva en el FMI': _pii(ACT, RESERVA, ("Posición de reserva en el FMI",)),
    '5.4 Otros activos de reserva': _pii(ACT, RESERVA, ("Otros activos de reserva",)),
    '5.4.1 Monedas y depósitos': _pii(ACT, RESERVA, ("Otros activos de reserva", "Moneda y depósitos",)),
    '5.4.2 Títulos': _pii(ACT, RESERVA, ("Otros activos de reserva", "Títulos",)),
    '5.4.3 Derivados financieros': _pii(ACT, RESERVA, ("Otros activos de reserva", DERIV,)),
    '5.4.4 Otros derechos sobre activos': _pii(ACT, RESERVA, ("Otros activos de reserva", "Otros derechos sobre activos",)),
    'P. PASIVOS': _pii(PAS, TOTAL),
    'P. PASIVOS - 1. Inversión directa': _pii(PAS, IED_),
    'P. PASIVOS - 1.1 Participaciones en el capital y utilidades reinvertidas': _pii(PAS, IED_, (PARTICIPACIONES,)),
    'P. PASIVOS - 1.2 Instrumentos de deuda': _pii(PAS, IED_, ("Instrumentos de deuda",)),
    'P. PASIVOS - 2. Inversión de cartera': _pii(PAS, CART),
    'P. PASIVOS - 2.1 Participación de capital y participaciones en fondos de inversión': _pii(PAS, CART, (PARTICIPACIONES,)),
    'P. PASIVOS - 2.2 Títulos de deuda': _pii(PAS, CART, ("Títulos de deuda",)),
    'P. PASIVOS - 3. Derivados financieros': _pii(PAS, DERIV),
    'P. PASIVOS - 4. Otra Inversión': _pii(PAS, OTRA),
    'P. PASIVOS - 4.1 Otras participaciones de capital': _pii(PAS, OTRA, ("Otras participaciones de capital",)),
    'P. PASIVOS - 4.2 Moneda y depósitos': _pii(PAS, OTRA, ("Moneda y depósitos",)),
    'P. PASIVOS - 4.3 Préstamos': _pii(PAS, OTRA, ("Préstamos",)),
    'P. PASIVOS - 4.4 Seguros, pensiones y mecanismos normalizados de garantía': _pii(PAS, OTRA, ("Seguros, pensiones y mecanismos normalizados de garantía",)),
    'P. PASIVOS - 4.5 Créditos y anticipos comerciales': _pii(PAS, OTRA, (CRED_ANT,)),
    '4.6 Otras cuentas por pagar': _pii(PAS, OTRA, (CUENTAS_COBRAR_PAGAR,)),
    '4.7 Derechos especiales de giro': _pii(PAS, OTRA, (DEG_ASIGNACIONES,)),
}

_CUADRO_18 = {
    'Posición de inversión internacional neta de otros sectores (A-P)': _pii(POSICION_NETA, TOTAL, (OS,)),
    'A. Activos': _pii(ACT, TOTAL, (OS,)),
    '1. Inversión directa': _pii(ACT, IED_, (OS,)),
    '2. Inversión de cartera': _pii(ACT, CART, (OS,)),
    '2.1 Participación de capital y participaciones en fondos de inversión': _pii(ACT, CART, (PARTICIPACIONES, OS,)),
    '2.2 Títulos de deuda': _pii(ACT, CART, ("Títulos de deuda", OS,)),
    '4. Otra Inversión': _pii(ACT, OTRA, (OS,)),
    '4.1 Otras participaciones de capital': _pii(ACT, OTRA, ("Otras participaciones de capital", OS,)),
    '4.2 Moneda y depósitos': _pii(ACT, OTRA, ("Moneda y depósitos", OS,)),
    '4.3 Préstamos': _pii(ACT, OTRA, ("Préstamos", OS,)),
    '4.4 Seguros, pensiones y mecanismos normalizados de garantía': _pii(ACT, OTRA, ("Seguros, pensiones y mecanismos normalizados de garantía", OS,)),
    '4.5 Créditos y anticipos comerciales': _pii(ACT, OTRA, (CRED_ANT, OS,)),
    '4.6 Otras cuentas por cobrar': _pii(ACT, OTRA, (CUENTAS_COBRAR_PAGAR, OS,)),
    'P. Pasivos': _pii(PAS, TOTAL, (OS,)),
    'P. Pasivos - 1. Inversión directa': _pii(PAS, IED_, (OS,)),
    'P. Pasivos - 2. Inversión de cartera': _pii(PAS, CART, (OS,)),
    'P. Pasivos - 2.1 Participación de capital y participaciones en fondos de inversión': _pii(PAS, CART, (PARTICIPACIONES, OS,)),
    'P. Pasivos - 2.2 Títulos de deuda': _pii(PAS, CART, ("Títulos de deuda", OS,)),
    '3. Derivados financieros': _pii(PAS, DERIV, (OS,)),
    'P. Pasivos - 4. Otra Inversión': _pii(PAS, OTRA, (OS,)),
    'P. Pasivos - 4.1 Otras participaciones de capital': _pii(PAS, OTRA, ("Otras participaciones de capital", OS,)),
    'P. Pasivos - 4.2 Moneda y depósitos': _pii(PAS, OTRA, ("Moneda y depósitos", OS,)),
    'P. Pasivos - 4.3 Préstamos': _pii(PAS, OTRA, ("Préstamos", OS,)),
    'P. Pasivos - 4.4 Seguros, pensiones y mecanismos normalizados de garantía': _pii(PAS, OTRA, ("Seguros, pensiones y mecanismos normalizados de garantía", OS,)),
    'P. Pasivos - 4.5 Créditos y anticipos comerciales': _pii(PAS, OTRA, (CRED_ANT, OS,)),
    '4.6 Otras cuentas por pagar': _pii(PAS, OTRA, (CUENTAS_COBRAR_PAGAR, OS,)),
    '4.7 Derechos especiales de giro': _pii(PAS, OTRA, (DEG_ASIGNACIONES, OS,)),
}

_PII_16A = {
    "144.2_AVOSCALES_0_T_24": _pii(ACT, TOTAL),
    "144.2_AVOSRELES_0_T_29": _pii(ACT, RESERVA),
    "144.2_CITOSARAL_0_T_41": _pii(ACT, OTRA, (CRED_ANT,)),
    "144.2_DCHOSERAL_0_T_35": _pii(ACT, RESERVA, ("Derechos especiales de giro",)),
    "144.2_IRSIONCTA_0_T_17": _pii(ACT, IED_),
    "144.2_IRSIONERA_0_T_17": _pii(ACT, CART),
    "144.2_MDADEPRAL_0_T_27": _pii(ACT, OTRA, ("Moneda y depósitos",)),
    "144.2_MDADEPRAL_0_T_31": _pii(ACT, RESERVA, ("Otros activos de reserva", "Moneda y depósitos",)),
    "144.2_OINVERRAL_0_T_25": _pii(ACT, OTRA),
    "144.2_OMONETARAL_0_T_24": _pii(ACT, RESERVA, ("Oro monetario",)),
    "144.2_OSACTILES_0_T_34": _pii(ACT, RESERVA, ("Otros activos de reserva",)),
    "144.2_OSDEREVOS_0_T_28": _pii(ACT, RESERVA, ("Otros activos de reserva", "Otros derechos sobre activos",)),
    "144.2_OSPARTLES_0_T_42": _pii(ACT, OTRA, ("Otras participaciones de capital",)),
    "144.2_PCIONETA_0_T_37": _pii(POSICION_NETA, TOTAL),
    "144.2_PCIONFMI_0_T_20": _pii(ACT, RESERVA, ("Posición de reserva en el FMI",)),
    "144.2_PICIPACRAL_0_T_51": _pii(ACT, CART, (PARTICIPACIONES,)),
    "144.2_PICIPACRIM_0_T_52": _pii(ACT, IED_, (PARTICIPACIONES,)),
    "144.2_PTAMOSMOS_0_T_9": _pii(ACT, OTRA, ("Préstamos",)),
    "144.2_PVOSCRLES_0_T_51": _pii(PAS, OTRA, (CRED_ANT,)),
    "144.2_PVOSDELES_0_T_42": _pii(PAS, DERIV),
    "144.2_PVOSDELES_0_T_45": _pii(PAS, OTRA, (DEG_ASIGNACIONES,)),
    "144.2_PVOSINLESC_0_T_38": _pii(PAS, CART),
    "144.2_PVOSINLES_0_T_38": _pii(PAS, IED_),
    "144.2_PVOSINUDA_0_T_26": _pii(PAS, IED_, ("Instrumentos de deuda",)),
    "144.2_PVOSMOLES_0_T_37": _pii(PAS, OTRA, ("Moneda y depósitos",)),
    "144.2_PVOSOTGAR_0_T_27": _pii(PAS, OTRA, (CUENTAS_COBRAR_PAGAR,)),
    "144.2_PVOSOTION_0_T_22": _pii(PAS, OTRA),
    "144.2_PVOSPADES_0_T_42": _pii(PAS, IED_, (PARTICIPACIONES,)),
    "144.2_PVOSPAION_0_T_48": _pii(PAS, CART, (PARTICIPACIONES,)),
    "144.2_PVOSPRMOS_0_T_17": _pii(PAS, OTRA, ("Préstamos",)),
    "144.2_PVOSTIUDA_0_T_21": _pii(PAS, CART, ("Títulos de deuda",)),
    "144.2_PVOSTRLES_0_T_20": _pii(PAS, TOTAL),
    "144.2_TLOSDEUDA_0_T_13": _pii(ACT, CART, ("Títulos de deuda",)),
    "144.2_TLOSVARES_0_T_15": _pii(ACT, RESERVA, ("Otros activos de reserva", "Títulos",)),
    "144.4_AVOSBALES_0_T_29": _pii(ACT, TOTAL, (BC,)),
    "144.4_AVOSGOLES_0_T_32": _pii(ACT, OTRA, (SCD,)),
    "144.4_AVOSINLESC_0_T_33": _pii(ACT, TOTAL, (GG,)),
    "144.4_AVOSINLES_0_T_30": _pii(ACT, CART, (SCD,)),
    "144.4_AVOSINLES_0_T_33": _pii(ACT, IED_, (SCD,)),
    "144.4_AVOSOILES_0_T_18": _pii(ACT, DERIV, (SCD,)),
    "144.4_AVOSOTLESIN_0_T_30": _pii(ACT, TOTAL, (OS,)),
    "144.4_AVOSOTLES_0_T_30": _pii(ACT, CART, (GG,)),
    "144.4_AVOSOTLES_0_T_45": _pii(ACT, OTRA, (GG,)),
    "144.4_AVOSOTLES_0_T_48": _pii(ACT, CART, (OS,)),
    "144.4_AVOSOTRAL_0_T_46": _pii(ACT, IED_, (OS,)),
    "144.4_AVOSOTUAL_0_T_54": _pii(ACT, OTRA, (OS,)),
    "144.4_AVOSRELES_0_T_24": _pii(ACT, RESERVA),
    "144.4_AVOSSELES_0_T_22": _pii(ACT, TOTAL),
    "144.4_AVOSSORAL_0_T_45": _pii(ACT, TOTAL, (SCD,)),
    "144.4_BOCENTRAL_0_T_19": _pii(POSICION_NETA, TOTAL, (BC,)),
    "144.4_GERNOGRAL_0_T_22": _pii(POSICION_NETA, TOTAL, (GG,)),
    "144.4_OSSECTRES_0_T_14": _pii(POSICION_NETA, TOTAL, (OS,)),
    "144.4_PCIONETA_0_T_37": _pii(POSICION_NETA, TOTAL),
    "144.4_PVOSBALES_0_T_29": _pii(PAS, TOTAL, (BC,)),
    "144.4_PVOSGOION_0_T_39": _pii(PAS, OTRA, (GG,)),
    "144.4_PVOSGOLES_0_T_32": _pii(PAS, TOTAL, (GG,)),
    "144.4_PVOSGOLES_0_T_50": _pii(PAS, CART, (GG,)),
    "144.4_PVOSGOLES_0_T_54": _pii(PAS, DERIV, (GG,)),
    "144.4_PVOSINLES_0_T_37": _pii(PAS, CART, (SCD,)),
    "144.4_PVOSINLES_0_T_39": _pii(PAS, IED_, (SCD,)),
    "144.4_PVOSINLES_0_T_40": _pii(PAS, CART, (BC,)),
    "144.4_PVOSOTCTA_0_T_40": _pii(PAS, IED_, (OS,)),
    "144.4_PVOSOTERA_0_T_40": _pii(PAS, CART, (OS,)),
    "144.4_PVOSOTIONC_0_T_22": _pii(PAS, OTRA, (OS,)),
    "144.4_PVOSOTIONIN_0_T_22": _pii(PAS, OTRA, (SCD,)),
    "144.4_PVOSOTION_0_T_22": _pii(PAS, OTRA, (BC,)),
    "144.4_PVOSOTRES_0_T_22": _pii(PAS, TOTAL, (OS,)),
    "144.4_PVOSSOTOS_0_T_39": _pii(PAS, TOTAL, (SCD,)),
    "144.4_PVOSTRLES_0_T_15": _pii(PAS, TOTAL),
    "144.4_SEDADESTOS_0_T_31": _pii(POSICION_NETA, TOTAL, (SCD,)),
    "144.1_AVOSCALES_0_A_19": _pii(ACT, TOTAL),
    "144.1_AVOSRELES_0_A_24": _pii(ACT, RESERVA),
    "144.1_CITOSAUAL_0_A_36": _pii(ACT, OTRA, (CRED_ANT,)),
    "144.1_DCHOSEUAL_0_A_30": _pii(ACT, RESERVA, ("Derechos especiales de giro",)),
    "144.1_IRSIONLES_0_A_25": _pii(ACT, CART),
    "144.1_IRSIONLES_0_A_29": _pii(ACT, IED_),
    "144.1_MDADEPUAL_0_A_22": _pii(ACT, OTRA, ("Moneda y depósitos",)),
    "144.1_MDADEPUAL_0_A_26": _pii(ACT, RESERVA, ("Otros activos de reserva", "Moneda y depósitos",)),
    "144.1_OINVERUAL_0_A_20": _pii(ACT, OTRA),
    "144.1_OMONETAUAL_0_A_19": _pii(ACT, RESERVA, ("Oro monetario",)),
    "144.1_OSACTILES_0_A_29": _pii(ACT, RESERVA, ("Otros activos de reserva",)),
    "144.1_OSDEREVOS_0_A_28": _pii(ACT, RESERVA, ("Otros activos de reserva", "Otros derechos sobre activos",)),
    "144.1_OSPARTLES_0_A_37": _pii(ACT, OTRA, ("Otras participaciones de capital",)),
    "144.1_PCIONETA_0_A_37": _pii(POSICION_NETA, TOTAL),
    "144.1_PCIONFMI_0_A_20": _pii(ACT, RESERVA, ("Posición de reserva en el FMI",)),
    "144.1_PICIPACUAL_0_A_46": _pii(ACT, CART, (PARTICIPACIONES,)),
    "144.1_PICIPACUAL_0_A_53": _pii(ACT, IED_, (PARTICIPACIONES,)),
    "144.1_PTAMOSMOS_0_A_9": _pii(ACT, OTRA, ("Préstamos",)),
    "144.1_PVOSANLES_0_A_15": _pii(PAS, TOTAL),
    "144.1_PVOSCRLES_0_A_46": _pii(PAS, OTRA, (CRED_ANT,)),
    "144.1_PVOSDELES_0_A_37": _pii(PAS, DERIV),
    "144.1_PVOSDELES_0_A_40": _pii(PAS, OTRA, (DEG_ASIGNACIONES,)),
    "144.1_PVOSINLES_0_A_33": _pii(PAS, CART),
    "144.1_PVOSINUAL_0_A_31": _pii(PAS, IED_),
    "144.1_PVOSINUDA_0_A_26": _pii(PAS, IED_, ("Instrumentos de deuda",)),
    "144.1_PVOSMOLES_0_A_32": _pii(PAS, OTRA, ("Moneda y depósitos",)),
    "144.1_PVOSOTGAR_0_A_27": _pii(PAS, OTRA, (CUENTAS_COBRAR_PAGAR,)),
    "144.1_PVOSOTION_0_A_22": _pii(PAS, OTRA),
    "144.1_PVOSPADES_0_A_42": _pii(PAS, IED_, (PARTICIPACIONES,)),
    "144.1_PVOSPAION_0_A_48": _pii(PAS, CART, (PARTICIPACIONES,)),
    "144.1_PVOSPRMOS_0_A_17": _pii(PAS, OTRA, ("Préstamos",)),
    "144.1_PVOSTIUDA_0_A_21": _pii(PAS, CART, ("Títulos de deuda",)),
    "144.1_TLOSDEUDA_0_A_13": _pii(ACT, CART, ("Títulos de deuda",)),
    "144.1_TLOSVARES_0_A_15": _pii(ACT, RESERVA, ("Otros activos de reserva", "Títulos",)),
    "144.3_ACVOSOTLES_0_A_30": _pii(ACT, TOTAL, (OS,)),
    "144.3_AVOSBALES_0_A_29": _pii(ACT, TOTAL, (BC,)),
    "144.3_AVOSGOLES_0_A_32": _pii(ACT, OTRA, (SCD,)),
    "144.3_AVOSINLESC_0_A_33": _pii(ACT, TOTAL, (GG,)),
    "144.3_AVOSINLES_0_A_30": _pii(ACT, CART, (SCD,)),
    "144.3_AVOSINLES_0_A_33": _pii(ACT, IED_, (SCD,)),
    "144.3_AVOSOILES_0_A_18": _pii(ACT, DERIV, (SCD,)),
    "144.3_AVOSOTLESC_0_A_48": _pii(ACT, CART, (OS,)),
    "144.3_AVOSOTLESP_0_A_45": _pii(ACT, OTRA, (GG,)),
    "144.3_AVOSOTLES_0_A_30": _pii(ACT, CART, (GG,)),
    "144.3_AVOSOTUAL_0_A_46": _pii(ACT, IED_, (OS,)),
    "144.3_AVOSOTUAL_0_A_54": _pii(ACT, OTRA, (OS,)),
    "144.3_AVOSRELES_0_A_24": _pii(ACT, RESERVA),
    "144.3_AVOSSELES_0_A_22": _pii(ACT, TOTAL),
    "144.3_AVOSSOUAL_0_A_45": _pii(ACT, TOTAL, (SCD,)),
    "144.3_BOCENTUAL_0_A_19": _pii(POSICION_NETA, TOTAL, (BC,)),
    "144.3_GERNOGUAL_0_A_22": _pii(POSICION_NETA, TOTAL, (GG,)),
    "144.3_OSSECTRES_0_A_14": _pii(POSICION_NETA, TOTAL, (OS,)),
    "144.3_PCIONETA_0_A_37": _pii(POSICION_NETA, TOTAL),
    "144.3_PVOSANLES_0_A_15": _pii(PAS, TOTAL),
    "144.3_PVOSBALES_0_A_29": _pii(PAS, TOTAL, (BC,)),
    "144.3_PVOSGOION_0_A_39": _pii(PAS, OTRA, (GG,)),
    "144.3_PVOSGOLES_0_A_32": _pii(PAS, TOTAL, (GG,)),
    "144.3_PVOSGOLES_0_A_50": _pii(PAS, CART, (GG,)),
    "144.3_PVOSGOLES_0_A_54": _pii(PAS, DERIV, (GG,)),
    "144.3_PVOSINLESC_0_A_40": _pii(PAS, IED_, (SCD,)),
    "144.3_PVOSINLES_0_A_37": _pii(PAS, CART, (SCD,)),
    "144.3_PVOSINLES_0_A_40": _pii(PAS, CART, (BC,)),
    "144.3_PVOSOTCTA_0_A_40": _pii(PAS, IED_, (OS,)),
    "144.3_PVOSOTERA_0_A_40": _pii(PAS, CART, (OS,)),
    "144.3_PVOSOTIONIN_0_A_22": _pii(PAS, OTRA, (OS,)),
    "144.3_PVOSOTIONI_0_A_22": _pii(PAS, OTRA, (SCD,)),
    "144.3_PVOSOTION_0_A_22": _pii(PAS, OTRA, (BC,)),
    "144.3_PVOSOTRES_0_A_22": _pii(PAS, TOTAL, (OS,)),
    "144.3_PVOSSOTOS_0_A_39": _pii(PAS, TOTAL, (SCD,)),
    "144.3_SEDADESTOS_0_A_31": _pii(POSICION_NETA, TOTAL, (SCD,)),
}

# Hoja 16 (PII base 1991, anual 1991-2001): ID → (lado, sector, detalle).
_PII_1991 = {
    "86.1_AEATEPL_1991_A_55": (PAS, "Gobiernos locales", ("Atrasos (excluidos atrasos de títulos)",)),
    "86.1_AEAT_1991_A_32": (PAS, "Gobierno nacional", ("Atrasos (excluidos atrasos de títulos)",)),
    "86.1_AEB_1991_A_21": (ACT, "BCRA", ()),
    "86.1_AEEPL_1991_A_39": (ACT, "Gobiernos locales", ()),
    "86.1_AEO_1991_A_20": (ACT, "Otras entidades financieras", ()),
    "86.1_AEPI_1991_A_35": (ACT, "", ()),
    "86.1_AESPNFT_1991_A_51": (ACT, "Sector privado no financiero", ()),
    "86.1_AESP_1991_A_31": (ACT, "Gobierno nacional", ()),
    "86.1_AOEF_1991_A_35": (PAS, "Otras entidades financieras", ("Atrasos",)),
    "86.1_AOEPL_1991_A_43": (PAS, "Gobiernos locales", ("Otros pasivos", "Acreedores oficiales",)),
    "86.1_AOI_1991_A_34": (ACT, "Gobierno nacional", ("Otros activos", "Aportes a organismos internacionales",)),
    "86.1_AOV_1991_A_21": (PAS, "Sector privado no financiero", ("Otros pasivos", "Agencias oficiales",)),
    "86.1_AO_1991_A_12": (ACT, "Otras entidades financieras", ("Inversión de cartera", "Acciones",)),
    "86.1_AO_1991_A_20": (PAS, "Gobierno nacional", ("Otros pasivos externos", "Acreedores oficiales",)),
    "86.1_AV_1991_A_11": (PAS, "Sector privado no financiero", ("Inversión de cartera", "Acciones",)),
    "86.1_A_1991_A_7": (PAS, "Gobierno nacional", ("Atrasos",)),
    "86.1_A_1991_A_8": (PAS, "Otras entidades financieras", ("Inversión de cartera", "Acciones",)),
    "86.1_BEPL_1991_A_29": (PAS, "Gobiernos locales", ("Otros pasivos", "Bancos",)),
    "86.1_BPI_1991_A_25": (PAS, "Gobierno nacional", ("Otros pasivos externos", "Bancos",)),
    "86.1_BV_1991_A_9": (PAS, "Sector privado no financiero", ("Otros pasivos", "Bancos",)),
    "86.1_B_1991_A_4": (POSICION_NETA, "BCRA", ()),
    "86.1_B_1991_A_6": (PAS, "Gobierno nacional", ("Bancos (TCA, TCDF, GRA)",)),
    "86.1_CFEB_1991_A_40": (ACT, "Gobierno nacional", ("Otros activos", "Créditos por financiación de entes binacionales",)),
    "86.1_DDO_1991_A_30": (ACT, "Otras entidades financieras", ("Otros activos", "Depósitos y disponibilidades",)),
    "86.1_DFF_1991_A_28": (ACT, "Gobierno nacional", ("Otros activos", "Depósitos de fondos fiduciarios",)),
    "86.1_DF_1991_A_22": (ACT, "Gobierno nacional", ("Otros activos", "Depósitos de fideicomisos",)),
    "86.1_DRPF1_1991_A_39": (PAS, "Gobierno nacional", ("Otros pasivos externos", "Deuda refinanciada en el Plan Financiero 1992",)),
    "86.1_D_1991_A_9": (PAS, "Otras entidades financieras", ("Otros pasivos", "Depósitos",)),
    "86.1_E_1991_A_8": (ACT, "Sector privado no financiero", ("Inversión directa", "Empresas",)),
    "86.1_GCOEPN_1991_A_43": (POSICION_NETA, "Gobierno nacional", ()),
    "86.1_GLOEPL_1991_A_41": (POSICION_NETA, "Gobiernos locales", ()),
    "86.1_ICACI_1991_A_43": (PAS, "Gobierno nacional", ("Inversión de cartera con atrasos de capital e intereses",)),
    "86.1_ICB_1991_A_22": (PAS, "BCRA", ("Inversión de cartera",)),
    "86.1_ICIACI_1991_A_51": (PAS, "Gobiernos locales", ("Inversión de cartera con atrasos de capital e intereses",)),
    "86.1_ICO_1991_A_21": (ACT, "Otras entidades financieras", ("Inversión de cartera",)),
    "86.1_ICO_1991_A_23": (PAS, "Otras entidades financieras", ("Inversión de cartera",)),
    "86.1_ICSPNFT_1991_A_52": (ACT, "Sector privado no financiero", ("Inversión de cartera",)),
    "86.1_ICV_1991_A_20": (PAS, "Sector privado no financiero", ("Inversión de cartera",)),
    "86.1_IC_1991_A_17": (ACT, "Gobierno nacional", ("Inversión de cartera",)),
    "86.1_IDO_1991_A_21": (ACT, "Otras entidades financieras", ("Inversión directa",)),
    "86.1_IDO_1991_A_23": (PAS, "Otras entidades financieras", ("Inversión directa",)),
    "86.1_IDSPNFT_1991_A_52": (ACT, "Sector privado no financiero", ("Inversión directa",)),
    "86.1_ID_1991_A_17": (PAS, "Sector privado no financiero", ("Inversión directa",)),
    "86.1_IFD_1991_A_34": (PAS, "Gobierno nacional", ("Instrumentos financieros derivados",)),
    "86.1_ISPNFT_1991_A_47": (ACT, "Sector privado no financiero", ("Inversión directa", "Inmobiliaria",)),
    "86.1_LCPP_1991_A_24": (ACT, "Gobierno nacional", ("Otros activos", "Líneas de crédito país/país",)),
    "86.1_LC_1991_A_14": (PAS, "Otras entidades financieras", ("Otros pasivos", "Líneas de crédito",)),
    "86.1_O2_1991_A_7": (PAS, "Otras entidades financieras", ("Otros pasivos", "Otros",)),
    "86.1_OAO_1991_A_17": (ACT, "Otras entidades financieras", ("Otros activos",)),
    "86.1_OASPNFT_1991_A_48": (ACT, "Sector privado no financiero", ("Otros activos",)),
    "86.1_OA_1991_A_13": (ACT, "Gobierno nacional", ("Otros activos",)),
    "86.1_OB_1991_A_10": (PAS, "BCRA", ("Otros pasivos",)),
    "86.1_ODI_1991_A_29": (ACT, "Gobierno nacional", ("Otros activos", "Otros depósitos indisponibles",)),
    "86.1_OD_1991_A_15": (ACT, "Gobierno nacional", ("Otros activos", "Otros depósitos",)),
    "86.1_OEF_1991_A_27": (POSICION_NETA, "Otras entidades financieras", ()),
    "86.1_OIV_1991_A_15": (PAS, "Sector privado no financiero", ("Otros pasivos", "Organismos internacionales",)),
    "86.1_OI_1991_A_12": (PAS, "Gobierno nacional", ("Otros pasivos externos", "Organismos internacionales",)),
    "86.1_OI_1991_A_19": (PAS, "Gobierno nacional", ("Otros pasivos externos", "Organismos internacionales", "FMI, BID, BIRF y BIS",)),
    "86.1_OI_1991_A_23": (PAS, "Otras entidades financieras", ("Otros pasivos", "Otros organismos internacionales",)),
    "86.1_OO_1991_A_9": (ACT, "Otras entidades financieras", ("Otros activos", "Otros",)),
    "86.1_OPEPL_1991_A_36": (PAS, "Gobiernos locales", ("Otros pasivos",)),
    "86.1_OPE_1991_A_22": (PAS, "Gobierno nacional", ("Otros pasivos externos",)),
    "86.1_OPV_1991_A_16": (PAS, "Sector privado no financiero", ("Otros pasivos",)),
    "86.1_OP_1991_A_17": (PAS, "Otras entidades financieras", ("Otros pasivos",)),
    "86.1_OTG_1991_A_22": (ACT, "Gobierno nacional", ("Inversión de cartera", "Otros títulos en garantía",)),
    "86.1_O_1991_A_13": (PAS, "Gobierno nacional", ("Otros pasivos",)),
    "86.1_PEB_1991_A_21": (PAS, "BCRA", ()),
    "86.1_PEEPL_1991_A_39": (PAS, "Gobiernos locales", ()),
    "86.1_PEO_1991_A_22": (PAS, "Otras entidades financieras", ()),
    "86.1_PESPNFT_1991_A_51": (PAS, "Sector privado no financiero", ()),
    "86.1_PES_1991_A_21": (PAS, "Gobierno nacional", ()),
    "86.1_PE_1991_A_16": (PAS, "", ()),
    "86.1_PII_1991_A_32": (POSICION_NETA, "", ()),
    "86.1_POEPL_1991_A_40": (PAS, "Gobiernos locales", ("Otros pasivos", "Proveedores y otros",)),
    "86.1_POV_1991_A_20": (PAS, "Sector privado no financiero", ("Otros pasivos", "Proveedores y otros",)),
    "86.1_PO_1991_A_17": (PAS, "Gobierno nacional", ("Otros pasivos externos", "Proveedores y otros",)),
    "86.1_RIB_1991_A_22": (ACT, "BCRA", ("Reservas internacionales",)),
    "86.1_SF_1991_A_18": (POSICION_NETA, "Sistema financiero", ()),
    "86.1_SPNFT_1991_A_34": (POSICION_NETA, "Sector privado no financiero", ()),
    "86.1_SPNF_1991_A_28": (POSICION_NETA, "Sector público no financiero", ()),
    "86.1_TBN_1991_A_18": (PAS, "BCRA", ("Inversión de cartera", "Títulos (BCRA notes)",)),
    "86.1_TDO_1991_A_19": (PAS, "Otras entidades financieras", ("Inversión de cartera", "Títulos de deuda",)),
    "86.1_TD_1991_A_13": (PAS, "Sector privado no financiero", ("Inversión de cartera", "Títulos de deuda",)),
    "86.1_TGPF1_1991_A_37": (ACT, "Gobierno nacional", ("Inversión de cartera", "Títulos en garantía del Plan Financiero 1992",)),
    "86.1_TLD_1991_A_28": (PAS, "Gobierno nacional", ("Inversión de cartera", "Títulos de libre disponibilidad",)),
    "86.1_TO_1991_A_11": (ACT, "Otras entidades financieras", ("Inversión de cartera", "Títulos",)),
    "86.1_VP_1991_A_18": (POSICION_NETA, "", ("Variación de la posición",)),
}


# ── Deuda externa ──────────────────────────────────────────────────────────────────────────────
# Nivel 3: «Total economía» (cuadros 23, 24, 29 y 30 del INDEC y hoja 15.a) o «Sector privado» (BCRA, hoja 33).
# Nivel 4: «Total» o el sector residente. El valor nominal es el habitual; el de mercado lleva «(a valor de mercado)».
DEUDA_BRUTA = "Deuda externa bruta"
TOTAL_ECONOMIA = "Total economía"
SECTOR_PRIVADO = "Sector privado"
OSF = "Otras sociedades financieras"
SNF = "Sociedades no financieras, hogares e ISFLSH"
SIN_IED = "Sin inversión directa"
LARGO_SIN_IED = "Largo plazo sin inversión directa"
A_VALOR_DE_MERCADO = "valor de mercado"
CORTO = "Corto plazo"
LARGO = "Largo plazo"

_SECTORES_DEUDA = {
    "gobierno general": GG, "banco central": BC,
    "sociedades captadoras de deposito, excepto el banco central": SCD,
    "sociedades captadoras de depositos, excepto el banco central": SCD,
    "otras sociedades financieras": OSF, "sociedades no financieras, hogares e isflsh": SNF,
    "otros sectores": OS,
}
_TOTALES_DEUDA = ("saldo deuda extrena bruta", "total deuda externa bruta", "saldo deuda externa bruta")
_PLAZOS_DEUDA = {"a corto plazo": CORTO, "a largo plazo": LARGO}
_INSTRUMENTOS_DEUDA = {
    "derechos especiales de giro (asignaciones)": DEG_ASIGNACIONES,
    "moneda y depositos": "Moneda y depósitos", "titulos de deuda": "Títulos de deuda", "prestamos": "Préstamos",
    "creditos y anticipos comerciales": CRED_ANT, "otros pasivos de deuda": "Otros pasivos de deuda",
    "inversion directa": "Inversión directa", "inversion directa: credito entre empresas": "Inversión directa",
    "pasivos de deuda de empresas de inversion directa frente a inversionistas directos":
        "Inversión directa", "pasivos de deuda de inversionistas directos frente a empresas de inversion directa":
        "Inversión directa", "pasivos de deuda entre empresas emparentadas": "Inversión directa",
}
_DETALLE_INVERSION_DIRECTA = {
    "pasivos de deuda de empresas de inversion directa frente a inversionistas directos":
        "Con inversionistas directos",
    "pasivos de deuda de inversionistas directos frente a empresas de inversion directa":
        "Inversión inversa",
    "pasivos de deuda entre empresas emparentadas": "Entre empresas emparentadas",
}


def _deuda(sector, plazo="", instrumento="", detalle="", mercado=False) -> Clasificacion:
    """Deuda externa bruta: sector residente (o total), plazo e instrumento."""
    if sector in ("", None):
        cobertura, apertura = TOTAL_ECONOMIA, TOTAL
        partes = [DEUDA_BRUTA]
    else:
        cobertura, apertura = TOTAL_ECONOMIA, TOTAL if sector == OS else sector   # «Otros sectores» es un subtotal
        partes = [DEUDA_BRUTA, sector, plazo, instrumento, detalle]
    nombre = con_medida(unir(*partes), A_VALOR_DE_MERCADO if mercado else "")
    return Clasificacion(tema=TEMA, indicador=INDICADOR_DEUDA, cobertura=cobertura, apertura=apertura,
                         nombre=nombre, unidad=UNIDAD)


def _deuda_por_rotulo(serie: Serie, mercado: bool) -> Clasificacion | None:
    """Cuadros 23, 24 y 29: «<sector> [- <plazo>] [- <instrumento>]» o el total."""
    partes = [p.strip() for p in serie.columna.split(" - ")]
    primero = plano(partes[0])
    if len(partes) == 1 and primero in _TOTALES_DEUDA:
        return _deuda("", mercado=mercado)
    sector = _SECTORES_DEUDA.get(primero)
    if sector is None:
        return None
    plazo = instrumento = detalle = ""
    for parte in partes[1:]:
        clave = plano(parte)
        if clave in _PLAZOS_DEUDA and not plazo:
            plazo = _PLAZOS_DEUDA[clave]
        elif clave in _INSTRUMENTOS_DEUDA and not instrumento:
            instrumento = _INSTRUMENTOS_DEUDA[clave]
            detalle = _DETALLE_INVERSION_DIRECTA.get(clave, "")
        else:
            return None
    if instrumento == "Inversión directa":
        plazo = ""      # el crédito entre empresas de inversión directa se publica sin plazo en los otros cuadros
    if serie.hoja == "Cuadro 23" and sector == SNF and not instrumento:
        # En el cuadro 23 el total del sector y su largo plazo no incluyen la inversión directa, que se lista aparte.
        plazo = SIN_IED if not plazo else (LARGO_SIN_IED if plazo == LARGO else plazo)
    return _deuda(sector, plazo, instrumento, detalle, mercado)


# Cuadro 30: deuda externa bruta del gobierno general con organismos internacionales.
_ORGANISMOS = {
    "total de deuda externa bruta con organismos internacionales": "",
    "fmi": "FMI", "bid": "BID", "birf": "BIRF", "caf": "CAF", "fida": "FIDA", "fonplata": "FONPLATA",
    "otros organismos (1)": "Otros organismos", "intereses devengados no exigibles de ooii":
        "Intereses devengados no exigibles",
}


def _deuda_organismos(serie: Serie) -> Clasificacion | None:
    organismo = _ORGANISMOS.get(plano(serie.columna))
    if organismo is None:
        return None
    nombre = unir(DEUDA_BRUTA, GG, "Organismos internacionales", organismo)
    return Clasificacion(tema=TEMA, indicador=INDICADOR_DEUDA, cobertura=TOTAL_ECONOMIA, apertura=GG,
                         nombre=nombre, unidad=UNIDAD)


# Hoja 15.a de datos.gob.ar (deuda externa bruta por sector residente, a valor nominal, trimestral): variable →
# (sector, instrumento). Los valores coinciden con el cuadro 24 del INDEC hasta el último dato publicado en el libro.
_DEUDA_15A = {
    "total_deuda_externa": ("", ""),
    "total_deuda_gobierno_general": (GG, ""),
    "total_deuda_banco_central": (BC, ""),
    "total_deuda_soc_captadoras_depositos_no_banco_central": (SCD, ""),
    "total_deuda_otras_entidades_financieras": (OSF, ""),
    "total_deuda_sociedades_no_financieras": (SNF, ""),
    "deuda_gobierno_general_monedas_depositos": (GG, "Moneda y depósitos"),
    "deuda_gobierno_general_titulos_deuda": (GG, "Títulos de deuda"),
    "deuda_gobierno_general_prestamos": (GG, "Préstamos"),
    "deuda_gobierno_general_creditos_anticipos_comerciales": (GG, CRED_ANT),
    "deuda_gobierno_general_otros_pasivos_deuda": (GG, "Otros pasivos de deuda"),
    "deuda_banco_central_derechos_especiales_giro": (BC, DEG_ASIGNACIONES),
    "deuda_banco_central_moneda_depositos": (BC, "Moneda y depósitos"),
    "deuda_banco_central_titulos_deuda": (BC, "Títulos de deuda"),
    "deuda_banco_central_prestamos": (BC, "Préstamos"),
    "deuda_banco_central_creditos_anticipos_comerciales": (BC, CRED_ANT),
    "deuda_banco_central_otros_pasivos_deuda": (BC, "Otros pasivos de deuda"),
    "deuda_soc_captadoras_depositos_moneda_depositos": (SCD, "Moneda y depósitos"),
    "deuda_soc_depositos_no_banco_central_titulos_deuda": (SCD, "Títulos de deuda"),
    "deuda_soc_captadoras_depositos_no_banco_central_prestamos": (SCD, "Préstamos"),
    "deuda_soc_captadoras_depositos_no_banco_central_ant_com": (SCD, CRED_ANT),
    "deuda_soc_captadoras_depositos_no_banco_central_otros": (SCD, "Otros pasivos de deuda"),
    "deuda_otras_sociedades_financieras_moneda_depositos": (OSF, "Moneda y depósitos"),
    "deuda_otras_sociedades_financieras_titulos_deuda": (OSF, "Títulos de deuda"),
    "deuda_otras_sociedades_financieras_prestamos": (OSF, "Préstamos"),
    "deuda_otras_soc_financieras_creditos_anticipos_comerciales": (OSF, CRED_ANT),
    "deuda_otras_sociedades_financiera_otros_pasivos_deuda": (OSF, "Otros pasivos de deuda"),
    "deuda_sociedades_no_financieras_moneda_depositos": (SNF, "Moneda y depósitos"),
    "deuda_sociedades_no_financieras_titulos_deuda": (SNF, "Títulos de deuda"),
    "deuda_sociedades_no_financieras_prestamos": (SNF, "Préstamos"),
    "deuda_soc_no_financieras_creditos_anticipos_comerciales": (SNF, CRED_ANT),
    "deuda_sociedades_no_financiera_otros_pasivos_deuda": (SNF, "Otros pasivos de deuda"),
    "deuda_sociedades_no_financieras_inversion_directa": (SNF, "Inversión directa"),
}

# Hoja 33: deuda externa privada del BCRA (trimestral, 2001-2017). La suma de las categorías, de los acreedores y
# de los instrumentos reproduce el total al decimal. «prestamos_financieros_titulos_deuda» es aditiva con
# «deuda_financiera_prestamos_financieros» (la suma de los cinco componentes da la deuda financiera), así que es
# la deuda en títulos; su descripción lo mezcla con los préstamos.
DEUDA_PRIVADA = "Deuda externa privada"
_DEUDA_PRIVADA = {
    "total": ("", ""),
    "por_categoria_deuda_financiera": ("Deuda financiera", ""),
    "deuda_financiera_prestamos_financieros": ("Deuda financiera", "Préstamos financieros"),
    "prestamos_financieros_titulos_deuda": ("Deuda financiera", "Títulos de deuda"),
    "deuda_financiera_linea_credito_financiera": ("Deuda financiera", "Líneas de crédito financieras"),
    "deuda_financiera_por_utilidades_dividendos": ("Deuda financiera", "Utilidades y dividendos"),
    "deuda_financiera_otras_deudas": ("Deuda financiera", "Otras deudas"),
    "por_categoria_deuda_comercial": ("Deuda comercial", ""),
    "deuda_por_importacion_bienes": ("Deuda comercial", "Importaciones de bienes"),
    "deuda_comercial_servicios": ("Deuda comercial", "Servicios"),
    "anticipos_prefinanciacion_bienes": ("Deuda comercial", "Anticipos y prefinanciaciones de exportaciones"),
    "deuda_comercial_lineas_credito": ("Deuda comercial", "Líneas de crédito comerciales"),
    "por_acreedor_empresas_del_mismo_grupo": ("Por acreedor", "Empresas del mismo grupo"),
    "por_acreedor_multiples_tenedores_titulos": ("Por acreedor", "Múltiples acreedores y tenedores de títulos"),
    "por_acreedor_proveedores_del_exterior": ("Por acreedor", "Proveedores del exterior"),
    "por_acreedor_entidades_financieras_internacionales": ("Por acreedor", "Entidades financieras internacionales"),
    "por_acreedor_organismos_internacionales_agencias_oficiales":
        ("Por acreedor", "Organismos internacionales y agencias oficiales"),
    "por_acreedor_clientes_del_exterior": ("Por acreedor", "Clientes del exterior"),
    "por_acreedor_otros": ("Por acreedor", "Otros acreedores"),
}
_RELACIONES_DEUDA_PRIVADA = {
    "en_porcentaje_del_pib": ("% del PIB", "Proporción"),
    "en_porcentaje_las_exportaciones": ("% de las exportaciones", "Proporción"),
    "en_porcentaje_rrii": ("% de las reservas internacionales", "Proporción"),
}


def _deuda_privada(serie: Serie) -> Clasificacion | None:
    if serie.variable in _RELACIONES_DEUDA_PRIVADA:
        medida, unidad = _RELACIONES_DEUDA_PRIVADA[serie.variable]
        return Clasificacion(tema=TEMA, indicador=INDICADOR_DEUDA, cobertura=SECTOR_PRIVADO,
                             nombre=con_medida(DEUDA_PRIVADA, medida), unidad=unidad)
    concepto = _DEUDA_PRIVADA.get(serie.variable)
    if concepto is None:
        return None
    grupo, detalle = concepto
    return Clasificacion(tema=TEMA, indicador=INDICADOR_DEUDA, cobertura=SECTOR_PRIVADO,
                         nombre=unir(DEUDA_PRIVADA, grupo, detalle), unidad=UNIDAD)


# ── Hoja 14.a de datos.gob.ar (balanza de pagos MBP6, resumen anual y trimestral): variable → concepto ──
_BP_14A = {
    "total_cuenta_corriente": _res("Cuenta corriente"),
    "cuenta_corriente_total_bienes_servicios": _res("Bienes y servicios"),
    "cuenta_corriente_bienes_servicios_total_bienes": _res("Bienes"),
    "cuenta_corrientes_bienes_servicios_bienes_exportaciones_fob": _res("Bienes", f=CRED),
    "cuenta_corrientes_bienes_servicios_bienes_importaciones_fob": _res("Bienes", f=DEB),
    "cuenta_corriente_bienes_servicios_total_servicios": _res("Servicios"),
    "cuenta_corriente_bienes_servicios_ingresos_servicios": _res("Servicios", f=CRED),
    "cuenta_corriente_bienes_servicios_egresos_servicios": _res("Servicios", f=DEB),
    "cuenta_corriente_total_ingresos_primarios": _res(_IP),
    "cuenta_corriente_ingresos_primarios_rem_empleados": _cc(_IP, "Remuneración de empleados"),
    "cuenta_corriente_ingresos_primarios_total_rentas_inversion": _cc(_IP, "Renta de la inversión"),
    "cta_cte_ingresos_primarios_rentas_inversion_directa": _cc(_IP, _R_DIR),
    "ingresos_primarios_rentas_inversion_inversion_cartera": _cc(_IP, _R_CART),
    "cuenta_corriente_ingresos_primarios_rentas_otra_inversion": _cc(_IP, _R_OTRA),
    "cuenta_corriente_ingresos_secundarios": _res(_IS),
    "total_cuenta_capital": _res("Cuenta de capital"),
    "total_cuenta_financiera": _res("Cuenta financiera"),
    "errores_omisiones_netos": _res("Errores y omisiones netos"),
    "cuenta_financiera_total_inversion_directa": _cf(IED_, NETO),
    "cta_fin_inversion_directa_activos_participaciones_capital": _cf(IED_, ACT, PARTICIPACIONES),
    "cuenta_financiera_inversion_directa_activos_instrumentosuda": _cf(IED_, ACT, "Instrumentos de deuda"),
    "cta_fin_inversion_directa_pasivos_participaciones_capital": _cf(IED_, PAS, PARTICIPACIONES),
    "cta_fin_inversion_directa_pasivos_instrumentos_deuda": _cf(IED_, PAS, "Instrumentos de deuda"),
    "cuenta_financiera_total_inversion_cartera": _cf(CART, NETO),
    "cta_fin_inversion_cartera_activos_participaciones_capital": _cf(CART, ACT, PARTICIPACIONES),
    "cuenta_financiera_inversion_cartera_activos_instrumentosuda": _cf(CART, ACT, "Títulos de deuda"),
    "cta_fin_inversion_cartera_pasivos_participaciones_capital": _cf(CART, PAS, PARTICIPACIONES),
    "cuenta_financiera_inversion_cartera_pasivos_instrumentosuda": _cf(CART, PAS, "Títulos de deuda"),
    "cuenta_financierarivados_financieros": _cf(DERIV, NETO),
    "cuenta_financiera_total_otra_inversion": _cf(OTRA, NETO),
    "cta_fin_otra_inversion_adquisicion_neta_activos_financieros": _cf(OTRA, ACT),
    "cuenta_financieratra_inversion_emision_neta_pasivos": _cf(OTRA, PAS),
    "activos_reserva": _cf(RESERVA, ""),
}

# ── Hoja 14 de datos.gob.ar (balanza de pagos MBP5, anual y trimestral): variable → (ruta del nombre, flujo) ──
# Se llama igual que su equivalente del MBP6 cuando mide el mismo concepto; los sectores de la cuenta financiera
# del MBP5 (sector bancario, sector público no financiero, sector privado no financiero) son otra clasificación.
_FS = "Cuenta financiera"
_MBP5 = {
    "cc_total": (("Cuenta corriente",), ""),
    "cc_total_mercancias": (("Bienes",), ""),
    "cc_mercancias_exp_fob": (("Bienes",), CRED),
    "cc_mercancias_imp_fob": (("Bienes",), DEB),
    "item_memorandum_imp_cif": (("Bienes", "Importaciones CIF (partida informativa)"), ""),
    "cc_servicios_total": (("Servicios",), ""),
    "cc_serv_exp_serv_reales": (("Servicios",), CRED),
    "cc_serv_imp_serv_reales": (("Servicios",), DEB),
    "cc_rentas_total": (("Ingreso primario",), ""),
    "cc_rentas_otras_rentas": (("Remuneración de empleados",), ""),
    "cc_rentas_total_rentas_inv": (("Renta de la inversión",), ""),
    "cc_rentas_renta_inv_int_total": (("Renta de la inversión", "Intereses"), ""),
    "cc_rentas_renta_inv_int_ganados": (("Renta de la inversión", "Intereses"), CRED),
    "cc_rentas_renta_inv_int_pagados": (("Renta de la inversión", "Intereses"), DEB),
    "cc_rentas_renta_inv_utilidades_dividendos_total": (("Renta de la inversión", "Utilidades y dividendos"), ""),
    "cc_rentas_renta_inv_utilidades_dividendos_ganados": (("Renta de la inversión", "Utilidades y dividendos"), CRED),
    "cc_rentas_renta_inv_utilidades_dividendos_pagados": (("Renta de la inversión", "Utilidades y dividendos"), DEB),
    "cc_rentas_transf_corrientes": (("Ingreso secundario",), ""),
    "ccf_cuenta_capital_total": (("Cuenta de capital",), ""),
    "ccf_cuenta_capital_activos_finans_no_producidos": (("Activos no financieros no producidos",), ""),
    "ccf_total_ccf": (("Cuenta de capital y financiera",), ""),
    "ccf_cuenta_financiera_total_cuenta_fianciera": ((_FS,), ""),
    "ccf_cuenta_financiera_sector_bancario_total": ((_FS, "Sector bancario"), ""),
    "ccf_cuenta_financiera_sector_bancario_bcra": ((_FS, "Sector bancario", BC), ""),
    "ccf_cuenta_financiera_sector_bancario_oef": ((_FS, "Sector bancario", "Otras entidades financieras"), ""),
    "ccf_cuenta_financiera_sector_pub_no_finan_total_spnf": ((_FS, "Sector público no financiero"), ""),
    "ccf_cuenta_financiera_sector_pub_no_finan_gob_nac": ((_FS, "Sector público no financiero", "Gobierno nacional"), ""),
    "ccf_cuenta_financiera_sector_pub_no_finan_gob_locales": ((_FS, "Sector público no financiero", "Gobiernos locales"), ""),
    "ccf_cuenta_financiera_sector_pub_no_finan_emp_otros": ((_FS, "Sector público no financiero", "Empresas y otros"), ""),
    "ccf_cuenta_financiera_sector_priv_no_finan": ((_FS, "Sector privado no financiero"), ""),
    "errores_omisiones_netos": (("Errores y omisiones netos",), ""),
    "variacion_reservas_int_total": ((RESERVA,), ""),
    "variacion_reservas_int_reservas_int_bcra": ((RESERVA, "Reservas internacionales del BCRA"), ""),
    "variacion_reservas_int_ajuste_tipo_pase": ((RESERVA, "Ajuste por tipo de pase"), ""),
}
HISTORICAS = "Series históricas"


def _bp_mbp5(serie: Serie) -> Clasificacion | None:
    concepto = _MBP5.get(serie.variable)
    if concepto is None:
        return None
    ruta, flujo = concepto
    return Clasificacion(tema=TEMA, indicador=INDICADOR_BP, cobertura=HISTORICAS, apertura="MBP5",
                         nombre=con_medida(unir(INDICADOR_BP, *ruta), flujo), unidad=UNIDAD)


# ── Inversión extranjera directa (BCRA): hojas 24 a 32 ─────────────────────────────────────────
# Nivel 3: Stock, Flujos o Renta; nivel 4: Total (con la apertura por instrumento), Por sector de actividad o
# Por país de origen. Los totales de las hojas por sector y por país van al grupo «Total» con el mismo nombre.
IED = "IED"
_MEDIDA_IED = {
    "24. Stock IED": "Stock", "27. Stock IED Sector": "Stock", "30. Stock IED Pais": "Stock",
    "25. Flujos IED": "Flujos", "28. Flujos IED Sector": "Flujos", "31. Flujo IED Pais": "Flujos",
    "26. Ingreso IED": "Renta", "29. Ingresos IED Sector": "Renta", "32. Ingreso IED Pais": "Renta",
}
_INSTRUMENTO_IED = {
    "24. Stock IED": {"total": (), "participaciones_capital": ("Participaciones de capital",),
                      "instrumentos_deuda": ("Instrumentos de deuda",)},
    "25. Flujos IED": {
        "total": (), "total_particip_capital_distintas_reinversion_utilid": (SIN_REINVERSION,),
        "participaciones_capital_distint_reinv_utilid_aportes": (SIN_REINVERSION, "Aportes"),
        "particip_capital_distint_reinv_utilid_transferencias": (SIN_REINVERSION, "Transferencias"),
        "reinversion_utilidades": ("Utilidades reinvertidas",), "instrumentos_deuda": ("Instrumentos de deuda",)},
    "26. Ingreso IED": {
        "total": (), "total_rent_procedent_part_capital": (PARTICIPACIONES,),
        "renta_procedent_particip_capital_distrib_utilidad": (PARTICIPACIONES, "Distribución de utilidades"),
        "renta_procedent_particip_capital_reinv_utilid": (PARTICIPACIONES, "Utilidades reinvertidas"),
        "intereses": ("Intereses",)},
}
_SECTORES_IED = {
    "actividades_administrativas_servicios_apoyo": "Actividades administrativas y servicios de apoyo",
    "agricultura_ganaderia_caza_silvicultura_pesca": "Agricultura, ganadería, caza, silvicultura y pesca",
    "comercio_por_mayor_menor_reparac_automotores_motocicletas":
        "Comercio; reparación de vehículos automotores y motocicletas",
    "construccion": "Construcción", "ensenianza": "Enseñanza", "explotacion_minas_canteras": "Explotación de minas y canteras",
    "industria_manufacturera": "Industria manufacturera", "informacion_comunicaciones": "Información y comunicaciones",
    "otras_sociedades_financieras": "Otras sociedades financieras",
    "salud_humana_servicios_sociales": "Salud humana y servicios sociales", "seguros": "Seguros",
    "servicios_profesionales_cientificos_tecnicos": "Servicios profesionales, científicos y técnicos",
    "servicios_alojamiento_servicios_comida": "Servicios de alojamiento y servicios de comida",
    "servicios_inmobiliarios": "Servicios inmobiliarios",
    "servicios_asociaciones_servicios_personales": "Servicios de asociaciones y servicios personales",
    "servicios_artisticos_culturales_deportivos_esparcimiento":
        "Servicios artísticos, culturales, deportivos y de esparcimiento",
    "servicio_transporte_almacenamiento": "Servicio de transporte y almacenamiento",
    "sociedades_captadoras_depositos_excepto_el_banco_central": SCD,
    "suministro_electricidad_gas_vapor_aire_acondicionado": "Suministro de electricidad, gas, vapor y aire acondicionado",
    "suministr_agua_cloacs_resid_recuper_materiales_saneam_publ":
        "Suministro de agua; cloacas; gestión de residuos y saneamiento público",
}
_PAISES_IED = {
    "alemania": "Alemania", "australia": "Australia", "austria": "Austria", "bahamas": "Bahamas", "barbados": "Barbados",
    "belgica": "Bélgica", "bermudas": "Bermudas", "bolivia": "Bolivia", "brasil": "Brasil", "canada": "Canadá",
    "chile": "Chile", "china": "China", "colombia": "Colombia", "costa_rica": "Costa Rica", "dinamarca": "Dinamarca",
    "emiratos_arabes_unidos": "Emiratos Árabes Unidos", "espania": "España", "estados_unidos_america": "Estados Unidos",
    "francia": "Francia", "hong_kong": "Hong Kong", "irlanda": "Irlanda", "islas_caiman": "Islas Caimán",
    "islas_virgenes_britanicas": "Islas Vírgenes Británicas", "israel": "Israel", "italia": "Italia", "japon": "Japón",
    "liechtenstein": "Liechtenstein", "luxemburgo": "Luxemburgo", "malta": "Malta", "mexico": "México",
    "nueva_zelandia": "Nueva Zelandia", "otros": "Otros países", "paises_bajos": "Países Bajos", "panama": "Panamá",
    "paraguay": "Paraguay", "peru": "Perú", "reino_unido_gran_bretania": "Reino Unido",
    "republica_dominicana": "República Dominicana", "singapur": "Singapur", "suecia": "Suecia", "suiza": "Suiza",
    "uruguay": "Uruguay",
}


# Los cuadros de renta por sector y por país suman sólo la renta de participaciones de capital (dividendos y
# utilidades reinvertidas): su total coincide con «total_rent_procedent_part_capital» de la hoja 26, no con el total
# de la renta de la IED, que además incluye intereses.
_RENTA_SOLO_CAPITAL = ("29. Ingresos IED Sector", "32. Ingreso IED Pais")


def _ied(serie: Serie) -> Clasificacion | None:
    medida = _MEDIDA_IED.get(serie.hoja)
    if medida is None:
        return None
    instrumentos = _INSTRUMENTO_IED.get(serie.hoja)
    if instrumentos is not None:
        detalle = instrumentos.get(serie.variable)
        apertura = "Total"
    elif serie.variable == "total":
        detalle, apertura = (), "Total"
        if serie.hoja in _RENTA_SOLO_CAPITAL:
            detalle = (PARTICIPACIONES,)
    elif "Sector" in serie.hoja:
        sector = _SECTORES_IED.get(serie.variable)
        detalle, apertura = ((sector,) if sector else None), "Por sector de actividad"
    else:
        pais = _PAISES_IED.get(serie.variable)
        detalle, apertura = ((pais,) if pais else None), "Por país de origen"
    if detalle is None:
        return None
    if serie.hoja in _RENTA_SOLO_CAPITAL and detalle != (PARTICIPACIONES,):
        detalle = (PARTICIPACIONES, *detalle)
    return Clasificacion(tema=TEMA, indicador=INDICADOR_IED, cobertura=medida, apertura=apertura,
                         nombre=unir(IED, medida, *detalle), unidad=UNIDAD)


# ── Despacho ───────────────────────────────────────────────────────────────────────────────────
def _tabla(cuadro: dict) -> dict:
    return {plano(rotulo): concepto for rotulo, concepto in cuadro.items()}


_TABLAS_BP = {
    "Cuadro 1": _tabla(_CUADRO_1), "Cuadro 2": _tabla(_CUADRO_2), "Cuadro 3": _tabla(_CUADRO_3),
    "Cuadro 4": _tabla(_CUADRO_4), "Cuadro 5": _tabla(_CUADRO_5), "Cuadro 6": _tabla(_CUADRO_6),
    "Cuadro 7": _tabla(_CUADRO_7), "Cuadro 8": _tabla(_CUADRO_8), "Cuadro 9": _tabla(_CUADRO_9),
    "Cuadro 10": _tabla(_CUADRO_10), "Cuadro 11": _tabla(_CUADRO_11), "Cuadro 12": _tabla(_CUADRO_12),
    "Cuadro 13": _tabla(_CUADRO_13),
}
_TABLAS_PII = {"Cuadro 15": _tabla(_CUADRO_15), "Cuadro 18": _tabla(_CUADRO_18)}


def _cuadro_14(serie: Serie) -> Clasificacion | None:
    campos = serie.id.split("|")[-1].split(".")
    if len(campos) == 1:
        # Tres series finales del cuadro (descomposición de la variación de las reservas) sin clave SDMX.
        detalle = _VARIACION_RESERVAS.get(campos[0])
        return _bp(_cf(RESERVA, "", *detalle)) if detalle else None
    if len(campos) != 16:
        return None
    asiento = campos[7]
    if campos[8] == "FA":
        lado = _LADO_ASIENTO.get(asiento)
        partida = _partida_financiera(campos)
        if lado is None or partida is None:
            return None
        categoria, detalle = partida
        if categoria == TOTAL:
            return _bp(_res("Cuenta financiera")) if lado == NETO else None
        return _bp(_cf(categoria, lado, *detalle))
    concepto = _CLAVES_CUENTAS.get(".".join([campos[4], campos[5], *campos[8:12]]))
    flujo = _FLUJO_ASIENTO.get(asiento)
    if concepto is None or flujo is None:
        return None
    area, ruta, _, nombre = concepto
    return _bp((area, ruta, flujo, nombre))


_VARIACION_RESERVAS = {
    "transacciones-de-balanza-de-pagos-de-activos-de-reserva-1":
        ("Variación del stock", "Transacciones de balanza de pagos"),
    "variaciones-del-tipo-de-cambio-2": ("Variación del stock", "Variaciones del tipo de cambio"),
    "variacion-del-stock-de-activos-de-reserva-1-2": ("Variación del stock",),
}


def _numero_de_cuadro(hoja: str) -> int | None:
    partes = hoja.split()
    if len(partes) == 2 and partes[0] == "Cuadro" and partes[1].isdigit():
        return int(partes[1])
    return None


def _clasificar_cin(serie: Serie) -> Clasificacion | None:
    cuadro = _numero_de_cuadro(serie.hoja)
    if cuadro is None:
        return None
    if cuadro in range(1, 14):
        concepto = _TABLAS_BP[serie.hoja].get(plano(serie.columna))
        return _bp(concepto) if concepto else None
    if cuadro == 14:
        return _cuadro_14(serie)
    if cuadro in (15, 18):
        concepto = _TABLAS_PII[serie.hoja].get(plano(serie.columna))
        return _pii_clasificacion(concepto) if concepto else None
    if cuadro in (16, 21):
        return _pii_sectores(serie, nominal=cuadro == 21)
    if cuadro == 17:
        return _pii_monedas(serie)
    if cuadro in (19, 22):
        return _pii_sdmx(serie, nominal=cuadro == 22)
    if cuadro == 20:
        return _pii_factores(serie)
    if cuadro in (23, 24, 29):
        return _deuda_por_rotulo(serie, mercado=cuadro == 29)
    if cuadro == 30:
        return _deuda_organismos(serie)
    return None


def _pii_1991(serie: Serie) -> Clasificacion | None:
    concepto = _PII_1991.get(serie.id)
    if concepto is None:
        return None
    lado, sector, detalle = concepto
    return Clasificacion(tema=TEMA, indicador=INDICADOR_PII, cobertura=HISTORICAS, apertura="Base 1991",
                         nombre=unir(PII, lado, sector, *detalle), unidad=UNIDAD)


def _clasificar_datos(serie: Serie) -> Clasificacion | None:
    hoja = serie.hoja
    if hoja == "14. Bce Pagos 3-17":
        return _bp_mbp5(serie)
    if hoja == "14.a Bce Pagos 6-17":
        concepto = _BP_14A.get(serie.variable)
        return _bp(concepto) if concepto else None
    if hoja == "15.a Deuda Ext. Bruta x S. 6-17":
        concepto = _DEUDA_15A.get(serie.variable)
        return _deuda(concepto[0], instrumento=concepto[1]) if concepto else None
    if hoja == "16. PII 6-16":
        return _pii_1991(serie)
    if hoja == "16.a PII 6-17":
        concepto = _PII_16A.get(serie.id)
        return _pii_clasificacion(concepto) if concepto else None
    if hoja == "33. Deuda Externa Privada":
        return _deuda_privada(serie)
    if hoja in _MEDIDA_IED:
        return _ied(serie)
    return None


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente == CIN:
        return _clasificar_cin(serie)
    if serie.fuente == DATOS and serie.archivo == LIBRO:
        return _clasificar_datos(serie)
    return None
