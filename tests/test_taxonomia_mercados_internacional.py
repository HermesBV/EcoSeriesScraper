"""Pruebas de las reglas de mercados financieros y economía internacional."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia import mercados_internacional as mi
from tools.taxonomia.mercados_internacional import clasificar

FINANZAS = "finanzas.xlsx"
INTERNACIONAL = "internacional.xlsx"


def serie(hoja: str, variable: str, descripcion: str, dataset: str, archivo: str = FINANZAS,
          frecuencia: str = "D", id_: str = "") -> Serie:
    return Serie(
        fuente="datos.gob.ar", id=id_ or f"{archivo}|{hoja}|{variable}", nombre=dataset,
        variable=variable, descripcion=descripcion, unidad="", frecuencia=frecuencia,
        archivo=archivo, hoja=hoja, columna=variable, dataset=dataset, estado="VIGENTE",
    )


def esperado(tema, indicador, nombre, cobertura="", apertura="", unidad=""):
    return Clasificacion(tema=tema, indicador=indicador, nombre=nombre, cobertura=cobertura,
                         apertura=apertura, unidad=unidad)


MF, EI, SE = "Mercados financieros", "Economía internacional", "Sector externo"
CAMBIARIO = "Tipo de cambio $-USD - futuro dólar. Valores diarios"
FCI_ANUAL = "Fondos Comunes de Inversión por tipo. Datos anuales"
FCI_MENSUAL = "Fondos Comunes de Inversión por tipo. Datos mensuales"
FINANCIAMIENTO = "Financiamiento Privado en Mercado de Capitales. Dólares."
MERVAL = "Títulos Públicos de Deuda. Merval"
PAISES_IED = "Flujos monetarios netos de Inversión Extranjera Directa"
PAISES_IED_PCT = "Flujos porcentuales netos de Inversión Extranjera Directa"
CC = "Cuenta Corriente del Balance de Pagos. Flujos monetarios"
CC_PCT = "Cuenta Corriente del Balance de Pagos. En porcentaje del producto"
F = "Financiamiento en el mercado de capitales"

CASOS = (
    # Riesgo país (otra fuente y otro archivo)
    (Serie(fuente="ambito-riesgo-pais", id="ambito-riesgo-pais-puntos", nombre="Riesgo país (EMBI)",
           variable="ambito-riesgo-pais-puntos", descripcion="Riesgo país (EMBI). Promedio de observaciones",
           unidad="puntos básicos", frecuencia="D", archivo="history.csv", hoja="Histórico",
           columna="Puntos", dataset="Riesgo país (EMBI)", estado="VIGENTE"),
     esperado(MF, "Riesgo país", "Riesgo país · Argentina", unidad="Puntos básicos")),
    # Hoja 1: índices generales
    (serie("1. Renta Variable", "merval", "Índice Merval del IAMC", "Índices Bursátiles Argentina"),
     esperado(MF, "Índices bursátiles", "Índice bursátil · Merval", cobertura="Generales")),
    (serie("1. Renta Variable", "merval_arg", "Índice Merval Argentina del IAMC", "Índices Bursátiles Argentina"),
     esperado(MF, "Índices bursátiles", "Índice bursátil · Merval Argentina", cobertura="Generales")),
    (serie("1. Renta Variable", "burcap", "Índice Bucarp del IAMC", "Índices Bursátiles Argentina"),
     esperado(MF, "Índices bursátiles", "Índice bursátil · Burcap", cobertura="Generales")),
    # Hoja 6: índices sectoriales
    (serie("6. Indices Sectoriales", "ind_burs_min_no_met", "Índices bursátiles por sector. Minerales no Metálicos",
           "Indices Bursátiles Argentina. Desagregación sectorial"),
     esperado(MF, "Índices bursátiles", "Índice bursátil sectorial · Minerales no metálicos", cobertura="Por sector")),
    # Hoja 2: dólar futuro, interés abierto, volumen y tipo de cambio spot
    (serie("2. Mercado Cambiario", "futuro_rofex_usd1m", "Tipo de cambio ARS/USD. Futuro ROFEX. 1 mes.", CAMBIARIO),
     esperado(MF, "Dólar futuro", "Dólar futuro · 1 mes", unidad="Pesos por US$")),
    (serie("2. Mercado Cambiario", "futuro_rofex_usd_12m", "Futuro ROFEX USD 12M", CAMBIARIO),
     esperado(MF, "Dólar futuro", "Dólar futuro · 12 meses", unidad="Pesos por US$")),
    (serie("2. Mercado Cambiario", "interes_abierto_3m", "Interes abierto 3M", CAMBIARIO),
     esperado(MF, "Dólar futuro", "Dólar futuro · 3 meses (interés abierto)", unidad="Contratos")),
    (serie("2. Mercado Cambiario", "volumen_mae", "Volumen MAE", CAMBIARIO),
     esperado(MF, "Volumen operado en el mercado de cambios", "Volumen operado en el mercado de cambios · MAE",
              unidad="Miles de dólares")),
    (serie("2. Mercado Cambiario", "tipo_cambio_a3500", "Tipo de Cambio A3500", CAMBIARIO),
     esperado(SE, "Tipo de cambio nominal", "Tipo de cambio · Oficial mayorista (A 3500)", unidad="Pesos por US$")),
    (serie("2. Mercado Cambiario", "tipo_cambio_bna_vendedor", "Tipo de Cambio BNA (Vendedor)", CAMBIARIO),
     esperado(SE, "Tipo de cambio nominal", "Tipo de cambio · Banco Nación (vendedor)", unidad="Pesos por US$")),
    (serie("2. Mercado Cambiario", "tipo_cambio_implicito_en_adrs", "Tipo de cambio implícito en ADRs", CAMBIARIO),
     esperado(SE, "Tipo de cambio nominal", "Tipo de cambio · Contado con liquidación (implícito en ADR)",
              cobertura="Paralelos y financieros", unidad="Pesos por US$")),
    # Hoja 3: FCI, anual y mensual con el mismo nombre
    (serie("3. FCI", "fci_renta_fija", "Fondos Comunes de Inversión de Renta Fija, patrimonio a diciembre de cada año",
           FCI_ANUAL, frecuencia="A"),
     esperado(MF, "Fondos comunes de inversión", "Fondos comunes de inversión · Renta fija (patrimonio neto)",
              unidad="Millones de pesos")),
    (serie("3. FCI", "fci_renta_fija", "Fondos Comunes de Inversión de Renta Fija, patrimonio mensual",
           FCI_MENSUAL, frecuencia="M"),
     esperado(MF, "Fondos comunes de inversión", "Fondos comunes de inversión · Renta fija (patrimonio neto)",
              unidad="Millones de pesos")),
    (serie("3. FCI", "fci_patrimonio_neto_total", "Patrimonio Neto Total de Fondos Comunes de Inversión, patrimonio mensual",
           FCI_MENSUAL, frecuencia="M"),
     esperado(MF, "Fondos comunes de inversión", "Fondos comunes de inversión · Total (patrimonio neto)",
              unidad="Millones de pesos")),
    # Hoja 4: financiamiento en el mercado de capitales
    (serie("4. Financiamiento Privado", "financiamiento_mercado_capitales_total_general",
           "Monto total general emitido. Mensual.", FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · Total", cobertura="Total", unidad="Dólares")),
    (serie("4. Financiamiento Privado", "financiamiento_mercado_capitales_pymes_on_vcp",
           "Monto emitido por PyMEs de Obligaciones Negociables y Valores a Corto Plazo (ON/VCP). Mensual.",
           FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · ON y valores de corto plazo · PyMEs", cobertura="Obligaciones negociables",
              apertura="Por tamaño de empresa", unidad="Dólares")),
    (serie("4. Financiamiento Privado", "emisiones_ge_ff",
           "Cantidad de emisiones de grandes empresas de Fideicomisos Financieros (FF). Cantidades mensuales.",
           FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · Fideicomisos financieros · Grandes empresas (cantidad de emisiones)",
              cobertura="Fideicomisos financieros", apertura="Por tamaño de empresa", unidad="Emisiones")),
    (serie("4. Financiamiento Privado", "emisiones_pymes_pag", "Cantidad de emisiones de PyMEs de pagarés. Cantidades mensuales.",
           FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · Pagarés · PyMEs (cantidad de emisiones)", cobertura="Pagarés", unidad="Emisiones")),
    (serie("4. Financiamiento Privado", "industria_petroleo_gas", "Emisión de obligaciones negociables de industria petroleo gas",
           FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · Obligaciones negociables · Industria del petróleo y gas (% del total)",
              cobertura="Obligaciones negociables", apertura="Por sector", unidad="Proporción (1 = 100%)")),
    (serie("4. Financiamiento Privado", "eff_prestamos_personales_consumo",
           "Emisión de Fideicomisos Financieros por Préstamos Personales y de Consumo como % del total.",
           FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · Fideicomisos financieros · Préstamos personales y de consumo (% del total)",
              cobertura="Fideicomisos financieros", apertura="Por activo subyacente", unidad="Proporción (1 = 100%)")),
    (serie("4. Financiamiento Privado", "tasa_interes_cpdp",
           "Tasa de interés nominal anual de Cheques de Pago Diferido Patrocinados (CPDP)", FINANCIAMIENTO, frecuencia="M"),
     esperado(MF, F, f"{F} · Cheques de pago diferido · Patrocinados (tasa nominal anual)",
              cobertura="Cheques de pago diferido", apertura="Tasas de interés", unidad="Proporción (1 = 100%)")),
    # Hoja 5.1: títulos públicos (grupo por moneda y ajuste, denominación de la especie, cotización por 100 de VN)
    (serie("5.1 Renta Fija Histórico", "aa19_cotizacion", "Bono de la Nación Argentina Bonar 2025 - AA19. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bonar 2019 AA19 (cotización)", cobertura="Bonos en dólares",
              unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "am18_dur", "Bono de la Nación Argentina Bonad 2018 - AM18. Duration", MERVAL),
     esperado(MF, "Títulos públicos", "Bonad 2018 AM18 (duración modificada)", cobertura="Bonos vinculados al dólar",
              unidad="Años")),
    (serie("5.1 Renta Fija Histórico", "tc25p_ppv_en_anios", "Boncer 2025 - tc25p. Promedio ponderado de vida", MERVAL),
     esperado(MF, "Títulos públicos", "Boncer 2025 TC25P (vida promedio ponderada)", cobertura="Bonos ajustables por CER",
              unidad="Años")),
    (serie("5.1 Renta Fija Histórico", "dicy_dur", "Bono de la Nación Argentina Discount 2033 - DICY. Dur", MERVAL),
     esperado(MF, "Títulos públicos", "Discount en dólares ley Nueva York 2033 DICY (duración modificada)",
              cobertura="Bonos del canje de deuda (Discount, Par y Cuasipar)", unidad="Años")),
    (serie("5.1 Renta Fija Histórico", "pr15_yield_anios", "Bono del Tesoro Nacional 2015 - PR15. Yield", MERVAL),
     esperado(MF, "Títulos públicos", "Bono de consolidación serie 8 2022 PR15 (rendimiento)",
              cobertura="Bonos de consolidación y garantizados", unidad="Proporción (1 = 100%)")),
    (serie("5.1 Renta Fija Histórico", "pr15_cot_anios", "Bono del Tesoro Nacional 2015 - PR15. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bono de consolidación serie 8 2022 PR15 (cotización)",
              cobertura="Bonos de consolidación y garantizados", unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "af20d_yield", "Bono Dual 2020 - en dólares - AF20. Yield", MERVAL),
     esperado(MF, "Títulos públicos", "Bono dual 2020 AF20D (rendimiento)", cobertura="Bonos duales",
              unidad="Proporción (1 = 100%)")),
    (serie("5.1 Renta Fija Histórico", "a2j9d_cotizacion", "Bono Dual 2019 - en dólares - A2J9D. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bono dual 2019 A2J9D (cotización en dólares)", cobertura="Bonos duales",
              unidad="Dólares por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "a2j9_cotizacion", "Bono Dual 2019 - en pesos - A2J9. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bono dual 2019 A2J9 (cotización)", cobertura="Bonos duales",
              unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "aa22_cotizacion", "Bono Bocan 2022 - AA22. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bocan 2022 AA22 (cotización)", cobertura="Bonos en pesos a tasa variable",
              unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "td21_cotizacion",
           "Bono en $ Ajustable por Clausula Gatillo - TD21. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bono del Tesoro en pesos a tasa dual 2021 TD21 (cotización)",
              cobertura="Bonos en pesos a tasa variable", unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "para_cotizacion", "Bono de la Nación Argentina Par 2038 - PARA. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Par en dólares ley argentina 2038 PARA (cotización)",
              cobertura="Bonos del canje de deuda (Discount, Par y Cuasipar)",
              unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "tvpp_cotizacion", "Cupón PIB - TVPP. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Cupón PIB en pesos TVPP (cotización)", cobertura="Cupones vinculados al PIB",
              unidad="Pesos por cada 100 de valor nominal")),
    (serie("5.1 Renta Fija Histórico", "nf18_ppv_en_anios", "Bono Garantizado Bogar 18 - NF18. Promedio ponderado de vida", MERVAL),
     esperado(MF, "Títulos públicos", "Bogar 2018 NF18 (vida promedio ponderada)",
              cobertura="Bonos de consolidación y garantizados", unidad="Años")),
    # Hoja 15: ponderaciones (los ponderadores del ITCRM)
    (serie("15 Ponderaciones Comercio", "comercio_pct_zona_euro", "Participación de Zona Euro en el total del comercio de Argentina",
           "Participación de los principales socios comerciales en el comercio argentino", INTERNACIONAL, "M"),
     esperado(SE, "Tipo de cambio real", "ITCRM · Ponderador · Zona Euro", cobertura="Ponderadores",
              unidad="%")),
    # Hojas 16 a 19: Brasil
    (serie("16 Brasil PIB", "pib_pib", "Del PIB de Brasil", "Brasil PIB Oferta y demanda global", INTERNACIONAL, "T"),
     esperado(EI, "Brasil", "PIB · Brasil (precios constantes)", cobertura="Producto interno bruto",
              unidad="Millones de reales de 1995")),
    (serie("16 Brasil PIB", "pib_gasto_publico", "Del sector Gasto público de Brasil", "Brasil PIB Oferta y demanda global",
           INTERNACIONAL, "T"),
     esperado(EI, "Brasil", "PIB · Gasto público · Brasil (precios constantes)", cobertura="Producto interno bruto",
              unidad="Millones de reales de 1995")),
    (serie("17 Bras Activid Indust Autos", "indice_actividad_ibc_br_desest", "Índice de actividad (IBC-Br) Desestacionalizado",
           "Brasil Actividad económica", INTERNACIONAL, "M"),
     esperado(EI, "Brasil", "IBC-Br · Brasil (desestacionalizado)", cobertura="Actividad económica",
              unidad="Índice 2012=100")),
    (serie("17 Bras Activid Indust Autos", "produccion_automotriz_unidades", "Producción automotriz Unidades",
           "Brasil Actividad económica", INTERNACIONAL, "M"),
     esperado(EI, "Brasil", "Producción automotriz · Brasil", cobertura="Actividad económica", unidad="Unidades")),
    (serie("18 Bras Salario real", "remuneracion_media_real_todos_los_trabajos", "Remuneración media real Todos los trabajos",
           "Brasil Salario medio real", INTERNACIONAL, "M"),
     esperado(EI, "Brasil", "Remuneración media real · Todos los trabajos · Brasil", cobertura="Remuneración media real")),
    (serie("19. Bras Tipo de cambio", "tipo_de_cambio_real", "Tipo de cambio real", "Brasil Tipo de cambio nominal y real",
           INTERNACIONAL, "M"),
     esperado(EI, "Brasil", "Tipo de cambio real · Brasil", cobertura="Tipo de cambio")),
    # Hoja 5: tasas internacionales; hoja 6: bonos del Tesoro
    (serie("5 tasa de interes", "selic_brasil_tasa", "Selic - Brasil - Tasa", "Principales Tasas de Interés de Referencia",
           INTERNACIONAL, "M"),
     esperado(EI, "Tasas de interés internacionales", "Tasa de interés de referencia · Brasil (Selic)", unidad="%")),
    (serie("5 tasa de interes", "ff_eeuu_tasa", "FF (Federal Funds) - EE.UU - Tasa", "Principales Tasas de Interés de Referencia",
           INTERNACIONAL, "M"),
     esperado(EI, "Tasas de interés internacionales", "Tasa de interés de referencia · Estados Unidos (objetivo de fondos federales)",
              unidad="%")),
    (serie("6 bonos usa", "titulos_us_10_anios", "Rendimiento Bono del Tesoro a 10 años",
           "Rendimiento Títulos del Tesoro de Estados Unidos", INTERNACIONAL),
     esperado(EI, "Rendimiento de los bonos del Tesoro de Estados Unidos", "Rendimiento del Tesoro de Estados Unidos · 10 años",
              cobertura="Nominales", unidad="%")),
    (serie("6 bonos usa", "titulos_us_1_anios", "Rendimiento Bono del Tesoro a 1 año",
           "Rendimiento Títulos del Tesoro de Estados Unidos", INTERNACIONAL),
     esperado(EI, "Rendimiento de los bonos del Tesoro de Estados Unidos", "Rendimiento del Tesoro de Estados Unidos · 1 año",
              cobertura="Nominales", unidad="%")),
    (serie("6 bonos usa", "titulos_us_3_meses", "Rendimiento Letra del Tesoro a 3 meses",
           "Rendimiento Títulos del Tesoro de Estados Unidos", INTERNACIONAL),
     esperado(EI, "Rendimiento de los bonos del Tesoro de Estados Unidos", "Rendimiento del Tesoro de Estados Unidos · 3 meses",
              cobertura="Nominales", unidad="%")),
    (serie("6 bonos usa", "titulos_us_indexado_20_anios", "Rendimiento Bono del Tesoro Indexado a 20 años",
           "Rendimiento Títulos del Tesoro de Estados Unidos", INTERNACIONAL),
     esperado(EI, "Rendimiento de los bonos del Tesoro de Estados Unidos",
              "Rendimiento del Tesoro de Estados Unidos · 20 años (indexado a la inflación)",
              cobertura="Indexados a la inflación", unidad="%")),
    # Hojas 20 y 4.b: IED y cuenta corriente por país
    (serie("20 IED", "alemania", "Flujos netos de inversión extranjera directa Alemania", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Entradas netas de inversión extranjera directa · Alemania", cobertura="Europa",
              unidad="Millones de dólares")),
    (serie("20 IED", "alemania", "Flujos netos de inversión extranjera directa Alemania", PAISES_IED_PCT, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Entradas netas de inversión extranjera directa · Alemania (% del PIB)",
              cobertura="Europa", unidad="Proporción (1 = 100%)")),
    (serie("20 IED", "espanna", "Flujos netos de inversión extranjera directa España", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Entradas netas de inversión extranjera directa · España", cobertura="Europa",
              unidad="Millones de dólares")),
    (serie("20 IED", "brasil", "Flujos netos de inversión extranjera directa Brasil", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Entradas netas de inversión extranjera directa · Brasil", cobertura="América",
              unidad="Millones de dólares")),
    (serie("20 IED", "mejico", "Flujos netos de inversión extranjera directa Méjico", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Entradas netas de inversión extranjera directa · México", cobertura="América",
              unidad="Millones de dólares")),
    (serie("20 IED", "rep_dem_del_congo", "Flujos netos de inversión extranjera directa Rep. Dem. Del Congo", PAISES_IED,
           INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Entradas netas de inversión extranjera directa · República Democrática del Congo",
              cobertura="África", unidad="Millones de dólares")),
    (serie("4.b cuenta corriente", "alemania", "Cuenta corriente de balance de pago Alemania", CC, INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Saldo de la cuenta corriente · Alemania", cobertura="Europa",
              unidad="Millones de dólares")),
    (serie("4.b cuenta corriente", "alemania", "Cuenta corriente de balance de pago Alemania", CC_PCT, INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Saldo de la cuenta corriente · Alemania (% del PIB)", cobertura="Europa",
              unidad="Proporción (1 = 100%)")),
    (serie("4.b cuenta corriente", "rep_de_corea", "Cuenta corriente de balance de pago Rep. De Corea", CC_PCT, INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Saldo de la cuenta corriente · República de Corea (% del PIB)", cobertura="Asia",
              unidad="Proporción (1 = 100%)")),
    (serie("4.b cuenta corriente", "yemen_republica_arabe", "Cuenta corriente de balance de pago Yemen, República Arabe", CC,
           INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Saldo de la cuenta corriente · República Árabe del Yemen", cobertura="Asia",
              unidad="Millones de dólares")),
    (serie("4.b cuenta corriente", "polinesia_frances", "Cuenta corriente de balance de pago Polinesia francés", CC,
           INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Saldo de la cuenta corriente · Polinesia Francesa", cobertura="Oceanía",
              unidad="Millones de dólares")),
)


class MercadosInternacionalTest(unittest.TestCase):
    def test_casos_representativos(self):
        for entrada, esperada in CASOS:
            with self.subTest(hoja=entrada.hoja, variable=entrada.variable, dataset=entrada.dataset):
                self.assertEqual(clasificar(entrada), esperada)

    def test_hay_al_menos_25_casos_y_cubren_todas_las_hojas(self):
        self.assertGreaterEqual(len(CASOS), 25)
        hojas = {entrada.hoja for entrada, _ in CASOS}
        esperadas = set(mi._FINANZAS) | set(mi._INTERNACIONAL) | {"Histórico"}
        self.assertEqual(hojas, esperadas)

    def test_fuera_de_alcance_devuelve_none(self):
        otras = (
            serie("Cuadro 1", "x", "Otra cosa", "Otro", archivo="supermercados.xlsx"),
            serie("1. Renta Variable", "merval", "Índice Merval", "Índices", archivo="otro.xlsx"),
            serie("Hoja inexistente", "merval", "Índice Merval", "Índices"),
            serie("20 IED", "xx", "Flujos netos de inversión extranjera directa Atlantida", PAISES_IED, INTERNACIONAL, "A"),
            serie("2. Mercado Cambiario", "otra_variable", "Otra", CAMBIARIO),
            serie("5.1 Renta Fija Histórico", "zzzz_cotizacion", "Bono Z - ZZZZ. Cotización", MERVAL),
        )
        for entrada in otras:
            with self.subTest(hoja=entrada.hoja, archivo=entrada.archivo):
                self.assertIsNone(clasificar(entrada))
        ajena = Serie(fuente="indec-ipc", id="a", nombre="a", variable="merval", descripcion="", unidad="",
                      frecuencia="M", archivo=FINANZAS, hoja="1. Renta Variable", columna="a", dataset="", estado="VIGENTE")
        self.assertIsNone(clasificar(ajena))

    def test_es_determinista(self):
        for entrada, _ in CASOS:
            self.assertEqual(clasificar(entrada), clasificar(entrada))

    def test_los_paises_estan_en_un_continente_y_con_nombres_unicos(self):
        continentes = {mi.AFRICA, mi.AMERICA, mi.ASIA, mi.EUROPA, mi.OCEANIA}
        nombres = [nombre for nombre, _ in mi._PAISES.values()]
        self.assertEqual(len(nombres), len(set(nombres)))
        self.assertTrue(all(continente in continentes for _, continente in mi._PAISES.values()))
        # Ningún continente supera el tope de series por grupo (dos medidas por país).
        for continente in continentes:
            cantidad = sum(1 for _, c in mi._PAISES.values() if c == continente)
            self.assertLessEqual(2 * cantidad, 150, continente)

    def test_los_bonos_se_abren_en_grupos_manejables(self):
        por_tipo: dict[str, int] = {}
        for tipo, _ in mi._BONOS.values():
            por_tipo[tipo] = por_tipo.get(tipo, 0) + 1
        self.assertTrue(all(4 * cantidad <= 150 for cantidad in por_tipo.values()), por_tipo)
        self.assertEqual(len(mi._BONOS), 59)
        self.assertEqual(len(por_tipo), 10)

    def test_bonos_corregidos_respecto_de_la_fuente(self):
        grupo = {codigo: tipo for codigo, (tipo, _) in mi._BONOS.items()}
        # PR15 cotiza hasta 2020: no es un bono de 2015 sino la consolidación serie 8; TD21 es de tasa
        # dual y no de cláusula gatillo; AA22 es un bono en pesos (Badlar + 200) y no de consolidación;
        # AC17 vence en 2117; las especies en pesos no se mezclan con las de dólares.
        self.assertEqual(grupo["pr15"], mi.CONSOLIDACION)
        self.assertEqual(grupo["td21"], mi.TASA_VARIABLE)
        self.assertEqual(grupo["aa22"], mi.TASA_VARIABLE)
        self.assertEqual(grupo["ac17"], mi.USD)
        self.assertEqual(mi._BONOS["ac17"][1], "Bono internacional 2117 AC17")
        for codigo in ("am20", "amx8", "amx9", "ao17", "tm18", "ts18"):
            self.assertNotEqual(grupo[codigo], mi.USD, codigo)
        for codigo in ("an18", "ao20", "ay24", "aa25"):
            self.assertEqual(grupo[codigo], mi.USD, codigo)
        self.assertEqual({c for c, g in grupo.items() if g == mi.VINCULADOS}, {"am18", "as17", "tv21"})

    def test_cotizacion_duracion_y_vida_de_los_bonos(self):
        def bono(variable):
            return clasificar(serie("5.1 Renta Fija Histórico", variable, "x", MERVAL))
        self.assertEqual(bono("dicp_cotizacion").unidad, "Pesos por cada 100 de valor nominal")
        self.assertEqual(bono("af20d_cotizacion").unidad, "Dólares por cada 100 de valor nominal")
        self.assertEqual(bono("af20d_cotizacion").nombre, "Bono dual 2020 AF20D (cotización en dólares)")
        self.assertEqual(bono("pr15_dm_en_anios").nombre, "Bono de consolidación serie 8 2022 PR15 (duración modificada)")
        self.assertEqual(bono("a2e2_dur").nombre, "Bonar 2022 A2E2 (duración modificada)")
        self.assertEqual(bono("a2e2_yield").unidad, "Proporción (1 = 100%)")

    def test_la_serie_del_banco_nacion_esta_excluida(self):
        self.assertEqual(set(mi.EXCLUIDAS), {("datos.gob.ar", "168.1_T_CAMBIOR_D_0_0_26")})
        self.assertTrue(all(motivo for motivo in mi.EXCLUIDAS.values()))

    def test_participaciones_de_las_on_y_los_fideicomisos(self):
        self.assertEqual((len(mi._ON_POR_SECTOR), len(mi._FF_POR_ACTIVO)), (30, 12))
        for variable in list(mi._ON_POR_SECTOR) + list(mi._FF_POR_ACTIVO):
            c = clasificar(serie("4. Financiamiento Privado", variable, "x", FINANCIAMIENTO, frecuencia="M"))
            self.assertEqual(c.unidad, "Proporción (1 = 100%)", variable)
            self.assertTrue(c.nombre.endswith("(% del total)"), variable)

    def test_on_incluye_valores_de_corto_plazo_en_montos_y_cantidades(self):
        for variable in ("financiamiento_mercado_capitales_ge_on_vcp", "emisiones_pymes_on"):
            c = clasificar(serie("4. Financiamiento Privado", variable, "x", FINANCIAMIENTO, frecuencia="M"))
            self.assertIn("ON y valores de corto plazo", c.nombre)

    def test_los_nombres_no_llevan_restos_de_la_fuente(self):
        for entrada, esperada in CASOS:
            nombre = esperada.nombre
            self.assertNotIn("_", nombre)
            self.assertFalse(nombre.isupper(), nombre)
            self.assertNotRegex(nombre, r"\b(mensual|diario|trimestral)\b")


if __name__ == "__main__":
    unittest.main()
