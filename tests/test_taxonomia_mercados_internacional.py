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
     esperado(MF, "Volumen operado en el mercado de cambios", "Volumen operado en el mercado de cambios · MAE")),
    (serie("2. Mercado Cambiario", "tipo_cambio_a3500", "Tipo de Cambio A3500", CAMBIARIO),
     esperado(SE, "Tipo de cambio nominal", "Tipo de cambio · Oficial mayorista (A 3500)", unidad="Pesos por US$")),
    (serie("2. Mercado Cambiario", "tipo_cambio_bna_vendedor", "Tipo de Cambio BNA (Vendedor)", CAMBIARIO),
     esperado(SE, "Tipo de cambio nominal", "Tipo de cambio · Banco Nación (vendedor)", unidad="Pesos por US$")),
    (serie("2. Mercado Cambiario", "tipo_cambio_implicito_en_adrs", "Tipo de cambio implícito en ADRs", CAMBIARIO),
     esperado(SE, "Tipo de cambio nominal", "Tipo de cambio · Implícito en ADR", unidad="Pesos por US$")),
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
     esperado(MF, F, f"{F} · Obligaciones negociables · PyMEs", cobertura="Obligaciones negociables",
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
    # Hoja 5.1: títulos públicos
    (serie("5.1 Renta Fija Histórico", "aa19_cotizacion", "Bono de la Nación Argentina Bonar 2025 - AA19. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bonar AA19 (cotización)", cobertura="Bonar y Bonad")),
    (serie("5.1 Renta Fija Histórico", "am18_dur", "Bono de la Nación Argentina Bonad 2018 - AM18. Duration", MERVAL),
     esperado(MF, "Títulos públicos", "Bonad AM18 (duración)", cobertura="Bonar y Bonad", unidad="Años")),
    (serie("5.1 Renta Fija Histórico", "tc25p_ppv_en_anios", "Boncer 2025 - tc25p. Promedio ponderado de vida", MERVAL),
     esperado(MF, "Títulos públicos", "Boncer TC25P (vida promedio ponderada)", cobertura="Bonos ajustables por CER",
              unidad="Años")),
    (serie("5.1 Renta Fija Histórico", "dicy_dur", "Bono de la Nación Argentina Discount 2033 - DICY. Dur", MERVAL),
     esperado(MF, "Títulos públicos", "Discount DICY (duración)", cobertura="Bonos soberanos en dólares", unidad="Años")),
    (serie("5.1 Renta Fija Histórico", "pr15_yield_anios", "Bono del Tesoro Nacional 2015 - PR15. Yield", MERVAL),
     esperado(MF, "Títulos públicos", "Bono del Tesoro Nacional PR15 (rendimiento)", cobertura="Bonos del Tesoro Nacional",
              unidad="Proporción (1 = 100%)")),
    (serie("5.1 Renta Fija Histórico", "pr15_cot_anios", "Bono del Tesoro Nacional 2015 - PR15. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Bono del Tesoro Nacional PR15 (cotización)", cobertura="Bonos del Tesoro Nacional")),
    (serie("5.1 Renta Fija Histórico", "af20d_yield", "Bono Dual 2020 - en dólares - AF20. Yield", MERVAL),
     esperado(MF, "Títulos públicos", "Bono dual en dólares AF20D (rendimiento)", cobertura="Bonos duales",
              unidad="Proporción (1 = 100%)")),
    (serie("5.1 Renta Fija Histórico", "tvpp_cotizacion", "Cupón PIB - TVPP. Cotización", MERVAL),
     esperado(MF, "Títulos públicos", "Cupón PIB TVPP (cotización)", cobertura="Cupones vinculados al PIB")),
    (serie("5.1 Renta Fija Histórico", "nf18_ppv_en_anios", "Bono Garantizado Bogar 18 - NF18. Promedio ponderado de vida", MERVAL),
     esperado(MF, "Títulos públicos", "Bogar NF18 (vida promedio ponderada)",
              cobertura="Bonos de consolidación y garantizados", unidad="Años")),
    # Hoja 15: ponderaciones (los ponderadores del ITCRM)
    (serie("15 Ponderaciones Comercio", "comercio_pct_zona_euro", "Participación de Zona Euro en el total del comercio de Argentina",
           "Participación de los principales socios comerciales en el comercio argentino", INTERNACIONAL, "M"),
     esperado(SE, "Tipo de cambio real", "Ponderador del tipo de cambio real · Zona Euro", cobertura="Ponderadores",
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
     esperado(EI, "Brasil", "IBC-Br · Brasil (desestacionalizado)", cobertura="Actividad económica")),
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
     esperado(EI, "Tasas de interés internacionales", "Tasa de interés de referencia · Estados Unidos (Fed funds)", unidad="%")),
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
     esperado(EI, "Inversión extranjera directa en el mundo", "Inversión extranjera directa · Alemania", cobertura="Europa",
              unidad="Millones de dólares")),
    (serie("20 IED", "alemania", "Flujos netos de inversión extranjera directa Alemania", PAISES_IED_PCT, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Inversión extranjera directa · Alemania (% del PIB)",
              cobertura="Europa", unidad="Proporción (1 = 100%)")),
    (serie("20 IED", "espanna", "Flujos netos de inversión extranjera directa España", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Inversión extranjera directa · España", cobertura="Europa",
              unidad="Millones de dólares")),
    (serie("20 IED", "brasil", "Flujos netos de inversión extranjera directa Brasil", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Inversión extranjera directa · Brasil", cobertura="América",
              unidad="Millones de dólares")),
    (serie("20 IED", "mejico", "Flujos netos de inversión extranjera directa Méjico", PAISES_IED, INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Inversión extranjera directa · México", cobertura="América",
              unidad="Millones de dólares")),
    (serie("20 IED", "rep_dem_del_congo", "Flujos netos de inversión extranjera directa Rep. Dem. Del Congo", PAISES_IED,
           INTERNACIONAL, "A"),
     esperado(EI, "Inversión extranjera directa en el mundo", "Inversión extranjera directa · República Democrática del Congo",
              cobertura="África", unidad="Millones de dólares")),
    (serie("4.b cuenta corriente", "alemania", "Cuenta corriente de balance de pago Alemania", CC, INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Cuenta corriente · Alemania", cobertura="Europa",
              unidad="Millones de dólares")),
    (serie("4.b cuenta corriente", "alemania", "Cuenta corriente de balance de pago Alemania", CC_PCT, INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Cuenta corriente · Alemania (% del PIB)", cobertura="Europa",
              unidad="Proporción (1 = 100%)")),
    (serie("4.b cuenta corriente", "rep_de_corea", "Cuenta corriente de balance de pago Rep. De Corea", CC_PCT, INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Cuenta corriente · República de Corea (% del PIB)", cobertura="Asia",
              unidad="Proporción (1 = 100%)")),
    (serie("4.b cuenta corriente", "yemen_republica_arabe", "Cuenta corriente de balance de pago Yemen, República Arabe", CC,
           INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Cuenta corriente · República Árabe del Yemen", cobertura="Asia",
              unidad="Millones de dólares")),
    (serie("4.b cuenta corriente", "polinesia_frances", "Cuenta corriente de balance de pago Polinesia francés", CC,
           INTERNACIONAL, "A"),
     esperado(EI, "Cuenta corriente por país", "Cuenta corriente · Polinesia Francesa", cobertura="Oceanía",
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

    def test_los_nombres_no_llevan_restos_de_la_fuente(self):
        for entrada, esperada in CASOS:
            nombre = esperada.nombre
            self.assertNotIn("_", nombre)
            self.assertFalse(nombre.isupper(), nombre)
            self.assertNotRegex(nombre, r"\b(mensual|diario|trimestral)\b")


if __name__ == "__main__":
    unittest.main()
