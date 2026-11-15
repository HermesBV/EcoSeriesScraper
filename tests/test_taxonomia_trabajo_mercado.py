"""Pruebas de las reglas de ``tools/taxonomia/trabajo_mercado.py`` con series reales del inventario."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.trabajo_mercado import (
    _AGLOMERADOS, _cobertura, clasificar,
)

TRABAJO = "Trabajo e ingresos"
TASAS = "Tasas del mercado de trabajo (EPH)"
POBLACION = "Población según condición de actividad (EPH)"
POBREZA = "Pobreza e indigencia"
PROP = "Proporción (1 = 100%)"


def serie(hoja: str, id: str, variable: str, descripcion: str, frecuencia: str = "A",
          estado: str = "VIGENTE", fuente: str = "datos.gob.ar", archivo: str = "empleo_ingresos.xlsx") -> Serie:
    return Serie(fuente=fuente, id=id, nombre="Título del cuadro", variable=variable, descripcion=descripcion,
                 unidad="", frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=id,
                 dataset="Título del dataset", estado=estado)


def esperado(tema, indicador, nombre, cobertura="", apertura="", medida="", unidad="") -> Clasificacion:
    return Clasificacion(tema, indicador, nombre, cobertura, apertura, medida, unidad)


class TasasTest(unittest.TestCase):
    def test_total_aglomerados(self):
        s = serie("TA 03-", "43.1_ECTAT_0_A_33", "eph_continua_tasa_actividad_total",
                  "Tasa de actividad total. En porcentaje.")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de actividad",
                                                 "Total aglomerados", "Actividad", unidad=PROP))

    def test_aglomerado_con_grafia_oficial(self):
        s = serie("TD 03-", "45.1_ECTDTGC_0_A_46", "eph_continua_tasa_desempleo_total_gran_cordoba",
                  "Tasa de desempleo total Gran Cordoba. En porcentaje.")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de desocupación · Gran Córdoba",
                                                 "Aglomerados", "Desocupación", unidad=PROP))

    def test_aglomerado_compuesto_y_trimestral(self):
        s = serie("TE 03-", "44.2_ECTETVCP_0_T_54", "x",
                  "Tasa de empleo total Viedma - Carmen de Patagones. En porcentaje.", "T", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de empleo · Viedma – Carmen de Patagones",
                                                 "Aglomerados", "Empleo", unidad=PROP))

    def test_region_se_toma_de_la_descripcion_y_no_de_la_sigla_del_id(self):
        # En TE 03- la sigla «NE» del ID está en la descripción de la región noroeste.
        s = serie("TE 03-", "44.1_ECTETNE_0_A_39", "eph_continua_tasa_empleo_total_noroeste",
                  "Tasa de empleo total región noroeste. En porcentaje.")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de empleo · Noroeste",
                                                 "Regiones", "Empleo", unidad=PROP))

    def test_region_nordeste_se_llama_noreste(self):
        s = serie("TS 03-", "46.1_ECTSTNE_0_A_45", "x", "Tasa de subocupación total región nordeste.")
        self.assertEqual(clasificar(s).nombre, "Tasa de subocupación · Noreste")

    def test_region_pampeana_sin_la_palabra_region(self):
        s = serie("TSD 03-", "47.1_ECTSDTRP_0_A_58", "x",
                  "Tasa de subocupación demandante total región pampeana.")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de subocupación demandante · Pampeana",
                                                 "Regiones", "Subocupación demandante", unidad=PROP))

    def test_partidos_y_caba(self):
        s = serie("TSND 03-", "48.1_ECTSNDTPG_0_A_52", "x",
                  "Tasa de subocupación no demandante total partidos GBA.")
        self.assertEqual(clasificar(s).nombre, "Tasa de subocupación no demandante · Partidos del GBA")
        s = serie("TA 03-", "43.1_ECTATC_0_A_38", "x", "Tasa de actividad total CABA. En porcentaje.")
        self.assertEqual(clasificar(s).nombre, "Tasa de actividad · Ciudad Autónoma de Buenos Aires")

    def test_hoja_eph_anual_empalmada_total_gba_y_aip(self):
        s = serie("EPH", "42.1_EPDT_0_A_30", "eph_puntual_continua_desocupacion_total",
                  "Tasa de desocupación total. En porcentaje. EPH puntual desde 1982 hasta 2002.")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de desocupación",
                                                 "Total aglomerados", "Desocupación", unidad=PROP))
        s = serie("EPH", "42.1_EPSTDG_0_A_45", "eph_puntual_continua_subocupacion_total_demandante_gba",
                  "Tasa de subocupación total demandante en GBA.")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de subocupación demandante · Gran Buenos Aires",
                                                 "Aglomerados", "Subocupación demandante", unidad=PROP))
        s = serie("EPH", "42.3_EPH_PUNTUAAIP_0_M_48", "eph_continua_subocupacion_total_no_demandante_aip",
                  "Tasa de subocupación total no demandanteen AIP.", "T")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS,
                                                 "Tasa de subocupación no demandante · Total aglomerados del interior",
                                                 "Total aglomerados", "Subocupación no demandante", unidad=PROP))

    def test_hoja_eph_total_de_subocupacion_y_empleo(self):
        s = serie("EPH", "42.1_EPST_0_A_30", "eph_puntual_continua_subocupacion_total", "Tasa de subocupación total.")
        self.assertEqual(clasificar(s).nombre, "Tasa de subocupación")
        s = serie("EPH", "42.3_EPH_PUNTUATAL_0_M_24", "eph_continua_empleo_total", "Tasa de empleo total.", "T")
        self.assertEqual(clasificar(s).nombre, "Tasa de empleo")

    def test_eph_puntual_semestral_va_a_series_historicas(self):
        s = serie("EPH", "42.2_EPAG_0_M_25", "eph_puntual_actividad_gba", "Tasa de actividad GBA. En porcentaje.",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de actividad · Gran Buenos Aires",
                                                 "Series históricas", "Actividad", unidad=PROP))

    def test_eph_puntual_1974_2003(self):
        s = serie("TA 74-03", "341.1_PROVINCIA_UAN__32", "provincia_san_juan_gran_san_juan",
                  "Provincia San Juan Gran San Juan", "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de actividad · Gran San Juan",
                                                 "Series históricas", "Actividad", unidad=PROP))
        s = serie("TS 74-03", "344.1_PROVINCIA_OJA__27", "x", "Provincia La Rioja. La Rioja capital", "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Tasa de subocupación · La Rioja")
        s = serie("TD 74-03", "343.1_TOTAL_AGLONOS__25", "total_aglomerados_urbanos", "Total aglomerados urbanos",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de desocupación",
                                                 "Series históricas", "Desocupación", unidad=PROP))
        s = serie("TE 74-03", "342.1_PARTIDOS_A_98__24", "partidos_a_partir_del_98", "Partidos a partir del 98",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Tasa de empleo · Partidos del GBA")
        s = serie("TA 74-03", "341.1_PARTIDOS_DANO__22", "x", "Partidos del Conurbano", "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Tasa de actividad · Partidos del conurbano")

    def test_asalarizacion(self):
        s = serie("EPH - Asal", "52.1_TA_0_0_18", "tasa_asalarizacion",
                  "Tasa de asalariados de la Encuesta Permanente de Hogares")
        self.assertEqual(clasificar(s), esperado(TRABAJO, TASAS, "Tasa de asalarización",
                                                 "Total aglomerados", "Asalarización", unidad=PROP))
        s = serie("EPH - Asal", "52.2_ASDJ_0_0_37", "asalariados_sin_descuento_jubilatorio",
                  "Tasa de asalariados sin descuento jubilatorio de la Encuesta Permanente de Hogares", "T")
        self.assertEqual(clasificar(s).nombre, "Tasa de asalariados sin descuento jubilatorio")

    def test_asalarizacion_por_regiones_y_tamano(self):
        base = "Evolución de la tasa de asalarizacion. EPH Continua. Trimestral."
        casos = [
            ("tasa_asalarizacion_aglomerados_interior_pais_region_pampeana", base + "Region Pampeana.",
             "Tasa de asalarización · Pampeana", "Regiones"),
            ("tasa_asalarizacion_aglomerados_gba", base + " Gran Buenos Aires",
             "Tasa de asalarización · Gran Buenos Aires", "Aglomerados"),
            ("asalariados_sin_desc_jubilat_aglo_500000_mas_hab", base + " Aglomeradosde mas de 500.000 habitantes .",
             "Tasa de asalariados sin descuento jubilatorio · Aglomerados de más de 500.000 habitantes",
             "Total aglomerados"),
            ("tasa_asalarizacion_aglomerados_interior_pais_total", base + "Total Aglomerados Interior del Pais",
             "Tasa de asalarización · Total aglomerados del interior", "Total aglomerados"),
            ("tasa_asalarizacion_aglomerados_total", base + "Total Aglomerados .",
             "Tasa de asalarización", "Total aglomerados"),
        ]
        for variable, descripcion, nombre, grupo in casos:
            with self.subTest(variable=variable):
                r = clasificar(serie("EPH - Asal regiones", "303.1_X_0_T_1", variable, descripcion, "T", "CERRADA"))
                self.assertEqual((r.nombre, r.cobertura), (nombre, grupo))


class PoblacionTest(unittest.TestCase):
    def test_total_aglomerados(self):
        s = serie("EPH-Poblaciones", "49.2_TAEP_0_0_25", "total_aglomerados_eph_pea",
                  "Población activa del total de los aglomerados de la EPH, en miles de personas", "T")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBLACION, "Población económicamente activa",
                                                 "Total aglomerados", unidad="Miles de personas"))

    def test_subocupados_no_demandantes(self):
        s = serie("EPH-Poblaciones", "49.1_TOTAL_AGLOTES_0_S_48_98", "total_aglomerados_eph_subocupados_no_demandantes",
                  "Población subocupada no demandante del total de los aglomerados", "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Población subocupada no demandante")

    def test_total_urbano_vigente(self):
        s = serie("Expansion EPH a TU - Desde 2010", "461.3_POBLACION_ANO_AEA_T_28_3", "poblacion_total_total_urbano",
                  "Población total del total urbano, en miles de personas", "T")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBLACION, "Población total · Total urbano",
                                                 "Total urbano", unidad="Miles de personas"))

    def test_total_urbano_historico(self):
        s = serie("Expansion EPH a TU - 1991-2010", "461.1_SUBOCUPADOANO_0_S_36_73",
                  "subocupados_demandantes_total_urbano",
                  "Población subocupada demandante de empleo del total urbano, en miles de personas", "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBLACION, "Población subocupada demandante · Total urbano",
                                                 "Series históricas", unidad="Miles de personas"))


class EmpleoEilTest(unittest.TestCase):
    def test_total_sector_y_aglomerado(self):
        unidad = "Índice ago-2001=100"
        s = serie("EIL - Sector", "50.3_ICS_0_M_12", "indice_total", "Índice del Empleo total", "M")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Empleo (EIL)", "Índice de empleo", "Total", unidad=unidad))
        s = serie("EIL - Sector", "50.1_ICS_0_0_25", "indice_comercio_servicios", "Índice del Empleo en el Comercio y Servicios")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Empleo (EIL)", "Índice de empleo · Comercio y servicios",
                                                 "Sectores de actividad", unidad=unidad))
        s = serie("EIL - Aglo", "51.2_IGC_0_0_19", "indice_gran_cordoba", "Índice del Empleo en el Gran Cordoba", "T")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Empleo (EIL)", "Índice de empleo · Gran Córdoba",
                                                 "Aglomerados", unidad=unidad))


class PobrezaTest(unittest.TestCase):
    def test_pobreza_hogares_continua(self):
        s = serie("LP - Hog", "63.2_HOGARES_PONUA_0_0_40_49", "x",
                  "Hogares con ingresos debajo de línea de pobreza (%) desde 2003. Gran Córdoba. EPH continua.",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBREZA, "Incidencia de la pobreza · Hogares · Gran Córdoba",
                                                 "Aglomerados", "Pobreza", unidad=PROP))

    def test_pobreza_personas_total_e_interior(self):
        s = serie("LP - Pers", "64.2_POBLACION_NUA_0_0_33_21", "x",
                  "Población con ingresos debajo de línea de pobreza (%) desde 2003. TOTAL. EPH continua.", "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBREZA, "Incidencia de la pobreza · Personas",
                                                 "Total aglomerados", "Pobreza", unidad=PROP))
        s = serie("LP - Pers", "64.2_POBLACION_NUA_0_0_53_41", "x",
                  "Población con ingresos debajo de línea de pobreza (%) desde 2003. Aglomerados del Interior del País. EPH continua.",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Incidencia de la pobreza · Personas · Total aglomerados del interior")

    def test_indigencia_region(self):
        s = serie("LI - Hog", "61.1_RNE_0_0_15", "hogares_indigentes_pct_region_nordeste",
                  "Hogares con ingresos debajo de línea de indigencia (%) de 2001 a 2003. Región Nordeste", "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBREZA, "Incidencia de la indigencia · Hogares · Noreste",
                                                 "Regiones", "Indigencia", unidad=PROP))

    def test_indigencia_aglomerado_compuesto(self):
        s = serie("LI - Pers", "62.1_GTTV_0_0_23", "x",
                  "Población con ingresos debajo de línea de indigencia (%) de 2001 a 2003. Gran Tucumán - Tafí Viejo",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Incidencia de la indigencia · Personas · Gran Tucumán – Tafí Viejo")
        s = serie("LI - Hog", "61.1_G_0_0_3", "x",
                  "Hogares con ingresos debajo de línea de indigencia (%) de 2001 a 2003. GBA", "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Incidencia de la indigencia · Hogares · Gran Buenos Aires")

    def test_cuadros_rotulados_puntual_van_a_series_historicas(self):
        s = serie("LP - Hog", "63.1_C_0_0_4", "x",
                  "Hogares con ingresos debajo de línea de pobreza (%) de 2001 a 2003. Ciudad de Buenos Aires. EPH puntual.",
                  "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(
            TRABAJO, POBREZA, "Incidencia de la pobreza · Hogares · Ciudad Autónoma de Buenos Aires",
            "Series históricas", unidad=PROP))

    def test_eph_puntual_gba(self):
        s = serie("EPH Puntual - PeI", "60.1_PP_0_0_15", "pobres_personas",
                  "Población con ingresos debajo de línea de pobreza (%) desde 1988 hasta 2003.", "S", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, POBREZA, "Incidencia de la pobreza · Personas · Gran Buenos Aires",
                                                 "Series históricas", unidad=PROP))


class IngresoTest(unittest.TestCase):
    def test_gini_y_brecha(self):
        s = serie("DP", "65.1_CGI_0_0_21", "coeficiente_gini_ipcf", "Coeficiente de Gini del Ingreso per cápita familiar",
                  "T", "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Distribución del ingreso",
                                                 "Coeficiente de Gini · Ingreso per cápita familiar",
                                                 "Ingreso per cápita familiar", unidad="Coeficiente (0 a 1)"))
        s = serie("DP", "65.1_BII_0_0_19", "brecha_ingresos_iop", "Brecha del Ingreso de la ocupación principal", "T",
                  "CERRADA")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Distribución del ingreso",
                                                 "Brecha de ingresos P90/P10 · Ingreso de la ocupación principal",
                                                 "Ingreso de la ocupación principal", unidad="Cociente P90/P10"))

    def test_deciles(self):
        s = serie("DP Deciles", "301.1_DECIL_10_IPCF_0_T_13", "decil_10_ipcf", "Decil 10", "T")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Distribución del ingreso",
                                                 "Ingreso per cápita familiar · Decil 10",
                                                 "Ingreso per cápita familiar", unidad="Pesos corrientes"))
        s = serie("DP Deciles", "301.1_DECIL_1_OIOIP_0_T_11", "decil_1_oip", "Decil 1", "T")
        self.assertEqual(clasificar(s).nombre, "Ingreso de la ocupación principal · Decil 1")
        s = serie("DP Deciles", "301.1_TOTAL_OIPOIP_0_T_9", "total_oip", "Total decil", "T")
        self.assertEqual(clasificar(s).nombre, "Ingreso de la ocupación principal · Total")


class PoliticaDeIngresosTest(unittest.TestCase):
    def test_salario_minimo(self):
        for variable, nombre in (("salario_minimo_vital_movil_mensual", "Salario mínimo, vital y móvil · Por mes"),
                                 ("salario_minimo_vital_movil_diario", "Salario mínimo, vital y móvil · Por día"),
                                 ("salario_minimo_vital_movil_hora", "Salario mínimo, vital y móvil · Por hora")):
            with self.subTest(variable=variable):
                r = clasificar(serie("SMVM", "57.1_X_0_M_1", variable, "Salario mínimo", "M"))
                self.assertEqual(r, esperado(TRABAJO, "Salario mínimo y haberes", nombre, unidad="Pesos corrientes"))

    def test_haber_minimo(self):
        s = serie("HaberMin", "58.1_MP_0_M_24", "mensual_pesos_corrientes", "Haber mínimo jubilatorio", "M")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Salario mínimo y haberes", "Haber mínimo jubilatorio",
                                                 unidad="Pesos corrientes"))

    def test_auh(self):
        s = serie("AUH", "165.1_AAUH_0_0_3", "auh", "Asignación Universal por Hijo, en pesos corrientes", "M")
        self.assertEqual(clasificar(s), esperado(TRABAJO, "Asignación Universal por Hijo",
                                                 "Asignación Universal por Hijo", unidad="Pesos corrientes"))


class CanastasTest(unittest.TestCase):
    def test_historicas_gba(self):
        s = serie("CBA y CBT", "59.4_CSTA_BATAL_0_0_20", "canasta_basica_total", "Canasta básica total", "M", "CERRADA")
        self.assertEqual(clasificar(s), esperado("Precios", "Canastas básicas", "Canasta básica total · Adulto equivalente",
                                                 "Series históricas", unidad="Pesos corrientes"))
        s = serie("CBA y CBT", "59.3_LA_POBREZA_0_0_13", "linea_pobreza", "Línea de pobreza", "S", "CERRADA")
        self.assertEqual(clasificar(s).nombre, "Línea de pobreza · Hogar de referencia")
        s = serie("CBA y CBT", "59.4_IRSA_COGEL_0_0_25", "inversa_coeficiente_engel", "Índice de Coeficiente de Engel",
                  "M", "CERRADA")
        self.assertEqual(clasificar(s).unidad, "Cociente CBT/CBA")

    def test_ciudad_de_buenos_aires(self):
        s = serie("CBA y CBT BA", "444.1_CANASTA_BARIA_0_0_26_47", "canasta_basica_alimentaria",
                  "Canasta Básica Alimentaria", "M")
        self.assertEqual(clasificar(s), esperado(
            "Precios", "Canastas básicas", "Canasta básica alimentaria · Adulto equivalente · Ciudad Autónoma de Buenos Aires",
            "Ciudad Autónoma de Buenos Aires", unidad="Pesos corrientes"))

    def test_regiones(self):
        s = serie("CBA y CBT Reg", "444.1_CANASTA_batotGBA_0_0_26_47", "gran_buenos_aires",
                  "Canasta Básica Total. Gran Buenos Aires", "M")
        self.assertEqual(clasificar(s), esperado("Precios", "Canastas básicas",
                                                 "Canasta básica total · Adulto equivalente · GBA", "GBA",
                                                 unidad="Pesos corrientes"))
        s = serie("CBA y CBT Reg", "444.1_CANASTA_BARIAPampeana_0_0_26_47", "pampeana",
                  "Canasta Básica Alimentaria. Pampeana", "M")
        self.assertEqual(clasificar(s).nombre, "Canasta básica alimentaria · Adulto equivalente · Pampeana")

    def test_la_hoja_2016_no_es_de_este_modulo(self):
        s = serie("CBA y CBT 2016", "x", "canasta_basica_alimentaria", "Canasta básica alimentaria desde 2016", "M")
        self.assertIsNone(clasificar(s))


class AlcanceTest(unittest.TestCase):
    def test_fuera_de_alcance(self):
        self.assertIsNone(clasificar(serie("TA 03-", "x", "x", "Tasa de actividad total.", fuente="otra-fuente")))
        self.assertIsNone(clasificar(serie("TA 03-", "x", "x", "Tasa de actividad total.", archivo="otro.xlsx")))
        self.assertIsNone(clasificar(serie("CGI Remun", "x", "x", "Remuneraciones")))

    def test_es_determinista(self):
        s = serie("TD 03-", "45.1_ECTDTGC_0_A_46", "x", "Tasa de desempleo total Gran Cordoba. En porcentaje.")
        self.assertEqual(clasificar(s), clasificar(s))

    def test_cada_descripcion_de_aglomerado_de_1974_2003_da_un_nombre_distinto(self):
        descripciones = [
            "Buenos Aires - Santa Fe San Nicolás - Villa Constitución", "Chubut Rawson - Trelew",
            "Ciudad de Buenos Aires", "Gran Buenos Aires", "Partidos a partir del 98", "Partidos del Conurbano",
            "Provincia Jujuy San Salvador de Jujuy - Palpalá", "Provincia Entre Ríos Gran Paraná",
            "Provincia Buenos Aires Gran La Plata", "Provincia Chaco Gran Resistencia", "Provincia Misiones Posadas",
            "Provincia Entre Ríos Concordia", "Provincia Tucumán Gran San Martín de Tucumán - Tafí Viejo",
            "Provincia Santa Fe Gran Santa Fe", "Provincia Santa Cruz Rio Gallegos",
            "Provincia Neuquén Nuequén - Plottier", "Provincia San Luis San Luis - El Chorrillo",
            "Provincia Chubut Comodoro Rivadavia - Rada Tilly", "Provincia Salta Salta",
            "Provincia Santiago del Estero Santiago del Estero - La Banda",
            "Provincia Tierra del Fuego Ushuaia - Río Grande", "Provincia La Pampa Santa Rosa - Toay",
            "Provincia Córdoba Gran Córdoba", "Provincia La Rioja La Rioja", "Provincia Formosa Formosa",
            "Provincia Mendoza Gran Mendoza", "Provincia Catamarca Gran Catamarca", "Provincia Santa Fe Gran Rosario",
            "Provincia Buenos Aires Bahía Blanca - Cerri", "Provincia Córdoba Río Cuarto",
            "Provincia Buenos Aires Mar del Plata y Batán", "Provincia Corrientes Corrientes",
            "Provincia San Juan Gran San Juan", "Río Negro - Buenos Aires Viedma - Carmen de Patagones",
            "Total aglomerados del Interior", "Total aglomerados urbanos",
        ]
        nombres = [_cobertura(d) for d in descripciones]
        self.assertNotIn(None, nombres)
        self.assertEqual(len(set(nombres)), len(descripciones))

    def test_la_tabla_de_aglomerados_tiene_34_entradas(self):
        # 30 aglomerados del interior + Ciudad Autónoma + Partidos del GBA + Gran Buenos Aires + conurbano histórico.
        self.assertEqual(len(_AGLOMERADOS), 34)


if __name__ == "__main__":
    unittest.main()
