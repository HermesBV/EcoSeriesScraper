"""Pruebas de las reglas de comercio exterior (``tools/taxonomia/externo_comercio.py``)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia import precios_otros
from tools.taxonomia.externo_comercio import clasificar

EXTERNO = "Sector externo"
INTERCAMBIO = "Intercambio comercial argentino"
INDICES = "Índices de precios y cantidades del comercio exterior"
MM = "Millones de dólares"

TITULO_ICA = "Balanza comercial argentina, total y variaciones porcentuales. Datos mensuales, desde 1990 en adelante"


def serie(hoja, id, variable="", descripcion="", frecuencia="M", archivo="sector_externo.xlsx",
          fuente="datos.gob.ar", nombre="", columna=""):
    return Serie(fuente=fuente, id=id, nombre=nombre, variable=variable, descripcion=descripcion,
                 unidad="", frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=columna,
                 dataset="", estado="VIGENTE")


def indec_ica(rotulo):
    return serie("FOB-CIF", "3672", fuente="indec-ica", archivo="balanmensual.xls",
                 nombre=f"{TITULO_ICA} - {rotulo}", columna=rotulo, descripcion=TITULO_ICA)


def c(indicador, nombre, cobertura="", apertura="", medida="", unidad="", tema=EXTERNO):
    return Clasificacion(tema, indicador, nombre, cobertura=cobertura, apertura=apertura, medida=medida,
                         unidad=unidad)


class IndecIca(unittest.TestCase):
    def test_exportaciones_mensuales(self):
        self.assertEqual(
            clasificar(indec_ica("Exportaciones - Total mensual - Millones de dólares")),
            c(INTERCAMBIO, "Exportaciones", "Exportaciones", "Total", unidad=MM))

    def test_variacion_mensual_es_interanual(self):
        self.assertEqual(
            clasificar(indec_ica("Exportaciones - Var.% mensual - Millones de dólares")),
            c(INTERCAMBIO, "Exportaciones (var. % interanual)", "Exportaciones", "Total", unidad="%"))

    def test_importaciones_acumuladas(self):
        self.assertEqual(
            clasificar(indec_ica("Importaciones - Total acumulado - Millones de dólares")),
            c(INTERCAMBIO, "Importaciones (acumulado del año)", "Importaciones", "Total", unidad=MM))

    def test_saldo_rotulado_como_variacion_es_el_saldo_mensual(self):
        self.assertEqual(
            clasificar(indec_ica("Saldo - Var.% acumulada - Millones de dólares")),
            c(INTERCAMBIO, "Saldo comercial", "Saldo comercial", "Total", unidad=MM))


class IntercambioComercial(unittest.TestCase):
    def test_total_de_la_hoja_1_comparte_nombre_con_indec_ica(self):
        resultado = clasificar(serie("1. ICA", "74.3_IET_0_M_16", "ica_expo_totales"))
        self.assertEqual(resultado, c(INTERCAMBIO, "Exportaciones", "Exportaciones", "Total", unidad=MM))
        self.assertEqual(resultado.nombre,
                         clasificar(indec_ica("Exportaciones - Total mensual - Millones de dólares")).nombre)

    def test_gran_rubro_de_las_hojas_1_y_2_coincide(self):
        esperado = c(INTERCAMBIO, "Exportaciones · Manufacturas de origen agropecuario", "Exportaciones",
                     "Por grandes rubros", unidad=MM)
        self.assertEqual(clasificar(serie("1. ICA", "74.1_IEMOA_0_A_48", "ica_exportacion_manufacturas_origen_agropecuario",
                                         frecuencia="A")), esperado)
        self.assertEqual(clasificar(serie("2. X Rubro", "75.3_ITMOA_0_M_13", "ica_total_moa")), esperado)

    def test_suma_de_usos(self):
        self.assertEqual(
            clasificar(serie("1. ICA", "74.2_IBCPP_0_T_32", "ica_bienes_capital_partes_piezas", frecuencia="T")),
            c(INTERCAMBIO, "Importaciones · Bienes de capital, piezas y accesorios", "Importaciones",
              "Por grupos de usos económicos", unidad=MM))

    def test_saldo(self):
        self.assertEqual(clasificar(serie("1. ICA", "74.2_SC_0_T_15", "saldo_comercial", frecuencia="T")),
                         c(INTERCAMBIO, "Saldo comercial", "Saldo comercial", "Total", unidad=MM))
        self.assertEqual(clasificar(serie("1. ICA", "74.3_ISC_0_M_19", "ica_saldo_comercial")),
                         c(INTERCAMBIO, "Saldo comercial", "Saldo comercial", "Total", unidad=MM))

    def test_rubro_con_su_gran_rubro_como_quinto_nivel(self):
        self.assertEqual(
            clasificar(serie("2. X Rubro", "75.1_IEC_0_A_26", "ica_exportaciones_cereales", frecuencia="A")),
            c(INTERCAMBIO, "Exportaciones · Cereales", "Exportaciones", "Por rubro",
              "Productos primarios", MM))

    def test_rubros_de_nombre_repetido_en_el_id_y_variable_distinta_por_frecuencia(self):
        # En la hoja 2 los códigos del ID cambian con la frecuencia: se reconoce por la variable.
        self.assertEqual(
            clasificar(serie("2. X Rubro", "75.3_IMCM_0_M_36", "ica_manufacturas_cuero_marroquineria")),
            c(INTERCAMBIO, "Exportaciones · Manufacturas de cuero y marroquinería", "Exportaciones",
              "Por rubro", "Manufacturas de origen industrial", MM))
        self.assertEqual(
            clasificar(serie("2. X Rubro", "75.3_IRM_0_M_13", "ica_resto_moi")),
            c(INTERCAMBIO, "Exportaciones · Resto de manufacturas de origen industrial", "Exportaciones",
              "Por rubro", "Manufacturas de origen industrial", MM))
        self.assertEqual(
            clasificar(serie("2. X Rubro", "75.1_RM_0_A_9", "resto_moi", frecuencia="A")).nombre,
            "Exportaciones · Resto de manufacturas de origen industrial")

    def test_importaciones_por_uso_en_las_hojas_1_y_3(self):
        esperado = c(INTERCAMBIO, "Importaciones · Vehículos automotores de pasajeros", "Importaciones",
                     "Por uso económico", unidad=MM)
        self.assertEqual(clasificar(serie("3. M usos", "76.3_IVAP_0_M_35", "ica_vehiculos_automotores_pasajeros")),
                         esperado)
        self.assertEqual(clasificar(serie("1. ICA", "74.3_IIVAP_0_M_49", "ica_importaciones_vehiculos_automotores_pasajeros")),
                         esperado)

    def test_total_de_importaciones(self):
        self.assertEqual(clasificar(serie("3. M usos", "76.1_ITG_0_A_17", "ica_total_general", frecuencia="A")),
                         c(INTERCAMBIO, "Importaciones", "Importaciones", "Total", unidad=MM))


class IntensidadTecnologica(unittest.TestCase):
    def test_lall_importaciones_alta_tecnologia(self):
        self.assertEqual(
            clasificar(serie("19. X y M Intensidad tecnol.", "181.1_LALL_IMPO_CAS_0_A_52",
                             "lall_impo_manufct_alta_tecno_electronicas_electricas", frecuencia="A")),
            c(INTERCAMBIO,
              "Importaciones · Clasificación de Lall · Manufacturas de alta tecnología · Electrónicas y eléctricas",
              "Importaciones", "Por intensidad tecnológica", "Clasificación de Lall", MM))

    def test_unctad_exportaciones_con_rotulo_incompleto_en_la_fuente(self):
        # La fuente no dice «alta especialización»; se ubica por la suma de los hijos.
        self.assertEqual(
            clasificar(serie("19. X y M Intensidad tecnol.", "181.1_UNCTAD_X_PTR_0_A_59",
                             "unctad_x_prod_manu_partes_compon_para_bienes_elects_electr", frecuencia="A")),
            c(INTERCAMBIO,
              "Exportaciones · Clasificación de la UNCTAD · Alta especialización · Partes y componentes eléctricos y electrónicos",
              "Exportaciones", "Por intensidad tecnológica", "Clasificación de la UNCTAD", MM))

    def test_totales_de_cada_clasificacion_no_se_confunden(self):
        lall = clasificar(serie("19. X y M Intensidad tecnol.", "181.1_LALL_TOTALNES_0_A_24",
                                "lall_total_exportaciones", frecuencia="A"))
        unctad = clasificar(serie("19. X y M Intensidad tecnol.", "181.1_UNCTAD_TOTNES_0_A_26",
                                  "unctad_total_exportaciones", frecuencia="A"))
        self.assertEqual(lall.nombre, "Exportaciones · Clasificación de Lall · Total")
        self.assertEqual(unctad.nombre, "Exportaciones · Clasificación de la UNCTAD · Total")


class IndicesDePreciosYCantidades(unittest.TestCase):
    def test_indice_de_precios_de_un_gran_rubro(self):
        self.assertEqual(
            clasificar(serie("10. X pyq", "80.1_IIPNI_0_A_33", "ica_indice_precio_nivel_industria",
                             "Índice de precio de exportaciones. Industria", "A")),
            c(INDICES, "Índice de precios de exportación · Manufacturas de origen industrial",
              cobertura="Por grandes rubros", unidad="Índice 2004=100"))

    def test_nivel_general_de_valor_va_sin_cobertura(self):
        self.assertEqual(
            clasificar(serie("10. X pyq", "80.2_IIVENG_0_T_44", "ica_indice_valor_exportaciones_nivel_general",
                             "Índice de valor de exportaciones. Nivel general", "T")),
            c(INDICES, "Índice de valor de exportación", unidad="Índice 2004=100"))

    def test_importaciones_por_uso(self):
        self.assertEqual(
            clasificar(serie("11. M pyq", "81.2_IICICL_0_T_58", "ica_indice_cantidad_importaciones_combustibles_lubricantes",
                             "Índice de cantidad de importaciones. Combustibles y lubricantes", "T")),
            c(INDICES, "Índice de cantidad de importación · Combustibles y lubricantes",
              cobertura="Por uso económico", unidad="Índice 2004=100"))

    def test_hoja_4_9_1_comparte_nombre_con_la_hoja_10(self):
        general = clasificar(serie("4.9.1", "117.1_ENGC_0_A_36", "Índice de cantidad de las exportaciones de bienes. 2004=100",
                                   frecuencia="A", archivo="precios.xlsx"))
        hoja_10 = clasificar(serie("10. X pyq", "80.1_IICNG_0_A_33", "ica_indice_cantidad_nivel_general",
                                   "Índice de cantidad de exportaciones. Nivel general", "A"))
        self.assertEqual(general.nombre, "Índice de cantidad de exportación")
        self.assertEqual((general.nombre, general.indicador), (hoja_10.nombre, hoja_10.indicador))

    def test_variaciones_trimestrales_de_la_hoja_12_se_leen_de_los_datos(self):
        # Las descripciones de la fuente están cruzadas: VPP1 es la trimestral y VPP2 la interanual.
        self.assertEqual(
            clasificar(serie("12. TdI", "82.2_VPP1_2004_T_23", "var_porcentaje_precio_1",
                             "Variación anual del índice de precios de importación en porcentaje", "T")),
            c(INDICES, "Índice de precios de importación (var. % trimestral)", unidad="%"))
        self.assertEqual(
            clasificar(serie("12. TdI", "82.2_VPP2_2004_T_23", "var_porcentaje_precio_2",
                             "Variación trimestral del índice de precios de importación en porcentaje", "T")),
            c(INDICES, "Índice de precios de importación (var. % interanual)", unidad="%"))
        self.assertEqual(
            clasificar(serie("12. TdI", "82.2_VAP_2004_T_20", "var_anual_porcentaje",
                             "Variación trimestral del índice de precios de exportación en porcentaje", "T")).nombre,
            "Índice de precios de exportación (var. % interanual)")

    def test_variacion_anual_de_terminos_del_intercambio(self):
        self.assertEqual(
            clasificar(serie("12. TdI", "82.1_VPTI1_2004_A_37", "var_porcentaje_terminos_intercambio_1",
                             "Variacion anual del índice de terminos del intercambio", "A")),
            c(INDICES, "Términos del intercambio (var. % interanual)", unidad="%"))

    def test_poder_de_compra_y_variacion_de_la_ganancia(self):
        self.assertEqual(
            clasificar(serie("13. Poder de Compra X", "83.1_PCEMD_2004_A_43", "poder_compra_exportaciones_millones_dolares",
                             frecuencia="A")),
            c(INDICES, "Poder de compra de las exportaciones", unidad="Millones de dólares de 2004"))
        self.assertEqual(
            clasificar(serie("13. Poder de Compra X", "83.1_VPA_2004_A_26", "variacion_periodo_anterior",
                             frecuencia="A")).nombre,
            "Ganancia o pérdida del intercambio (variación interanual)")

    def test_base_1993_va_a_series_historicas_con_el_mismo_nombre(self):
        self.assertEqual(
            clasificar(serie("4.9.4", "120.1_PCE_1993_0_24", "poder_compra_exportacion", frecuencia="A",
                             archivo="precios.xlsx")),
            c(INDICES, "Poder de compra de las exportaciones", cobertura="Series históricas",
              apertura="Base 1993", unidad="Millones de dólares de 1993"))

    def test_mismo_indicador_y_forma_de_nombre_que_la_hoja_4_9_3(self):
        otra = precios_otros.clasificar(serie("4.9.3", "119.1_IPE_0_A_26", "Índice de precios de exportación. 2004=100",
                                              frecuencia="A", archivo="precios.xlsx"))
        mia = clasificar(serie("12. TdI", "82.1_IPE_2004_A_26", "indice_precios_exportacion", frecuencia="A"))
        self.assertEqual((otra.tema, otra.indicador, otra.nombre), (mia.tema, mia.indicador, mia.nombre))
        self.assertEqual(clasificar(serie("4.9.3", "119.1_IPE_0_A_26", archivo="precios.xlsx", frecuencia="A")), None)


class Turismo(unittest.TestCase):
    def test_total_receptivo_mensual(self):
        self.assertEqual(
            clasificar(serie("17. ETI", "322.3_TURISMO_REIVO__17", "turismo_receptivo")),
            c("Turismo internacional", "Turismo internacional · Receptivo", "Receptivo", "Total",
              unidad="Miles de turistas"))

    def test_emisivo_por_pais_solo_trimestral(self):
        self.assertEqual(
            clasificar(serie("17. ETI", "322.2_TURISMO_EMUAY__24_40", "turismo_emisivo_paraguay", frecuencia="T")),
            c("Turismo internacional", "Turismo internacional · Emisivo · Paraguay", "Emisivo", "Por país o región",
              unidad="Miles de turistas"))

    def test_total_de_regiones_no_se_confunde_con_el_total(self):
        self.assertEqual(
            clasificar(serie("17. ETI", "322.1_TURISMO_RETAL__23_97", "turismo_receptivo_total", frecuencia="A")).nombre,
            "Turismo internacional · Receptivo · Total de países y regiones")

    def test_saldo(self):
        self.assertEqual(
            clasificar(serie("17. ETI", "322.3_SALDOLDO__5", "saldo")),
            c("Turismo internacional", "Turismo internacional · Saldo", "Saldo", unidad="Miles de turistas"))


class Granos(unittest.TestCase):
    def test_embarques_de_soja(self):
        self.assertEqual(
            clasificar(serie("34. Embarques", "369.3_POROTO_SOJOJA__11", "poroto_soja")),
            c("Complejos exportadores de oleaginosas y cereales", "Embarques · Poroto de soja", "Embarques",
              unidad="Millones de toneladas"))

    def test_total_de_embarques(self):
        self.assertEqual(clasificar(serie("34. Embarques", "369.1_TOTALTAL__5", "total", frecuencia="A")).nombre,
                         "Embarques")

    def test_liquidacion_promedio_diario(self):
        self.assertEqual(
            clasificar(serie("35. Liquidaciones OyC CIARA-CEC", "349.2_PROMEDIO_DIO__16", "promedio_diario")),
            c("Complejos exportadores de oleaginosas y cereales", "Liquidación de divisas (promedio diario)",
              "Liquidación de divisas", unidad=MM))


class Precios(unittest.TestCase):
    def test_ipmp_vigente_y_serie_historica(self):
        self.assertEqual(
            clasificar(serie("18. IPMP", "88.3_IPMPI_1995_M_35", "indice_precios_materias_primas_ipmp")),
            c("IPMP", "IPMP", unidad="Índice 2001=100", tema="Precios"))
        self.assertEqual(
            clasificar(serie("18. IPMP", "88.3_IPMPP_1995_M_36", "indice_precios_materias_primas_pesos")),
            c("IPMP", "IPMP (en pesos)", "Series históricas", "Base 1995", unidad="Índice 1995=100", tema="Precios"))

    def test_fob_oficial_cebada_no_cervecera(self):
        self.assertEqual(
            clasificar(serie("38. Precios FOB oficiales", "358.1_CEBADA_CERADO__52", "cebada_cervecera_granel_hasta_15_porciento_embolsado",
                             "Cebada, (no) Cervecera, A granel con hasta un 15 % embolsado", "D")),
            c("Precios FOB oficiales", "Precio FOB oficial · Cebada · No cervecera · A granel, hasta 15 % embolsado",
              "Cereales y derivados", unidad="US$ por tonelada", tema="Precios"))

    def test_fob_oficial_arroz_con_rotulos_parecidos_no_colapsa(self):
        nombres = {
            clasificar(serie("38. Precios FOB oficiales", id_, variable)).nombre
            for id_, variable in (
                ("358.1_ARR_SB_B_PORC_II__59", "arroz_pulido_glasaseado_cont_gra_quebr_men_igual_10porc"),
                ("358.1_ARR_SB_B_PORC__59", "arroz_no_parbolizado_cont_grano_quebrado_men_igual_10porc"),
                ("358.1_ARR_SB_B_PORC_IV__53", "arroz_pulido_glaseado_contenido_grano_quebrado_may_10porc"),
                ("358.1_ARR_SB_B_PORC__53", "arroz_puli_glas_no_parbo_cont_grano_quebrado_may_10porc"))}
        self.assertEqual(len(nombres), 4)

    def test_fob_oficial_algodon(self):
        self.assertEqual(
            clasificar(serie("38. Precios FOB oficiales", "358.1_S_CARDAR_P_32__49",
                             "s_cardar_peinar_desmotado_grado_long_26_mm_1_1_32")).nombre,
            "Precio FOB oficial · Algodón desmotado · Grado E, largo 26,2 mm")


class EmpresasExportadoras(unittest.TestCase):
    def test_rango_con_rotulo_corrido_en_la_hoja_39(self):
        # Rotulado «Entre USD 5 M y USD 20 M», pero son las empresas de 20 a 100 M (222 en 2018-08).
        self.assertEqual(
            clasificar(serie("39. Empresas exportadoras", "360.1_DETALLE_PO0_M__48",
                             "detalle_por_rangos_de_exp_entre_usd_5_m_usd_20_m",
                             "Detalle por rangos de exportación Entre USD 5 M y USD 20 M")),
            c("Empresas exportadoras", "Empresas exportadoras · Con exportaciones entre USD 20 M y USD 100 M",
              "Por rango de exportación", unidad="Empresas"))

    def test_rango_de_la_hoja_40(self):
        self.assertEqual(
            clasificar(serie("40. Exportaciones por empresas", "361.1_RANGOS_EXP0_M_IIE__39",
                             "rangos_exp_m_usd_exportan_may_usd_100_m",
                             "Detalle por rangos de exportación, en M de USD Exportan > USD 100 M")),
            c("Empresas exportadoras",
              "Empresas exportadoras · Con exportaciones entre USD 100 M y USD 500 M (acumulado 12 meses)",
              "Por rango de exportación", unidad=MM))

    def test_sector_y_condicion(self):
        self.assertEqual(
            clasificar(serie("39. Empresas exportadoras", "360.1_DETALLE_PODAS__57",
                             "detalle_por_sector_agric_ganaderia_caza_silv_equilibradas",
                             "Detalle por sector de actividad y condición A. Agric., ganadería, caza y silv. Equilibradas")),
            c("Empresas exportadoras",
              "Empresas exportadoras · Agricultura, ganadería, caza y silvicultura · Con saldo equilibrado",
              "Por sector y saldo comercial", unidad="Empresas"))

    def test_sector_y_condicion_con_variable_abreviada_en_la_hoja_40(self):
        self.assertEqual(
            clasificar(serie("40. Exportaciones por empresas", "361.1_SECTOR_ACTDEF__53",
                             "sector_activ_saldo_g_comercio_mayorista_minorista_def",
                             "Detalle por sector de actividad y saldo comercial G. Comercio mayorista y minorista Deficitarias")).nombre,
            "Empresas exportadoras · Comercio al por mayor y al por menor · Con saldo deficitario (acumulado 12 meses)")

    def test_total(self):
        self.assertEqual(
            clasificar(serie("39. Empresas exportadoras", "360.1_TOTAL_EMPRRAS__27", "total_empresas_exportadoras",
                             "Total empresas exportadoras")),
            c("Empresas exportadoras", "Empresas exportadoras", "Total", unidad="Empresas"))


class IndicesDeDifusion(unittest.TestCase):
    DIFUSION = "Índices de difusión de las exportaciones"

    def hoja_41(self, descripcion):
        return serie("41. Indices de difusión", "362.1_X__1", descripcion=descripcion)

    def test_indice_por_rango(self):
        self.assertEqual(
            clasificar(self.hoja_41("Índice de difusión por rango de exportación Mayor a 500M")),
            c(self.DIFUSION, "Índice de difusión · Empresas · Con exportaciones de más de USD 500 M", "Empresas",
              "Por rango de exportación", unidad="Proporción"))

    def test_conteo_por_sector_con_rotulo_de_ia(self):
        self.assertEqual(
            clasificar(self.hoja_41("Índice de difusión por Sector de actividad A. Agric, ganadería, caza y silv. Crecimiento i.a.")),
            c(self.DIFUSION,
              "Índice de difusión · Empresas · Agricultura, ganadería, caza y silvicultura (cantidad con crecimiento interanual)",
              "Empresas", "Por sector de actividad", unidad="Empresas"))

    def test_total_por_rango_es_la_cantidad_de_empresas_de_la_hoja_39(self):
        self.assertEqual(
            clasificar(self.hoja_41("Índice de difusión por Sector de actividad Entre USD 5M y USD 20M Total")),
            c("Empresas exportadoras", "Empresas exportadoras · Con exportaciones entre USD 5 M y USD 20 M",
              "Por rango de exportación", unidad="Empresas"))

    def test_total_de_un_sector_con_sufijo_propio(self):
        self.assertEqual(
            clasificar(self.hoja_41("Índice de difusión por Sector de actividad Resto Total Resto")).nombre,
            "Empresas exportadoras · Resto")

    def test_indice_del_sector_resto_no_se_confunde_con_su_total(self):
        self.assertEqual(
            clasificar(self.hoja_41("Índice de difusión por Sector de actividad Resto")).nombre,
            "Índice de difusión · Empresas · Resto")

    def test_total_general_y_empresas_exportadoras(self):
        esperado = c("Empresas exportadoras", "Empresas exportadoras", "Total", unidad="Empresas")
        self.assertEqual(clasificar(self.hoja_41("Índice de difusión por Sector de actividad Total general")), esperado)
        self.assertEqual(clasificar(self.hoja_41("Cantidad de empresas con: Empresas exportadoras")), esperado)

    def test_destinos(self):
        self.assertEqual(
            clasificar(self.hoja_41("Cantidad de destinos con: Total de destinos")),
            c(self.DIFUSION, "Índice de difusión · Destinos (cantidad total)", "Destinos", unidad="Destinos"))
        self.assertEqual(
            clasificar(self.hoja_41("Indice de difusión por destino")),
            c(self.DIFUSION, "Índice de difusión · Destinos", "Destinos", unidad="Proporción"))

    def test_productos_por_nomenclatura(self):
        self.assertEqual(
            clasificar(self.hoja_41("Indice de difusión por producto\n(NCM 8 dígitos)".replace("\n", " "))),
            c(self.DIFUSION, "Índice de difusión · Productos a 8 dígitos del NCM", "Productos", "NCM de 8 dígitos",
              unidad="Proporción"))
        self.assertEqual(
            clasificar(self.hoja_41("Cantidad de productos a 4 dígitos del NCM con: Sin variación")),
            c(self.DIFUSION, "Índice de difusión · Productos a 4 dígitos del NCM (cantidad sin variación)", "Productos",
              "NCM de 4 dígitos", unidad="Productos"))

    def test_empresas_estables(self):
        self.assertEqual(
            clasificar(self.hoja_41("Cantidad de empresas con: Estable")),
            c(self.DIFUSION, "Índice de difusión · Empresas (cantidad sin variación)", "Empresas", "Total",
              unidad="Empresas"))


class Alcance(unittest.TestCase):
    def test_fuera_de_alcance_devuelve_none(self):
        self.assertIsNone(clasificar(serie("4. X NCM", "74.1_X_0_A_1", "x_ncm")))
        self.assertIsNone(clasificar(serie("14. Bce Pagos 3-17", "1.1_X_0_A_1", "x")))
        self.assertIsNone(clasificar(serie("1. ICA", "74.1_IET_0_A_16", fuente="otra", archivo="sector_externo.xlsx")))
        self.assertIsNone(clasificar(serie("1. ICA", "74.1_IET_0_A_16", archivo="otro.xlsx")))
        self.assertIsNone(clasificar(serie("FOB-CIF", "3672", fuente="indec-ica", archivo="balanmensual.xls",
                                           nombre="Otra cosa - Total mensual")))

    def test_dos_llamadas_dan_lo_mismo(self):
        s = serie("2. X Rubro", "75.1_IEC_0_A_26", "ica_exportaciones_cereales", frecuencia="A")
        self.assertEqual(clasificar(s), clasificar(s))


if __name__ == "__main__":
    unittest.main()
