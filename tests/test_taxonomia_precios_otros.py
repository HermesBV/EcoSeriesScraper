import unittest
from collections import Counter
from pathlib import Path

from tools.taxonomia import Clasificacion, Serie, clasificar as despachar
from tools.taxonomia import precios_otros

CATALOGO = "Índice de Precios Internos al por Mayor (IPIM), base año 1993, periodo de referencia diciembre 2015=100"


def serie(fuente="datos.gob.ar", archivo="precios.xlsx", hoja="", id="", variable="", descripcion="",
          frecuencia="M", columna="", nombre="", estado="VIGENTE", unidad=""):
    """Serie con los valores que publica el inventario; lo que no importa a la regla queda vacío."""
    return Serie(fuente=fuente, id=id, nombre=nombre, variable=variable, descripcion=descripcion,
                 unidad=unidad, frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=columna,
                 dataset="", estado=estado)


def sipm(hoja, codigo, descripcion):
    return serie(fuente="indec-sipm", archivo="series_sipm_dic2015.xls", hoja=hoja, id=f"{hoja}|{codigo}",
                 variable=codigo, columna=f"{codigo} - {descripcion}", nombre=f"{hoja}: {descripcion}")


def esperado(tema, indicador, nombre, cobertura="", apertura="", medida="", unidad=""):
    return Clasificacion(tema, indicador, nombre, cobertura, apertura, medida, unidad)


class PreciosMayoristasVigentesTests(unittest.TestCase):
    def test_nivel_general_y_agregados(self):
        self.assertEqual(
            precios_otros.clasificar(sipm("IPIM", "NG", "Nivel general")),
            esperado("Precios", "Precios mayoristas", "IPIM · Nivel general", "IPIM",
                     "Nivel general y agregados", unidad="Índice dic-2015=100"))
        self.assertEqual(
            precios_otros.clasificar(sipm("IPP", "27", "Productos metálicos básicos")),
            esperado("Precios", "Precios mayoristas", "IPP · Productos metálicos básicos", "IPP",
                     "Minerales no metálicos y metales", unidad="Índice dic-2015=100"))

    def test_nombres_oficiales_en_oracion(self):
        resultado = precios_otros.clasificar(sipm("IPIM", "2", "Industria Manufacturera y Energía Eléctrica"))
        self.assertEqual(resultado.nombre, "IPIM · Industria manufacturera y energía eléctrica")
        self.assertEqual(resultado.apertura, "Nivel general y agregados")

    def test_primarios_por_codigo(self):
        resultado = precios_otros.clasificar(sipm("IPIM", "0111", "Cereales y oleaginosas"))
        self.assertEqual(resultado.nombre, "IPIM · Cereales y oleaginosas")
        self.assertEqual(resultado.apertura, "Productos primarios")

    def test_ncep_se_escribe_completo(self):
        resultado = precios_otros.clasificar(sipm("IPIB", "1549", "Otros productos alimenticios n.c.e.p."))
        self.assertEqual(resultado.nombre, "IPIB · Otros productos alimenticios no clasificados en otra parte")
        self.assertEqual(resultado.apertura, "Alimentos, bebidas y tabaco")

    def test_nombres_repetidos_por_el_indec_se_distinguen(self):
        grupo = precios_otros.clasificar(sipm("IPIM", "241", "Sustancias químicas básicas"))
        clase = precios_otros.clasificar(sipm("IPIM", "2411", "Sustancias químicas básicas"))
        self.assertEqual(grupo.nombre, "IPIM · Sustancias químicas básicas, abonos y plásticos")
        self.assertEqual(clase.nombre, "IPIM · Sustancias químicas básicas")
        self.assertEqual(
            precios_otros.clasificar(sipm("IPIM", "2429", "Otros productos químicos")).nombre,
            "IPIM · Otros productos químicos no clasificados en otra parte")
        self.assertEqual(
            precios_otros.clasificar(sipm("IPIM", "3599", "Otros medios de transporte")).nombre,
            "IPIM · Otros medios de transporte no clasificados en otra parte")

    def test_codigo_desconocido_no_se_clasifica(self):
        self.assertIsNone(precios_otros.clasificar(sipm("IPIM", "99", "Algo nuevo")))
        self.assertIsNone(precios_otros.clasificar(sipm("IPC", "NG", "Nivel general")))

    def test_nivel_general_de_las_tablas_de_cuatro_digitos(self):
        casos = (
            ("4.3.1 IPIM 4 dígitos", "448.1_NIVEL_GENERAL_0_0_13_46", "IPIM"),
            ("4.4.1 IPIB 4 dígitos", "449.1_NIVEL_GENERAL_0_0_13_97", "IPIB"),
            ("4.5.1 IPP 4 dígitos", "450.1_NIVEL_GENERAL_0_0_13_92", "IPP"),
        )
        for hoja, id_, indice in casos:
            with self.subTest(hoja=hoja):
                resultado = precios_otros.clasificar(serie(hoja=hoja, id=id_, variable="nivel_general"))
                # Misma ruta y mismo nombre que el nivel general del libro del INDEC.
                self.assertEqual(resultado, precios_otros.clasificar(sipm(indice, "NG", "Nivel general")))


class PreciosMayoristasHistoricosTests(unittest.TestCase):
    def test_variacion_interanual_anual(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.3 IPIM", id="106.1_I1NGVA_1993_A_39", frecuencia="A", estado="CERRADA",
            variable="ipim_1993_nivel_general_variacion_anual"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios mayoristas", "IPIM · Nivel general (var. % interanual)",
            "Series históricas", "IPIM 1993=100", "Nivel general y agregados", "Proporción (1 = 100%)"))

    def test_variacion_del_periodo_anterior_segun_frecuencia(self):
        trimestral = precios_otros.clasificar(serie(
            hoja="4.3 IPIM", id="106.2_I1NGVM_1993_T_41", frecuencia="T",
            variable="ipim_1993_nivel_general_variacion_mensual"))
        mensual = precios_otros.clasificar(serie(
            hoja="4.3 IPIM", id="106.3_I1NGVM_1993_M_41", frecuencia="M",
            variable="ipim_1993_nivel_general_variacion_mensual"))
        self.assertEqual(trimestral.nombre, "IPIM · Nivel general (var. % trimestral)")
        self.assertEqual(mensual.nombre, "IPIM · Nivel general (var. % mensual)")

    def test_aperturas_y_precios_relativos(self):
        relativo = precios_otros.clasificar(serie(
            hoja="4.3 IPIM", id="106.3_I1PRNI_1993_M_42", variable="ipim_1993_precios_relativos_nac_importados"))
        self.assertEqual(relativo.nombre, "IPIM · Precios relativos nacionales/importados")
        self.assertEqual(relativo.unidad, "Índice 1993=100")
        manufacturas = precios_otros.clasificar(serie(
            hoja="4.3 IPIM", id="106.3_I1PNMEEVM_1993_M_51",
            variable="ipim_1993_prod_nac_manuf_energia_electrica_var_mens"))
        self.assertEqual(manufacturas.nombre, "IPIM · Industria manufacturera y energía eléctrica (var. % mensual)")
        importados = precios_otros.clasificar(serie(
            hoja="4.4 IPIB", id="107.3_I1PIVM_1993_M_48", variable="ipib_1993_productos_importados_variacion_mensual"))
        self.assertEqual(importados.nombre, "IPIB · Productos importados (var. % mensual)")
        self.assertEqual(importados.apertura, "IPIB 1993=100")

    def test_el_ipp_historico_no_agrega_un_nivel_de_un_solo_hijo(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.5 IPP", id="108.3_I1PPVM_1993_M_46", variable="ipp_1993_productos_primarios_variacion_mensual"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios mayoristas", "IPP · Primarios (var. % mensual)", "Series históricas",
            "IPP 1993=100", "", "Proporción (1 = 100%)"))

    def test_cuatro_digitos_nacional_e_importado(self):
        nacional = precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_MA_1993_0_24", variable="maderas_aserradas_m_2010",
            descripcion="Maderas aserradas índice mensual IPIM. Cod. 2010"))
        importado = precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_MAA_1993_0_24", variable="maderas_aserradas_m_7010",
            descripcion="Maderas aserradas índice mensual IPIM. Cod. 7010"))
        self.assertEqual(nacional, esperado(
            "Precios", "Precios mayoristas", "IPIM · Maderas aserradas", "Series históricas",
            "IPIM 1993=100", "Madera, papel e impresiones", "Índice 1993=100"))
        self.assertEqual(importado.nombre, "IPIM · Maderas aserradas · Importados")

    def test_las_etiquetas_cruzadas_de_la_fuente_no_cambian_el_resultado(self):
        # En la tabla anual el producto importado (ID «AFA») figura como «abonos_fertilizantes»
        # y el nacional (ID «AF») como «(7412)»; manda el código del ID.
        nacional = precios_otros.clasificar(serie(
            hoja="4.11", id="126.1_AF_1993_0_23", frecuencia="A", variable="abonos_fertilizantes_ii",
            descripcion="Abonos y fertilizantes (7412) índice anual IPIB"))
        importado = precios_otros.clasificar(serie(
            hoja="4.11", id="126.1_AFA_1993_0_20", frecuencia="A", variable="abonos_fertilizantes",
            descripcion="Abonos y fertilizantes índice anual IPIB"))
        self.assertEqual(nacional.nombre, "IPIM · Abonos y fertilizantes")
        self.assertEqual(importado.nombre, "IPIM · Abonos y fertilizantes · Importados")
        cruzada = precios_otros.clasificar(serie(
            hoja="4.12", id="125.2_OAMNMA_1993_0_47", variable="otros_articulos_minerales_no_metaliferos_m_2699",
            descripcion="Otros artículos de minerales no metalíferos n.c.e.p. índice mensual IPIB. Cod 2699"))
        self.assertEqual(
            cruzada.nombre,
            "IPIB · Otros artículos de minerales no metalíferos no clasificados en otra parte · Importados")

    def test_hierro_y_acero_importado_por_su_nombre_en_singular(self):
        singular = precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_HAFB_1993_0_29", variable="hierro_acero_formas_basicas_m",
            descripcion="Hierro y acero en formas básicas índice mensual IPIM"))
        plural = precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_HAFB_1993_0_31", variable="hierros_aceros_formas_basicas_m",
            descripcion="Hierros y aceros en formas básicas índice mensual IPIM"))
        self.assertEqual(singular.nombre, "IPIM · Hierro y acero en formas básicas · Importados")
        self.assertEqual(plural.nombre, "IPIM · Hierro y acero en formas básicas")

    def test_ncep_historico_y_agregados_de_la_tabla_de_cuatro_digitos(self):
        residual = precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_AD_1993_0_20", variable="alimentos_diversos_m",
            descripcion="Alimentos diversos n.c.e.p. índice mensual IPIM"))
        self.assertEqual(residual.nombre, "IPIM · Alimentos diversos no clasificados en otra parte")
        energia = precios_otros.clasificar(serie(
            hoja="4.12", id="125.2_EE_1993_0_11", variable="energia_electrica_m",
            descripcion="Energía eléctrica índice mensual IPIB"))
        self.assertEqual(energia.medida, "Nivel general y agregados")

    def test_items_que_solo_existen_importados_llevan_el_sufijo(self):
        # Códigos 7xxx y 8xxx del encabezado de la tabla, sin contraparte nacional.
        casos = {
            "126.2_MOI_1993_0_30": ("Máquinas de oficina e informática", "maquinas_oficina_informatica_m"),
            "126.2_CE_1993_0_26": ("Componentes electrónicos", "componentes_electronicos_m"),
            "126.2_PPP_1993_0_25": ("Papeles y productos de papel", "papeles_productos_papel_m"),
        }
        for id_, (item, variable) in casos.items():
            with self.subTest(item=item):
                resultado = precios_otros.clasificar(serie(
                    hoja="4.11", id=id_, variable=variable, descripcion=f"{item} índice mensual IPIM"))
                esperado_item = "Papel y productos de papel" if item.startswith("Papeles") else item
                self.assertEqual(resultado.nombre, f"IPIM · {esperado_item} · Importados")
        # «Componentes electrónicos» comparte el acrónimo «CE» con «Conductores eléctricos» (nacional).
        conductores = precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_CE_1993_0_24", variable="conductores_electricos_m",
            descripcion="Conductores eléctricos índice mensual IPIM"))
        self.assertEqual(conductores.nombre, "IPIM · Conductores eléctricos")

    def test_grupos_de_un_solo_item_se_llaman_como_en_el_libro_vigente(self):
        casos = {
            "Otros equipos eléctricos n.c.e.p.": "Otros equipos eléctricos",
            "Otros productos manufacturados n.c.e.p.": "Otros productos manufacturados",
            "Aparatos de uso doméstico": "Otros aparatos de uso doméstico",
            "Productos de plástico excepto muebles": "Productos de plástico",
        }
        vigentes = {"319": "Otros equipos eléctricos", "369": "Otros productos manufacturados",
                    "293": "Otros aparatos de uso doméstico", "252": "Productos de plástico"}
        for descripcion, nombre in casos.items():
            with self.subTest(item=nombre):
                historico = precios_otros.clasificar(serie(
                    hoja="4.11", id="126.2_X_1993_0_1", variable="x_m", descripcion=f"{descripcion} índice mensual IPIM"))
                codigo = next(c for c, n in vigentes.items() if n == nombre)
                vigente = precios_otros.clasificar(sipm("IPIM", codigo, nombre))
                self.assertEqual(historico.nombre, vigente.nombre)
                self.assertEqual(historico.medida, vigente.apertura)

    def test_item_desconocido_de_cuatro_digitos_no_se_clasifica(self):
        self.assertIsNone(precios_otros.clasificar(serie(
            hoja="4.11", id="126.2_XX_1993_0_1", variable="algo_nuevo_m",
            descripcion="Algo nuevo índice mensual IPIM")))


class CostoDeLaConstruccionTests(unittest.TestCase):
    def test_nivel_y_componentes(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.6 ICC", id="109.3_I1MOM_1993_A_29", variable="icc_1993_mano_obra_materiales"))
        self.assertEqual(resultado, esperado(
            "Precios", "ICC", "ICC · Precios relativos mano de obra/materiales", unidad="Índice 1993=100"))
        gastos = precios_otros.clasificar(serie(
            hoja="4.6 ICC", id="109.1_I1GG_1993_A_25", frecuencia="A", variable="icc_1993_gastos_generales"))
        self.assertEqual(gastos.nombre, "ICC · Gastos generales")

    def test_variaciones(self):
        anual = precios_otros.clasificar(serie(
            hoja="4.6 ICC", id="109.1_I1NGVA_1993_A_38", frecuencia="A",
            variable="icc_1993_nivel_general_variacion_anual"))
        trimestral = precios_otros.clasificar(serie(
            hoja="4.6 ICC", id="109.2_I1NGVM_1993_A_40", frecuencia="T",
            variable="icc_1993_nivel_general_variacion_mensual"))
        self.assertEqual(anual.nombre, "ICC · Nivel general (var. % interanual)")
        self.assertEqual(trimestral.nombre, "ICC · Nivel general (var. % trimestral)")
        self.assertEqual(trimestral.unidad, "Proporción (1 = 100%)")


class PreciosImplicitosTests(unittest.TestCase):
    def test_sector_de_actividad(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.7.1", id="112.1_IAPDSS_2004_A_51", frecuencia="A",
            variable="ipi_administracion_publica_defensa_seguridad_social"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios implícitos del PIB",
            "Precios implícitos · Administración pública y defensa; planes de seguridad social de afiliación obligatoria",
            "Valor agregado por sector", unidad="Índice 2004=100"))

    def test_el_pib_se_llama_igual_en_las_dos_tablas(self):
        por_sector = precios_otros.clasificar(serie(
            hoja="4.7.1", id="112.2_IPIBPM_2004_T_42", frecuencia="T",
            variable="ipi_producto_interno_bruto_precios_mercado"))
        por_componente = precios_otros.clasificar(serie(
            hoja="4.7.2", id="113.2_IOGP_2004_T_21", frecuencia="T", variable="ipi_oferta_global_pib"))
        self.assertEqual(por_sector.nombre, "Precios implícitos · PIB")
        self.assertEqual(por_componente.nombre, "Precios implícitos · PIB")
        self.assertEqual(por_sector.cobertura, "Valor agregado por sector")
        self.assertEqual(por_componente.cobertura, "Oferta y demanda globales")

    def test_componente_de_la_demanda(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.7.2", id="113.1_IDGIEDPMEI_2004_A_59", frecuencia="A",
            variable="ipi_dg_ibif_equipo_durable_prod_maquinaria_equipo_importado"))
        self.assertEqual(resultado.nombre, "Precios implícitos · Inversión en maquinaria y equipo · Importados")

    def test_base_1993_cuya_variacion_trimestral_e_interanual_estan_cruzadas_en_la_fuente(self):
        interanual = precios_otros.clasificar(serie(
            hoja="4.13.1", id="110.2_IPCVA_1993_T_33", frecuencia="T", estado="CERRADA",
            variable="ipi_pib_corriente_variacion_trimestral",
            descripcion="Índice de Precios Implícitos en el PIB corriente. Variación % trimestral. Base 1993"))
        trimestral = precios_otros.clasificar(serie(
            hoja="4.13.1", id="110.2_IPCVT_1993_T_38", frecuencia="T", estado="CERRADA",
            variable="ipi_pib_corriente_variacion_anual",
            descripcion="Índice de Precios Implícitos en el PIB corriente. Variación % interanual. Base 1993"))
        self.assertEqual(interanual, esperado(
            "Precios", "Precios implícitos del PIB", "Precios implícitos · PIB (var. % interanual)",
            "Series históricas", unidad="%"))
        self.assertEqual(trimestral.nombre, "Precios implícitos · PIB (var. % trimestral)")

    def test_base_1993_anual_viene_como_fraccion(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.13.1", id="110.1_IPCVA_1993_A_33", frecuencia="A", variable="ipi_pib_corriente_variacion_anual"))
        self.assertEqual(resultado.unidad, "Proporción (1 = 100%)")

    def test_sectores_productores_de_la_base_1993(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.13.2", id="111.2_IPBS_1993_T_24", frecuencia="T", variable="ipi_pib_bienes_servicios"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios implícitos del PIB", "Precios implícitos · Precios relativos bienes/servicios",
            "Series históricas", unidad="Índice 1993=100"))


class PreciosInternacionalesTests(unittest.TestCase):
    def test_exclusion_del_carbon_de_sudafrica(self):
        # La columna es un índice 2005=100 rotulado como precio en US$ por tonelada.
        self.assertEqual(
            sorted(clave for clave in precios_otros.EXCLUIDAS),
            [("datos.gob.ar", f"121.{n}_CS_0_0_16") for n in (1, 2, 3)])

    def test_producto_individual(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.10", id="121.3_A_0_0_8", variable="aluminio", descripcion="Aluminio. US$ por tonelada. Mensual",
            estado="CERRADA"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios internacionales de productos básicos", "Precio internacional · Aluminio",
            "Productos", "Metales y minerales", unidad="US$ por tonelada"))

    def test_mercado_como_cobertura_y_unidad_de_la_descripcion(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.10", id="121.1_AE_0_0_11", frecuencia="A", variable="azucar_eeuu",
            descripcion="Azúcar (EE.UU.). Centavos de US$ por libra. Anual"))
        self.assertEqual(resultado.nombre, "Precio internacional · Azúcar · Estados Unidos")
        self.assertEqual(resultado.unidad, "Centavos de US$ por libra")
        self.assertEqual(resultado.apertura, "Alimentos")

    def test_te_y_petroleo_promedio(self):
        te = precios_otros.clasificar(serie(
            hoja="4.10", id="121.1_T_0_0_2", frecuencia="A", variable="te",
            descripcion="Té. Centavos de US$ por Kg. Anual"))
        self.assertEqual((te.nombre, te.unidad, te.apertura), ("Precio internacional · Té", "Centavos de US$ por kg", "Bebidas"))
        petroleo = precios_otros.clasificar(serie(
            hoja="4.10", id="121.3_P_0_0_16", variable="petroleo_general", descripcion="Petróleo. US$ por barril. Mensual"))
        self.assertEqual(petroleo.nombre, "Precio internacional · Petróleo · Promedio")

    def test_indice_de_commodities(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.10", id="121.2_A_0_0_9", frecuencia="T", variable="alimentos",
            descripcion="Índice de Commodities. Alimentos. Trimestral"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios internacionales de productos básicos",
            "Índice de precios de productos básicos · Alimentos", "Índices", unidad="Índice 2005=100"))

    def test_producto_desconocido_no_se_clasifica(self):
        self.assertIsNone(precios_otros.clasificar(serie(hoja="4.10", id="121.3_X_0_0_1", variable="vino")))


class ExpectativasTests(unittest.TestCase):
    def test_ipc_mensual_esperado(self):
        resultado = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IPT_1_M_0_0_31_29", hoja="4.30", variable="rem_ipc_nac_var_mensual_t_1"))
        self.assertEqual(resultado, esperado(
            "Precios", "Expectativas", "REM · IPC · mes t+1 (mediana, var. % mensual)", "REM", "IPC",
            unidad="Proporción (1 = 100%)"))

    def test_ipc_nucleo_gba_anual_cerrado(self):
        resultado = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IP016_M_0_0_34_80", hoja="4.30", variable="rem_ipc_gba_nucleo_var_ia_2016",
            estado="CERRADA"))
        self.assertEqual(resultado.nombre, "REM · IPC núcleo · GBA · año 2016 (mediana, var. % interanual)")
        self.assertEqual(resultado.apertura, "IPC núcleo")

    def test_variable_con_doble_guion_bajo(self):
        resultado = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IP_24_M_0_0_28_4", hoja="4.30", variable="rem_ipc__nac_var_ia_t_24"))
        self.assertEqual(resultado.nombre, "REM · IPC · mes t+24 (mediana, var. % interanual)")
        gba = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IP_24_M_0_0_28_67", hoja="4.30", variable="rem_ipc__gba_var_ia_t_24"))
        self.assertEqual(gba.nombre, "REM · IPC · GBA · mes t+24 (mediana, var. % interanual)")

    def test_el_tipo_de_cambio_es_un_nivel_aunque_la_fuente_diga_variacion(self):
        resultado = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IPT_1_M_0_0_31_2", hoja="4.30", variable="rem_tcn_var_mensual_t_1"))
        self.assertEqual(resultado, esperado(
            "Precios", "Expectativas", "REM · Tipo de cambio nominal · mes t+1 (mediana)", "REM",
            "Tipo de cambio nominal", unidad="Pesos por US$"))

    def test_tasa_de_interes_pib_y_desocupacion(self):
        tasa = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IPT_3_M_0_0_41_26", hoja="4.30", variable="rem_tasa_politica_var_mensual_t_3"))
        self.assertEqual(tasa.nombre, "REM · Tasa de interés · mes t+3 (mediana)")
        pib = precios_otros.clasificar(serie(
            id="430.1_REM_PIB_VA_MENOS_M_0_0_27_54", hoja="4.30", variable="rem_pib_var_trimestral_t_menos_1"))
        self.assertEqual(pib.nombre, "REM · PIB · trimestre t-1 (mediana, desestacionalizado, var. % trimestral)")
        trimestre_actual = precios_otros.clasificar(serie(
            id="430.1_MEDIANA_IPL_T_M_0_0_46_42", hoja="4.30", variable="rem_pib_var_trimestral_t"))
        self.assertEqual(
            trimestre_actual.nombre, "REM · PIB · trimestre t (mediana, desestacionalizado, var. % trimestral)")
        desocupacion = precios_otros.clasificar(serie(
            id="430.1_REM_DESOCU023_M_0_0_24_53", hoja="4.30", variable="rem_desocupacion_4t_2023"))
        self.assertEqual(desocupacion.nombre, "REM · Tasa de desocupación abierta · 4.º trimestre 2023 (mediana)")
        actual = precios_otros.clasificar(serie(
            id="430.1_REM_DESOCUN_T_M_0_0_18_84", hoja="4.30", variable="rem_desocupacion_t"))
        self.assertEqual(actual.nombre, "REM · Tasa de desocupación abierta · trimestre t (mediana)")

    def test_sector_externo_y_resultado_primario_con_su_unidad(self):
        exportaciones = precios_otros.clasificar(serie(
            id="430.1_REM_EXPO_MT_3_M_0_0_20_15", hoja="4.30", variable="rem_expo_mensual_t_3"))
        self.assertEqual(exportaciones.nombre, "REM · Exportaciones · mes t+3 (mediana)")
        self.assertEqual(exportaciones.unidad, "Millones de US$")
        importaciones = precios_otros.clasificar(serie(
            id="430.1_REM_IMPO_A023_M_0_0_19_46", hoja="4.30", variable="rem_impo_anual_2024"))
        self.assertEqual(importaciones.nombre, "REM · Importaciones · año 2024 (mediana)")
        resultado = precios_otros.clasificar(serie(
            id="430.1_REM_RESULT026_M_0_0_39_81", hoja="4.30", variable="rem_resultado_primario_spnf_var_ia_2026"))
        self.assertEqual(
            resultado.nombre, "REM · Resultado primario del sector público no financiero · año 2026 (mediana)")
        self.assertEqual(resultado.unidad, "Miles de millones de pesos")

    def test_variable_del_rem_desconocida_no_se_clasifica(self):
        self.assertIsNone(precios_otros.clasificar(serie(id="430.1_X", hoja="4.30", variable="rem_inflacion_t_9")))
        self.assertIsNone(precios_otros.clasificar(serie(id="430.1_X", hoja="4.30", variable="rem_pib_var_mensual_t_1")))

    def test_encuesta_de_expectativas_de_inflacion(self):
        resultado = precios_otros.clasificar(serie(
            id="431.1_EXPECTATIVANA_M_0_0_29_85", hoja="4.31", variable="expectativa_inflacion_mediana"))
        self.assertEqual(resultado, esperado(
            "Precios", "Expectativas", "Expectativas de inflación · 12 meses (mediana)",
            "Encuesta de expectativas de inflación", unidad="%"))


class OtrosIndicadoresTests(unittest.TestCase):
    def test_canastas_y_lineas(self):
        cba = precios_otros.clasificar(serie(
            archivo="empleo_ingresos.xlsx", hoja="CBA y CBT 2016", id="150.1_CSTA_BARIA_0_D_26",
            variable="canasta_basica_alimentaria"))
        self.assertEqual(cba, esperado(
            "Precios", "Canastas básicas", "Canasta básica alimentaria · Adulto equivalente · GBA", "GBA",
            unidad="Pesos corrientes"))
        pobreza = precios_otros.clasificar(serie(
            archivo="empleo_ingresos.xlsx", hoja="CBA y CBT 2016", id="150.1_LA_POBREZA_0_D_13",
            variable="linea_pobreza"))
        self.assertEqual(pobreza.nombre, "Línea de pobreza · Hogar de referencia · GBA")
        engel = precios_otros.clasificar(serie(
            archivo="empleo_ingresos.xlsx", hoja="CBA y CBT 2016", id="150.1_IRSA_COGEL_0_D_25",
            variable="inversa_coeficiente_engel"))
        self.assertEqual((engel.nombre, engel.unidad), ("Inversa del coeficiente de Engel · GBA", "Cociente CBT/CBA"))

    def test_precio_de_cigarrillos(self):
        resultado = precios_otros.clasificar(serie(
            hoja="4.32", id="399.1_PROMEDIO_PADO_M_PAQU_0_18_89", variable="promedio_ponderado"))
        self.assertEqual(resultado, esperado(
            "Precios", "Precios de cigarrillos",
            "Precio de cigarrillos · Paquete de 20 unidades (promedio ponderado)", unidad="Pesos"))

    def test_indices_de_comercio_exterior_salen_de_precios(self):
        exportacion = precios_otros.clasificar(serie(
            hoja="4.9.3", id="119.1_IPE_0_A_26", frecuencia="A", estado="DISCONTINUADA",
            variable="Índice de precios de exportación. 2004=100"))
        self.assertEqual(exportacion, esperado(
            "Sector externo", "Índices de precios y cantidades del comercio exterior",
            "Índice de precios de exportación", unidad="Índice 2004=100"))
        terminos = precios_otros.clasificar(serie(
            hoja="4.9.3", id="119.1_TI_0_A_20", frecuencia="A", estado="DISCONTINUADA",
            variable="Índice de Términos del Intercambio. 2004=100"))
        self.assertEqual(terminos.nombre, "Términos del intercambio")

    def test_tasas_trimestrales_van_con_su_indicador(self):
        casos = {
            "ipim": ("Precios mayoristas", "IPIM · Nivel general (var. % acumulada en el trimestre)",
                     "Series históricas", "IPIM 1993=100", "Nivel general y agregados"),
            "ipibm": ("Precios mayoristas", "IPIB · Nivel general (var. % acumulada en el trimestre)",
                      "Series históricas", "IPIB 1993=100", "Nivel general y agregados"),
            "ipbp": ("Precios mayoristas", "IPP · Nivel general (var. % acumulada en el trimestre)",
                     "Series históricas", "IPP 1993=100", ""),
            "ic_construccion": ("ICC", "ICC · Nivel general (var. % acumulada en el trimestre)", "", "", ""),
            "ipc": ("IPC", "IPC · Nivel general (var. % acumulada en el trimestre)",
                    "Series históricas", "Tasas trimestrales de variación", ""),
            "ip_combinado": ("Índice de precios combinado",
                             "Índice de precios combinado · IPC-IPIM (var. % acumulada en el trimestre)", "", "", ""),
        }
        for variable, (indicador, nombre, cobertura, apertura, medida) in casos.items():
            with self.subTest(variable=variable):
                resultado = precios_otros.clasificar(serie(
                    hoja="4.19", id=f"95.1_{variable}", frecuencia="T", estado="CERRADA", variable=variable))
                self.assertEqual(resultado, esperado(
                    "Precios", indicador, nombre, cobertura, apertura, medida, "%"))


class AlcanceTests(unittest.TestCase):
    def test_series_fuera_del_alcance_devuelven_none(self):
        fuera = (
            serie(hoja="4.1.1 IPC NG", id="148.3_INGNACUAL_DICI_M_26", variable="ipc_nivel_general_nacional"),
            serie(hoja="4.8.2", id="x", variable="tipo_cambio_real_brasil"),
            serie(hoja="4.9.1", id="y", variable="Índice de cantidad de las exportaciones de bienes. 2004=100"),
            serie(archivo="actividad.xlsx", hoja="1.7.1 Precios impl BASE 2004", id="z", variable="ipi_pib"),
            serie(archivo="empleo_ingresos.xlsx", hoja="CBA y CBT", id="w", variable="canasta_basica_alimentaria"),
            serie(archivo="empleo_ingresos.xlsx", hoja="4.30", id="v", variable="rem_ipc_nac_var_ia_2016"),
            serie(archivo="otro.xlsx", hoja="4.30", id="u", variable="rem_ipc_nac_var_ia_2016"),
            serie(fuente="indec-ipc", archivo="latest.xls", hoja="IPIM", id="IPIM|NG", variable="NG",
                  columna="NG - Nivel general"),
            serie(fuente="bcra-itc", archivo="ITCNMSerie.xlsx", hoja="4.10", id="t", variable="aluminio"),
        )
        for caso in fuera:
            with self.subTest(hoja=caso.hoja, archivo=caso.archivo, fuente=caso.fuente):
                self.assertIsNone(precios_otros.clasificar(caso))

    def test_es_deterministica_y_no_lee_la_clasificacion_previa(self):
        original = sipm("IPIM", "NG", "Nivel general")
        reclasificada = Serie(**{**original.__dict__, "nombre": "IPIM · Nivel general", "unidad": "Índice dic-2015=100"})
        primera = precios_otros.clasificar(original)
        self.assertEqual(primera, precios_otros.clasificar(original))
        self.assertEqual(primera, precios_otros.clasificar(reclasificada))

    def test_el_despachador_de_taxonomia_usa_estas_reglas(self):
        resultado = despachar(sipm("IPIM", "NG", "Nivel general"))
        self.assertEqual(resultado.nombre, "IPIM · Nivel general")
        self.assertEqual(resultado.tema, "Precios")


INVENTARIO = Path(__file__).resolve().parents[1] / "IndiceSeries.xlsx"


@unittest.skipUnless(INVENTARIO.is_file(), "no hay inventario publicado para recorrer")
class InventarioCompletoTests(unittest.TestCase):
    """Recorre todo el inventario: cobertura completa y nombres sin choques accidentales."""

    @classmethod
    def setUpClass(cls):
        from tools.taxonomia import serie_de
        from tools.taxonomia.revisar import cargar

        inventario = cargar()
        cls.resultados = []
        for fila in inventario.to_dict("records"):
            s = serie_de(fila)
            resultado = precios_otros.clasificar(s)
            if resultado is not None or cls._en_alcance(s):
                cls.resultados.append((s, resultado))

    @staticmethod
    def _en_alcance(s):
        if s.fuente == "indec-sipm":
            return True
        if s.fuente != "datos.gob.ar":
            return False
        hojas = set(precios_otros._HOJAS_PRECIOS)
        return (s.archivo == "precios.xlsx" and s.hoja in hojas) or (
            s.archivo == "empleo_ingresos.xlsx" and s.hoja == "CBA y CBT 2016")

    def test_todas_las_series_del_alcance_tienen_regla(self):
        self.assertGreater(len(self.resultados), 1400)
        sin_regla = [s.id for s, c in self.resultados if c is None]
        self.assertEqual(sin_regla, [])

    def test_no_hay_nombres_repetidos_en_una_misma_hoja_y_frecuencia(self):
        conteo = Counter((s.fuente, s.hoja, s.frecuencia, c.nombre) for s, c in self.resultados if c)
        self.assertEqual([clave for clave, n in conteo.items() if n > 1], [])

    def test_ningun_grupo_final_supera_las_150_series(self):
        conteo = Counter(c.niveles() for _, c in self.resultados if c)
        self.assertEqual([(ruta, n) for ruta, n in conteo.items() if n > 150], [])

    def test_las_series_del_mismo_indice_comparten_nombre_entre_fuentes(self):
        nombres = {}
        for s, c in self.resultados:
            if c and s.hoja in ("IPIM", "4.3.1 IPIM 4 dígitos") and c.nombre == "IPIM · Nivel general":
                nombres[s.hoja] = c.niveles()
        self.assertEqual(len(set(nombres.values())), 1)


if __name__ == "__main__":
    unittest.main()
