"""Pruebas de las reglas de cuentas nacionales (INDEC, informe del PIB, y actividad.xlsx de datos.gob.ar)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.actividad_cuentas_nacionales import clasificar

INDEC = "indec-pib"
DATOS = "datos.gob.ar"


def serie(fuente: str, hoja: str, id_: str, variable: str, frecuencia: str = "T",
          archivo: str | None = None) -> Serie:
    if archivo is None:
        archivo = "latest.xls" if fuente == INDEC else "actividad.xlsx"
    return Serie(
        fuente=fuente, id=id_, nombre=variable, variable=variable, descripcion="", unidad="",
        frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=variable, dataset="", estado="VIGENTE",
    )


def esperado(tema, indicador, nombre, cobertura="", apertura="", medida="", unidad=""):
    return Clasificacion(tema=tema, indicador=indicador, nombre=nombre, cobertura=cobertura,
                         apertura=apertura, medida=medida, unidad=unidad)


def pib(nombre, cobertura, unidad, apertura="", medida=""):
    return esperado("Actividad", "PIB", nombre, cobertura, apertura, medida, unidad)


def implicitos(nombre, cobertura, unidad, apertura=""):
    return esperado("Precios", "Precios implícitos del PIB", nombre, cobertura, apertura, "", unidad)


def ingreso(nombre, cobertura, unidad, apertura=""):
    return esperado("Actividad", "Ingreso nacional y ahorro", nombre, cobertura, apertura, "", unidad)


M04 = "Millones de pesos de 2004"
M93 = "Millones de pesos de 1993"
MC = "Millones de pesos"
HIST = "Series históricas"
OYD = "Oferta y demanda globales"
FBCF = "Formación bruta de capital fijo"
SECTOR = "Por sector de actividad"

# (fuente, hoja, id, variable, frecuencia, clasificación esperada)
CASOS = (
    # ---- INDEC: oferta y demanda (cuadros 1, 2, 3, 8, 11)
    (INDEC, "cuadro 1", "cuadro-1|B1b", "producto-interno-bruto", "T",
     pib("PIB (precios constantes)", OYD, M04)),
    (INDEC, "cuadro 1", "cuadro-1|P52", "variacion-de-existencias-3", "T",
     pib("PIB · Variación de existencias (precios constantes)", OYD, M04)),
    (INDEC, "cuadro 2", "cuadro-2|P3_S14_S15", "consumo-privado-5", "T",
     pib("PIB · Consumo privado (precios constantes, var. % interanual)", OYD, "%")),
    (INDEC, "cuadro 3", "cuadro-3|P51b", "formacion-bruta-de-capital-fijo", "T",
     pib("PIB · Formación bruta de capital fijo (precios constantes, % del PIB)", OYD, "%")),
    (INDEC, "cuadro 8", "cuadro-8|oferta-global", "oferta-global", "T",
     pib("PIB · Oferta global", OYD, MC)),
    (INDEC, "cuadro 11", "cuadro-11|P51b", "formacion-bruta-de-capital-fijo", "T",
     pib("PIB · Formación bruta de capital fijo (% del PIB)", OYD, "%")),
    # ---- INDEC: PIB por sector (cuadros 4, 5, 12)
    (INDEC, "cuadro 4", "cuadro-4|L",
     "administracion-publica-y-defensa-planes-de-seguridad-social-de-afiliacion-obligatoria", "T",
     pib("VAB · Administración pública y defensa (precios constantes)", SECTOR, M04, "Precios constantes")),
    (INDEC, "cuadro 4", "cuadro-4|D214 - D31",
     "impuesto-a-los-productos-netos-de-subsidios-a-los-ingresos-brutos-especificos-a-los-debitos-y-creditos-bancarios-a-las-exportaciones",
     "T", pib("PIB · Impuestos a los productos netos de subsidios (precios constantes)", SECTOR, M04,
              "Precios constantes")),
    (INDEC, "cuadro 5", "cuadro-5|E", "electricidad-gas-y-agua", "T",
     pib("VAB · Suministro de electricidad, gas y agua (precios constantes, var. % interanual)", SECTOR, "%",
         "Precios constantes")),
    (INDEC, "cuadro 12", "cuadro-12|B1b", "producto-interno-bruto", "T",
     pib("PIB", SECTOR, MC, "Precios corrientes")),
    # ---- INDEC: inversión (cuadros 6, 7, 14)
    (INDEC, "cuadro 6", "cuadro-6|importado-2838e412", "importado-2838e412", "T",
     pib("PIB · Inversión en material de transporte · Importados (precios constantes)", FBCF, M04)),
    (INDEC, "cuadro 7", "cuadro-7|nacional", "nacional", "T",
     pib("PIB · Inversión en maquinaria y equipo · Nacionales (precios constantes, var. % interanual)", FBCF, "%")),
    (INDEC, "cuadro 14", "cuadro-14|AN1131", "equipo-de-transporte", "T",
     pib("PIB · Inversión en material de transporte", FBCF, MC)),
    # ---- INDEC: precios implícitos (cuadros 9, 10, 13, 15) van a Precios
    (INDEC, "cuadro 9", "cuadro-9|P7", "importaciones-fob-bienes-y-servicios-reales", "T",
     implicitos("Precios implícitos · Importaciones", OYD, "Índice 2004=100")),
    (INDEC, "cuadro 10", "cuadro-10|B1b", "producto-interno-bruto", "T",
     implicitos("Precios implícitos · PIB (var. % interanual)", OYD, "%")),
    (INDEC, "cuadro 13", "cuadro-13|D212", "impuesto-a-los-productos-importados-derechos-de-importacion", "T",
     implicitos("Precios implícitos · Derechos de importación", "Valor agregado por sector", "Índice 2004=100")),
    (INDEC, "cuadro 15", "cuadro-15|importado", "importado", "T",
     implicitos("Precios implícitos · Inversión en maquinaria y equipo · Importados", OYD, "Índice 2004=100")),
    # ---- datos.gob.ar: oferta y demanda, base 2004 y base 1993
    (DATOS, "1.1.1 OyD real BASE 2004", "4.2_OGP_2004_T_17", "oferta_global_pib", "T",
     pib("PIB (precios constantes)", OYD, M04)),
    (DATOS, "1.1.1 OyD real BASE 2004", "4.1_DGIEDPMTN_2004_A_56",
     "dg_ibif_equipo_durable_prod_material_transporte_nacional", "A",
     pib("PIB · Inversión en material de transporte · Nacionales (precios constantes)", FBCF, M04)),
    (DATOS, "1.1 OyD real", "2.2_DEVE_1993_T_46", "discrepancia_estadistica_variacion_existencias", "T",
     pib("PIB · Variación de existencias y discrepancia estadística (precios constantes)", HIST, M93,
         "Base 1993", OYD)),
    (DATOS, "1.2 OyD real s.e.", "1.2_DGI_D_1993_T_19", "demanda_global_ibif", "T",
     pib("PIB · Formación bruta de capital fijo (precios constantes, desestacionalizado)", HIST, M93,
         "Base 1993", OYD)),
    (DATOS, "1.2.1 OyD real s.e. BASE 2004", "3.2_DGCP_D_2004_T_27", "demanda_global_consumo_priv", "T",
     pib("PIB · Consumo privado (precios constantes, desestacionalizado)", OYD, M04)),
    (DATOS, "1.5.1 OyD corr. 2004", "4.4_DE_2004_T_24", "discrepancia_estadistica", "T",
     pib("PIB · Discrepancia estadística", OYD, MC)),
    (DATOS, "1.5 OyD corr.", "2.3_DGIEDP_1993_A_45", "demanda_global_ibif_equipo_durable_produccion", "A",
     pib("PIB · Inversión en equipos durables de producción", HIST, MC, "Base 1993", OYD)),
    # ---- datos.gob.ar: PIB por sector
    (DATOS, "1.3.1 PIB real x sector BASE 04", "6.2_APDPSSAO_2004_T_60",
     "admin_publica_planes_seguridad_social_afiliacion_obligatoria", "T",
     pib("VAB · Administración pública y defensa (precios constantes)", SECTOR, M04, "Precios constantes")),
    (DATOS, "1.6.1 Pib corr x sector 2004", "8.1_VABPB_2004_A_36", "valor_agregado_bruto_precios_basicos", "A",
     pib("VAB", SECTOR, MC, "Precios corrientes")),
    (DATOS, "1.3 PIB real x sector", "5.1_II_1993_A_22", "impuesto_importaciones", "A",
     pib("PIB · Derechos de importación (precios constantes)", HIST, M93, "Base 1993", SECTOR)),
    (DATOS, "1.6 Pib corriente x sector", "7.1_SIFMI_1993_A_58",
     "servicios_intermediacion_financiera_medidos_indirectamente", "A",
     pib("PIB · Servicios de intermediación financiera medidos indirectamente", HIST, MC, "Base 1993", SECTOR)),
    # ---- datos.gob.ar: valor agregado y valor bruto de producción por rama
    (DATOS, "1.3.2 VAB real x rama 2004", "307.1_AGRICULTURXOS_0_A_44",
     "agricultura_ganaderia_caza_servicios_conexos", "A",
     pib("VAB · Cultivos agrícolas (precios constantes)", "Valor agregado bruto por rama", M04,
         "Precios constantes", "Divisiones y grupos")),
    (DATOS, "1.6.2 VAB corriente x rama 2004", "305.1_IND_MANUFAROS_0_52",
     "ind_manufac_curtido_term_cueros_fab_art_marroq_otros", "A",
     pib("VAB · Curtido y terminación de cueros; marroquinería, talabartería y calzado",
         "Valor agregado bruto por rama", MC, "Precios corrientes", "Divisiones y grupos")),
    (DATOS, "1.6.2 VAB corriente x rama 2004", "305.1_IND_MANUFATAL_0_17", "ind_manufac_total", "A",
     pib("VAB · Industria manufacturera", "Valor agregado bruto por rama", MC, "Precios corrientes",
         "Secciones")),
    (DATOS, "1.3.3 VBP real x rama 2004", "440.1_TOTAL_AGRIURA_2004_0_17_3", "total_agricultura", "A",
     pib("VBP · Agricultura, ganadería, caza y silvicultura (precios constantes)",
         "Valor bruto de producción por rama", M04, "Precios constantes", "Secciones")),
    (DATOS, "1.6.3 VBP corriente x rama 2004", "441.1_RESTOSTO_0_0_5_49", "resto", "A",
     pib("VBP · Resto de actividades inmobiliarias, empresariales y de alquiler",
         "Valor bruto de producción por rama", MC, "Precios corrientes", "Divisiones y grupos")),
    # ---- datos.gob.ar: precios implícitos (van a Precios)
    (DATOS, "1.7 Precios impl", "22.1_PIBPM_1993_A_38", "producto_interno_bruto_precios_mercado", "A",
     implicitos("Precios implícitos · PIB", HIST, "Índice 1993=100", "Base 1993")),
    (DATOS, "1.7.1 Precios impl BASE 2004", "23.2_VABPB_2004_T_36", "valor_agregado_bruto_precios_basicos", "T",
     implicitos("Precios implícitos · Valor agregado bruto a precios básicos", "Valor agregado por sector",
                "Índice 2004=100")),
    (DATOS, "1.7.1b Precios impl BASE 2004", "25.1_DGIEDPMEN_2004_A_54",
     "dg_ibif_equipo_durable_prod_maquinaria_equipo_nacional", "A",
     implicitos("Precios implícitos · Inversión en maquinaria y equipo · Nacionales", OYD, "Índice 2004=100")),
    (DATOS, "1.7.2 Precios impl. x rama 2004", "306.1_TRANSPORTETAL_II_0_A_46", "transporte", "A",
     implicitos("Precios implícitos · Transporte", "Valor agregado por rama", "Índice 2004=100", "Divisiones y grupos")),
    (DATOS, "1.7b Precios impl", "24.2_DGI_1993_T_19", "demanda_global_ibif", "T",
     implicitos("Precios implícitos · Formación bruta de capital fijo", HIST, "Índice 1993=100", "Base 1993")),
    # ---- datos.gob.ar: PIB en dólares y per cápita
    (DATOS, "1.8 PIB US$", "9.1_PPCDC_2004_A_33", "pib_per_capita_dolares_corrientes", "A",
     pib("PIB (en dólares, per cápita)", "En dólares y per cápita", "Dólares")),
    (DATOS, "1.8 PIB US$", "9.2_PP2_2004_T_16", "pib_precios_2004", "T",
     pib("PIB (precios constantes)", "En dólares y per cápita", M04)),
    # ---- datos.gob.ar: ingreso nacional y ahorro
    (DATOS, "1.9 Ingreso Nac", "26.1_INBD_0_A_33", "ingreso_nacional_bruto_disponible", "A",
     ingreso("Ingreso nacional y ahorro · Ingreso nacional bruto disponible", HIST, MC, "Base 1993")),
    (DATOS, "1.9 Ingreso Nac (cont. 1)", "27.2_IIBF_0_T_28", "inversion_interna_bruta_fija", "T",
     ingreso("PIB · Formación bruta de capital fijo", HIST, MC, "Base 1993")),
    (DATOS, "1.9 Ingreso Nac (cont. 2)", "28.1_AGPRI_0_A_35", "a_ganancia_perdida_real_intercambio", "A",
     ingreso("Ingreso nacional y ahorro · Ganancia o pérdida real del intercambio (precios constantes, cuenta de ahorro)",
             HIST, M93, "Base 1993")),
    (DATOS, "1.9 Ingreso Nac (cont. 2)", "28.1_IGPRDI_0_A_39", "i_ganancia_perdida_real_del_intercambio", "A",
     ingreso("Ingreso nacional y ahorro · Ganancia o pérdida real del intercambio (precios constantes, cuenta de ingreso)",
             HIST, M93, "Base 1993")),
    (DATOS, "1.9.1 Ingreso Nac BASE 2004", "166.2_ARO_NACANB_0_0_20", "ahorro_nac_bruto_anb", "T",
     ingreso("Ingreso nacional y ahorro · Ahorro nacional bruto", "Ingreso, ahorro y préstamo neto", MC)),
    (DATOS, "1.9.1 Ing Nac (cont. 1) BASE 04", "167.1_SO_CORREXT_2004_0_14", "saldo_corr_ext", "A",
     ingreso("Ingreso nacional y ahorro · Ahorro del resto del mundo", "Ahorro del resto del mundo", MC)),
    (DATOS, "1.9.1 Ing Nac (cont. 1) BASE 04", "167.1_PT_ENDEETO_2004_0_19", "prest_endeudam_neto", "A",
     ingreso("Ingreso nacional y ahorro · Préstamo / endeudamiento neto · Resto del mundo",
             "Ahorro del resto del mundo", MC)),
)


class PruebasCuentasNacionales(unittest.TestCase):
    def test_hay_al_menos_25_casos(self):
        self.assertGreaterEqual(len(CASOS), 25)

    def test_casos_representativos(self):
        for fuente, hoja, id_, variable, frecuencia, buscado in CASOS:
            with self.subTest(hoja=hoja, id=id_):
                self.assertEqual(clasificar(serie(fuente, hoja, id_, variable, frecuencia)), buscado)

    def test_fuera_de_alcance_devuelve_none(self):
        fuera = (
            serie(DATOS, "1.10 SS PP", "x", "servicios_publicos"),          # prefijo parecido, otro agente
            serie(DATOS, "1.11 Super", "x", "demanda_global_total"),
            serie(DATOS, "1.14 Emi", "x", "oferta_global_pib"),
            serie(DATOS, "1.1.1 OyD real BASE 2004", "x", "oferta_global_pib", archivo="otro.xlsx"),
            serie("indec-supermercados", "cuadro 1", "x", "producto-interno-bruto", archivo="supermercados.xlsx"),
            serie("indec-pib", "cuadro 16", "cuadro-16|B1b", "producto-interno-bruto"),
            serie(INDEC, "cuadro 1", "cuadro-1|codigo-inexistente", "inexistente"),
            serie(DATOS, "1.3.2 VAB real x rama 2004", "x", "variable_que_no_existe", "A"),
        )
        for s in fuera:
            with self.subTest(hoja=s.hoja, fuente=s.fuente):
                self.assertIsNone(clasificar(s))

    def test_dos_llamadas_dan_lo_mismo(self):
        for fuente, hoja, id_, variable, frecuencia, _ in CASOS:
            s = serie(fuente, hoja, id_, variable, frecuencia)
            self.assertEqual(clasificar(s), clasificar(s))

    def test_mismo_concepto_mismo_nombre_entre_fuentes(self):
        # El cuadro 4 del INDEC y la hoja 1.3.1 son la misma serie (valor agregado de la industria).
        indec = clasificar(serie(INDEC, "cuadro 4", "cuadro-4|D", "industria-manufacturera"))
        datos = clasificar(serie(DATOS, "1.3.1 PIB real x sector BASE 04", "6.2_IM_2004_T_23", "industria_manufacturera"))
        self.assertEqual((indec.nombre, indec.cobertura, indec.apertura), (datos.nombre, datos.cobertura, datos.apertura))
        # Oferta global en pesos corrientes: cuadro 8 y hoja 1.5.1.
        indec = clasificar(serie(INDEC, "cuadro 8", "cuadro-8|B1b", "producto-interno-bruto"))
        datos = clasificar(serie(DATOS, "1.5.1 OyD corr. 2004", "4.4_OGP_2004_T_17", "oferta_global_pib"))
        self.assertEqual((indec.nombre, indec.cobertura), (datos.nombre, datos.cobertura))
        # Inversión en material de transporte importado, a precios constantes: cuadro 6 y hoja 1.1.1.
        indec = clasificar(serie(INDEC, "cuadro 6", "cuadro-6|importado-2838e412", "importado-2838e412"))
        datos = clasificar(serie(DATOS, "1.1.1 OyD real BASE 2004", "4.2_DGIEDPMTI_2004_T_57",
                                 "dg_ibif_equipo_durable_prod_material_transporte_importado"))
        self.assertEqual((indec.nombre, indec.cobertura), (datos.nombre, datos.cobertura))
        # Deflactor del PIB: cuadro 9 del INDEC y hoja 1.7.1b.
        indec = clasificar(serie(INDEC, "cuadro 9", "cuadro-9|B1b", "producto-interno-bruto"))
        datos = clasificar(serie(DATOS, "1.7.1b Precios impl BASE 2004", "25.2_OGP_2004_T_17", "oferta_global_pib"))
        self.assertEqual((indec.nombre, indec.cobertura, indec.tema), (datos.nombre, datos.cobertura, datos.tema))

    def test_ramas_se_escriben_igual_en_todas_las_hojas(self):
        # La misma rama (alimentos) en valor agregado corriente, constante y valor bruto de producción.
        nombres = {
            clasificar(serie(DATOS, "1.3.2 VAB real x rama 2004", "x", "elaboracion_productos_alimenticios_bebidas", "A")).nombre
            .replace(" (precios constantes)", ""),
            clasificar(serie(DATOS, "1.6.2 VAB corriente x rama 2004", "x", "ind_manufac_elaboracion_prod_alimenticios_bebidas", "A")).nombre,
            clasificar(serie(DATOS, "1.3.3 VBP real x rama 2004", "x", "elaboracion_productos_alimenticios_bebidas", "A")).nombre
            .replace(" (precios constantes)", "").replace("VBP", "VAB"),
        }
        self.assertEqual(nombres, {"VAB · Elaboración de productos alimenticios y bebidas"})

    def test_distingue_valoracion_en_el_nombre(self):
        corriente = clasificar(serie(DATOS, "1.5.1 OyD corr. 2004", "x", "demanda_global_consumo_priv"))
        constante = clasificar(serie(DATOS, "1.1.1 OyD real BASE 2004", "x", "manda_global_consumo_priv"))
        desestacionalizada = clasificar(serie(DATOS, "1.2.1 OyD real s.e. BASE 2004", "x", "demanda_global_consumo_priv"))
        self.assertEqual(corriente.nombre, "PIB · Consumo privado")
        self.assertEqual(constante.nombre, "PIB · Consumo privado (precios constantes)")
        self.assertEqual(desestacionalizada.nombre, "PIB · Consumo privado (precios constantes, desestacionalizado)")

    def test_los_nombres_no_pasan_de_120_caracteres(self):
        for fuente, hoja, id_, variable, frecuencia, _ in CASOS:
            self.assertLessEqual(len(clasificar(serie(fuente, hoja, id_, variable, frecuencia)).nombre), 120)

    def test_toda_rama_de_las_hojas_por_rama_tiene_nombre_unico(self):
        # Las 68 ramas de una hoja por rama quedan con nombres distintos (sin colapsos).
        from tools.taxonomia import actividad_cuentas_nacionales as modulo
        claves = {clave for _, clave in modulo._REGLAS_RAMA}
        self.assertEqual(len(claves), 17 + len(modulo._SUBRAMAS))


if __name__ == "__main__":
    unittest.main()
