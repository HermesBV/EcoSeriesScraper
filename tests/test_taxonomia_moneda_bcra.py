"""Pruebas de las reglas del dinero, las tasas y el balance del BCRA."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.moneda_bcra import clasificar

MONEDA = "Moneda y sistema financiero"
EXTERNO = "Sector externo"
H_TASAS = "8.1 Tasas de interes"
H_AGREGADOS = "8.2 Agregados monetarios"
H_PRESTAMOS = "8.3 Prest. de Entidades finac."
H_ENCAJES = "8.5 Exigencia encajes"
H_COEFICIENTES = "8.6 CER, UVA y UVI"
H_HISTORICAS = "8.7 Series historicas"
H_BALANCE = "8.9 Balance BCRA"
H_LEBAC = "8.10 Lebac mercado secundario"
H_FACTORES = "8.11 Factores Base Monetaria"


def dmd(id_: str, nombre: str, hoja: str) -> Serie:
    return Serie(
        fuente="bcra-dmd", id=id_, nombre=nombre, variable="", descripcion=nombre, unidad="",
        frecuencia="D", archivo="series.xlsm", hoja=hoja, columna=nombre,
        dataset="Datos monetarios diarios", estado="VIGENTE")


def datos(hoja: str, id_: str, variable: str, descripcion: str, frecuencia: str = "M") -> Serie:
    return Serie(
        fuente="datos.gob.ar", id=id_, nombre="Título del cuadro", variable=variable,
        descripcion=descripcion, unidad="", frecuencia=frecuencia, archivo="dinero_bancos.xlsx",
        hoja=hoja, columna=id_, dataset="Título del cuadro", estado="VIGENTE")


def esperado(tema, indicador, nombre, cobertura="", apertura="", unidad=""):
    return Clasificacion(tema=tema, indicador=indicador, cobertura=cobertura, apertura=apertura,
                         nombre=nombre, unidad=unidad)


CASOS = (
    # --- bcra-dmd: base monetaria
    (dmd("71", "Saldo de base monetaria (en millones de $)", "BASE MONETARIA"),
     esperado(MONEDA, "Base monetaria", "Base monetaria", "Total", "", "Millones de pesos")),
    (dmd("66", "Variación diaria de la base monetaria más variación diaria de cuasimonedas (en millones de $)",
         "BASE MONETARIA"),
     esperado(MONEDA, "Base monetaria", "Base monetaria más cuasimonedas (variación diaria)", "Total", "",
              "Millones de pesos")),
    (dmd("67", "Saldo de billetes y monedas en poder del público (en millones de $)", "BASE MONETARIA"),
     esperado(MONEDA, "Base monetaria", "Base monetaria · Circulación monetaria · En poder del público",
              "Por componente", "", "Millones de pesos")),
    (dmd("48", "Efecto monetario de las compras netas de divisas al Tesoro Nacional (en millones de $)",
         "BASE MONETARIA"),
     esperado(MONEDA, "Base monetaria",
              "Base monetaria · Factores de explicación · Compras de divisas · Al Tesoro Nacional",
              "Factores de explicación", "Compras de divisas", "Millones de pesos")),
    (dmd("58", "Efecto monetario de las operaciones con Letras Fiscales de Liquidez (en millones de $)",
         "BASE MONETARIA"),
     esperado(MONEDA, "Base monetaria",
              "Base monetaria · Factores de explicación · Títulos y letras · Letras Fiscales de Liquidez (LEFI)",
              "Factores de explicación", "Títulos y letras", "Millones de pesos")),
    # --- bcra-dmd: depósitos, agregados y préstamos
    (dmd("94", "Saldo de depósitos en pesos cuentas corrientes del sector privado no financiero (en millones de $)",
         "DEPOSITOS"),
     esperado(MONEDA, "Depósitos", "Depósitos · Cuenta corriente · Sector privado", "Sector privado", "En pesos",
              "Millones de pesos")),
    (dmd("108", "Saldo de depósitos en dólares del sector privado no financiero, expresados en dólares (en millones de USD)",
         "DEPOSITOS"),
     esperado(MONEDA, "Depósitos", "Depósitos · Total · Sector privado (en dólares)", "Sector privado",
              "En dólares", "Millones de dólares")),
    (dmd("103", "Saldo de depósitos en dólares de los sectores público y privado no financieros, expresados en pesos (en millones de $)",
         "DEPOSITOS"),
     esperado(MONEDA, "Depósitos", "Depósitos · Total (en dólares, expresados en pesos)", "Total", "En dólares",
              "Millones de pesos")),
    (dmd("197", "Saldo del agregado monetario M2 transaccional privado (en millones de $)", "DEPOSITOS"),
     esperado(MONEDA, "Agregados monetarios", "Agregados monetarios · M2 transaccional privado",
              unidad="Millones de pesos")),
    (dmd("123", "Saldo de préstamos en dólares mediante tarjetas de crédito otorgados al sector privado(en millones de USD)",
         "PRESTAMOS"),
     esperado(MONEDA, "Préstamos", "Préstamos · Tarjetas de crédito · Sector privado (en dólares)", "En dólares",
              "", "Millones de dólares")),
    # --- bcra-dmd: tasas, mercado de call, instrumentos
    (dmd("140", "BADLAR de bancos privados,TEA (en %)", "TASAS DE MERCADO"),
     esperado(MONEDA, "Tasas de interés", "Tasas de interés · BADLAR · Bancos privados (TEA)", "Depósitos", "", "%")),
    (dmd("1194", "Tasa de interés de depósitos a plazo fijo, sector privado no financiero en dólares, personas humanas, TNA (en %)",
         "TASAS DE MERCADO"),
     esperado(MONEDA, "Tasas de interés",
              "Tasas de interés · Plazo fijo · Personas humanas (en dólares, TNA)", "Depósitos", "", "%")),
    (dmd("145", "Tasa de interés por adelantos en cuenta corriente en pesos, con acuerdo de 1 a 7 días y de 10 millones o más, a empresas del sector privado, TNA (en %)",
         "TASAS DE MERCADO"),
     esperado(MONEDA, "Tasas de interés",
              "Tasas de interés · Adelantos en cuenta corriente a empresas · Acuerdo de 1 a 7 días y de 10 millones o más (TNA)",
              "Préstamos", "", "%")),
    (dmd("160", "Tasas de interés de política monetaria, TNA (en %)", "INSTRUMENTOS DEL BCRA"),
     esperado(MONEDA, "Tasas de interés", "Tasas de interés · Tasa de política monetaria (TNA)",
              "Política monetaria", "", "%")),
    (dmd("147", "Monto de operaciones de préstamos entre entidades financieras locales privados (BAIBAR, en millones de $)",
         "TASAS DE MERCADO"),
     esperado(MONEDA, "Mercado de call y pases", "Mercado de call y pases · BAIBAR · Monto operado",
              unidad="Millones de pesos")),
    (dmd("155", "Saldo de LELIQ y NOTALIQ (en millones de $)", "INSTRUMENTOS DEL BCRA"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Leliq y Notaliq", "Leliq y Notaliq", "",
              "Millones de pesos")),
    (dmd("171", "Tasas de interés de LEBAC en Pesos / LELIQ a 6 meses, TNA (en %)", "INSTRUMENTOS DEL BCRA"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Lebac y Leliq en pesos · A 6 meses (TNA)",
              "Lebac y Nobac", "Tasas de interés", "%")),
    (dmd("188", "Tasas de interés de LEBAC en dólares, con liquidación en dólares, de 3 meses, TNA (en %)",
         "INSTRUMENTOS DEL BCRA"),
     esperado(MONEDA, "Instrumentos del BCRA",
              "Instrumentos del BCRA · Lebac en dólares liquidables en dólares · A 3 meses (en dólares, TNA)",
              "Lebac y Nobac", "Tasas de interés", "%")),
    (dmd("164", "Tasas de interés del BCRA para pases activos en pesos a 1 días, TNA (en %)",
         "INSTRUMENTOS DEL BCRA"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Pases activos · A 1 día (TNA)", "Pases",
              "Tasas de interés", "%")),
    (dmd("193", "Margen sobre BADLAR Total de NOBAC de 2 Años (en %)", "INSTRUMENTOS DEL BCRA"),
     esperado(MONEDA, "Instrumentos del BCRA",
              "Instrumentos del BCRA · Nobac a 2 años · Margen sobre BADLAR total de bancos",
              "Lebac y Nobac", "Márgenes de Nobac", "%")),
    # --- bcra-dmd: reservas y tipo de cambio (Sector externo)
    (dmd("74", "Saldo de reservas internacionales (excluidas asignaciones DEG 2009, en millones de USD)", "RESERVAS"),
     esperado(EXTERNO, "Reservas internacionales", "Reservas internacionales · Brutas", unidad="Millones de dólares")),
    (dmd("78", "Variación diaria de reservas internacionales por compra de divisas (en millones de USD)", "RESERVAS"),
     esperado(EXTERNO, "Reservas internacionales", "Reservas internacionales · Variación diaria · Compra de divisas",
              "Variación diaria", "", "Millones de dólares")),
    (dmd("84", "Tipo de cambio peso / dólar estadounidense de valuación contable", "RESERVAS"),
     esperado(EXTERNO, "Tipo de cambio nominal", "Tipo de cambio nominal · Valuación contable",
              unidad="Pesos por dólar")),
    # --- 8.1 Tasas de interés
    (datos(H_TASAS, "89.1_TIB_0_0_20", "tasas_interes_badlar", "Tasas de interés publicadas por el BCRA. Badlar.  En porcentaje"),
     esperado(MONEDA, "Tasas de interés", "Tasas de interés · BADLAR · Bancos privados (TNA)", "Depósitos", "", "%")),
    (datos(H_TASAS, "89.1_TS_INTEIOS_0_M_34", "tasas_interes_activas_hipotecarios", "Tasas de interes activas. Hipotecarios"),
     esperado(MONEDA, "Tasas de interés", "Tasas de interés · Préstamos hipotecarios (TNA)", "Préstamos", "", "%")),
    (datos(H_TASAS, "89.1_IR_BCRA0_M_0_M_27", "instr_bcra_lebac_pesos_10_m", "Instrumentos del BCRA. LEBAC en pesos 10 meses"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Lebac en pesos · A 10 meses (TNA)",
              "Lebac y Nobac", "Tasas de interés", "%")),
    (datos(H_TASAS, "89.2_TS_INTEALL_0_D_18", "tasas_interes_call", "Tasas de interés publicadas por el BCRA. Call.", "D"),
     esperado(MONEDA, "Mercado de call y pases", "Mercado de call y pases · Call (TNA)", unidad="%")),
    # --- 8.2 Agregados monetarios
    (datos(H_AGREGADOS, "90.1_AMTMA_0_0_43", "agregados_monetarios_totales_m3_alternativa",
           "Agregados monetarios totales. M3*. En millones de pesos"),
     esperado(MONEDA, "Agregados monetarios", "Agregados monetarios · M3*", unidad="Millones de pesos")),
    (datos(H_AGREGADOS, "90.1_BMT_0_0_20", "base_monetaria_total", "Base monetaria total. Promedios mensuales en millones de pesos"),
     esperado(MONEDA, "Base monetaria", "Base monetaria (promedio mensual)", "Total", "", "Millones de pesos")),
    (datos(H_AGREGADOS, "90.1_DPACUPRI_0_0_38", "depositos_pf_ajustable_cer_uva_publico",
           "Depósitos del sector privado y del sector público. Plazos fijos ajustables por CER/UVA sector público"),
     esperado(MONEDA, "Depósitos", "Depósitos · Plazo fijo ajustable por CER/UVA · Sector público", "Sector público",
              "En pesos", "Millones de pesos")),
    (datos(H_AGREGADOS, "90.1_DTDT_0_0_31", "depositos_totales_dolares_total",
           "Depósitos del sector privado y del sector público. Total en dólares"),
     esperado(MONEDA, "Depósitos", "Depósitos · Total (en dólares, expresados en pesos)", "Total", "En dólares",
              "Millones de pesos")),
    (datos(H_AGREGADOS, "90.1_DET_DESAGRIAS_0_0_50", "det_desagreg_plazo_fijo_sec_priv_pesos_90_179_dias",
           "Detalle desagregado de Plazo Fijo Sector Privado en pesos de 90 a 179 días"),
     esperado(MONEDA, "Depósitos", "Depósitos · Plazo fijo de 90 a 179 días · Sector privado", "Sector privado",
              "En pesos", "Millones de pesos")),
    (datos(H_AGREGADOS, "90.1_DET_DESAGRTAS_0_0_52", "det_desagreg_plazo_fijo_sec_priv_usd_mayoristas",
           "Detalle desagregado de Plazo Fijo Sector Privado en dolares. Mayoristas."),
     esperado(MONEDA, "Depósitos", "Depósitos · Plazo fijo mayorista · Sector privado (en dólares)", "Sector privado",
              "En dólares", "Millones de dólares")),
    (datos(H_AGREGADOS, "90.1_DET_DESAGRTAL_0_0_34", "det_desagreg_caja_ahorro_usd_total",
           "Detalle desagregado depósitos del sector privado y del sector público en dólares (en mill. USD). Total de cajaa de ahorro en dolares."),
     esperado(MONEDA, "Depósitos", "Depósitos · Caja de ahorro (en dólares)", "Total", "En dólares",
              "Millones de dólares")),
    # --- 8.3 Préstamos
    (datos(H_PRESTAMOS, "91.1_DETALLE_PRTAS_0_0_60", "detalle_prestamos_sector_priv_pesos_privado_consumo_tarjetas",
           "Detalle préstamos al sector privado en pesos: consumo tarjetas"),
     esperado(MONEDA, "Préstamos", "Préstamos · Tarjetas de crédito · Sector privado", "En pesos", "",
              "Millones de pesos")),
    (datos(H_PRESTAMOS, "91.1_DETALLE_PRTOS_0_0_57", "detalle_prestamos_sect_priv_dolares_comerciales_adelantos",
           "Detalle Préstamos al sector privado es dolares: comerciales adelantos"),
     esperado(MONEDA, "Préstamos", "Préstamos · Adelantos en cuenta corriente · Sector privado (en dólares)",
              "En dólares", "", "Millones de dólares")),
    (datos(H_PRESTAMOS, "91.1_PEFPC_0_0_35", "prestamos_sect_priv_pesos_y_dolares",
           "Préstamos al sector privado en pesos y dólares"),
     esperado(MONEDA, "Préstamos", "Préstamos · Total · Sector privado (en pesos y dólares, expresados en pesos)",
              "En pesos y dólares", "", "Millones de pesos")),
    (datos(H_PRESTAMOS, "91.1_PRESTAMOS_EAL_0_0_39", "prestamos_sect_priv_pesos_garantia_real",
           "Préstamos al sector privado en pesos: garantía real"),
     esperado(MONEDA, "Préstamos", "Préstamos · Garantía real · Sector privado", "En pesos", "", "Millones de pesos")),
    # --- 8.5 Encajes
    (datos(H_ENCAJES, "93.1_P35DCI_0_0_19", "pf_30_59_dias_cat_i", "Plazo fijo entre 30-59 días en millones de pesos. Categoría I"),
     esperado(MONEDA, "Encajes", "Encajes · Exigencia · Plazo fijo de 30 a 59 días · Categoría I", "Categoría I", "", "%")),
    (datos(H_ENCAJES, "93.1_UUM9DCIV_0_0_30", "uva_uvis_mas_90_dias_cat_ii_vi",
           "UVA y UVIs en millones de pesos a más de 90 días. Categoría II-VI"),
     esperado(MONEDA, "Encajes", "Encajes · Exigencia · Depósitos en UVA y UVI de más de 90 días · Categorías II a VI",
              "Categorías II a VI", "", "%")),
    (datos(H_ENCAJES, "93.1_PM3D_0_0_15", "pf_mas_360_dias", "Plazo fijo a más de 365 días en millones de pesos"),
     esperado(MONEDA, "Encajes", "Encajes · Exigencia · Plazo fijo de más de 365 días", "Series históricas", "", "%")),
    # --- 8.6 Coeficientes
    (datos(H_COEFICIENTES, "94.2_UVAD_D_0_0_10", "uva_diario", "UVA diario", "D"),
     esperado(MONEDA, "Coeficientes de actualización", "Coeficientes de actualización · UVA", unidad="Pesos")),
    (datos(H_COEFICIENTES, "447.1_TOTAL_REGIADO_0_0_16_61", "total_registrado", "Índice de salarios total registrado", "D"),
     esperado(MONEDA, "Coeficientes de actualización", "Coeficientes de actualización · CVS · Salarios registrados",
              unidad="Índice oct-2016=100")),
    # --- 8.7 Series históricas
    (datos(H_HISTORICAS, "174.1_AGADOS_M1_0_0_16", "agregados_usd_m1", "Agregados USD M1"),
     esperado(MONEDA, "Agregados monetarios", "Agregados monetarios · M1 (en dólares, expresados en pesos)",
              "Series históricas", "", "Millones de pesos")),
    (datos(H_HISTORICAS, "174.1_AGADOS_ION_0_0_44", "agregados_monetarios_en_pesos_bym_en_circulacion",
           "Agregados Monetarios en $ ByM en circulación"),
     esperado(MONEDA, "Base monetaria", "Base monetaria · Circulación monetaria · En poder del público",
              "Series históricas", "", "Millones de pesos")),
    (datos(H_HISTORICAS, "174.1_DSITOS_TAL_I_0_0_33", "depositos_al_sector_publico_total", "Depósitos al sector público Total"),
     esperado(MONEDA, "Depósitos", "Depósitos · Total · Sector público (en pesos y dólares, expresados en pesos)",
              "Series históricas", "", "Millones de pesos")),
    (datos(H_HISTORICAS, "174.1_PTAMOS_USD_0_0_31", "prestamos_al_sector_privado_usd", "Préstamos al sector privado USD"),
     esperado(MONEDA, "Préstamos", "Préstamos · Total · Sector privado (en dólares, expresados en pesos)",
              "Series históricas", "", "Millones de pesos")),
    (datos(H_HISTORICAS, "174.1_RRVAS_IIOS_0_0_60", "reservas_internacionales_bcra_prom_mensual_de_saldos_diarios",
           "Reservas Internacionales BCRA Promedio mensual de saldos diarios"),
     esperado(EXTERNO, "Reservas internacionales", "Reservas internacionales · Brutas (promedio mensual)",
              "Series históricas", "", "Millones de dólares")),
    (datos(H_HISTORICAS, "174.1_T_DE_CANTO_0_0_37", "tipo_de_cambio_moneda_de_cada_momento",
           "Tipo de Cambio Moneda de cada momento"),
     esperado(EXTERNO, "Tipo de cambio nominal", "Tipo de cambio nominal (en moneda de cada momento)",
              "Series históricas", "", "Moneda nacional de cada momento por dólar")),
    (datos(H_HISTORICAS, "174.1_T_INTERUS_0_0_43", "tasa_interes_deposito_plazo_fijo_30_59d_usd",
           "Tasa de interés Por depósitos a plazo fijo de 30 a 59 días De moneda extranjera"),
     esperado(MONEDA, "Tasas de interés", "Tasas de interés · Plazo fijo de 30 a 59 días (en dólares, TNA)",
              "Series históricas", "", "%")),
    (datos(H_HISTORICAS, "174.1_T_INTERO_0_0_57", "tasa_interes_prestamos_interfinancieros_hasta_15d_pesos",
           "Tasa de interés Por préstamos interfinancieros hasta 15 días de plazo De moneda nacional"),
     esperado(MONEDA, "Mercado de call y pases", "Mercado de call y pases · Préstamos interfinancieros hasta 15 días (TNA)",
              "Series históricas", "", "%")),
    # --- 8.9 Balance del BCRA
    (datos(H_BALANCE, "300.1_AP_ACT_RESTER_0_M_21", "ap_act_reservas_inter", "Apertura activo. Reservas internacionales."),
     esperado(MONEDA, "Balance del BCRA", "Balance del BCRA · Reservas internacionales", "Activo",
              "Reservas internacionales", "Miles de pesos")),
    (datos(H_BALANCE, "300.2_AP_ACT_TIT112_0_A_60", "ap_act_tit_pub_ley_nac_ltas_intransf_tn_vto_2022_resol_13112",
           "Apertura activo. Letra intransferible vencimiento 2022 Resol 131/12", "A"),
     esperado(MONEDA, "Balance del BCRA",
              "Balance del BCRA · Títulos públicos · Bajo ley nacional · Letras intransferibles · Vencimiento 2022 (Res. 131/12)",
              "Activo", "Títulos públicos", "Miles de pesos")),
    (datos(H_BALANCE, "300.1_AP_PAS_BASIRC_0_M_50", "ap_pas_base_monet_circ_monet_billetes_monedas_circ",
           "Apertura pasivo. Apertura de base monetaria. Apertura de circulación monetaria. Billetes y monedas en circulación."),
     esperado(MONEDA, "Balance del BCRA", "Balance del BCRA · Base monetaria · Circulación monetaria · Billetes y monedas en circulación",
              "Pasivo", "Base monetaria", "Miles de pesos")),
    (datos(H_BALANCE, "300.1_TOTAL_PATRETO_0_M_21", "total_patrimonio_neto", "Total patrimonio neto"),
     esperado(MONEDA, "Balance del BCRA", "Balance del BCRA · Patrimonio neto", "Patrimonio neto", "", "Miles de pesos")),
    (datos(H_BALANCE, "300.1_TIPO_CAMBIBIO_0_M_11", "tipo_cambio", "Tipo de cambio"),
     esperado(EXTERNO, "Tipo de cambio nominal", "Tipo de cambio nominal · Valuación contable",
              unidad="Pesos por dólar")),
    # --- 8.10 Lebac en el mercado secundario
    (datos(H_LEBAC, "304.1_LEBAQ_INTE1_4_0_0_28", "lebac_interna_pesos_2017_1_4",
           "Tasa de interes interna de lebac según fecha de corte 2017-01-04", "D"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Lebac en pesos · Vencimiento 2017-01-04 (TNA)",
              "Lebac y Nobac", "Tasas en el mercado secundario", "Proporción (1 = 100%)")),
    # La descripción dice 2017-12-15 pero la serie termina el 2017-11-14: vale la fecha de la variable.
    (datos(H_LEBAC, "304.1_LEBAQ_INTE_15_0_0_30", "lebac_interna_pesos_2017_11_15",
           "Tasa de interes interna de lebac según fecha de corte 2017-12-15", "D"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Lebac en pesos · Vencimiento 2017-11-15 (TNA)",
              "Lebac y Nobac", "Tasas en el mercado secundario", "Proporción (1 = 100%)")),
    # Variable y descripción dicen 2018-10-21, pero la serie llega hasta el 2018-11-20.
    (datos(H_LEBAC, "304.1_LEBAQ_INTE_21_IIII_0_0_30_12", "lebac_interna_pesos_2018_10_21",
           "Tasa de interes interna de lebac según fecha de corte 2018-10-21", "D"),
     esperado(MONEDA, "Instrumentos del BCRA", "Instrumentos del BCRA · Lebac en pesos · Vencimiento 2018-11-21 (TNA)",
              "Lebac y Nobac", "Tasas en el mercado secundario", "Proporción (1 = 100%)")),
    # --- 8.11 Factores de explicación de la base monetaria
    (datos(H_FACTORES, "331.1_PASES_REDELIQ_M_MONE_0_24_24", "pases_redescuentos_leliq", "Pases y Redescuentos: Leliq"),
     esperado(MONEDA, "Base monetaria", "Base monetaria · Factores de explicación · Pases y redescuentos · Leliq y Notaliq",
              "Factores de explicación", "Pases y redescuentos", "Millones de pesos")),
    (datos(H_FACTORES, "331.1_TOTAL_VARIION__27", "total_variacion", "Total variación"),
     esperado(MONEDA, "Base monetaria", "Base monetaria · Factores de explicación · Total",
              "Factores de explicación", "Total", "Millones de pesos")),
    (datos(H_FACTORES, "331.2_OTROSROS__13", "otros", "Otros", "D"),
     esperado(MONEDA, "Base monetaria", "Base monetaria · Factores de explicación · Otros",
              "Factores de explicación", "Otros", "Millones de pesos")),
)


class PruebasMonedaBcra(unittest.TestCase):
    def test_casos_representativos(self):
        for serie, esperada in CASOS:
            with self.subTest(serie=f"{serie.fuente}::{serie.id}"):
                self.assertEqual(clasificar(serie), esperada)

    def test_hay_al_menos_25_casos(self):
        self.assertGreaterEqual(len(CASOS), 25)

    def test_series_fuera_de_alcance(self):
        ajena_fuente = Serie(
            fuente="indec-ipc", id="1", nombre="IPC", variable="", descripcion="", unidad="", frecuencia="M",
            archivo="ipc.xlsx", hoja="Cuadro 1", columna="IPC", dataset="IPC", estado="VIGENTE")
        self.assertIsNone(clasificar(ajena_fuente))
        # Hojas de dinero_bancos.xlsx que son de otros módulos.
        self.assertIsNone(clasificar(datos("8.4 Res. Int. y Pasivos BCRA", "x", "reservas", "Reservas")))
        self.assertIsNone(clasificar(datos("8.12 Indicadores financieros", "x", "liquidez", "Liquidez")))
        # Misma hoja pero otro archivo.
        otro = Serie(
            fuente="datos.gob.ar", id="2", nombre="t", variable="base_monetaria_total", descripcion="",
            unidad="", frecuencia="M", archivo="otro.xlsx", hoja=H_AGREGADOS, columna="c", dataset="t",
            estado="VIGENTE")
        self.assertIsNone(clasificar(otro))
        # Un ID de bcra-dmd que no es de este alcance.
        self.assertIsNone(clasificar(dmd("99999", "Algo nuevo (en %)", "OTRA HOJA")))

    def test_variable_desconocida_no_se_inventa(self):
        self.assertIsNone(clasificar(datos(H_TASAS, "x", "tasas_interes_inventada", "Tasa inventada")))
        self.assertIsNone(clasificar(datos(H_BALANCE, "x", "ap_act_inventada", "Apertura activo. Inventada.")))

    def test_idempotente(self):
        for serie, _ in CASOS:
            self.assertEqual(clasificar(serie), clasificar(serie))

    def test_el_nombre_no_lleva_unidad_ni_frecuencia(self):
        for serie, esperada in CASOS:
            nombre = clasificar(serie).nombre
            self.assertNotIn("en millones", nombre, serie.id)
            self.assertNotIn("(en %)", nombre, serie.id)


if __name__ == "__main__":
    unittest.main()
