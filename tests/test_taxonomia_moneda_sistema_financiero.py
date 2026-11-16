"""Pruebas de las reglas del Informe sobre Bancos del BCRA (hojas 8.12, 8.13 y 8.14)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.moneda_sistema_financiero import clasificar

H12 = "8.12 Indicadores financieros"
H13 = "8.13 Rentabilidad financiera"
H14 = "8.14 Situacion Patrimonial"
TITULOS = {
    H12: "Principales Indicadores del Sistema Financiero. Datos {tipo}",
    H13: "Rentabilidad del sistema financiero. Datos {tipo}",
    H14: "Situación patrimonial del sistema financiero. Datos {tipo}",
}
TIPOS = {"A": "anuales", "M": "mensuales"}


def serie(hoja, id_, frecuencia, variable, descripcion, fuente="datos.gob.ar", archivo="dinero_bancos.xlsx"):
    titulo = TITULOS[hoja].format(tipo=TIPOS[frecuencia])
    return Serie(
        fuente=fuente, id=id_, nombre=titulo, variable=variable, descripcion=descripcion, unidad="",
        frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=id_, dataset=titulo.split(". ")[0],
        estado="VIGENTE",
    )


I, R, P = ("Indicadores del sistema financiero", "Rentabilidad del sistema financiero",
           "Situación patrimonial del sistema financiero")
SF, BP, BV = "Sistema financiero", "Bancos públicos", "Bancos privados"
BN, BE, NB = "Bancos privados nacionales", "Bancos privados extranjeros", "Entidades financieras no bancarias"
T = "Moneda y sistema financiero"


def c(indicador, cobertura, nombre, unidad, apertura=""):
    return Clasificacion(tema=T, indicador=indicador, cobertura=cobertura, apertura=apertura,
                         nombre=nombre, unidad=unidad)


CASOS = (
    # 8.12 Indicadores: liquidez (en pesos, amplia, con pases y la variante sin prefijo)
    (serie(H12, "332.2_BANCOS_PRIDEZ__22_11", "M", "bancos_priv_2_liquidez_pesos", "Bancos Privados liquidez en pesos"),
     c(I, BV, "Liquidez · En pesos · Bancos privados (% de los depósitos en pesos)", "%")),
    (serie(H12, "332.2_SISTEMA_FILIA__24", "M", "sistema_finan_1_liquidez_amplia", "Sistema Financiero liquidez amplia"),
     c(I, SF, "Liquidez amplia (% de los depósitos)", "%")),
    (serie(H12, "332.2_LIQUIDEZ_CSES__18", "M", "liquidez_con_pases", "Sistema Financiero liquidez con pases"),
     c(I, SF, "Liquidez con pases (% de los depósitos)", "%")),
    (serie(H12, "332.1_SISTEMA_FISES__32", "A", "sistema_finan_liquidez_con_pases", "Sistema Financiero liquidez con pases"),
     c(I, SF, "Liquidez con pases (% de los depósitos)", "%")),
    (serie(H12, "332.2_BANCOS_PRISES__42", "M", "bancos_priv_extranjeros_liquidezconpases",
           "Bancos Privados extranjeros liquidez con pases"),
     c(I, BE, "Liquidez con pases · Bancos privados extranjeros (% de los depósitos)", "%")),
    (serie(H12, "332.1_BANCOS_PRISES__44", "A", "bancos_priv_extranjeros_2_liquidez_con_pases",
           "Bancos Privados extranjeros liquidez con pases"),
     c(I, BE, "Liquidez con pases · Bancos privados extranjeros (% de los depósitos)", "%")),
    # 8.12: cartera, exposición, crédito
    (serie(H12, "332.2_SISTEMA_FIADA__53", "M", "sistema_finan_5_irregularidad_cartera_privada",
           "Sistema Financiero irregularidad cartera privada"),
     c(I, SF, "Irregularidad de la cartera privada (% de la cartera privada)", "%")),
    (serie(H12, "332.2_ENTIDADES_RIV__45", "M", "entidades_finan_no_banc_6_exposicion_pat_sector_priv",
           "Entidades Financieras no Bancarias Exposición pat sector priv"),
     c(I, NB, "Exposición patrimonial al sector privado · Entidades financieras no bancarias (% del patrimonio neto)", "%")),
    (serie(H12, "332.2_SISTEMA_FIADO__45", "M", "sistema_finan_4_credito_sector_privado",
           "Sistema Financiero Crédito sector privado"),
     c(I, SF, "Crédito al sector privado (% del activo)", "%")),
    (serie(H12, "332.1_SISTEMA_FIICO__38", "A", "sistema_finan_3_credito_sector_publico",
           "Sistema Financiero Crédito sector publico"),
     c(I, SF, "Crédito al sector público (% del activo)", "%")),
    # 8.12: rentabilidad y eficiencia
    (serie(H12, "332.2_SISTEMA_FIUAL__42", "M", "sistema_finan_8_roe_acumulado_anual", "Sistema Financiero roe acumulado anual"),
     c(I, SF, "ROE (% anualizado del patrimonio neto)", "%")),
    (serie(H12, "332.1_BANCOS_PUBUAL__37", "A", "bancos_publicos_7_roa_acumulado_anual", "Bancos Públicos roa acumulado anual"),
     c(I, BP, "ROA · Bancos públicos (% anualizado del activo neteado)", "%")),
    (serie(H12, "332.2_BANCOS_PUBUAL__40", "M", "bancos_publicos_9_eficiencia_acumulado_anual",
           "Bancos Públicos eficiencia acumulado anual"),
     c(I, BP, "Eficiencia · Bancos públicos (margen financiero y servicios, % de los gastos de administración)", "%")),
    # 8.12: capital, con las distintas grafías de la fuente
    (serie(H12, "332.2_BANCOS_PRIAL___50", "M", "bancos_priv_10a_integracion_capital_", "Bancos Privados Integración capital"),
     c(I, BV, "Integración de capital · Bancos privados (% de los activos ponderados por riesgo)", "%")),
    (serie(H12, "332.2_BANCOS_PRITAL__54", "M", "bancos_priv_nacionales_10a_integracion_capital",
           "Bancos Privados nacionales Integración capital"),
     c(I, BN, "Integración de capital · Bancos privados nacionales (% de los activos ponderados por riesgo)", "%")),
    (serie(H12, "332.2_SISTEMA_FIL_1__57", "M", "sistema_finan_11a_integracion_capital_nivel_1",
           "Sistema Financiero Integración capital nivel 1"),
     c(I, SF, "Integración de capital · Nivel 1 (% de los activos ponderados por riesgo)", "%")),
    (serie(H12, "332.2_BANCOS_PRIRED__43", "M", "bancos_priv_nacionales_11b_integ_cap_nivel_1_riesgo_cred",
           "Bancos Privados nacionales Integración cap nivel riesgo cred"),
     c(I, BN, "Integración de capital · Nivel 1 · Riesgo de crédito · Bancos privados nacionales "
              "(% de los activos ponderados por riesgo de crédito)", "%")),
    (serie(H12, "332.2_BANCOS_PRIRED__55", "M", "bancos_priv_extranjeros_10b_integracion_cap_riesgo_cred",
           "Bancos Privados extranjeros Integración cap riesgo cred"),
     c(I, BE, "Integración de capital · Riesgo de crédito · Bancos privados extranjeros "
              "(% de los activos ponderados por riesgo de crédito)", "%")),
    (serie(H12, "332.2_BANCOS_PRITAL__40", "M", "bancos_priv_12_posicion_capital", "Bancos Privados Posición capital"),
     c(I, BV, "Posición de capital · Bancos privados (exceso sobre la exigencia, % de la exigencia)", "%")),
    # 8.13 Rentabilidad
    (serie(H13, "333.2_SIST_FINANANC_M_25", "M", "sist_financ_margen_financ", "Sist financ margen financ"),
     c(R, SF, "Margen financiero", "Millones de pesos")),
    (serie(H13, "333.1_SIST_FINANSES_A_48", "A", "sist_financ_margen_financ_ingresos_por_intereses",
           "Sist financ margen financ ingresos por intereses"),
     c(R, SF, "Margen financiero · Ingresos por intereses", "Millones de pesos")),
    (serie(H13, "333.2_BANCOS_PRIRES_M_55", "M", "bancos_priv_nac_margen_financ_resultado_titulos_valores",
           "Bancos priv nac margen financ resultado titulos valores"),
     c(R, BN, "Margen financiero · Resultado por títulos valores · Bancos privados nacionales", "Millones de pesos")),
    (serie(H13, "333.2_BANCOS_EXTION_M_53", "M", "bancos_ext_priv_margen_financ_diferencias_cotizacion",
           "Bancos ext priv margen financ diferencias cotizacion"),
     c(R, BE, "Margen financiero · Diferencias de cotización · Bancos privados extranjeros", "Millones de pesos")),
    (serie(H13, "333.2_BANCOS_PUBTAL_M_26", "M", "bancos_pub_resultado_total", "Bancos pub resultado total"),
     c(R, BP, "Resultado total · Bancos públicos", "Millones de pesos")),
    (serie(H13, "333.2_BANCOS_PRIADO_M_50", "M", "bancos_priv_nac_resultado_total_integrado_ajustado",
           "Bancos priv nac resultado total integrado ajustado"),
     c(R, BN, "Resultado total integrado ajustado · Bancos privados nacionales", "Millones de pesos")),
    (serie(H13, "333.2_SIST_FINANUST_M_43", "M", "sist_financ_resultado_total_integrado_ajust",
           "Sist financ resultado total Integrado ajustado"),
     c(R, SF, "Resultado total integrado ajustado", "Millones de pesos")),
    (serie(H13, "333.2_SIST_FINANORI_M_15", "M", "sist_financ_ori", "Sist financ ori"),
     c(R, SF, "Otros resultados integrales", "Millones de pesos")),
    (serie(H13, "333.2_BANCOS_PRIION_M_33", "M", "bancos_priv_gastos_administracion", "Bancos priv gastos administracion"),
     c(R, BV, "Gastos de administración · Bancos privados", "Millones de pesos")),
    (serie(H13, "333.1_ENTI_FINANPUB_A_57", "A", "enti_finan_no_banc_ajustes_valuacion_prestamos_sector_pub",
           "Datos enti finan no banc ajustes valuacion préstamos al sector publico"),
     c(R, NB, "Ajustes de valuación de préstamos al sector público · Entidades financieras no bancarias",
       "Millones de pesos")),
    (serie(H13, "333.2_ENTI_FINANDAD_M_44", "M", "enti_finan_no_banc_cargos_por_incobrabilidad",
           "Datos enti finan no banc cargos por incobrabilidad"),
     c(R, NB, "Cargos por incobrabilidad · Entidades financieras no bancarias", "Millones de pesos")),
    # 8.14 Situación patrimonial
    (serie(H14, "334.2_SIST_FINANIVO__24", "M", "sist_financ_total_activo", "Sistema Financiero. Total Activo"),
     c(P, SF, "Activo", "Millones de pesos", "Activo")),
    (serie(H14, "334.1_SIST_FINANADO__38", "A", "sist_financ_activo_neteado_consolidado",
           "Sistema Financiero. Activo neteado consolidado"),
     c(P, SF, "Activo neteado consolidado", "Millones de pesos", "Activo")),
    (serie(H14, "334.2_BCOS_PUB_AADO__48", "M", "bcos_pub_activo_detalle_prestamos_sector_privado",
           "Bancos Públicos. Activo. Préstamos. Sector Privado"),
     c(P, BP, "Activo · Préstamos · Sector privado no financiero · Bancos públicos", "Millones de pesos", "Activo")),
    (serie(H14, "334.2_BCOS_PRIV_IVO__44", "M", "bcos_priv_activo_por_operaciones_pase_activo",
           "Bancos Privados. Activo. Títulos Públicos. Lebac y Nobac. Tenencia por operaciones de pase activo"),
     c(P, BV, "Activo · Títulos públicos · Lebac y Nobac · Tenencia por operaciones de pase activo · Bancos privados",
       "Millones de pesos", "Activo")),
    (serie(H14, "334.2_EFNB_ACTIVBIR__50", "M", "efnb_activo_otros_cred_inter_finan_compens_recibir",
           "Entidades Financieras No Bancarias. Activo. Otros créditos por intermediación financieraCompensación a recibir"),
     c(P, NB, "Activo · Otros créditos por intermediación financiera · Compensación a recibir · "
              "Entidades financieras no bancarias", "Millones de pesos", "Activo")),
    (serie(H14, "334.2_BCOS_PRIV_IJO31__56", "A", "bcos_priv_ext_pasivo_depositos_sector_privado_plazo_fijo",
           "Bancos Privados Extranjeros. Pasivo. Depósitos Sector Privado.  Plazo Fijo"),
     c(P, BE, "Pasivo · Depósitos · Sector privado no financiero · Plazo fijo · Bancos privados extranjeros",
       "Millones de pesos", "Pasivo y patrimonio neto")),
    (serie(H14, "334.2_BCOS_PRIV_EXT__58", "M", "bcos_priv_nac_pasivo_otras_oblig_inter_finan_prestamos_ext",
           "Bancos Privados Nacionales. Pasivo. Otras obligaciones por intermediación financiera. Líneas de préstamos del exterior"),
     c(P, BN, "Pasivo · Otras obligaciones por intermediación financiera · Líneas de préstamos del exterior · "
              "Bancos privados nacionales", "Millones de pesos", "Pasivo y patrimonio neto")),
    (serie(H14, "334.1_SIST_FINANIVO_56__24", "A", "sist_financ_total_pasivo", "Sistema Financiero. Total Pasivo"),
     c(P, SF, "Pasivo", "Millones de pesos", "Pasivo y patrimonio neto")),
    (serie(H14, "334.2_BCOS_PRIV_ETO__25", "M", "bcos_priv_patrimonio_neto", "Bancos Privados. Patrimonio neto"),
     c(P, BV, "Patrimonio neto · Bancos privados", "Millones de pesos", "Pasivo y patrimonio neto")),
    (serie(H14, "334.2_BCOS_PRIV_MOS14__46", "M", "bcos_priv_ext_activo_previsiones_por_prestamos",
           "Bancos Privados Extranjeros. Activo. Previsiones por Préstamos"),
     c(P, BE, "Activo · Previsiones por préstamos · Bancos privados extranjeros", "Millones de pesos", "Activo")),
)


class PruebasSistemaFinanciero(unittest.TestCase):
    def test_casos_representativos(self):
        for entrada, esperado in CASOS:
            with self.subTest(id=entrada.id):
                self.assertEqual(clasificar(entrada), esperado)

    def test_hay_casos_de_cada_hoja(self):
        self.assertGreaterEqual(len(CASOS), 25)
        self.assertEqual({entrada.hoja for entrada, _ in CASOS}, {H12, H13, H14})

    def test_fuera_de_alcance(self):
        base = CASOS[0][0]
        self.assertIsNone(clasificar(serie(H12, base.id, "M", base.variable, base.descripcion, fuente="bcra")))
        self.assertIsNone(clasificar(serie(H12, base.id, "M", base.variable, base.descripcion, archivo="otro.xlsx")))
        otra_hoja = Serie(**{**base.__dict__, "hoja": "8.9 Balance BCRA"})
        self.assertIsNone(clasificar(otra_hoja))

    def test_misma_serie_anual_y_mensual_mismo_nombre(self):
        anual = serie(H12, "332.1_SISTEMA_FISES__32", "A", "sistema_finan_liquidez_con_pases",
                      "Sistema Financiero liquidez con pases")
        mensual = serie(H12, "332.2_LIQUIDEZ_CSES__18", "M", "liquidez_con_pases",
                        "Sistema Financiero liquidez con pases")
        self.assertEqual(clasificar(anual).nombre, clasificar(mensual).nombre)

    def test_idempotente(self):
        for entrada, _ in CASOS:
            self.assertEqual(clasificar(entrada), clasificar(entrada))

    def test_partida_desconocida_no_se_inventa(self):
        self.assertIsNone(clasificar(serie(H13, "x", "M", "sist_financ_partida_nueva", "Sist financ partida nueva")))
        self.assertIsNone(clasificar(serie(H14, "x", "M", "sist_financ_otra", "Sistema Financiero. Activo. Algo nuevo")))


if __name__ == "__main__":
    unittest.main()
