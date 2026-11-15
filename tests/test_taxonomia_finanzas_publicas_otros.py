"""Pruebas de las reglas de finanzas públicas de ``finanzas_publicas.xlsx`` (datos.gob.ar)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.finanzas_publicas_otros import clasificar

TEMA = "Finanzas públicas"
AIF = "Cuenta ahorro-inversión-financiamiento (base devengado)"
GASTO = "Gasto público consolidado"
POLITICA = "Gasto en política de ingresos"
PRESTACIONES = "Prestaciones de la seguridad social"
RECAUDACION = "Recaudación tributaria"
MILLONES = "Millones de pesos"


def serie(hoja: str, id_: str, variable: str, descripcion: str = "", frecuencia: str = "A",
          nombre: str = "", fuente: str = "datos.gob.ar", archivo: str = "finanzas_publicas.xlsx") -> Serie:
    return Serie(
        fuente=fuente, id=id_, nombre=nombre or descripcion or variable, variable=variable,
        descripcion=descripcion, unidad="", frecuencia=frecuencia, archivo=archivo, hoja=hoja,
        columna=id_, dataset=nombre, estado="VIGENTE",
    )


def esperado(indicador, cobertura, apertura, nombre, unidad):
    return Clasificacion(tema=TEMA, indicador=indicador, cobertura=cobertura, apertura=apertura,
                         nombre=nombre, unidad=unidad)


CASOS = (
    # --- Cuenta devengado: un caso por bloque y por forma de nombre ---
    (serie("AC_Dev_61", "432.1_REC_CORR_IBUT_DEVE_0_20_16", "rec_corr_ingr_tribut",
           "Recursos corrientes . Ingresos tributarios"),
     esperado(AIF, "Administración central", "Recursos",
              "Recursos corrientes · Ingresos tributarios · Administración central", MILLONES)),
    (serie("AC_Dev_61", "432.4_REC_CORR_IBUT_DEVE_0_20_87", "rec_corr_ingr_tribut",
           "Recursos corrientes . Ingresos tributarios"),
     esperado(AIF, "Administración central", "Recursos",
              "Recursos corrientes · Ingresos tributarios · Administración central", MILLONES)),
    (serie("SPA_Dev_61", "439.4_TOTAL_RECUSOS_DEVE_0_14_30", "total_recursos", "Total de recursos"),
     esperado(AIF, "Sector Público Argentino", "Recursos", "Recursos totales", MILLONES)),
    (serie("AN_Dev_61", "436.3_TOTAL_REC_GUR_DEVE_0_20_28", "total_rec_desp_figur",
           "Total de recursos despues de figurativas"),
     esperado(AIF, "Administración nacional", "Recursos",
              "Recursos totales después de operaciones figurativas · Administración nacional", MILLONES)),
    (serie("OD_Dev_61", "434.2_EROG_CORR_NAL_DEVE_0_18_25", "erog_corr_personal",
           "Erogaciones corrientes . Personal"),
     esperado(AIF, "Organismos descentralizados", "Erogaciones",
              "Erogaciones corrientes · Personal · Organismos descentralizados", MILLONES)),
    (serie("PROV_MCBA_Dev_61", "437.3_EROG_CORR_RIV_DEVE_0_30_35", "erog_corr_transctescap_enspriv",
           "Erogaciones corrientes . Transferencias ctes y de capital . Enseñanza privada"),
     esperado(AIF, "Provincias y Municipalidad de la Ciudad de Buenos Aires", "Erogaciones",
              "Erogaciones corrientes · Transferencias · Enseñanza privada · "
              "Provincias y Municipalidad de la Ciudad de Buenos Aires", MILLONES)),
    (serie("EP_Dev_61", "438.3_EROG_CORR_DESQ_DEVE_0_20_1", "erog_corr_desq_cteemppubl",
           "Erogaciones corrientes . Desequilibrio cte de empresas públicas"),
     esperado(AIF, "Empresas públicas", "Erogaciones",
              "Erogaciones corrientes · Desequilibrio corriente de empresas públicas · Empresas públicas", MILLONES)),
    (serie("CE_Dev_61", "433.1_EROG_CAP_ISTR_DEVE_0_24_18", "erog_cap_inv_real_constr",
           "Erogaciones de capital . Inversión real . Construcciones"),
     esperado(AIF, "Cuentas especiales", "Erogaciones",
              "Erogaciones de capital · Inversión real · Construcciones · Cuentas especiales", MILLONES)),
    (serie("SPA_Dev_61", "439.2_TOTAL_EROGGUR_DEVE_0_26_14", "total_erog_prim_desp_figur",
           "Total de erogaciones primarias despues de figurativas"),
     esperado(AIF, "Sector Público Argentino", "Erogaciones",
              "Erogaciones primarias después de operaciones figurativas", MILLONES)),
    (serie("ISS_Dev_61", "435.1_RESULTADOADO_DEVE_0_9_73", "resultado", "Resultado"),
     esperado(AIF, "Instituciones de la seguridad social", "Resultados",
              "Resultado financiero · Instituciones de la seguridad social", MILLONES)),
    (serie("AC_Dev_61", "432.1_AHORRORRO_DEVE_0_6_6", "ahorro", "Ahorro"),
     esperado(AIF, "Administración central", "Resultados", "Ahorro corriente · Administración central", MILLONES)),
    (serie("AC_Dev_61", "432.1_NECES_FINAGUR_DEVE_0_23_99", "neces_finan_antes_figur",
           "Necesidad de financiamiento antes figurativas"),
     esperado(AIF, "Administración central", "Resultados",
              "Necesidad de financiamiento antes de operaciones figurativas · Administración central", MILLONES)),
    (serie("AN_Dev_61", "436.3_FIN_NETO_URED_DEVE_0_30_38", "fin_neto_usonetocr_ext_usocred",
           "Financiamiento neto . Uso neto del crédito . Externo . Uso del crédito"),
     esperado(AIF, "Administración nacional", "Financiamiento",
              "Uso neto del crédito externo · Uso del crédito · Administración nacional", MILLONES)),
    (serie("PROV_MCBA_Dev_61", "437.2_FIN_NETO_UONT_DEVE_0_27_90", "fin_neto_usonetoad_provcontejeant",
           "Financiamiento neto . Uso neto de los adelantos . Prov y cont ejercicio anterior"),
     esperado(AIF, "Provincias y Municipalidad de la Ciudad de Buenos Aires", "Financiamiento",
              "Adelantos a proveedores y contratistas del ejercicio anterior · "
              "Provincias y Municipalidad de la Ciudad de Buenos Aires", MILLONES)),
    (serie("OD_Dev_61", "434.4_FINAN_CONT_CE_DEVE_0_22_38", "finan_contrib_figur_ce",
           "Financiamiento por contribuciones figurativas . De cuentas especiales"),
     esperado(AIF, "Organismos descentralizados", "Financiamiento",
              "Contribuciones figurativas · De cuentas especiales · Organismos descentralizados", MILLONES)),
    (serie("SPA_Dev_61", "439.1_BCO_CTRAL_ARG_DEVE_0_17_43", "bco_ctral_rep_arg", "B.C.R.A"),
     esperado(AIF, "Sector Público Argentino", "Financiamiento",
              "Financiamiento del Banco Central de la República Argentina", MILLONES)),
    (serie("SPA_Dev_93", "398.1_RESULTADO_ERO_DEVE_A_20_94", "Resultado financiero devengado (1993-2018)"),
     esperado(AIF, "Sector Público Argentino", "Resultados", "Resultado financiero", MILLONES)),
    # --- Gasto público consolidado ---
    (serie("GPC_$", "451.1_GPCGPC_0_0_3_66", "gpc", "Gasto público consolidado"),
     esperado(GASTO, "Consolidado", "", "Gasto público · Total", MILLONES)),
    (serie("GPC_%", "451.2_GPC_PIBPIB_0_0_7_85", "gpc_pib", "Gasto público consolidado en porcentaje del PIB"),
     esperado(GASTO, "Consolidado", "", "Gasto público · Total (% del PIB)", "% del PIB")),
    (serie("GPN_$", "451.3_GPN_EDUCACICA_0_0_20_53", "gpn_educacion_basica",
           "Gasto público nacional en Educación básica"),
     esperado(GASTO, "Nación", "Servicios sociales", "Gasto público · Educación básica · Nación", MILLONES)),
    (serie("GPP_%", "451.6_GPP_ADMINIPIB_0_0_30_26", "gpp_administracion_general_pib",
           "Gasto público provincial en Administración general en porcentaje del PIB"),
     esperado(GASTO, "Provincias y Ciudad Autónoma de Buenos Aires", "Funcionamiento del Estado",
              "Gasto público · Administración general · Provincias y Ciudad Autónoma de Buenos Aires (% del PIB)",
              "% del PIB")),
    (serie("GPM_$", "451.7_GPM_INSSJYLUD_0_0_17_22", "gpm_inssjyp_salud",
           "Gasto público municipal en INSSJYP - atención de la salud"),
     esperado(GASTO, "Municipios", "Servicios sociales",
              "Gasto público · Atención de la salud del INSSJyP · Municipios", MILLONES)),
    (serie("GPC_$", "451.1_GPC_SERVICICA_0_0_27_71", "gpc_servicios_deuda_publica",
           "Gasto público consolidado en Servicios de la deuda pública"),
     esperado(GASTO, "Consolidado", "Servicios de la deuda pública",
              "Gasto público · Servicios de la deuda pública", MILLONES)),
    (serie("GPN_%", "451.4_GPN_SERVICPIB_0_0_28_68", "gpn_servicios_economicos_pib",
           "Gasto público nacional en Servicios económicos en porcentaje del PIB"),
     esperado(GASTO, "Nación", "Servicios económicos",
              "Gasto público · Servicios económicos · Nación (% del PIB)", "% del PIB")),
    # --- Gasto en política de ingresos ---
    (serie("Gasto Pol. Ing", "186.3_TOTAL_POLISOS_0_0_23", "total_politica_ingresos",
           "GPN en Política de Ingreso. Total. Devengado mensual .", "M"),
     esperado(POLITICA, "Total", "", "Gasto en política de ingresos · Total", MILLONES)),
    (serie("Gasto Pol. Ing", "186.1_JUBILACIONINO_0_0_53", "jubilaciones_pensiones_sist_int_previsional_argentino",
           "GPN en Jubilaciones y Pensiones del SIPA. Devengado mensual .", "A"),
     esperado(POLITICA, "Jubilaciones y pensiones", "",
              "Gasto en política de ingresos · Jubilaciones y pensiones del SIPA", MILLONES)),
    (serie("Gasto Pol. Ing", "186.2_PROGRAMA_RINA_0_0_39", "programa_respaldo_estudiantes_argentina",
           "GPN en PROG.R.ES.AR. Devengado mensual .", "T"),
     esperado(POLITICA, "Becas y programas para estudiantes", "",
              "Gasto en política de ingresos · PROGRESAR", MILLONES)),
    (serie("Gasto Pol. Ing", "186.1_PROGRAMA_JDOS_0_0_38", "programa_jefas_jefes_hogar_desocupados",
           "GPN en Programa Jefas y Jefes de Hogar Desocupados. Devengado mensual .", "A"),
     esperado(POLITICA, "Programas de empleo e inclusión social", "",
              "Gasto en política de ingresos · Programa Jefas y Jefes de Hogar Desocupados", MILLONES)),
    # --- Prestaciones de la seguridad social ---
    (serie("JUB, PNC; SEG DES.", "187.1_JUBILACIONINO_0_0_53", "jubilaciones_sistema_intregrado_previsional_argentino",
           "Jubilaciones de SIPA. BESS-MTESS. Anuales a diciembre"),
     esperado(PRESTACIONES, "Jubilaciones y pensiones", "",
              "Jubilaciones y pensiones · Jubilaciones · SIPA (cantidad de beneficios)", "Beneficios")),
    (serie("FUENTE ANSES PRESTACIONES", "189.1_JUBILACIONINO_2_0_0_53", "jubilaciones_sipa_datos_abiertos",
           "Jubilaciones de SIPA. ANSES-Abiertos. Mensuales", "M"),
     esperado(PRESTACIONES, "Jubilaciones y pensiones", "",
              "Jubilaciones y pensiones · Jubilaciones · SIPA (cantidad de beneficios)", "Beneficios")),
    (serie("Fuente Cuenta Inversion- Presta", "190.1_JUBILACIONLES_0_0_57",
           "jubilaciones_pensiones_retiros_otros_regimenes_nacionales",
           "Prestaciones previsionales de otros regímenes nacionales no integrados a SIPA. Anuales"),
     esperado(PRESTACIONES, "Jubilaciones y pensiones", "",
              "Jubilaciones y pensiones · Total · Otros regímenes nacionales (cantidad de beneficios)", "Beneficios")),
    (serie("JUB, PNC; SEG DES.", "187.1_PENSIONES_OBA_0_0_34", "pensiones_no_contributivas_cordoba",
           "PNC por provincia. Coridoba. BESS-MTESS. Anuales a diciembre"),
     esperado(PRESTACIONES, "Pensiones no contributivas", "Por jurisdicción",
              "Pensiones no contributivas · Total · Córdoba (cantidad de beneficios)", "Beneficios")),
    (serie("JUB, PNC; SEG DES.", "187.1_PENSIONES_DEZ_0_0_50", "pensiones_no_contributivas_asistenciales_invalidez",
           "PNC asistenciales por invalidez. BESS-MTESS. Anuales a diciembre"),
     esperado(PRESTACIONES, "Pensiones no contributivas", "Por tipo de pensión",
              "Pensiones no contributivas · Asistenciales por invalidez (cantidad de beneficios)", "Beneficios")),
    (serie("JUB, PNC; SEG DES.", "187.1_BENEFICIARLEO_0_0_53", "beneficiarios_sistema_integral_prestaciones_desempleo",
           "Beneficiarios del Sistema Integral de Prestaciones por Desempleo. BESS-MTESS. Anuales a diciembre"),
     esperado(PRESTACIONES, "Seguro de desempleo", "",
              "Seguro de desempleo (cantidad de beneficiarios)", "Beneficiarios")),
    (serie("AUH-AUE", "463.1_AUH_ASIGNA019_0_M_66_38", "AUH (Agosto 19 - Hoy)", "", "M"),
     esperado(PRESTACIONES, "Asignaciones familiares y universales", "Asignación Universal por Hijo",
              "Asignación Universal por Hijo · Total (cantidad de beneficios)", "Beneficios")),
    (serie("AUH-AUE", "188.1_AUE_ASIGNAAZO_0_0_23", "aue_asignacion_embarazo",
           "Asignación Universal por Embarazo (AUE). BESS-MTESS. Anuales a diciembre", "S"),
     esperado(PRESTACIONES, "Asignaciones familiares y universales", "Asignación Universal por Embarazo",
              "Asignación Universal por Embarazo (cantidad de beneficios)", "Beneficios")),
    (serie("FUENTE ANSES PRESTACIONES", "189.1_ASIGNACIONICO_0_0_44", "asignacion_universal_hijo_servicio_domestico",
           "Asignación Universal por Hijo o Hijo Discapacitado según condición del adulto asociado. "
           "Servicio Doméstico. ANSES-Boletín Mensual de AUH. Mensuales", "M"),
     esperado(PRESTACIONES, "Asignaciones familiares y universales", "Asignación Universal por Hijo",
              "Asignación Universal por Hijo · Adulto en servicio doméstico (cantidad de beneficios)", "Beneficios")),
    (serie("FUENTE ANSES PRESTACIONES", "189.1_ASIGNACIONIVA_0_0_55", "asignaciones_familiares_pasivos_pension_no_contributiva",
           "AAFF por Hijo o Hijo Discapacitado según condición del adulto asociado. Pasivos-PNC. "
           "ANSES-Boletín Mensual de AUH. Mensuales", "M"),
     esperado(PRESTACIONES, "Asignaciones familiares y universales", "Asignaciones familiares por hijo",
              "Asignaciones familiares por hijo · Adulto pasivo con pensión no contributiva (cantidad de beneficios)",
              "Beneficios")),
    (serie("FUENTE ANSES PRESTACIONES", "189.1_NINIOS_ADPADP_0_0_10", "ninios_adp",
           "Población total menor de 18 años en el Administrador de De Personas (ADP). "
           "ANSES-Boletín mensual de AUH. Mensuales", "M"),
     esperado(PRESTACIONES, "Asignaciones familiares y universales", "",
              "Asignaciones familiares y universales · Menores de 18 años en el Administrador de Personas "
              "(cantidad de personas)", "Personas")),
    (serie("FUENTE ANSES PRESTACIONES", "189.1_PENSION_UNYOR_0_0_30", "pension_universal_adulto_mayor",
           "Pensión Universal para el Adulto Mayor (PUAM). ANSES-Abiertos. Mensuales", "M"),
     esperado(PRESTACIONES, "Jubilaciones y pensiones", "",
              "Jubilaciones y pensiones · Pensión Universal para el Adulto Mayor (cantidad de beneficios)", "Beneficios")),
    (serie("PROGRESAR", "191.1_BENEFICIARSAR_0_0_23", "beneficiarios_progresar",
           "Beneficiarios de PROG.R.ES.AR. Trimestrales.", "T"),
     esperado(PRESTACIONES, "Becas y programas para estudiantes", "",
              "PROGRESAR (cantidad de beneficiarios)", "Beneficiarios")),
    (serie("Fuente Cuenta Inversion- Presta", "190.1_BECAS_ESTULES_0_0_19", "becas_estudiantiles",
           "Becas Estudiantiles. Anuales"),
     esperado(PRESTACIONES, "Becas y programas para estudiantes", "",
              "Becas estudiantiles (cantidad de beneficios)", "Beneficios")),
    # --- Recaudación tributaria ---
    (serie("RECA", "142.1_TOTAL_2001_A_26", "total_recursos_tributarios",
           "Total recursos tributarios de los Recursos tributarios totales y por tributo. Anual (Base 2001)"),
     esperado(RECAUDACION, "Total", "", "Recaudación · Total", MILLONES)),
    (serie("RECA", "142.3_IVA_D_2001_M_7", "iva_dgi",
           "Iva dgi de los Recursos tributarios totales y por tributo. Mensual (Base 2001)", "M"),
     esperado(RECAUDACION, "Por tributo", "IVA", "Recaudación · IVA · Dirección General Impositiva", MILLONES)),
    (serie("RECA", "142.2_GANAN_2001_T_9", "ganancias",
           "Ganancias de los Recursos tributarios totales y por tributo. Trimestral (Base 2001)", "T"),
     esperado(RECAUDACION, "Por tributo", "Ganancias", "Recaudación · Ganancias", MILLONES)),
    (serie("RECA", "142.3_FACIL_2001_M_16", "faciliades_de_pago",
           "Faciliades de pago de los Recursos tributarios totales y por tributo. Mensual (Base 2001)", "M"),
     esperado(RECAUDACION, "Por tributo", "Devoluciones, reintegros y otros conceptos",
              "Recaudación · Facilidades de pago", MILLONES)),
    (serie("RECA", "142.1_COMBUSTIBLROS_0_A_28_93", "combustibles_ley_23966_otros",
           "Combustibles ley 23.966 otros de los Recursos tributarios totales y por tributo. Anual (Base 2001)"),
     esperado(RECAUDACION, "Por tributo", "Combustibles", "Recaudación · Combustibles · Ley 23.966 · Otros", MILLONES)),
    (serie("RECA", "142.1_DEREC_2001_A_20", "derechos_exportacion",
           "Derechos exportacion de los Recursos tributarios totales y por tributo. Anual (Base 2001)"),
     esperado(RECAUDACION, "Por tributo", "Comercio exterior", "Recaudación · Derechos de exportación", MILLONES)),
    (serie("RECA", "142.1_SISTE_2001_A_24", "sistema_seguridad_social",
           "Sistema seguridad social de los Recursos tributarios totales y por tributo. Anual (Base 2001)"),
     esperado(RECAUDACION, "Por tributo", "Seguridad social", "Recaudación · Seguridad social", MILLONES)),
    (serie("RECA_SUB", "172.3_SRIDAD_IAL_M_0_0_16", "seguridad_social", "Recaudación Seguridad Social. Anuales", "M"),
     esperado(RECAUDACION, "Por tributo", "Seguridad social", "Recaudación · Seguridad social", MILLONES)),
    (serie("RECA_SUB", "172.3_SRIDAD_IA_M_0_0_23", "seguridad_social_var_ia",
           "Recaudación Seguridad Social. Variación interanual", "M"),
     esperado(RECAUDACION, "Por tributo", "Seguridad social",
              "Recaudación · Seguridad social (var. % interanual)", "Proporción")),
    (serie("RECA_SUB", "172.1_SOTAL_DDGA_0_0_12", "subtotal_dga", "Recaudación DGA. Anuales"),
     esperado(RECAUDACION, "Por dirección general", "", "Recaudación · Dirección General de Aduanas", MILLONES)),
    (serie("RECA_SUB", "172.2_TL_RECA_IA_T_0_0_24", "total_recaudacion_var_ia",
           "Total recaudación. Variación interanual", "T"),
     esperado(RECAUDACION, "Total", "", "Recaudación · Total (var. % interanual)", "Proporción")),
    (serie("RECA_HISTORICA", "429.1_RECAUDACIO969_A_0_0_38_99", "Recaudacion historia impuestos (1932-1969)"),
     esperado(RECAUDACION, "Series históricas", "", "Recaudación · Total", "Millones de pesos moneda nacional")),
)


class PruebasFinanzasPublicasOtros(unittest.TestCase):
    def test_casos_representativos(self):
        for entrada, esperada in CASOS:
            with self.subTest(hoja=entrada.hoja, id=entrada.id):
                self.assertEqual(clasificar(entrada), esperada)

    def test_hay_al_menos_25_casos_y_cubren_todas_las_hojas(self):
        self.assertGreaterEqual(len(CASOS), 25)
        hojas = {entrada.hoja for entrada, _ in CASOS}
        esperadas = {
            "AC_Dev_61", "AN_Dev_61", "CE_Dev_61", "EP_Dev_61", "ISS_Dev_61", "OD_Dev_61",
            "PROV_MCBA_Dev_61", "SPA_Dev_61", "SPA_Dev_93", "GPC_$", "GPC_%", "GPN_$", "GPN_%",
            "GPP_%", "GPM_$", "Gasto Pol. Ing", "AUH-AUE", "FUENTE ANSES PRESTACIONES",
            "Fuente Cuenta Inversion- Presta", "JUB, PNC; SEG DES.", "PROGRESAR", "RECA",
            "RECA_SUB", "RECA_HISTORICA",
        }
        self.assertLessEqual(esperadas - hojas, set())

    def test_mismo_concepto_en_dos_periodos_tiene_el_mismo_nombre(self):
        primero = clasificar(serie("AC_Dev_61", "432.1_RESULTADOADO_DEVE_0_9_73", "resultado", "Resultado"))
        ultimo = clasificar(serie("AC_Dev_61", "432.4_RESULTADOADO_DEVE_0_9_22", "resultado", "Resultado"))
        self.assertEqual(primero, ultimo)

    def test_mismo_concepto_en_dos_frecuencias_tiene_el_mismo_nombre(self):
        anual = clasificar(serie("Gasto Pol. Ing", "186.1_BECASCAS_0_0_5", "becas", frecuencia="A"))
        mensual = clasificar(serie("Gasto Pol. Ing", "186.3_BECASCAS_0_0_5", "becas", frecuencia="M"))
        self.assertEqual(anual, mensual)

    def test_publicaciones_de_anses_comparten_nombre(self):
        transparencia = clasificar(serie("FUENTE ANSES PRESTACIONES", "189.1_PENSIONES_INO_0_0_49",
                                         "pensiones_sipa_transparencia", frecuencia="M"))
        abiertos = clasificar(serie("FUENTE ANSES PRESTACIONES", "189.1_PENSIONES_INO_INT_0_0_49",
                                    "pensiones_sipa_datos_abiertos", frecuencia="M"))
        self.assertEqual(transparencia, abiertos)

    def test_fuera_de_alcance_devuelve_none(self):
        # Otra fuente, otro archivo, hojas de otros módulos y las series de actividad.xlsx.
        self.assertIsNone(clasificar(serie("AC_Dev_61", "x", "ahorro", fuente="indec")))
        self.assertIsNone(clasificar(serie("AC_Dev_61", "x", "ahorro", archivo="actividad.xlsx")))
        self.assertIsNone(clasificar(serie("SPN", "x", "ahorro")))
        self.assertIsNone(clasificar(serie("IMIG", "x", "total")))
        self.assertIsNone(clasificar(serie("1.12 Centros compras", "452.1_SALAS_CABAABA_0_0_10_33", "salas_caba",
                                           archivo="actividad.xlsx")))

    def test_variable_desconocida_queda_sin_regla(self):
        self.assertIsNone(clasificar(serie("AC_Dev_61", "x", "variable_que_no_existe")))
        self.assertIsNone(clasificar(serie("RECA", "x", "variable_que_no_existe")))
        self.assertIsNone(clasificar(serie("GPN_$", "x", "gpn_rubro_que_no_existe")))
        self.assertIsNone(clasificar(serie("GPN_$", "x", "gpc_educacion_basica")))

    def test_es_determinista(self):
        for entrada, _ in CASOS:
            self.assertEqual(clasificar(entrada), clasificar(entrada))


if __name__ == "__main__":
    unittest.main()
