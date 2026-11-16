"""Reglas del sector externo cambiario (tools/taxonomia/externo_cambiario.py)."""

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia import correcciones
from tools.taxonomia.externo_cambiario import EXCLUIDAS, UNIDADES, clasificar

SECTOR_EXTERNO = "Sector externo"
MONEDA = "Moneda y sistema financiero"
MILLONES_USD = "Millones de dólares estadounidenses"
ITC = "Índice 17-dic-2015=100"


def serie(fuente, id_, *, variable="", frecuencia="M", archivo="", hoja="", nombre="", unidad="",
          columna="") -> Serie:
    return Serie(fuente=fuente, id=id_, nombre=nombre, variable=variable, descripcion="", unidad=unidad,
                 frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=columna, dataset="",
                 estado="VIGENTE")


def datos(archivo, hoja, variable, frecuencia="M") -> Serie:
    return serie("datos.gob.ar", f"{hoja}|{variable}", variable=variable, frecuencia=frecuencia,
                 archivo=archivo, hoja=hoja)


def balance(codigo, hoja="Balance Cambiario") -> Serie:
    return serie("bcra-mc-bc", f"{hoja}|{codigo}", variable=codigo, hoja=hoja,
                 archivo="anexo-estadistico-mercado-cambios-balance-cambiario.xlsx",
                 nombre='BALANCE CAMBIARIO, Serie_mensual (1), equivalente en dólares estadounidenses - Total')


def itc(archivo, hoja, variable, frecuencia="D") -> Serie:
    return serie("bcra-itc", f"{archivo}|{hoja}|{variable}", variable=variable, frecuencia=frecuencia,
                 archivo=archivo, hoja=hoja)


def esperado(indicador, nombre, *niveles, unidad="", tema=SECTOR_EXTERNO) -> Clasificacion:
    cobertura, apertura, medida = (list(niveles) + ["", "", ""])[:3]
    return Clasificacion(tema=tema, indicador=indicador, nombre=nombre, cobertura=cobertura,
                         apertura=apertura, medida=medida, unidad=unidad)


class TipoDeCambioNominal(unittest.TestCase):
    def test_a3500_diario(self):
        s = serie("bcra-com3500", "TCR diario y TCNPM|Tipo de Cambio de Referencia - en Pesos - por Dólar",
                  variable="Tipo de Cambio de Referencia - en Pesos - por Dólar", frecuencia="D",
                  archivo="com3500.xls", hoja="TCR diario y TCNPM")
        self.assertEqual(clasificar(s), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · Oficial mayorista (A 3500)", "Oficial mayorista",
            unidad="Pesos por dólar"))

    def test_promedio_mensual_en_las_dos_hojas_del_libro(self):
        esperada = esperado("Tipo de cambio nominal", "Tipo de cambio · Oficial mayorista (A 3500) (promedio)",
                            "Oficial mayorista", unidad="Pesos por dólar")
        a = serie("bcra-com3500", "TCR diario y TCNPM|Tipo de Cambio Nominal Promedio Mensual",
                  variable="Tipo de Cambio Nominal Promedio Mensual", archivo="com3500.xls",
                  hoja="TCR diario y TCNPM")
        b = serie("bcra-com3500", "Serie de TCNPM|nan", archivo="com3500.xls", hoja="Serie de TCNPM")
        self.assertEqual(clasificar(a), esperada)
        self.assertEqual(clasificar(b), esperada)
        # La segunda es una copia de la primera y no se publica.
        self.assertIn(("bcra-com3500", "Serie de TCNPM|nan"), EXCLUIDAS)

    def test_bandas_cambiarias(self):
        for lado in ("Inferior", "Superior"):
            variable = f"Bandas {lado} a partir del 01/01/2026"
            s = serie("bcra-bandas", f"Bandas de Flotacion Cambiaria|{variable}", variable=variable,
                      frecuencia="D", archivo="bandas.xlsx", hoja="Bandas de Flotacion Cambiaria")
            self.assertEqual(clasificar(s), esperado(
                "Tipo de cambio nominal", f"Banda cambiaria · {lado}", "Bandas cambiarias",
                unidad="Pesos por dólar"))

    def test_yuan_tipo_de_cotizacion_y_de_pase(self):
        cotizacion = serie("bcra-cotizaciones", "CNY-tipo-cotizacion-d", variable="tipo_cotizacion_cny",
                           frecuencia="D", archivo="cotizaciones_CNY.csv", hoja="CNY")
        pase = serie("bcra-cotizaciones", "CNY-tipo-pase-d", variable="tipo_pase_cny", frecuencia="D",
                     archivo="cotizaciones_CNY.csv", hoja="CNY")
        self.assertEqual(clasificar(cotizacion), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · Yuan renminbi · Tipo de cotización", "Otras monedas",
            unidad="Pesos por yuan"))
        self.assertEqual(clasificar(pase), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · Yuan renminbi · Tipo de pase", "Otras monedas",
            unidad="Dólares estadounidenses por yuan"))

    def test_dolar_blue_y_mep(self):
        venta = serie("ambito-dolar-blue", "ambito-dolar-blue-venta", frecuencia="D", archivo="history.csv",
                      hoja="Histórico")
        mep = serie("ambito-dolar-mep", "ambito-dolar-mep-referencia", frecuencia="D", archivo="history.csv",
                    hoja="Histórico")
        self.assertEqual(clasificar(venta), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · Dólar blue · Venta", "Dólar blue", unidad="Pesos por dólar"))
        self.assertEqual(clasificar(mep), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · Dólar MEP", "Dólar MEP", unidad="Pesos por dólar"))

    def test_tipos_de_cambio_historicos(self):
        hoja = "8.8. Tipos de cambio historicos"
        casos = {
            "dolar_referencia_com_3500": ("Tipo de cambio · Oficial mayorista (A 3500)", "Oficial mayorista"),
            "dolar_estadounidense": ("Tipo de cambio · Dólar estadounidense · Tipo de cotización",
                                     "Oficial mayorista"),
            "dolar_libre_venta": ("Tipo de cambio · Dólar libre · Venta", "Series históricas"),
            "dolar_finan_esp_compra": ("Tipo de cambio · Dólar financiero especial · Compra", "Series históricas"),
            "dolar_tipo_unico": ("Tipo de cambio · Tipo de cambio único", "Series históricas"),
        }
        for variable, (nombre, grupo) in casos.items():
            with self.subTest(variable):
                resultado = clasificar(datos("dinero_bancos.xlsx", hoja, variable, "D"))
                self.assertEqual(resultado, esperado("Tipo de cambio nominal", nombre, grupo,
                                                     unidad="Pesos por dólar"))

    def test_a3500_de_datos_gob_ar_y_del_bcra_son_la_misma_serie(self):
        bcra = serie("bcra-com3500", "TCR diario y TCNPM|Tipo de Cambio de Referencia - en Pesos - por Dólar",
                     variable="Tipo de Cambio de Referencia - en Pesos - por Dólar", frecuencia="D",
                     archivo="com3500.xls", hoja="TCR diario y TCNPM")
        otra = datos("dinero_bancos.xlsx", "8.8. Tipos de cambio historicos", "dolar_referencia_com_3500", "D")
        self.assertEqual(clasificar(bcra), clasificar(otra))

    def test_promedio_anual_del_dolar_estadounidense(self):
        s = datos("precios.xlsx", "4.8.1", "Cotizacion Peso argentino", "I")
        self.assertEqual(clasificar(s), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · Dólar estadounidense · Tipo de cotización (promedio)",
            "Oficial mayorista", unidad="Pesos por dólar"))

    def test_el_dolar_estadounidense_no_se_llama_como_la_a3500(self):
        # Difieren (cierre contra promedio de la rueda): hasta 4-6 % en días sueltos.
        hoja = "8.8. Tipos de cambio historicos"
        a3500 = clasificar(datos("dinero_bancos.xlsx", hoja, "dolar_referencia_com_3500", "D"))
        dolar = clasificar(datos("dinero_bancos.xlsx", hoja, "dolar_estadounidense", "D"))
        self.assertNotEqual(a3500.nombre, dolar.nombre)

    def test_unidad_de_los_tipos_de_cambio_historicos(self):
        # Moneda de cada época, sin convertir (peso ley, peso argentino, austral, peso).
        ids = {"175.1_DR_TIPOICO_0_0_16": "Pesos ley 18.188 por dólar",
               "175.1_DR_FINANTA_0_0_22": "Pesos ley 18.188 por dólar",
               "175.1_DR_LIBRNTA_0_0_17": "Moneda de cada época por dólar (sin convertir)",
               "175.1_DR_OFICPRA_0_0_20": "Moneda de cada época por dólar (sin convertir)"}
        for id_, unidad in ids.items():
            with self.subTest(id_):
                self.assertEqual(UNIDADES[("datos.gob.ar", id_)], unidad)
        # La serie vigente y la A 3500 siguen en pesos por dólar.
        self.assertNotIn(("datos.gob.ar", "175.1_DR_ESTANSE_0_0_20"), UNIDADES)
        self.assertNotIn(("datos.gob.ar", "175.1_DR_REFE500_0_0_25"), UNIDADES)


class TipoDeCambioReal(unittest.TestCase):
    def test_itcrm_multilateral(self):
        s = itc("ITCRMSerie.xlsx", "ITCRM y bilaterales", "itcrm")
        self.assertEqual(clasificar(s), esperado("Tipo de cambio real", "ITCRM", "Multilateral (ITCRM)", unidad=ITC))

    def test_itcrb_estados_unidos_mismo_nombre_y_grupo_en_las_tres_fuentes(self):
        bcra_d = itc("ITCRMSerie.xlsx", "ITCRM y bilaterales", "itcrb-estados-unidos", "D")
        bcra_m = itc("ITCRMSerie.xlsx", "ITCRM y bilaterales prom. mens.", "itcrb-estados-unidos", "M")
        iiep = serie("iiep", "itcrb-eeuu-empalmado-importacion-m", variable="itcrb_estados_unidos_empalmado",
                     archivo="3 Tipo de Cambio Real.xlsx", hoja="Mes", unidad=ITC)
        gob = datos("precios.xlsx", "4.8.2", "tipo_cambio_real_estados_unidos")
        esperada = esperado("Tipo de cambio real", "ITCRB · Estados Unidos", "Bilaterales (ITCRB)", unidad=ITC)
        for s in (bcra_d, bcra_m, iiep, gob):
            with self.subTest(s.fuente + s.hoja):
                self.assertEqual(clasificar(s), esperada)

    def test_itcrm_de_datos_gob_ar_igual_al_del_bcra(self):
        bcra = itc("ITCRMSerie.xlsx", "ITCRM y bilaterales prom. mens.", "itcrm", "M")
        gob = datos("precios.xlsx", "4.8.2", "tipo_cambio_real_multilateral_actual", "D")
        self.assertEqual(clasificar(bcra), clasificar(gob))

    def test_bilaterales_de_datos_gob_ar_con_variable_unica_por_pais(self):
        # El ID cambia según la frecuencia (116.1_TCRC_… es Chile o China o Canadá); la variable no.
        casos = {"tipo_cambio_real_canada": "Canadá", "tipo_cambio_real_china": "China",
                 "tipo_cambio_real_chile": "Chile", "tipo_cambio_real_zona_euro": "Zona Euro",
                 "tipo_cambio_real_vietnam": "Vietnam", "tipo_cambio_real_reino_unido": "Reino Unido"}
        for variable, pais in casos.items():
            with self.subTest(variable):
                self.assertEqual(clasificar(datos("precios.xlsx", "4.8.2", variable, "A")).nombre,
                                 f"ITCRB · {pais}")

    def test_nominal_multilateral_y_bilaterales_cuelgan_del_tipo_de_cambio_nominal(self):
        nominal = itc("ITCNMSerie.xlsx", "ITCNM y bilaterales",
                      "indice-de-tipo-de-cambio-nominal-multilateral-itcnm")
        brasil = itc("ITCNMSerie.xlsx", "ITCNM y bilaterales prom. mens.", "itcnb-brasil", "M")
        sudamerica = itc("ITCNMSerie.xlsx", "ITCNM y bilaterales prom. mens.", "itcnb-sudamerica", "M")
        grupo = "Índices nominales (ITCNM e ITCNB)"
        self.assertEqual(clasificar(nominal), esperado("Tipo de cambio nominal", "ITCNM", grupo, unidad=ITC))
        self.assertEqual(clasificar(brasil), esperado("Tipo de cambio nominal", "ITCNB · Brasil", grupo, unidad=ITC))
        self.assertEqual(clasificar(sudamerica), esperado(
            "Tipo de cambio nominal", "ITCNB · Sudamérica (promedio simple de Brasil, Chile y Uruguay)", grupo,
            unidad=ITC))

    def test_sudamerica_del_itcrb_es_el_promedio_simple_de_tres_paises(self):
        s = itc("ITCRMSerie.xlsx", "ITCRM y bilaterales", "itcrb-sudamerica")
        self.assertEqual(clasificar(s), esperado(
            "Tipo de cambio real", "ITCRB · Sudamérica (promedio simple de Brasil, Chile y Uruguay)",
            "Bilaterales (ITCRB)", unidad=ITC))

    def test_ponderadores(self):
        s = itc("ITCRMSerie.xlsx", "Ponderadores", "zona-euro", "M")
        self.assertEqual(clasificar(s), esperado(
            "Tipo de cambio real", "ITCRM · Ponderador · Zona Euro", "Ponderadores", unidad="%"))

    def test_itcrm_historico_base_2001(self):
        for frecuencia in "ATM":
            s = datos("precios.xlsx", "4.14", "tipo_cambio_real_multilateral", frecuencia)
            self.assertEqual(clasificar(s), esperado(
                "Tipo de cambio real", "ITCRM", "Series históricas", unidad="Índice dic-2001=100"))


class Reservas(unittest.TestCase):
    def irfcl(self, variable, hoja="IRFCLDT1_IRFCL65_USD", archivo="irfcl_fmi_argentina.csv"):
        return serie("bcra-irfcl", variable.replace("_", "-"), variable=variable, archivo=archivo, hoja=hoja)

    def test_brutas_mismo_nombre_y_grupo_en_irfcl_iiep_y_datos_gob_ar(self):
        irfcl = self.irfcl("irfcl_ia_activos_reserva")
        iiep = serie("iiep-rin", "rin-brutas-m", variable="rin_brutas", archivo="Reservas.csv", hoja="Series")
        gob = datos("dinero_bancos.xlsx", "8.4 Res. Int. y Pasivos BCRA", "reservas_internacionales_dolares", "D")
        esperada = esperado("Reservas internacionales", "Reservas internacionales · Brutas", "Saldos",
                            unidad=MILLONES_USD)
        for s in (irfcl, iiep, gob):
            with self.subTest(s.fuente):
                self.assertEqual(clasificar(s), esperada)

    def test_brutas_mensual_de_datos_gob_ar_es_promedio(self):
        gob = datos("dinero_bancos.xlsx", "8.4 Res. Int. y Pasivos BCRA", "reservas_internacionales_dolares", "M")
        self.assertEqual(clasificar(gob), esperado(
            "Reservas internacionales", "Reservas internacionales · Brutas (promedio)", "Saldos",
            unidad=MILLONES_USD))

    def test_composicion_por_seccion_de_la_planilla(self):
        casos = (
            ("irfcl_ia3_deg", "Reservas internacionales · Brutas · Derechos especiales de giro",
             "I.A Activos de reserva oficiales"),
            ("irfcl_ia1b_bancos_residentes",
             "Reservas internacionales · Brutas · Depósitos en bancos con casa matriz en el país",
             "I.A Activos de reserva oficiales"),
            ("irfcl_ib_otros_activos_me",
             "Reservas internacionales · Otros activos en moneda extranjera no incluidos en reservas",
             "I.B Otros activos en moneda extranjera"),
            ("irfcl_ii2_1_a_3_meses",
             "Reservas internacionales · Drenajes predeterminados · Posiciones cortas en forwards, futuros y swaps"
             " · De 1 a 3 meses", "II Drenajes netos predeterminados"),
            ("irfcl_nota_depositos_gobierno",
             "Reservas internacionales · Custodia · Depósitos del Gobierno Nacional en otras monedas",
             "Notas del país"),
            ("irfcl_nota_depositos_otras_monedas", "Reservas internacionales · Custodia · Depósitos en otras monedas",
             "Notas del país"),
            ("irfcl_nota_cheques_cdi",
             "Reservas internacionales · Custodia · Cheques cancelatorios y certificados de depósito para la"
             " inversión", "Notas del país"),
        )
        for variable, nombre, seccion in casos:
            with self.subTest(variable):
                self.assertEqual(clasificar(self.irfcl(variable)), esperado(
                    "Reservas internacionales", nombre, "Composición", seccion, unidad=MILLONES_USD))

    def test_oro_en_onzas_lleva_su_unidad(self):
        resultado = clasificar(self.irfcl("irfcl_ia4_oro_onzas"))
        self.assertEqual(resultado.nombre, "Reservas internacionales · Brutas · Oro monetario (volumen)")
        self.assertEqual(resultado.unidad, "Millones de onzas troy finas")

    def test_medidas_alternativas_del_iiep(self):
        casos = {"rin_netas": "Netas", "rin_liquidas": "Líquidas", "rin_custodia": "Custodia",
                 "rin_rin_eff": "Netas del acuerdo con el FMI (RIN-EFF)",
                 "rin_netas_planilla": "Netas según planilla IRFCL"}
        for variable, parte in casos.items():
            with self.subTest(variable):
                s = serie("iiep-rin", variable, variable=variable, hoja="Series")
                self.assertEqual(clasificar(s), esperado(
                    "Reservas internacionales", f"Reservas internacionales · {parte}", "Saldos",
                    unidad=MILLONES_USD))

    def test_custodia_se_compone_de_las_notas_del_pais(self):
        nota = clasificar(self.irfcl("irfcl_nota_cheques_cdi", hoja="Notas del país", archivo="tempMMAA.pdf"))
        self.assertTrue(nota.nombre.startswith("Reservas internacionales · Custodia · "))
        total = clasificar(serie("iiep-rin", "rin-custodia-m", variable="rin_custodia"))
        self.assertEqual(total.nombre, "Reservas internacionales · Custodia")

    def test_pasivos_del_bcra_van_a_moneda_y_sistema_financiero(self):
        hoja = "8.4 Res. Int. y Pasivos BCRA"
        self.assertEqual(
            clasificar(datos("dinero_bancos.xlsx", hoja, "pasivos_monetarios", "D")),
            esperado("Balance del BCRA", "Balance del BCRA · Pasivos monetarios", "Pasivo",
                     unidad="Millones de pesos", tema=MONEDA))
        self.assertEqual(
            clasificar(datos("dinero_bancos.xlsx", hoja, "posicion_neta_pases", "M")),
            esperado("Balance del BCRA", "Balance del BCRA · Pases · Posición neta (promedio)", "Pases",
                     unidad="Millones de pesos", tema=MONEDA))

    def test_letras_en_dolares_estan_en_millones_de_pesos(self):
        # El título dice «millones de dólares» pero el valor es el monto en dólares por el tipo de cambio de
        # valuación (el cociente es constante de un día al siguiente).
        hoja = "8.4 Res. Int. y Pasivos BCRA"
        for frecuencia, nombre in (("D", "Balance del BCRA · Pasivos monetarios · Letras en dólares"),
                                   ("M", "Balance del BCRA · Pasivos monetarios · Letras en dólares (promedio)")):
            with self.subTest(frecuencia):
                self.assertEqual(
                    clasificar(datos("dinero_bancos.xlsx", hoja, "pasivos_monetarios_letras_dolares", frecuencia)),
                    esperado("Balance del BCRA", nombre, "Pasivo", unidad="Millones de pesos", tema=MONEDA))
        for variable in ("letras_liquidez_saldo_pesos", "pos_neta_det_des_pases_pasivos",
                         "pos_neta_det_des_pases_activos"):
            with self.subTest(variable):
                self.assertEqual(clasificar(datos("dinero_bancos.xlsx", hoja, variable, "D")).unidad,
                                 "Millones de pesos")

    def test_tipo_de_cambio_de_valuacion(self):
        hoja = "8.4 Res. Int. y Pasivos BCRA"
        self.assertEqual(clasificar(datos("dinero_bancos.xlsx", hoja, "tipo_cambio_valuacion", "D")), esperado(
            "Tipo de cambio nominal", "Tipo de cambio · De valuación de las reservas", "Oficial mayorista",
            unidad="Pesos por dólar"))


class BalanceCambiario(unittest.TestCase):
    def test_total_y_componentes_de_la_cuenta_corriente(self):
        self.assertEqual(clasificar(balance("bal001")), esperado(
            "Balance cambiario", "Balance cambiario · Cuenta corriente", "Cuenta corriente", "Total",
            unidad=MILLONES_USD))
        self.assertEqual(clasificar(balance("bal003")), esperado(
            "Balance cambiario", "Balance cambiario · Bienes · Cobros de exportaciones", "Cuenta corriente",
            "Bienes", "Total", unidad=MILLONES_USD))
        self.assertEqual(clasificar(balance("bal012")), esperado(
            "Balance cambiario", "Balance cambiario · Ingreso primario · Intereses · Egresos · Pagos al FMI",
            "Cuenta corriente", "Ingreso primario", unidad=MILLONES_USD))

    def test_cuenta_capital_y_financiera(self):
        self.assertEqual(clasificar(balance("bal023")), esperado(
            "Balance cambiario", "Balance cambiario · Cuenta capital", "Cuenta capital y financiera",
            "Cuenta de capital", unidad=MILLONES_USD))
        self.assertEqual(clasificar(balance("bal041")), esperado(
            "Balance cambiario",
            "Balance cambiario · Formación de activos externos del sector privado no financiero · Ingresos",
            "Cuenta capital y financiera", "Cuenta financiera", unidad=MILLONES_USD))

    def test_variacion_de_reservas_y_memorandum(self):
        self.assertEqual(clasificar(balance("bal050")), esperado(
            "Balance cambiario", "Balance cambiario · Variación de reservas internacionales · Por transacciones",
            "Variación de reservas", unidad=MILLONES_USD))
        memo_balance = clasificar(balance("bal053"))
        memo_mercado = clasificar(balance("mlc101", hoja="Mercado de Cambios"))
        self.assertEqual(memo_balance, memo_mercado)
        self.assertEqual(memo_balance.cobertura, "Ítem de memorándum")

    def test_columna_sin_rotulo_no_queda_sin_clasificar(self):
        s = balance("nan")
        resultado = clasificar(s)
        self.assertEqual(resultado.indicador, "Balance cambiario")
        self.assertEqual(resultado.cobertura, "Otros")

    def test_lo_que_no_es_una_serie_o_es_copia_no_se_publica(self):
        mc = "bcra-mc-bc"
        self.assertEqual(set(EXCLUIDAS), {
            (mc, "Balance Cambiario|nan"),                      # contador
            (mc, "Datos Mercado de Cambios|Monto"),             # valor suelto de una tabla de operaciones
            (mc, "Cta. Financiera por sector|cap001"),          # = bal024
            (mc, "Mercado de Cambios|mlc101"),                  # = bal053
            (mc, "Anexo tarjetas y viajes|viajes010"),          # = mlc022
            (mc, "Anexo tarjetas y viajes|viajes019"),          # = mlc039
            ("bcra-com3500", "Serie de TCNPM|nan"),             # = promedio mensual de la otra hoja
        })
        self.assertTrue(set(EXCLUIDAS) <= set(correcciones()[0]))
        for motivo in EXCLUIDAS.values():
            self.assertTrue(motivo.strip())

    def test_cuenta_financiera_por_sector(self):
        s = balance("cap002", hoja="Cta. Financiera por sector")
        self.assertEqual(clasificar(s), esperado(
            "Balance cambiario", "Balance cambiario · Cuenta financiera · Sector privado no financiero",
            "Cuenta capital y financiera", "Cuenta financiera", unidad=MILLONES_USD))
        # El total por sector es la cuenta financiera del balance (copia exacta, por eso está excluido).
        self.assertEqual(clasificar(balance("cap001", hoja="Cta. Financiera por sector")),
                         clasificar(balance("bal024")))
        self.assertIn(("bcra-mc-bc", "Cta. Financiera por sector|cap001"), EXCLUIDAS)

    def test_hoja_20_de_datos_gob_ar_comparte_nombre_con_el_bcra(self):
        pares = (
            ("c_corriente_ingreso_primariopagos_intereses_al_fmi", "bal012"),
            ("cuenta_corriente_cambiaria_total_bienes", "bal002"),
            ("c_cap_financ_total_cuenta_financ", "bal024"),
            ("c_k_finc_c_finc_egres_form_activos_ext_sect_priv_no_finc", "bal042"),
            ("variacion_reservas_internacionales_transacciones", "bal050"),
            ("total_cuenta_corriente_cambiaria", "bal001"),
        )
        for variable, codigo in pares:
            with self.subTest(variable):
                self.assertEqual(clasificar(datos("sector_externo.xlsx", "20.Balance cambiario", variable)),
                                 clasificar(balance(codigo)))

    def test_total_cuenta_capital_y_financiera_solo_esta_en_datos_gob_ar(self):
        s = datos("sector_externo.xlsx", "20.Balance cambiario", "total_cuenta_capital_financiera_cambiaria")
        self.assertEqual(clasificar(s), esperado(
            "Balance cambiario", "Balance cambiario · Cuenta capital y financiera", "Cuenta capital y financiera",
            "Total", unidad=MILLONES_USD))

    def test_hoja_21_modalidad_de_pago(self):
        s = datos("sector_externo.xlsx", "21.Bienes por modalidad de pago", "bienes_cobros_anticipados_exportaciones")
        self.assertEqual(clasificar(s), esperado(
            "Balance cambiario", "Balance cambiario · Bienes · Cobros de exportaciones · Cobros anticipados",
            "Cuenta corriente", "Bienes", "Por modalidad de pago", unidad=MILLONES_USD))
        # La primera modalidad se llama como en el BCRA («Cobros de exportaciones»), no «sin anticipo…».
        simple = datos("sector_externo.xlsx", "21.Bienes por modalidad de pago", "bienes_cobros_exportaciones")
        self.assertEqual(clasificar(simple).nombre,
                         "Balance cambiario · Bienes · Cobros de exportaciones · Cobros de exportaciones")
        self.assertEqual(clasificar(balance("mlc005", hoja="Mercado de Cambios")).nombre,
                         "Mercado de cambios · Bienes · Cobros de exportaciones · Cobros de exportaciones")
        total = datos("sector_externo.xlsx", "21.Bienes por modalidad de pago", "pagos_importaciones_bienes")
        self.assertEqual(clasificar(total), clasificar(balance("bal004")))

    def test_hoja_22_sectores_con_la_grafia_del_bcra(self):
        casos = {
            "bienes_cobr_prod_minerales_no_metalicos":
                "Balance cambiario · Bienes · Cobros de exportaciones · "
                "Productos minerales no metálicos (cementos, cerámicos y otros)",
            "bienes_pagos_mineria": "Balance cambiario · Bienes · Pagos de importaciones · Minería",
            "bienes_pag_prod_minerales_no_metalicos":
                "Balance cambiario · Bienes · Pagos de importaciones · "
                "Productos minerales no metálicos (cementos, cerámicos y otros)",
        }
        for variable, nombre in casos.items():
            with self.subTest(variable):
                resultado = clasificar(datos("sector_externo.xlsx", "22.Bienes por sector", variable))
                self.assertEqual(resultado.nombre, nombre)
                self.assertEqual((resultado.cobertura, resultado.apertura, resultado.medida),
                                 ("Cuenta corriente", "Bienes", "Por sector"))
        total = datos("sector_externo.xlsx", "22.Bienes por sector", "bienes_cobros_totales_exportaciones")
        self.assertEqual(clasificar(total), clasificar(balance("bal003")))

    def test_hoja_23_servicios_por_tipo(self):
        s = datos("sector_externo.xlsx", "23.Servicios por tipo", "pago_servicios_turismo_viajes")
        self.assertEqual(clasificar(s), esperado(
            "Balance cambiario", "Balance cambiario · Servicios · Egresos · Consumos con tarjetas y viajes",
            "Cuenta corriente", "Servicios", "Por tipo de servicio", unidad=MILLONES_USD))
        total = datos("sector_externo.xlsx", "23.Servicios por tipo", "servicios_total_ingresos_servicios")
        self.assertEqual(clasificar(total), clasificar(balance("bal006")))


class MercadoDeCambios(unittest.TestCase):
    def mercado(self, codigo):
        return balance(codigo, hoja="Mercado de Cambios")

    def test_el_mercado_de_cambios_no_es_el_balance_cambiario(self):
        mercado = clasificar(self.mercado("mlc003"))
        self.assertEqual(mercado, esperado(
            "Mercado de cambios", "Mercado de cambios · Bienes", "Cuenta corriente", "Bienes", "Total",
            unidad=MILLONES_USD))
        self.assertNotEqual(mercado.nombre, clasificar(balance("bal002")).nombre)

    def test_servicios_por_tipo_con_sentido(self):
        self.assertEqual(clasificar(self.mercado("mlc025")).nombre,
                         "Mercado de cambios · Servicios · Ingresos · Servicios relacionados con el comercio")
        self.assertEqual(clasificar(self.mercado("mlc048")).nombre,
                         "Mercado de cambios · Servicios · Egresos · Otros servicios")

    def test_cuenta_financiera(self):
        self.assertEqual(clasificar(self.mercado("mlc083")), esperado(
            "Mercado de cambios",
            "Mercado de cambios · Préstamos, deuda y líneas de crédito · Egresos · "
            "Organismos internacionales y agencias oficiales",
            "Cuenta capital y financiera", "Cuenta financiera", unidad=MILLONES_USD))
        self.assertEqual(clasificar(self.mercado("mlc095")).nombre,
                         "Mercado de cambios · Formación de activos externos del sector privado no financiero"
                         " · Egresos · Otras inversiones")

    def test_resultado(self):
        self.assertEqual(clasificar(self.mercado("mlc001")), esperado(
            "Mercado de cambios", "Mercado de cambios · Resultado", "Resultado", unidad=MILLONES_USD))

    def test_viajes_y_tarjetas(self):
        viajes = lambda n: balance(f"viajes{n:03d}", hoja="Anexo tarjetas y viajes")
        self.assertEqual(clasificar(viajes(5)), esperado(
            "Mercado de cambios",
            "Mercado de cambios · Viajes y tarjetas · Ingresos · Tarjetas, incluye viajes · Servicios digitales",
            "Cuenta corriente", "Servicios", "Viajes y tarjetas", unidad=MILLONES_USD))
        # El transporte de pasajeros es idéntico al del mercado de cambios (se excluye por duplicado).
        for viaje, mlc in ((10, "mlc022"), (19, "mlc039")):
            with self.subTest(viaje):
                self.assertEqual(clasificar(viajes(viaje)), clasificar(self.mercado(mlc)))
                self.assertIn(("bcra-mc-bc", f"Anexo tarjetas y viajes|viajes{viaje:03d}"), EXCLUIDAS)
        # Los totales de consumos con tarjetas incluyen los servicios digitales desde julio de 2025 (el
        # mercado de cambios los computa en «Otros servicios»): no son la misma serie.
        for viaje, mlc in ((3, "mlc016"), (12, "mlc033")):
            with self.subTest(viaje):
                propia, ajena = clasificar(viajes(viaje)), clasificar(self.mercado(mlc))
                self.assertNotEqual(propia.nombre, ajena.nombre)
                self.assertIn("incluidos los servicios digitales", propia.nombre)

    def test_base_de_operaciones(self):
        s = balance("Monto", hoja="Datos Mercado de Cambios")
        resultado = clasificar(s)
        self.assertEqual((resultado.indicador, resultado.cobertura), ("Mercado de cambios", "Base de operaciones"))
        self.assertIn(("bcra-mc-bc", "Datos Mercado de Cambios|Monto"), EXCLUIDAS)


class Alcance(unittest.TestCase):
    def test_fuera_de_alcance_devuelve_none(self):
        self.assertIsNone(clasificar(serie("indec-emae", "x", hoja="Cuadro 1", archivo="emae.xls")))
        self.assertIsNone(clasificar(datos("dinero_bancos.xlsx", "8.9 Balance BCRA", "activos_totales")))
        self.assertIsNone(clasificar(datos("precios.xlsx", "4.8.3", "tipo_cambio_real_brasil")))
        self.assertIsNone(clasificar(datos("sector_externo.xlsx", "1. ICA", "exportaciones")))
        self.assertIsNone(clasificar(serie("bcra-mc-bc", "Hoja ajena|x1", variable="x1", hoja="Hoja ajena")))

    def test_dos_llamadas_dan_lo_mismo(self):
        s = balance("mlc010", hoja="Mercado de Cambios")
        self.assertEqual(clasificar(s), clasificar(s))
        self.assertIsNot(clasificar(s), None)

    def test_los_nombres_no_arrastran_restos_de_la_fuente(self):
        for codigo in ("bal005", "bal015", "bal049", "bal052"):
            nombre = clasificar(balance(codigo)).nombre
            self.assertNotIn("Serie_mensual", nombre)
            self.assertNotIn("Otros pagos de Gobierno Nacional -", nombre)
            self.assertLessEqual(len(nombre), 120)
            self.assertEqual(nombre, nombre.strip())


if __name__ == "__main__":
    unittest.main()
