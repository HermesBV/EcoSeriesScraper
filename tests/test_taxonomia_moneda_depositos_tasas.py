"""Pruebas de las reglas del libro de tasas y montos de depósitos del BCRA (``bcra-pas``)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.moneda_depositos_tasas import clasificar

TEMA = "Moneda y sistema financiero"
REF = "Tasas de interés"
DEP = "Tasas y montos de depósitos"


def serie(hoja: str, variable: str, nombre: str, fuente: str = "bcra-pas", archivo: str = "pas2026.xls") -> Serie:
    return Serie(
        fuente=fuente, id=f"{hoja}|{variable}", nombre=nombre, variable=variable, descripcion=nombre,
        unidad="", frecuencia="D", archivo=archivo, hoja=hoja, columna=variable,
        dataset="Series diarias de tasas de interés y montos colocados", estado="VIGENTE",
    )


# (hoja, variable, nombre de la fuente, indicador, cobertura, apertura, nombre esperado, unidad)
CASOS = (
    ('Totales_diarios', 'tamar05',
     'Totales_diarios: TAMAR - En pesos - (miles) - Bancos - privados - Montos - operados',
     REF, 'Tasas de referencia por depósitos', 'TAMAR',
     'TAMAR · Bancos privados (monto operado)', 'Miles de pesos'),
    ('Totales_diarios', 'tan327',
     'Totales_diarios: TASAS DE INTERES POR DEPOSITOS A PLAZO FIJO DE MAS DE UN MILLON DE PESOS O DOLARES, serie diaria (3), en % n.a. (BADLAR) - Depósitos de más de un millón de pesos - de 30 a 35 días de plazo - Total de - bancos - informantes',
     REF, 'Tasas de referencia por depósitos', 'BADLAR',
     'BADLAR · Total de bancos', '% nominal anual'),
    ('Totales_diarios', 'TM2004',
     'Totales_diarios: Depósitos de 20 o más millones de dólares - de 30 a 35 días de plazo - Total de - bancos - informantes',
     REF, 'Tasas de referencia por depósitos', 'TM20',
     'TM20 en dólares · Total de bancos', '% nominal anual'),
    ('Totales_diarios', 'monpesbadpub',
     'Totales_diarios: BADLAR - En pesos - (miles) - Bancos - públicos - Montos - operados',
     REF, 'Tasas de referencia por depósitos', 'BADLAR',
     'BADLAR · Bancos públicos (monto operado)', 'Miles de pesos'),
    ('Totales_diarios', 'mondolbadpri',
     'Totales_diarios: BADLAR - En dólares - (miles) - Bancos - privados - Montos - operados',
     REF, 'Tasas de referencia por depósitos', 'BADLAR',
     'BADLAR en dólares · Bancos privados (monto operado)', 'Miles de dólares'),
    ('Totales_diarios', 'tamar03',
     'Totales_diarios: TAMAR- TASA MAYORISTA DE ARGENTINA, serie diaria (7), en % n.a. - Bancos - Privados',
     REF, 'Tasas de referencia por depósitos', 'TAMAR',
     'TAMAR · Bancos privados', '% nominal anual'),
    ('Totales_diarios', 'TM2011',
     'Totales_diarios: TM20 - En dólares - (miles) - Bancos - públicos - Montos - operados',
     REF, 'Tasas de referencia por depósitos', 'TM20',
     'TM20 en dólares · Bancos públicos (monto operado)', 'Miles de dólares'),
    ('Totales_diarios', 'tanpes1',
     'Totales_diarios: TASAS DE INTERES POR DEPOSITOS EN CAJA DE AHORROS COMUN Y A PLAZO FIJO, serie diaria (1), en % n.a. - Depósitos de pesos - En caja - de - ahorros - común',
     DEP, 'Total de entidades financieras', 'Caja de ahorros en pesos',
     'Caja de ahorros en pesos', '% nominal anual'),
    ('Totales_diarios', 'tanpes4',
     'Totales_diarios: A 30 días - (2)',
     DEP, 'Total de entidades financieras', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · A 30 días', '% nominal anual'),
    ('Totales_diarios', 'tandol3',
     'Totales_diarios: De 60 o - más - días - de plazo',
     DEP, 'Total de entidades financieras', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · 60 días o más', '% nominal anual'),
    ('Totales_diarios', 'tanplapes',
     'Totales_diarios: A plazo fijo - Total',
     DEP, 'Total de entidades financieras', 'Plazo fijo en pesos',
     'Plazo fijo en pesos', '% nominal anual'),
    ('Series_diarias', 'bantanplapes',
     'Series_diarias: A plazo fijo - Total',
     DEP, 'Total de entidades financieras', 'Plazo fijo en pesos',
     'Plazo fijo en pesos', '% nominal anual'),
    ('Series_diarias_Bancos', 'bantanpes2',
     'Series_diarias_Bancos: De 30 a 59 días - de plazo - Total',
     DEP, 'Bancos', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · 30 a 59 días · Bancos', '% nominal anual'),
    ('Series_diarias_bcos.privados', 'privtandol1',
     'Series_diarias_bcos.privados: Depósitos de dólares estadounidenses - En caja - de - ahorros - común',
     DEP, 'Bancos privados', 'Caja de ahorros en dólares',
     'Caja de ahorros en dólares · Bancos privados', '% nominal anual'),
    ('Series_diarias_bcos.publicos', 'pubtanpes3',
     'Series_diarias_bcos.publicos: De 60 o - más - días - de plazo',
     DEP, 'Bancos públicos', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · 60 días o más · Bancos públicos', '% nominal anual'),
    ('Series_diarias_no_bancarias', 'nobtanpes4',
     'Series_diarias_no_bancarias: A 30 días - (2)',
     DEP, 'Entidades financieras no bancarias', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · A 30 días · Entidades financieras no bancarias', '% nominal anual'),
    ('Series_diarias_no_bancarias', 'nobtanpladol',
     'Series_diarias_no_bancarias: A plazo fijo - Total',
     DEP, 'Entidades financieras no bancarias', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · Entidades financieras no bancarias', '% nominal anual'),
    ('UVA_UVI', 'pasvar01',
     'UVA_UVI: TASAS DE INTERES POR DEPOSITOS A PLAZO FIJO - CAPITAL AJUSTABLE POR CER/UVIs - DESAG. POR ESTRATOS DE MONTO- , serie diaria (5), en % n.a. y en miles de $ - Depósitos de pesos - A plazo fijo - Hasta 99.999 - Monto',
     DEP, 'Total de entidades financieras', 'Plazo fijo en UVA y UVI',
     'Plazo fijo en UVA y UVI · Hasta $99.999 (monto)', 'Miles de pesos'),
    ('UVA_UVI', 'pasvar04',
     'UVA_UVI: De 100.000 - a 499.999 - Saldo',
     DEP, 'Total de entidades financieras', 'Plazo fijo en UVA y UVI',
     'Plazo fijo en UVA y UVI · De $100.000 a $499.999 (monto)', 'Miles de pesos'),
    ('UVA_UVI', 'pasvar09',
     'UVA_UVI: Plazo',
     DEP, 'Total de entidades financieras', 'Plazo fijo en UVA y UVI',
     'Plazo fijo en UVA y UVI · De $500.000 a $999.999 (plazo promedio)', 'Días'),
    ('UVA_UVI', 'pasvar13',
     'UVA_UVI: Hasta 49.999.999 - Monto',
     DEP, 'Total de entidades financieras', 'Plazo fijo en UVA y UVI',
     'Plazo fijo en UVA y UVI · Hasta $49.999.999 (monto)', 'Miles de pesos'),
    ('UVA_UVI', 'pasvar14',
     'UVA_UVI: Hasta 49.999.999 - Tasa de interés',
     DEP, 'Total de entidades financieras', 'Plazo fijo en UVA y UVI',
     'Plazo fijo en UVA y UVI · Hasta $49.999.999', '% nominal anual'),
    ('UVA_UVI', 'pasvar15',
     'UVA_UVI: Hasta 49.999.999 - Plazo',
     DEP, 'Total de entidades financieras', 'Plazo fijo en UVA y UVI',
     'Plazo fijo en UVA y UVI · Hasta $49.999.999 (plazo promedio)', 'Días'),
    ('Estra_dia', 'pasdes2',
     'Estra_dia: TASAS DE INTERES POR DEPOSITOS -DESAG. POR ESTRATOS DE MONTO- TOTAL - , serie diaria (1) (4), en % n.a. y en miles - Depósitos de pesos - En cuentas corrientes remuneradas - Hasta 99.999 - Saldo',
     DEP, 'Total de entidades financieras', 'Cuenta corriente remunerada en pesos',
     'Cuenta corriente remunerada en pesos · Hasta $99.999 (saldo)', 'Miles de pesos'),
    ('Estra_dia', 'pasdes3',
     'Estra_dia: Tasa - de - interés',
     DEP, 'Total de entidades financieras', 'Cuenta corriente remunerada en pesos',
     'Cuenta corriente remunerada en pesos · Hasta $99.999', '% nominal anual'),
    ('Estra_dia', 'pasdes16',
     'Estra_dia: De 1.000.000 - y más - Saldo',
     DEP, 'Total de entidades financieras', 'Caja de ahorros en pesos',
     'Caja de ahorros en pesos · De $1.000.000 y más (saldo)', 'Miles de pesos'),
    ('Estra_dia', 'pasdes24',
     'Estra_dia: De 1.000.000 - y más - Monto',
     DEP, 'Total de entidades financieras', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · De $1.000.000 y más (monto)', 'Miles de pesos'),
    ('Estra_dia', 'pasdes114',
     'Estra_dia: TASAS DE INTERES POR DEPOSITOS -DESAG. POR ESTRATOS DE MONTO- TOTAL - , serie diaria (1) (4), en % n.a. y en miles de $ - Depósitos de pesos - Plazo fijo total - Hasta 49.999.999 - Monto',
     DEP, 'Total de entidades financieras', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · Hasta $49.999.999 (monto)', 'Miles de pesos'),
    ('Estra_dia', 'pasdes115',
     'Estra_dia: TASAS DE INTERES POR DEPOSITOS -DESAG. POR ESTRATOS DE MONTO- TOTAL - , serie diaria (1) (4), en % n.a. y en miles de $ - Depósitos de pesos - Plazo fijo total - Hasta 49.999.999 - Tasa de interés',
     DEP, 'Total de entidades financieras', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · Hasta $49.999.999', '% nominal anual'),
    ('Estra_dia', 'pasdes66',
     'Estra_dia: TASAS DE INTERES POR DEPOSITOS -DESAG. POR ESTRATOS DE MONTO- TOTAL - , serie diaria (1) (4), en % n.a. y en miles de u$s - Depósitos de dólares estadounidenses - Caja de ahorros sin incentivos - Hasta 99.999 - Saldo',
     DEP, 'Total de entidades financieras', 'Caja de ahorros en dólares',
     'Caja de ahorros en dólares · Hasta US$99.999 (saldo)', 'Miles de dólares'),
    ('Estra_dia', 'pasdes88',
     'Estra_dia: De 1.000.000 - y más - Monto',
     DEP, 'Total de entidades financieras', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · Hasta 59 días · De US$1.000.000 y más (monto)', 'Miles de dólares'),
    ('Estra_dia', 'pasdes105',
     'Estra_dia: Tasa - de - interés',
     DEP, 'Total de entidades financieras', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · 45 a 59 días · De US$1.000.000 y más', '% nominal anual'),
    ('Estra_dia_bancos', 'pasdes36',
     'Estra_dia_bancos: De 100.000 - a 499.999 - Monto',
     DEP, 'Bancos', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · 30 a 44 días · De $100.000 a $499.999 · Bancos (monto)', 'Miles de pesos'),
    ('Estra_dia_bcos.priv', 'pasdes45',
     'Estra_dia_bcos.priv: Tasa - de - interés',
     DEP, 'Bancos privados', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · 45 a 59 días · De $100.000 a $499.999 · Bancos privados', '% nominal anual'),
    ('Estra_dia_bcos.pub', 'pasdes11',
     'Estra_dia_bcos.pub: Tasa - de - interés',
     DEP, 'Bancos públicos', 'Caja de ahorros en pesos',
     'Caja de ahorros en pesos · Hasta $99.999 · Bancos públicos', '% nominal anual'),
    ('Estra_dia_no_banc.', 'pasdes56',
     'Estra_dia_no_banc.: De 1.000.000 - y más - Monto',
     DEP, 'Entidades financieras no bancarias', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · 60 días o más · De $1.000.000 y más · Entidades financieras no bancarias (monto)', 'Miles de pesos'),
    ('Estra_dia_ext', 'pasdesext26',
     'Estra_dia_ext: TASAS DE INTERES POR DEPOSITOS DE RESIDENTES EN EL EXTERIOR -DESAG. POR ESTRATOS DE MONTO- , serie diaria (1) (4), en % n.a. y en miles de $ - Depósitos de pesos - Plazo fijo - Hasta 59 días de plazo - Hasta 99.999 - Saldo',
     DEP, 'Residentes en el exterior', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · Hasta 59 días · Hasta $99.999 · Residentes en el exterior (monto)', 'Miles de pesos'),
    ('Estra_dia_fin', 'pasdesfin58',
     'Estra_dia_fin: TASAS DE INT. POR DEP. DEL S. PRIV. - PERSONAS JURIDICAS - PREST. DE SERV.FINANC. -DESAG. POR EST. DE MONTO- , serie diaria (1) (4), en % n.a. y en miles - Depósitos de dólares estadounidenses - Cuentas corrientes remuneradas - Hasta 99.999 - Saldo',
     DEP, 'Prestadoras de servicios financieros', 'Cuenta corriente remunerada en dólares',
     'Cuenta corriente remunerada en dólares · Hasta US$99.999 · Prestadoras de servicios financieros (saldo)', 'Miles de dólares'),
    ('Estra_dia_fis', 'pasdesfis19',
     'Estra_dia_fis: Tasa - de - interés',
     DEP, 'Personas físicas', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · Hasta $99.999 · Personas físicas', '% nominal anual'),
    ('Estra_dia_jur', 'pasdesjur70',
     'Estra_dia_jur: De 500.000 - a 999.999 - Saldo',
     DEP, 'Personas jurídicas', 'Caja de ahorros en dólares',
     'Caja de ahorros en dólares · De US$500.000 a US$999.999 · Personas jurídicas (saldo)', 'Miles de dólares'),
    ('Estra_dia_pri', 'pasdespri88',
     'Estra_dia_pri: De 1.000.000 - y más - Monto',
     DEP, 'Sector privado', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · Hasta 59 días · De US$1.000.000 y más · Sector privado (monto)', 'Miles de dólares'),
    ('Estra_dia_res', 'pasdesres20',
     'Estra_dia_res: De 100.000 - a 499.999 - Monto',
     DEP, 'Otras personas jurídicas', 'Plazo fijo en pesos',
     'Plazo fijo en pesos · De $100.000 a $499.999 · Otras personas jurídicas (monto)', 'Miles de pesos'),
    ('Estra_dia_pub', 'pasdespub90',
     'Estra_dia_pub: Depósitos de dólares estadounidenses - Plazo fijo - Hasta 13 días de plazo - Hasta 99.999 - Monto',
     DEP, 'Sector público', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · Hasta 13 días · Hasta US$99.999 · Sector público (monto)', 'Miles de dólares'),
    ('Estra_dia_pub', 'pasdespub99',
     'Estra_dia_pub: Tasa - de - interés',
     DEP, 'Sector público', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · 14 a 29 días · Hasta US$99.999 · Sector público', '% nominal anual'),
    ('Estra_dia_pub', 'pasdespub122',
     'Estra_dia_pub: Plazo fijo - De 60 y más días de plazo - Hasta 99.999 - Monto',
     DEP, 'Sector público', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · 60 días o más · Hasta US$99.999 · Sector público (monto)', 'Miles de dólares'),
    ('Estra_dia_pub', 'pasdespub129',
     'Estra_dia_pub: Tasa - de - interés',
     DEP, 'Sector público', 'Plazo fijo en dólares',
     'Plazo fijo en dólares · 60 días o más · De US$1.000.000 y más · Sector público', '% nominal anual'),
)


class ClasificarDepositosTasas(unittest.TestCase):
    def test_casos_representativos(self):
        for hoja, variable, nombre, indicador, cobertura, apertura, esperado, unidad in CASOS:
            with self.subTest(hoja=hoja, variable=variable):
                self.assertEqual(
                    clasificar(serie(hoja, variable, nombre)),
                    Clasificacion(tema=TEMA, indicador=indicador, cobertura=cobertura, apertura=apertura,
                                  nombre=esperado, unidad=unidad),
                )

    def test_cubre_las_diecinueve_hojas(self):
        hojas = {caso[0] for caso in CASOS}
        self.assertEqual(len(hojas), 19)

    def test_fuera_de_alcance(self):
        nombre = "Estra_dia: Tasa - de - interés"
        self.assertIsNone(clasificar(serie("Estra_dia", "pasdes3", nombre, fuente="bcra-dmd")))
        self.assertIsNone(clasificar(serie("Estra_dia", "pasdes3", nombre, archivo="otro.xls")))
        self.assertIsNone(clasificar(serie("Hoja_inexistente", "pasdes3", nombre)))
        self.assertIsNone(clasificar(serie("Totales_diarios", "codigo_nuevo", "Totales_diarios: algo")))

    def test_codigo_fuera_de_la_disposicion_conocida_no_se_rotula(self):
        # Una columna agregada al final, o un rótulo que no coincide con la posición, queda sin regla.
        self.assertIsNone(clasificar(serie("Estra_dia", "pasdes200", "Estra_dia: Tasa - de - interés")))
        self.assertIsNone(clasificar(serie("Estra_dia", "pasdes3", "Estra_dia: De 100.000 - a 499.999 - Monto")))
        self.assertIsNone(clasificar(serie("Estra_dia_bancos", "pasdes24", "Estra_dia_bancos: De 100.000 - a 499.999 - Monto")))
        self.assertIsNone(clasificar(serie("UVA_UVI", "pasvar16", "UVA_UVI: Plazo")))
        self.assertIsNone(clasificar(serie("Estra_dia_pub", "pasdespub130", "Estra_dia_pub: Tasa - de - interés")))

    def test_es_determinista(self):
        for hoja, variable, nombre, *_ in CASOS:
            self.assertEqual(clasificar(serie(hoja, variable, nombre)), clasificar(serie(hoja, variable, nombre)))

    def test_los_nombres_no_llevan_restos_de_la_fuente(self):
        for hoja, variable, nombre, *_ in CASOS:
            resultado = clasificar(serie(hoja, variable, nombre))
            self.assertNotRegex(resultado.nombre, r"\s-\s|_|\d+/|\(\d\)")
            self.assertEqual(resultado.nombre, resultado.nombre.strip())


if __name__ == "__main__":
    unittest.main()
