"""Pruebas de las reglas de las cuentas internacionales (balanza de pagos, PII, deuda externa e IED)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.externo_balance_pagos import EXCLUIDAS, UNIDADES, clasificar

DATOS = "datos.gob.ar"
CIN = "indec-cin"


def serie(fuente, hoja, id_, columna, variable="", frecuencia="T", archivo="latest.xls") -> Serie:
    return Serie(fuente=fuente, id=id_, nombre=columna, variable=variable or columna, descripcion=hoja, unidad="",
                 frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=columna, dataset=hoja, estado="VIGENTE")


def esperado(indicador, cobertura, apertura, medida, nombre, unidad="Millones de dólares") -> Clasificacion:
    return Clasificacion(tema="Sector externo", indicador=indicador, cobertura=cobertura, apertura=apertura,
                         medida=medida, nombre=nombre, unidad=unidad)


# (argumentos de serie, clasificación esperada). Series reales del inventario; cada caso fija una decisión de diseño.
CASOS = (
    (('indec-cin', 'Cuadro 1', 'cuadro-1|1-a-a-bienes', '1.A.a Bienes', '1-a-a-bienes', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Resumen', '', '', 'Balanza de pagos · Bienes', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 1', 'cuadro-1|pasivos-3-2-inversion-de-cartera-activos', 'Pasivos - 3.2 Inversión de cartera - Activos', 'pasivos-3-2-inversion-de-cartera-activos', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Inversión de cartera', 'Activos', 'Balanza de pagos · Inversión de cartera · Activos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 1', 'cuadro-1|activos-3-4-otra-inversion-pasivos', 'Activos - 3.4 Otra inversión - Pasivos', 'activos-3-4-otra-inversion-pasivos', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 2', 'cuadro-2|debito-1-b-2-2-inversion-de-cartera-credito', 'Débito - 1.B.2.2 Inversión de cartera - Crédito', 'debito-1-b-2-2-inversion-de-cartera-credito', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta corriente', 'Ingreso primario', '', 'Balanza de pagos · Renta de la inversión de cartera (crédito)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 2', 'cuadro-2|pasivos-3-cuenta-financiera-s1z-otros-sectores-activos', 'Pasivos - 3. Cuenta financiera - S1Z. Otros sectores - Activos', 'pasivos-3-cuenta-financiera-s1z-otros-sectores-activos', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Resumen', '', '', 'Balanza de pagos · Cuenta financiera · Otros sectores · Activos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 3', 'cuadro-3|ajuste-por-correccion-fletes-de-importacion-cif-fob', 'Ajuste por corrección fletes de importación (CIF/FOB)', 'ajuste-por-correccion-fletes-de-importacion-cif-fob', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta corriente', 'Bienes', '', 'Balanza de pagos · Bienes · Conciliación con el comercio exterior · Ajuste CIF/FOB por fletes (débito)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 4', 'cuadro-4|1-a-b-3-transporte', '1.A.b.3 Transporte', '1-a-b-3-transporte', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta corriente', 'Servicios', 'Transporte', 'Balanza de pagos · Servicios · Transporte', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 4', 'cuadro-4|egresos-servicios', 'EGRESOS SERVICIOS', 'egresos-servicios', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Resumen', '', '', 'Balanza de pagos · Servicios (débito)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 5', 'cuadro-5|credito-1-b-2-3-otra-inversion-s1z-otros-sectores', 'Crédito - 1.B.2.3 Otra inversión - S1Z. Otros sectores', 'credito-1-b-2-3-otra-inversion-s1z-otros-sectores', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta corriente', 'Ingreso primario', '', 'Balanza de pagos · Renta de otra inversión · Otros sectores (crédito)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 6', 'cuadro-6|emision-neta-de-pasivos-3-1-2-instrumentos-de-deuda-s1z-otros-sectores', 'Emisión neta de pasivos - 3.1.2 Instrumentos de deuda - S1Z. Otros sectores', 'emision-neta-de-pasivos-3-1-2-instrumentos-de-deuda-s1z-otros-sectores', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Inversión directa', 'Pasivos', 'Balanza de pagos · Inversión directa · Pasivos · Instrumentos de deuda · Otros sectores', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 7', 'cuadro-7|emision-neta-de-pasivos-3-2-2-3-gobierno-general-s13', 'Emisión neta de pasivos - 3.2.2.3 Gobierno general (S13)', 'emision-neta-de-pasivos-3-2-2-3-gobierno-general-s13', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Inversión de cartera', 'Pasivos', 'Balanza de pagos · Inversión de cartera · Pasivos · Títulos de deuda · Gobierno general', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 8', 'cuadro-8|emision-neta-de-pasivos-3-4-3-3-gobierno-general-s13', 'Emisión neta de pasivos - 3.4.3.3 Gobierno general (S13)', 'emision-neta-de-pasivos-3-4-3-3-gobierno-general-s13', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos · Préstamos · Gobierno general', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 8', 'cuadro-8|3-4-7-derechos-especiales-de-giro-emision-neta-de-pasivos', '3.4.7 Derechos especiales de giro (emisión neta de pasivos)', '3-4-7-derechos-especiales-de-giro-emision-neta-de-pasivos', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos · Asignaciones de derechos especiales de giro', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 9', 'cuadro-9|3-5-4-2-titulos-valores', '3.5.4.2 Títulos-valores', '3-5-4-2-titulos-valores', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Activos de reserva', '', 'Balanza de pagos · Activos de reserva · Otros activos de reserva · Títulos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 10', 'cuadro-10|pasivos-3-4-otra-inversion', 'Pasivos - 3.4 Otra inversión', 'pasivos-3-4-otra-inversion', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos · Banco central', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 11', 'cuadro-11|pasivos-3-4-otra-inversion-prestamos', 'Pasivos - 3.4 Otra inversión - Préstamos', 'pasivos-3-4-otra-inversion-prestamos', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos · Préstamos · Gobierno general', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 12', 'cuadro-12|emision-neta-de-pasivos-inversion-de-cartera-s122-sociedades-captadoras-de-depositos-excepto-el-banco-central-titulos-de-deuda', 'Emisión neta de pasivos - Inversión de cartera - S122. Sociedades captadoras de depósitos, excepto el banco central - Títulos de deuda', 'emision-neta-de-pasivos-inversion-de-cartera-s122-sociedades-captadoras-de-depositos-excepto-el-banco-central-titulos-de-deuda', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Inversión de cartera', 'Pasivos', 'Balanza de pagos · Inversión de cartera · Pasivos · Títulos de deuda · Sociedades captadoras de depósitos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 13', 'cuadro-13|emision-neta-de-pasivos-inversion-directa-s1z-otros-sectores-reinversion-de-utilidades', 'Emisión neta de pasivos - Inversión directa - S1Z. Otros sectores - Reinversión de utilidades', 'emision-neta-de-pasivos-inversion-directa-s1z-otros-sectores-reinversion-de-utilidades', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Inversión directa', 'Pasivos', 'Balanza de pagos · Inversión directa · Pasivos · Utilidades reinvertidas · Otros sectores', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 14', 'cuadro-14|Q.N.AR.W1.S1.S1.T.D.SJ21._Z._Z._Z.USD._T._X.N', '1.A.b.10.2.1 - Débito', '1-a-b-10-2-1-debito', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta corriente', 'Servicios', 'Otros servicios empresariales', 'Balanza de pagos · Servicios · Servicios jurídicos, contables y de consultoría de gerencia (débito)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 14', 'cuadro-14|Q.N.AR.W1X1.S121.S1.T.L.FA.O.F4.L.USD._T.N.N', '3.4.3.1.3 - Otros pasivos a largo plazo', '3-4-3-1-3-otros-pasivos-a-largo-plazo', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos · Préstamos · Banco central · Otros pasivos a largo plazo', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 14', 'cuadro-14|Q.N.AR.W1.S1.S1.T.N.EO._Z._Z._Z.USD._T._X.N', 'Errores y omisiones netos', 'errores-y-omisiones-netos', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Resumen', '', '', 'Balanza de pagos · Errores y omisiones netos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 14', 'cuadro-14|variacion-del-stock-de-activos-de-reserva-1-2', 'Variación del stock de activos de reserva (1) + (2)', 'variacion-del-stock-de-activos-de-reserva-1-2', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Activos de reserva', '', 'Balanza de pagos · Activos de reserva · Variación del stock', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 14', 'cuadro-14|Q.N.AR.W1.S13.S1.T.N.FA.F.F7.T.USD._T.T.N', '3.3.3 - Gobierno general', '3-3-3-gobierno-general', 'T', 'latest.xls'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Derivados financieros', 'Neto', 'Balanza de pagos · Derivados financieros · Gobierno general', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 15', 'cuadro-15|p-pasivos-4-3-prestamos', 'P. PASIVOS - 4.3 Préstamos', 'p-pasivos-4-3-prestamos', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Otra inversión', '', 'PII · Pasivos · Otra inversión · Préstamos', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 16', 'cuadro-16|p-pasivos-s13-gobierno-general-4-otra-inversion', 'P. PASIVOS - S13. Gobierno general - 4. Otra Inversión', 'p-pasivos-s13-gobierno-general-4-otra-inversion', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Otra inversión', '', 'PII · Pasivos · Otra inversión · Gobierno general', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 17', 'cuadro-17|a-activos-2-inversion-de-cartera-instrumentos-de-deuda-euro', 'A. Activos - 2. Inversión de cartera - Instrumentos de deuda - Euro', 'a-activos-2-inversion-de-cartera-instrumentos-de-deuda-euro', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Activos', 'Inversión de cartera', '', 'PII · Activos · Inversión de cartera · Títulos de deuda · En euros', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 17', 'cuadro-17|a-activos-5-activos-de-reserva-canasta-de-monedas-del-deg-derechos-especiales-de-giro-1', 'A. Activos - 5. Activos de reserva - Canasta de monedas del DEG (Derechos especiales de giro) (1)', 'a-activos-5-activos-de-reserva-canasta-de-monedas-del-deg-derechos-especiales-de-giro-1', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Activos', 'Activos de reserva', '', 'PII · Activos · Activos de reserva · En la canasta de monedas del DEG', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 18', 'cuadro-18|4-6-otras-cuentas-por-pagar', '4.6 Otras cuentas por pagar', '4-6-otras-cuentas-por-pagar', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Otra inversión', '', 'PII · Pasivos · Otra inversión · Otras cuentas por cobrar/pagar · Otros sectores', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 19', 'cuadro-19|Q.N.AR.W1.S13.S1.LE.L.FA.O.F4.T.USD._T.N.N', 'Pasivos - 4.3.3 - Gobierno general', 'pasivos-4-3-3-gobierno-general', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Otra inversión', '', 'PII · Pasivos · Otra inversión · Préstamos · Gobierno general', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 20', 'cuadro-20|p-pasivos-i-inversion-directa-a-instrumentos-de-deuda-transacciones-financieras', 'P. Pasivos - I. Inversion directa - a. Instrumentos de deuda - Transacciones financieras', 'p-pasivos-i-inversion-directa-a-instrumentos-de-deuda-transacciones-financieras', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Factores de variación', 'Pasivos', '', 'PII · Factores de variación · Pasivos · Inversión directa · Instrumentos de deuda (transacciones financieras)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 20', 'cuadro-20|p-pasivos-i-inversion-directa-a-instrumentos-de-deuda-saldo-final', 'P. Pasivos - I. Inversion directa - a. Instrumentos de deuda - Saldo final', 'p-pasivos-i-inversion-directa-a-instrumentos-de-deuda-saldo-final', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Inversión directa', '', 'PII · Pasivos · Inversión directa · Instrumentos de deuda', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 20', 'cuadro-20|a-activos-ii-inversion-de-cartera-a-participaciones-de-capital-0-0', 'A. Activos - II. Inversion de cartera - a. Participaciones de capital - 0.0', 'a-activos-ii-inversion-de-cartera-a-participaciones-de-capital-0-0', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Factores de variación', 'Activos', '', 'PII · Factores de variación · Activos · Inversión de cartera · Participaciones de capital (columna sin rótulo)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 21', 'cuadro-21|p-pasivos-s13-gobierno-general-3-derivados-financieros', 'P. PASIVOS - S13. Gobierno general - 3. Derivados financieros', 'p-pasivos-s13-gobierno-general-3-derivados-financieros', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Derivados financieros', '', 'PII · Pasivos · Derivados financieros · Gobierno general (valor nominal)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 22', 'cuadro-22|Q.N.AR.W1.S1.S1.LE.L.FA.P.F3.T.USD._T.M.N', 'Pasivos - 2.1.4 - Otros sectores - Inversión de cartera - Títulos de deuda', 'pasivos-2-1-4-otros-sectores-inversion-de-cartera-titulos-de-deuda', 'T', 'latest.xls'),
     esperado('Posición de inversión internacional', 'Pasivos', 'Inversión de cartera', '', 'PII · Pasivos · Inversión de cartera · Títulos de deuda (valor nominal)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 23', 'cuadro-23|sociedades-no-financieras-hogares-e-isflsh-a-largo-plazo-inversion-directa-credito-entre-empresas', 'Sociedades no financieras, hogares e ISFLSH - A largo plazo - Inversión directa: Crédito entre empresas', 'sociedades-no-financieras-hogares-e-isflsh-a-largo-plazo-inversion-directa-credito-entre-empresas', 'T', 'latest.xls'),
     esperado('Deuda externa', 'Total economía', 'Sociedades no financieras, hogares e ISFLSH', '', 'Deuda externa bruta · Sociedades no financieras, hogares e ISFLSH · Inversión directa', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 23', 'cuadro-23|sociedades-no-financieras-hogares-e-isflsh', 'Sociedades no financieras, hogares e ISFLSH', 'sociedades-no-financieras-hogares-e-isflsh', 'T', 'latest.xls'),
     esperado('Deuda externa', 'Total economía', 'Sociedades no financieras, hogares e ISFLSH', '', 'Deuda externa bruta · Sociedades no financieras, hogares e ISFLSH · Sin inversión directa', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 24', 'cuadro-24|sociedades-no-financieras-hogares-e-isflsh', 'Sociedades no financieras, hogares e ISFLSH', 'sociedades-no-financieras-hogares-e-isflsh', 'T', 'latest.xls'),
     esperado('Deuda externa', 'Total economía', 'Sociedades no financieras, hogares e ISFLSH', '', 'Deuda externa bruta · Sociedades no financieras, hogares e ISFLSH', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 29', 'cuadro-29|gobierno-general-titulos-de-deuda', 'Gobierno general - Títulos de deuda', 'gobierno-general-titulos-de-deuda', 'T', 'latest.xls'),
     esperado('Deuda externa', 'Total economía', 'Gobierno general', '', 'Deuda externa bruta · Gobierno general · Títulos de deuda (valor de mercado)', 'Millones de dólares')),
    (('indec-cin', 'Cuadro 30', 'cuadro-30|fmi', 'FMI', 'fmi', 'T', 'latest.xls'),
     esperado('Deuda externa', 'Total economía', 'Gobierno general', '', 'Deuda externa bruta · Gobierno general · Organismos internacionales · FMI', 'Millones de dólares')),
    (('datos.gob.ar', '14. Bce Pagos 3-17', '84.2_CRRIUDP_0_T_49', '84.2_CRRIUDP_0_T_49', 'cc_rentas_renta_inv_utilidades_dividendos_pagados', 'T', 'sector_externo.xlsx'),
     esperado('Balanza de pagos', 'Series históricas', 'MBP5', '', 'Balanza de pagos · Renta de la inversión · Utilidades y dividendos (débito)', 'Millones de dólares')),
    (('datos.gob.ar', '14.a Bce Pagos 6-17', '160.2_CTA_FINVOS_0_T_51', '160.2_CTA_FINVOS_0_T_51', 'cuenta_financieratra_inversion_emision_neta_pasivos', 'T', 'sector_externo.xlsx'),
     esperado('Balanza de pagos', 'Cuenta financiera', 'Otra inversión', 'Pasivos', 'Balanza de pagos · Otra inversión · Pasivos', 'Millones de dólares')),
    (('datos.gob.ar', '15.a Deuda Ext. Bruta x S. 6-17', '161.1_DA_SOCICTA_0_0_49', '161.1_DA_SOCICTA_0_0_49', 'deuda_sociedades_no_financieras_inversion_directa', 'T', 'sector_externo.xlsx'),
     esperado('Deuda externa', 'Total economía', 'Sociedades no financieras, hogares e ISFLSH', '', 'Deuda externa bruta · Sociedades no financieras, hogares e ISFLSH · Inversión directa', 'Millones de dólares')),
    (('datos.gob.ar', '16. PII 6-16', '86.1_OI_1991_A_19', '86.1_OI_1991_A_19', 'organismos_internac', 'A', 'sector_externo.xlsx'),
     esperado('Posición de inversión internacional', 'Series históricas', 'Base 1991', '', 'PII · Pasivos · Gobierno nacional · Otros pasivos externos · Organismos internacionales · FMI, BID, BIRF y BIS', 'Millones de dólares')),
    (('datos.gob.ar', '16.a PII 6-17', '144.4_AVOSGOLES_0_T_32', '144.4_AVOSGOLES_0_T_32', 'activos_gobierno_general_anuales', 'T', 'sector_externo.xlsx'),
     esperado('Posición de inversión internacional', 'Activos', 'Otra inversión', '', 'PII · Activos · Otra inversión · Sociedades captadoras de depósitos', 'Millones de dólares')),
    (('datos.gob.ar', '16.a PII 6-17', '144.3_AVOSGOLES_0_A_32', '144.3_AVOSGOLES_0_A_32', 'activos_gobierno_general_anuales', 'A', 'sector_externo.xlsx'),
     esperado('Posición de inversión internacional', 'Activos', 'Otra inversión', '', 'PII · Activos · Otra inversión · Sociedades captadoras de depósitos', 'Millones de dólares')),
    (('datos.gob.ar', '24. Stock IED', '312.1_TOTALTAL_0_S_5', '312.1_TOTALTAL_0_S_5', 'total', 'S', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Stock', 'Total', '', 'IED · Stock', 'Millones de dólares')),
    (('datos.gob.ar', '25. Flujos IED', '314.1_PARTICIPACTES_0_T_52', '314.1_PARTICIPACTES_0_T_52', 'participaciones_capital_distint_reinv_utilid_aportes', 'T', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Flujos', 'Total', '', 'IED · Flujos · Participaciones de capital sin utilidades reinvertidas · Aportes', 'Millones de dólares')),
    (('datos.gob.ar', '26. Ingreso IED', '315.1_RENTA_PROCDAD_0_T_49', '315.1_RENTA_PROCDAD_0_T_49', 'renta_procedent_particip_capital_distrib_utilidad', 'T', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Renta', 'Total', '', 'IED · Renta · Participaciones de capital · Distribución de utilidades', 'Millones de dólares')),
    (('datos.gob.ar', '27. Stock IED Sector', '316.1_INDUSTRIA_ERA_0_S_23', '316.1_INDUSTRIA_ERA_0_S_23', 'industria_manufacturera', 'S', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Stock', 'Por sector de actividad', '', 'IED · Stock · Industria manufacturera', 'Millones de dólares')),
    (('datos.gob.ar', '28. Flujos IED Sector', '317.1_TOTALTAL_0_T_5', '317.1_TOTALTAL_0_T_5', 'total', 'T', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Flujos', 'Total', '', 'IED · Flujos', 'Millones de dólares')),
    (('datos.gob.ar', '29. Ingresos IED Sector', '318.1_SUMINISTR_UBL_0_T_58', '318.1_SUMINISTR_UBL_0_T_58', 'suministr_agua_cloacs_resid_recuper_materiales_saneam_publ', 'T', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Renta', 'Por sector de actividad', '', 'IED · Renta · Participaciones de capital · Suministro de agua; cloacas; gestión de residuos y saneamiento público', 'Millones de dólares')),
    (('datos.gob.ar', '30. Stock IED Pais', '319.1_ESTADOS_UNICA_0_S_22', '319.1_ESTADOS_UNICA_0_S_22', 'estados_unidos_america', 'S', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Stock', 'Por país de origen', '', 'IED · Stock · Estados Unidos', 'Millones de dólares')),
    (('datos.gob.ar', '31. Flujo IED Pais', '320.1_REINO_UNIDNIA_0_T_25', '320.1_REINO_UNIDNIA_0_T_25', 'reino_unido_gran_bretania', 'T', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Flujos', 'Por país de origen', '', 'IED · Flujos · Reino Unido', 'Millones de dólares')),
    (('datos.gob.ar', '32. Ingreso IED Pais', '321.1_OTROSROS_0_T_5', '321.1_OTROSROS_0_T_5', 'otros', 'T', 'sector_externo.xlsx'),
     esperado('Inversión extranjera directa (BCRA)', 'Renta', 'Por país de origen', '', 'IED · Renta · Participaciones de capital · Otros países', 'Millones de dólares')),
    (('datos.gob.ar', '33. Deuda Externa Privada', '313.1_EN_PORCENTPIB_0_T_21', '313.1_EN_PORCENTPIB_0_T_21', 'en_porcentaje_del_pib', 'T', 'sector_externo.xlsx'),
     esperado('Deuda externa', 'Sector privado', '', '', 'Deuda externa privada (% del PIB)', 'Proporción (1 = 100%)')),
    (('datos.gob.ar', '33. Deuda Externa Privada', '313.1_PRESTAMOS_UDA_0_T_35', '313.1_PRESTAMOS_UDA_0_T_35', 'prestamos_financieros_titulos_deuda', 'T', 'sector_externo.xlsx'),
     esperado('Deuda externa', 'Sector privado', '', '', 'Deuda externa privada · Deuda financiera · Títulos de deuda', 'Millones de dólares')),
)


class ClasificarCuentasInternacionalesTest(unittest.TestCase):
    def test_casos_reales(self):
        for argumentos, bueno in CASOS:
            with self.subTest(hoja=argumentos[1], id=argumentos[2]):
                self.assertEqual(clasificar(serie(*argumentos)), bueno)

    def test_otra_fuente_o_libro_devuelve_none(self):
        self.assertIsNone(clasificar(serie("indec-ipc", "Cuadro 1", "cuadro-1|1-a-a-bienes", "1.A.a Bienes")))
        self.assertIsNone(clasificar(serie(DATOS, "14.a Bce Pagos 6-17", "160.2_TL_CUENTAL_0_T_20", "x",
                                           "total_cuenta_capital", "T", "otro_libro.xlsx")))
        self.assertIsNone(clasificar(serie(DATOS, "2. X Rubro", "x", "x", "x", "M", "sector_externo.xlsx")))
        self.assertIsNone(clasificar(serie(CIN, "Cuadro 25", "cuadro-25|x", "x")))

    def test_rotulo_o_clave_desconocidos_devuelven_none(self):
        self.assertIsNone(clasificar(serie(CIN, "Cuadro 1", "cuadro-1|nueva", "1.D Una partida nueva")))
        self.assertIsNone(clasificar(serie(CIN, "Cuadro 24", "cuadro-24|x", "Hogares - Préstamos")))
        self.assertIsNone(clasificar(serie(CIN, "Cuadro 14", "cuadro-14|Q.N.AR.W1.S1.S1.T.B.ZZ._Z._Z._Z.USD._T._X.N",
                                           "9.9 - Partida nueva")))
        self.assertIsNone(clasificar(serie(DATOS, "33. Deuda Externa Privada", "313.1_X", "x", "otra_variable", "T",
                                           "sector_externo.xlsx")))

    def test_dos_llamadas_dan_lo_mismo(self):
        for argumentos, _ in CASOS:
            with self.subTest(id=argumentos[2]):
                self.assertEqual(clasificar(serie(*argumentos)), clasificar(serie(*argumentos)))

    def test_mismo_concepto_mismo_nombre_entre_fuentes(self):
        cuadro = clasificar(serie(CIN, "Cuadro 2", "cuadro-2|1-cuenta-corriente", "1. Cuenta corriente"))
        libro_14a = clasificar(serie(DATOS, "14.a Bce Pagos 6-17", "160.2_TL_CUENNTE_0_T_22", "160.2_TL_CUENNTE_0_T_22",
                                     "total_cuenta_corriente", "T", "sector_externo.xlsx"))
        historica = clasificar(serie(DATOS, "14. Bce Pagos 3-17", "84.2_CT_0_T_8", "84.2_CT_0_T_8", "cc_total", "T",
                                     "sector_externo.xlsx"))
        self.assertEqual(cuadro.nombre, libro_14a.nombre)
        self.assertEqual(cuadro.nombre, historica.nombre)
        self.assertEqual(historica.cobertura, "Series históricas")
        self.assertEqual(libro_14a.cobertura, "Resumen")

    def test_valor_de_mercado_y_valor_nominal_se_distinguen(self):
        mercado = clasificar(serie(CIN, "Cuadro 16", "cuadro-16|x", "P. PASIVOS - S13. Gobierno general"))
        nominal = clasificar(serie(CIN, "Cuadro 21", "cuadro-21|x", "P. PASIVOS - S13. Gobierno general"))
        self.assertEqual(nominal.nombre, mercado.nombre + " (valor nominal)")
        self.assertEqual((nominal.cobertura, nominal.apertura), (mercado.cobertura, mercado.apertura))
        nominal_deuda = clasificar(serie(CIN, "Cuadro 24", "cuadro-24|x", "Gobierno general - Préstamos"))
        mercado_deuda = clasificar(serie(CIN, "Cuadro 29", "cuadro-29|x", "Gobierno general - Préstamos"))
        self.assertEqual(mercado_deuda.nombre, nominal_deuda.nombre + " (valor de mercado)")

    def test_nombres_de_la_misma_hoja_no_colapsan(self):
        creditos = clasificar(serie(CIN, "Cuadro 1", "cuadro-1|c", "Crédito - 1.B.2.2 Inversión de cartera - Débito"))
        debitos = clasificar(serie(CIN, "Cuadro 1", "cuadro-1|d", "Débito - 1.B.2.2 Inversión de cartera - Crédito"))
        self.assertNotEqual(creditos.nombre, debitos.nombre)
        self.assertTrue(creditos.nombre.endswith("(débito)"))
        self.assertTrue(debitos.nombre.endswith("(crédito)"))

    def test_ningun_nombre_pasa_de_120_caracteres(self):
        for argumentos, bueno in CASOS:
            self.assertLessEqual(len(bueno.nombre), 120)


    def test_mbp5_no_comparte_nombre_con_conceptos_distintos_del_mbp6(self):
        def mbp5(variable):
            return clasificar(serie(DATOS, "14. Bce Pagos 3-17", "84.2_X", "84.2_X", variable, "T", "sector_externo.xlsx"))
        # Signo contrario y sin activos de reserva: no es la cuenta financiera del MBP6.
        cuenta = mbp5("ccf_cuenta_financiera_total_cuenta_fianciera")
        self.assertEqual(cuenta.nombre, "Balanza de pagos · Cuenta financiera sin reservas (entradas netas)")
        self.assertNotEqual(cuenta.nombre, "Balanza de pagos · Cuenta financiera")
        self.assertEqual(mbp5("ccf_cuenta_financiera_sector_pub_no_finan_gob_locales").nombre,
                         "Balanza de pagos · Cuenta financiera sin reservas · Sector público no financiero · "
                         "Gobiernos locales (entradas netas)")
        self.assertEqual(mbp5("ccf_total_ccf").nombre,
                         "Balanza de pagos · Cuenta de capital y financiera sin reservas (entradas netas)")
        self.assertEqual(mbp5("cc_rentas_transf_corrientes").nombre, "Balanza de pagos · Transferencias corrientes")
        self.assertEqual(mbp5("cc_rentas_otras_rentas").nombre, "Balanza de pagos · Otras rentas")
        # Lo que sí coincide con el MBP6 conserva el nombre.
        self.assertEqual(mbp5("cc_total").nombre, "Balanza de pagos · Cuenta corriente")
        self.assertEqual(mbp5("variacion_reservas_int_total").nombre, "Balanza de pagos · Activos de reserva")

    def test_unidades_corregidas(self):
        cuadro_20 = [k for k in UNIDADES if k[0] == CIN and k[1].startswith("cuadro-20|")]
        self.assertEqual(len(cuadro_20), 113)
        self.assertIn((CIN, "cuadro-20|p-pasivos-i-inversion-directa-a-instrumentos-de-deuda-saldo-final"), UNIDADES)
        self.assertIn((CIN, "cuadro-20|b90-posicion-de-inversion-internacional-neta-a-p-saldo-inicial"), UNIDADES)
        self.assertEqual({UNIDADES[k] for k in cuadro_20}, {"Millones de dólares"})
        self.assertEqual(UNIDADES[(CIN, "cuadro-14|variaciones-del-tipo-de-cambio-2")], "Millones de dólares")
        self.assertEqual(UNIDADES[(CIN, "cuadro-14|variacion-del-stock-de-activos-de-reserva-1-2")], "Millones de dólares")
        for id_ in ("313.1_EN_PORCENTPIB_0_T_21", "313.1_EN_PORCENTNES_0_T_31", "313.1_EN_PORCENTRII_0_T_18"):
            self.assertEqual(UNIDADES[(DATOS, id_)], "Proporción (1 = 100%)")
        self.assertEqual(len(UNIDADES), 118)

    def test_exclusiones_fijadas(self):
        self.assertEqual(len(EXCLUIDAS), 17)
        claves = set(EXCLUIDAS)
        # Columna espuria del cuadro 20.
        self.assertIn((CIN, "cuadro-20|a-activos-ii-inversion-de-cartera-a-participaciones-de-capital-0-0"), claves)
        # Cuadro 9: desglose en ceros de otros activos de reserva.
        self.assertEqual({k[1] for k in claves if k[1].startswith("cuadro-9|")},
                         {"cuadro-9|3-5-4-1-moneda-y-depositos", "cuadro-9|3-5-4-2-titulos-valores",
                          "cuadro-9|3-5-4-3-derivados-financieros", "cuadro-9|3-5-4-4-otros-activos"})
        # Cuadro 19: sectores cruzados en los derivados.
        self.assertIn((CIN, "cuadro-19|Q.N.AR.W1.S122.S1.LE.L.FA.F.F7.T.USD._T.T.N"), claves)
        self.assertIn((CIN, "cuadro-19|Q.N.AR.W1.S13.S1.LE.L.FA.F.F7.T.USD._T.T.N"), claves)
        # Hoja 14.a: signo invertido desde 2017.
        self.assertIn((DATOS, "160.2_CTA_FINVOS_0_T_51"), claves)
        self.assertIn((DATOS, "160.1_CTA_FINVOS_0_A_51"), claves)
        # Hoja 16.a: copias de las dos subtablas (la original queda).
        self.assertIn((DATOS, "144.4_PCIONETA_0_T_37"), claves)
        self.assertNotIn((DATOS, "144.2_PCIONETA_0_T_37"), claves)
        self.assertIn((DATOS, "144.3_PVOSANLES_0_A_15"), claves)
        self.assertNotIn((DATOS, "144.1_PVOSANLES_0_A_15"), claves)
        # Hoja 16: series de la base 1991 sin ubicación segura.
        # No ubicar una serie en una identidad es una duda, no una prueba de error: se publican.
        self.assertNotIn((DATOS, "86.1_B_1991_A_6"), claves)
        self.assertNotIn((DATOS, "86.1_O_1991_A_13"), claves)
        # Las excluidas siguen teniendo regla (el índice las marca como EXCLUIDA, no como «sin regla»).
        self.assertIsNotNone(clasificar(serie(DATOS, "14.a Bce Pagos 6-17", "160.2_CTA_FINVOS_0_T_51", "x",
                                              "cuenta_financieratra_inversion_emision_neta_pasivos", "T",
                                              "sector_externo.xlsx")))

    def test_un_nombre_por_concepto_en_la_pii_de_hoja_16a_excluida(self):
        copia = clasificar(serie(DATOS, "16.a PII 6-17", "144.4_PCIONETA_0_T_37", "x", "x", "T", "sector_externo.xlsx"))
        original = clasificar(serie(DATOS, "16.a PII 6-17", "144.2_PCIONETA_0_T_37", "x", "x", "T", "sector_externo.xlsx"))
        self.assertEqual(copia, original)


if __name__ == "__main__":
    unittest.main()
