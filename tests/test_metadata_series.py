import unittest

import pandas as pd

from tools.metadata_series import mejorar_metadatos


class MetadataSeriesTests(unittest.TestCase):
    def test_titulo_iiep_permanece_exacto_con_titulo_bcra_duplicado(self):
        rows = pd.DataFrame([
            {"ID": "itcrb-eeuu-empalmado-importacion-m", "Código fuente": "iiep",
             "Nombre serie": "ITCRB Estados Unidos (mensual)",
             "Descripción": "Empalme IIEP con BCRA", "Variable": "itcrb_empalmado",
             "Frecuencia": "M", "Valoración": "No aplica", "Unidad": "Índice"},
            {"ID": "itcrb-eeuu-m", "Código fuente": "bcra-itc",
             "Nombre serie": "ITCRB Estados Unidos (mensual)",
             "Descripción": "Serie BCRA", "Variable": "itcrb_bcra",
             "Frecuencia": "M", "Valoración": "No aplica", "Unidad": "Índice"},
        ])
        result = mejorar_metadatos(rows)
        self.assertEqual(result["Nombre serie"].tolist(), ["ITCRB Estados Unidos (mensual)"] * 2)
        self.assertEqual(result.loc[0, "Descripción"], "Empalme IIEP con BCRA")
        self.assertEqual(mejorar_metadatos(result)["Nombre serie"].tolist(), result["Nombre serie"].tolist())

    def test_titulos_repetidos_se_conservan_aunque_varien_variable_frecuencia_o_periodo(self):
        rows = pd.DataFrame([
            {"Nombre serie": "Intercambio Comercial Argentino. Valores anuales", "Descripción": "Exportaciones totales. En millones de dólares.", "Variable": "ica_expo_totales", "Frecuencia": "A", "Valoración": "No informado", "Unidad": "Millones de dólares", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Intercambio Comercial Argentino. Valores anuales", "Descripción": "Importaciones totales. En millones de dólares.", "Variable": "ica_importaciones_totales", "Frecuencia": "A", "Valoración": "No informado", "Unidad": "Millones de dólares", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Consumo de gas", "Descripción": "Residencial", "Variable": "residencial", "Frecuencia": "A", "Valoración": "No informado", "Unidad": "m3", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Consumo de gas", "Descripción": "Residencial", "Variable": "residencial", "Frecuencia": "T", "Valoración": "No informado", "Unidad": "m3", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Cuenta AIF (1990-1992)", "Descripción": "Ahorro", "Variable": "ahorro", "Frecuencia": "A", "Desde": "1987", "Hasta": "1989", "Valoración": "No informado", "Unidad": "Variación Porcentual", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Cuenta AIF (1990-1992)", "Descripción": "Ahorro", "Variable": "ahorro", "Frecuencia": "A", "Desde": "1990", "Hasta": "1992", "Valoración": "No informado", "Unidad": "Variación Porcentual", "Código fuente": "datos.gob.ar"},
        ])
        result = mejorar_metadatos(rows)
        self.assertEqual(result["Nombre serie"].tolist(), rows["Nombre serie"].tolist())
        self.assertEqual(result.loc[0, "Valoración"], "Precios corrientes")

    def test_valoracion_conservadora(self):
        rows = pd.DataFrame([
            {"Nombre serie": "PIB a precios de 2004", "Descripción": "Valor agregado", "Variable": "pib", "Valoración": "No informado", "Unidad": "Millones de pesos", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Indicador incierto", "Descripción": "Sin información de precios", "Variable": "x", "Valoración": "No informado", "Unidad": "Pesos", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Índice de producción", "Descripción": "Base 2016=100", "Variable": "indice", "Valoración": "No informado", "Unidad": "Ver descripción de la serie", "Código fuente": "indec-ipi-manufacturero"},
            {"Nombre serie": "Balanza - Exportaciones - Var.% mensual", "Descripción": "Balanza comercial", "Variable": "x", "Valoración": "No informado", "Unidad": "Ver descripción de la serie", "Código fuente": "indec-ica"},
        ])
        result = mejorar_metadatos(rows)
        self.assertEqual(result["Valoración"].tolist(), [
            "Precios constantes", "No informado", "No aplica", "No aplica",
        ])


if __name__ == "__main__":
    unittest.main()
