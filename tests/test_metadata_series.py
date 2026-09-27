import unittest

import pandas as pd

from tools.metadata_series import mejorar_metadatos


class MetadataSeriesTests(unittest.TestCase):
    def test_titulo_identifica_variable_y_frecuencia(self):
        rows = pd.DataFrame([
            {"Nombre serie": "Intercambio Comercial Argentino. Valores anuales", "Descripción": "Exportaciones totales. En millones de dólares.", "Variable": "ica_expo_totales", "Frecuencia": "A", "Valoración": "No informado", "Unidad": "Millones de dólares", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Intercambio Comercial Argentino. Valores anuales", "Descripción": "Importaciones totales. En millones de dólares.", "Variable": "ica_importaciones_totales", "Frecuencia": "A", "Valoración": "No informado", "Unidad": "Millones de dólares", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Consumo de gas", "Descripción": "Residencial", "Variable": "residencial", "Frecuencia": "A", "Valoración": "No informado", "Unidad": "m3", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Consumo de gas", "Descripción": "Residencial", "Variable": "residencial", "Frecuencia": "T", "Valoración": "No informado", "Unidad": "m3", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Cuenta AIF (1990-1992)", "Descripción": "Ahorro", "Variable": "ahorro", "Frecuencia": "A", "Desde": "1987", "Hasta": "1989", "Valoración": "No informado", "Unidad": "Variación Porcentual", "Código fuente": "datos.gob.ar"},
            {"Nombre serie": "Cuenta AIF (1990-1992)", "Descripción": "Ahorro", "Variable": "ahorro", "Frecuencia": "A", "Desde": "1990", "Hasta": "1992", "Valoración": "No informado", "Unidad": "Variación Porcentual", "Código fuente": "datos.gob.ar"},
        ])
        result = mejorar_metadatos(rows)
        self.assertTrue(result.loc[0, "Nombre serie"].startswith("Exportaciones totales |"))
        self.assertTrue(result.loc[1, "Nombre serie"].startswith("Importaciones totales |"))
        self.assertEqual(result["Nombre serie"].nunique(), 6)
        self.assertIn("1987-1989", result.loc[4, "Nombre serie"])
        self.assertIn("1990-1992", result.loc[5, "Nombre serie"])
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
