import unittest

import pandas as pd

from tools.agrupaciones import completar_agrupaciones


class AgrupacionesTests(unittest.TestCase):
    def test_ipc_conserva_libro_hojas_y_region(self):
        rows = pd.DataFrame([
            {"Código fuente": "indec-ipc", "Archivo origen": "latest.xls",
             "Hoja origen": sheet, "Nombre serie": title,
             "Variable": title.split("|")[-2].strip()}
            for sheet, title in [
                ("Índices IPC Cobertura Nacional",
                 "IPC nacional | Región Cuyo - Alimentos | region-cuyo-alimentos | mensual"),
                ("Índices IPC Cobertura Nacional",
                 "IPC nacional | Región Cuyo - Vivienda | region-cuyo-vivienda | mensual"),
                ("Variación mensual IPC Nacional",
                 "IPC nacional | Región Cuyo - Alimentos | region-cuyo-alimentos | mensual"),
                ("Variación mensual IPC Nacional",
                 "IPC nacional | Región Cuyo - Vivienda | region-cuyo-vivienda | mensual"),
            ]
        ])
        result = completar_agrupaciones(rows)
        self.assertEqual(set(result["Grupo de hojas"]), {"IPC cobertura nacional"})
        self.assertEqual(set(result["Grupo de series 1"]), {"Región Cuyo"})
        self.assertTrue(result["Grupo de series 2"].isna().all())

    def test_no_crea_grupos_solitarios(self):
        rows = pd.DataFrame([{"Código fuente": "indec-ipc", "Archivo origen": "latest.xls",
                             "Hoja origen": "IPC GBA Base dic 2016",
                             "Nombre serie": "IPC | Grupo aislado | Alimentos"}])
        result = completar_agrupaciones(rows)
        self.assertTrue(result["Grupo de hojas"].isna().all())
        self.assertTrue(result["Grupo de series 1"].isna().all())

    def test_familias_conceptuales_sin_capitulos_numericos(self):
        rows = pd.DataFrame([
            {"Código fuente": "indec-cin", "Archivo origen": "latest.xlsx",
             "Hoja origen": sheet, "Título dataset": title, "Nombre serie": "Total"}
            for sheet, title in [
                ("Cuadro 1", "Cuadro 1: Resumen de balanza de pagos"),
                ("Cuadro 14", "Cuadro 14: Detalle de balanza de pagos"),
                ("Cuadro 15", "Cuadro 15: Posición de Inversión Internacional"),
                ("Cuadro 16", "Cuadro 16: Posición de Inversión Internacional por sector"),
            ]
        ] + [
            {"Código fuente": "datos.gob.ar", "Archivo origen": "precios.xlsx",
             "Hoja origen": sheet, "Título dataset": "Precios", "Nombre serie": "Índice"}
            for sheet in ("4.1.1", "4.1.2")
        ])
        result = completar_agrupaciones(rows)
        self.assertEqual(result.iloc[:4]["Grupo de hojas"].tolist(), [
            "Balanza de pagos", "Balanza de pagos",
            "Posición de inversión internacional", "Posición de inversión internacional",
        ])
        self.assertTrue(result.iloc[4:]["Grupo de hojas"].isna().all())

    def test_grupos_fiscales_bancarios_y_geograficos(self):
        rows = pd.DataFrame([
            {"Código fuente": "datos.gob.ar", "Archivo origen": "finanzas_publicas.xlsx",
             "Hoja origen": "SPN", "Nombre serie": name}
            for name in ("Gastos corrientes intereses | Serie anual",
                         "Gastos corrientes transferencias | Serie anual")
        ] + [
            {"Código fuente": "datos.gob.ar", "Archivo origen": "dinero_bancos.xlsx",
             "Hoja origen": "8.14 Situacion Patrimonial", "Nombre serie": name}
            for name in ("Bancos Públicos. Activo", "Bancos Públicos. Pasivo")
        ] + [
            {"Código fuente": "indec-supermercados", "Archivo origen": "latest.xlsx",
             "Hoja origen": "Cuadro 5.", "Nombre serie": name}
            for name in ("Ventas - Córdoba - Alimentos - Pesos",
                         "Ventas - Córdoba - Bebidas - Pesos")
        ])
        result = completar_agrupaciones(rows)
        self.assertEqual(result["Grupo de series 1"].tolist(), [
            "Gastos", "Gastos", "Bancos Públicos", "Bancos Públicos", "Córdoba", "Córdoba",
        ])
        self.assertEqual(result.iloc[:2]["Grupo de series 2"].tolist(), ["Corrientes"] * 2)

    def test_base_devengado_separa_erogaciones_y_financiamiento(self):
        rows = pd.DataFrame([
            {"Código fuente": "datos.gob.ar", "Archivo origen": "finanzas_publicas.xlsx",
             "Hoja origen": "SPA_Dev_61", "Nombre serie": name}
            for name in (
                "Erogaciones corrientes . Personal | Serie anual",
                "Erogaciones corrientes . Bienes y servicios | Serie anual",
                "Financiamiento neto . Uso del crédito | Serie anual",
                "Financiamiento por contribuciones figurativas | Serie anual",
            )
        ])
        result = completar_agrupaciones(rows)
        self.assertEqual(result["Grupo de series 1"].tolist(),
                         ["Erogaciones", "Erogaciones", "Financiamiento", "Financiamiento"])
        self.assertEqual(result.iloc[:2]["Grupo de series 2"].tolist(), ["Corrientes"] * 2)

    def test_bancos_productos_y_regiones_del_ipc(self):
        rows = pd.DataFrame([
            {"Código fuente": "datos.gob.ar", "Archivo origen": "dinero_bancos.xlsx",
             "Hoja origen": "8.13 Rentabilidad financiera", "Nombre serie": name}
            for name in ("Bancos ext priv margen financ | Anual",
                         "Bancos ext priv resultado total | Mensual")
        ] + [
            {"Código fuente": "datos.gob.ar", "Archivo origen": "dinero_bancos.xlsx",
             "Hoja origen": "8.9 Balance BCRA", "Nombre serie": name}
            for name in ("Apertura activo. Reservas | Mensual", "Total del activo | Mensual")
        ] + [
            {"Código fuente": "datos.gob.ar", "Archivo origen": "actividad.xlsx",
             "Hoja origen": "1.17 Productos ind.", "Nombre serie": name}
            for name in ("Acero crudo | Producción mensual", "Acero crudo | Producción anual")
        ] + [
            {"Código fuente": "datos.gob.ar", "Archivo origen": "precios.xlsx",
             "Hoja origen": "4.1.2 IPC Capitulos", "Nombre serie": name}
            for name in ("IPC. Alimentos. Región pampeana. Base dic 2016. Trimestral",
                         "IPC. Bebidas. Región pampeana. Base dic 2016. Trimestral")
        ])
        result = completar_agrupaciones(rows)
        self.assertEqual(result["Grupo de series 1"].tolist(), [
            "Bancos privados extranjeros", "Bancos privados extranjeros",
            "Activo", "Activo", "Acero crudo", "Acero crudo",
            "Región pampeana", "Región pampeana",
        ])


if __name__ == "__main__":
    unittest.main()
