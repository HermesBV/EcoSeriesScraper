import unittest
from datetime import datetime
from pathlib import Path
import tempfile

import pandas as pd

from scrapers import scraper_MECON_imig as scraper


def hoja_mensual(mes: datetime, ingresos: float, extra: bool = False) -> pd.DataFrame:
    filas = [
        [None, "INFORME MENSUAL DE INGRESOS Y GASTOS", None, None, None, None, None, None],
        [None, None, None, None, None, None, "Dato mensual", None],
        [None, None, None, None, None, None, mes, datetime(mes.year - 1, mes.month, 1)],
        [None, "INGRESOS TOTALES", None, None, None, None, ingresos, ingresos / 2],
        [None, None, "Tributarios", None, None, None, 80.0, 40.0],
        [None, None, None, "IVA neto de reintegros", None, None, 30.0, 15.0],
        [None, "GASTOS PRIMARIOS", None, None, None, None, 90.0, 45.0],
        [None, None, "Otros Gastos Corrientes  ", None, None, None, 5.0, 2.0],
        [None, "RESULTADO PRIMARIO", None, None, None, None, ingresos - 90.0, None],
        [None, None, "Intereses Netos (2)", None, None, None, 7.0, 3.0],
        [None, "RESULTADO FINANCIERO", None, None, None, None, ingresos - 97.0, None],
        [None, "(*) Recursos extraordinarios: nota", None, None, None, None, None, None],
    ]
    if extra:
        filas.insert(8, [None, None, "Otros gastos corrientes", None, None, None, 5.0, 2.0])
    return pd.DataFrame(filas)


class ExtraerHojaTests(unittest.TestCase):
    def test_conceptos_jerarquicos_y_resultados_planos(self) -> None:
        series = scraper.extraer_hoja(hoja_mensual(datetime(2026, 8, 1), 100.0))
        self.assertEqual(series["INGRESOS TOTALES"], {pd.Timestamp("2026-08-01"): 100.0, pd.Timestamp("2025-08-01"): 50.0})
        self.assertEqual(series["INGRESOS TOTALES - Tributarios - IVA neto de reintegros"][pd.Timestamp("2026-08-01")], 30.0)
        self.assertEqual(series["RESULTADO PRIMARIO"][pd.Timestamp("2026-08-01")], 10.0)
        self.assertEqual(series["Intereses Netos"][pd.Timestamp("2026-08-01")], 7.0)
        self.assertNotIn("RESULTADO PRIMARIO - Intereses Netos", series)
        self.assertEqual(series["RESULTADO FINANCIERO"][pd.Timestamp("2026-08-01")], 3.0)

    def test_hoja_sin_fechas_se_ignora(self) -> None:
        self.assertEqual(scraper.extraer_hoja(pd.DataFrame([["CONCEPTO", "TOTAL"], ["Ingresos", 1.0]])), {})


class ConstruirSalidaTests(unittest.TestCase):
    def test_el_informe_mas_reciente_revisa_y_unifica_nombres(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            viejo = Path(carpeta) / "imig_julio_2026.xlsx"
            nuevo = Path(carpeta) / "agosto.xlsx"
            hoja_mensual(datetime(2026, 7, 1), 70.0).to_excel(viejo, header=False, index=False, sheet_name="Julio")
            with pd.ExcelWriter(nuevo) as writer:
                hoja_mensual(datetime(2026, 8, 1), 100.0, extra=True).to_excel(writer, header=False, index=False, sheet_name="Agosto")
                pd.DataFrame([["VarMensual"], ["Dato mensual"]]).to_excel(writer, header=False, index=False, sheet_name="VarMensual")
                mensualizacion = pd.DataFrame([
                    [None] * 8,
                    [None, None, None, None, None, None, datetime(2026, 7, 1), datetime(2026, 8, 1)],
                    [None, "INGRESOS TOTALES", None, None, None, None, 71.0, 100.0],
                ])
                mensualizacion.to_excel(writer, header=False, index=False, sheet_name="Mensualizacion")
            datos, inventario = scraper.construir_salida([("", viejo), ("", nuevo)])
        ingresos = datos.set_index("fecha")["INGRESOS TOTALES"]
        self.assertEqual(ingresos[pd.Timestamp("2026-07-01")], 71.0)   # revisado por el informe de agosto
        self.assertEqual(ingresos[pd.Timestamp("2026-08-01")], 100.0)
        self.assertEqual(ingresos[pd.Timestamp("2025-07-01")], 35.0)   # mismo mes del año anterior del informe viejo
        ids = set(inventario["ID"])
        self.assertIn("gastos-primarios-otros-gastos-corrientes", ids)
        self.assertEqual(inventario["ID"].duplicated().sum(), 0)
        self.assertEqual(set(inventario["Pestaña BD"]), {scraper.HOJA_BD})
        self.assertEqual(set(inventario["Tema"]), {"Finanzas públicas"})


if __name__ == "__main__":
    unittest.main()
