import unittest
from pathlib import Path

import pandas as pd

from scrapers import scraper_BCRA_indices_tipo_cambio as scraper


class ScraperBCRAIndicesTests(unittest.TestCase):
    def test_construye_todas_las_series_publicadas(self) -> None:
        archivos = {nombre: scraper.CARPETA_FUENTE / nombre for nombre in scraper.EXCEL_URLS}
        if not all(ruta.exists() for ruta in archivos.values()):
            self.skipTest("No están las copias locales de los Excel BCRA")
        hojas, inventario = scraper.construir_salida(archivos)
        self.assertEqual(set(hojas), scraper.HOJAS_ADMINISTRADAS)
        self.assertEqual(len(inventario), 73)
        self.assertFalse(inventario[["Código fuente", "ID"]].duplicated().any())
        mensual = hojas["BCRA ITCRM M"]
        self.assertTrue((mensual["fecha"].dt.day == 1).all())
        self.assertIn("ITCRB Estados Unidos", mensual.columns)

    def test_identidad_heymann_es_estable(self) -> None:
        esperado = (
            "ITCRMSerie.xlsx|ITCRM y bilaterales prom. mens.|ITCRB Estados Unidos"
        )
        self.assertEqual(scraper.CODIGO_FUENTE, "bcra-itc")
        self.assertEqual(
            esperado,
            "ITCRMSerie.xlsx|ITCRM y bilaterales prom. mens.|ITCRB Estados Unidos",
        )


if __name__ == "__main__":
    unittest.main()
