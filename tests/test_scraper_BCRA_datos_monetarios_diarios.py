import unittest

from scrapers import scraper_BCRA_datos_monetarios_diarios as scraper


class ScraperBCRADatosMonetariosDiariosTests(unittest.TestCase):
    def test_extrae_todas_las_hojas_y_conserva_ids_bcra(self) -> None:
        if not scraper.ARCHIVO_FUENTE.exists():
            self.skipTest("No esta la copia local de series.xlsm")
        hojas, inventario = scraper.construir_salida(scraper.ARCHIVO_FUENTE)
        self.assertEqual(set(hojas), scraper.HOJAS_ADMINISTRADAS)
        self.assertGreaterEqual(len(inventario), 154)
        self.assertFalse(inventario[["Código fuente", "ID"]].duplicated().any())
        self.assertIn("46", set(inventario["ID"]))
        self.assertIn("1196", set(inventario["ID"]))
        self.assertTrue((inventario["Frecuencia"] == "D").all())
        for datos in hojas.values():
            self.assertTrue(datos["fecha"].is_monotonic_increasing)
            self.assertFalse(datos["fecha"].duplicated().any())


if __name__ == "__main__":
    unittest.main()
