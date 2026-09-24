import unittest

from scrapers import scraper_BCRA_tasas_depositos as scraper


class ScraperBCRATasasDepositosTests(unittest.TestCase):
    def test_extrae_todas_las_hojas_y_codigos(self) -> None:
        if not scraper.ARCHIVO_FUENTE.exists():
            self.skipTest("No esta la copia local de pas2026.xls")
        hojas, inventario = scraper.construir_salida(scraper.ARCHIVO_FUENTE)
        self.assertEqual(len(hojas), 19)
        self.assertGreater(len(inventario), 1_000)
        self.assertFalse(inventario[["Código fuente", "ID"]].duplicated().any())
        self.assertTrue(inventario["ID"].str.contains("|", regex=False).all())
        for datos in hojas.values():
            self.assertFalse(datos["fecha"].duplicated().any())
            self.assertTrue(datos["fecha"].is_monotonic_increasing)


if __name__ == "__main__":
    unittest.main()
