import unittest

import pandas as pd

from scrapers import scraper_IIEP_tipo_cambio_real as scraper


class ScraperIIEPTipoCambioRealTests(unittest.TestCase):
    def test_empalma_en_primer_mes_bcra_y_preserva_bcra(self) -> None:
        iiep = pd.DataFrame({
            "fecha": pd.to_datetime(["1996-12-01", "1997-01-01", "1997-02-01"]),
            "valor": [80.0, 100.0, 110.0],
        })
        bcra = pd.DataFrame({
            "fecha": pd.to_datetime(["1997-01-01", "1997-02-01"]),
            "valor": [50.0, 60.0],
        })
        resultado, factor, inicio = scraper.empalmar(iiep, bcra)
        self.assertEqual(factor, 0.5)
        self.assertEqual(inicio, pd.Timestamp("1997-01-01"))
        self.assertEqual(resultado.iloc[0][scraper.COLUMNA_SALIDA], 40.0)
        self.assertEqual(resultado.iloc[-1][scraper.COLUMNA_SALIDA], 60.0)
        self.assertEqual(len(resultado), 3)


if __name__ == "__main__":
    unittest.main()
