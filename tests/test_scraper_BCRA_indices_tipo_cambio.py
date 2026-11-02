import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

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

    def test_ante_403_reintenta_con_cabeceras_de_navegador(self) -> None:
        def respuesta(estado: int) -> MagicMock:
            r = MagicMock()
            r.__enter__.return_value = r
            r.iter_content.return_value = [b"xlsx"]
            if estado != 200:
                r.raise_for_status.side_effect = requests.HTTPError(response=MagicMock(status_code=estado))
            return r

        sesion = MagicMock()
        sesion.__enter__.return_value = sesion
        sesion.get.side_effect = [respuesta(403), respuesta(200), respuesta(403), respuesta(200)]
        with TemporaryDirectory() as carpeta,                 patch.object(scraper, "CARPETA_FUENTE", Path(carpeta)),                 patch.object(scraper, "_sesion", return_value=sesion),                 patch.object(scraper, "_excel_valido", return_value=True):
            resultado = scraper.descargar_excels()
            self.assertEqual(set(resultado), set(scraper.EXCEL_URLS))
            self.assertTrue(all(ruta.read_bytes() == b"xlsx" for ruta in resultado.values()))
        cabeceras = [llamada.kwargs.get("headers") for llamada in sesion.get.call_args_list]
        self.assertEqual(cabeceras, [None, scraper.CABECERAS_NAVEGADOR] * 2)

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
