import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from openpyxl import Workbook, load_workbook

from scrapers import scraper_IED


class RespuestaFalsa:
    def __init__(self, bloques: list[bytes]) -> None:
        self.bloques = bloques
        self.cerrada = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cerrada = True

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        return iter(self.bloques)


class ScraperIEDTests(unittest.TestCase):
    def test_guardado_fusiona_historia_y_prioriza_valores_nuevos(self) -> None:
        ruta = Path(__file__).parent / "_BD_fusion_prueba.xlsx"
        temporal = Path(__file__).parent / "_BD_fusion_prueba.actualizacion.xlsx"
        self.addCleanup(ruta.unlink, missing_ok=True)
        self.addCleanup(temporal.unlink, missing_ok=True)
        libro = Workbook()
        hoja = libro.active
        hoja.title = "Serie M"
        hoja.append(["fecha", "serie", "otra"])
        hoja.append([pd.Timestamp(2020, 1, 1), 10, 100])
        hoja.append([pd.Timestamp(2020, 2, 1), 20, 200])
        hoja.append([pd.Timestamp(2020, 3, 1), 30, 300])
        ajena = libro.create_sheet("Otra fuente")
        ajena["A1"] = "intacta"
        libro.save(ruta)
        libro.close()

        nueva = pd.DataFrame({
            "fecha": [pd.Timestamp(2020, 2, 1), pd.Timestamp(2020, 3, 1),
                      pd.Timestamp(2020, 4, 1)],
            "serie": [22, None, 40],
        })
        scraper_IED.guardar_datos_preservando_formato(ruta, {"Serie M": nueva})

        actualizado = load_workbook(ruta, data_only=True)
        filas = list(actualizado["Serie M"].values)
        self.assertEqual(filas[0], ("fecha", "serie", "otra"))
        self.assertEqual(
            [(fila[0].strftime("%Y-%m-%d"), *fila[1:]) for fila in filas[1:]],
            [("2020-01-01", 10, 100), ("2020-02-01", 22, 200),
             ("2020-03-01", 30, 300), ("2020-04-01", 40, None)],
        )
        self.assertEqual(actualizado["Otra fuente"]["A1"].value, "intacta")
        actualizado.close()

    def test_guardar_descarga_escribe_bloques_y_cierra_respuesta(self) -> None:
        destino = Path(__file__).parent / "_descarga_prueba.xlsx"
        self.addCleanup(destino.unlink, missing_ok=True)
        respuesta = RespuestaFalsa([b"abc", b"", b"def"])

        scraper_IED._guardar_descarga(respuesta, destino)

        self.assertEqual(destino.read_bytes(), b"abcdef")
        self.assertTrue(respuesta.cerrada)

    def test_guardado_preserva_hojas_administrativas_y_reemplaza_atomico(self) -> None:
        ruta = Path(__file__).parent / "_BD_prueba.xlsx"
        temporal = Path(__file__).parent / "_BD_prueba.actualizacion.xlsx"
        self.addCleanup(ruta.unlink, missing_ok=True)
        self.addCleanup(temporal.unlink, missing_ok=True)
        libro = Workbook()
        libro.active.title = "Codificacion"
        libro["Codificacion"]["A1"] = "control"
        libro.create_sheet("Serie M")
        libro.save(ruta)

        datos = pd.DataFrame(
            {"fecha": [pd.Timestamp(2026, 8, 1)], "variable": [12.5]}
        )
        scraper_IED.guardar_datos_preservando_formato(ruta, {"Serie M": datos})

        actualizado = load_workbook(ruta)
        self.assertEqual(actualizado["Codificacion"]["A1"].value, "control")
        self.assertEqual(actualizado["Serie M"]["B2"].value, 12.5)
        self.assertEqual(actualizado["Serie M"]["A2"].number_format, "yyyy-mm")
        actualizado.close()
        self.assertFalse(temporal.exists())

    def test_ejecutar_informa_series_fallidas(self) -> None:
        resumen = {"total": 3, "exitosas": 2, "fallidas": 1}
        with patch.object(scraper_IED, "procesar_datos", return_value=resumen):
            with self.assertRaisesRegex(RuntimeError, "1 de 3 series fallidas"):
                scraper_IED.ejecutar()


if __name__ == "__main__":
    unittest.main()
