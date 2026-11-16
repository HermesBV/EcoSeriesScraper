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


class ExtraccionPorBloquesTests(unittest.TestCase):
    """Casos tomados de los libros del Ministerio (hojas 4.15.1 y «LP - Hog»)."""

    def test_trimestre_pegado_al_anio(self) -> None:
        esperado = pd.Timestamp(2014, 6, 30)
        for texto in ("II14", "II 14", "II-14", "II.14", "II 2014", "ii14", "II 14*"):
            with self.subTest(texto=texto):
                self.assertEqual(scraper_IED.parse_fecha_manual(texto), esperado)
                self.assertEqual(scraper_IED._tipo_fecha_cruda(texto), "trimestre")
        self.assertEqual(scraper_IED._tipo_fecha_cruda("2014"), "anio")
        self.assertTrue(pd.isna(scraper_IED.parse_fecha_manual("I.S.03")))

    def test_bloque_trimestral_no_se_corta_en_el_rotulo_pegado(self) -> None:
        hoja = pd.DataFrame([
            ["98.2_IT", "98.2_INNG"],
            ["I 14", 97.8], ["II14", 103.8], ["III 14", 108.2], ["IV 14", 112.2], ["I 15", 115.9],
            [None, None],
            ["98.3_IT", "98.3_INNG"],
            [pd.Timestamp(2013, 12, 1), 91.5],
        ])
        serie = scraper_IED.extraer_serie_desde_indice({"4.15.1": hoja}, ("4.15.1", 0, 1))
        self.assertEqual(serie["valor"].tolist(), [97.8, 103.8, 108.2, 112.2, 115.9])

    def test_la_columna_de_fechas_es_la_del_bloque_del_id(self) -> None:
        # Bloque 63.1: fechas en la columna 1. Debajo, el 63.2, más largo, con fechas en la columna 0.
        filas = [[None, "63.1_IT", "63.1_T"]]
        filas += [[None, pd.Timestamp(2001 + n // 2, 5 if n % 2 == 0 else 10, 1), valor]
                  for n, valor in enumerate((0.262, 0.28, 0.414, 0.457, 0.426))]
        filas += [[None, None, None], [None, "63.2_IT", "63.2_H"]]
        filas += [[pd.Timestamp(2003 + n // 2, 1 if n % 2 == 0 else 7, 1), f"{'I' if n % 2 == 0 else 'II'}.S.{3 + n // 2:02d}", valor]
                  for n, valor in enumerate((0.427, 0.365, 0.335, 0.298, 0.288, 0.247, 0.231, 0.192))]
        hoja = pd.DataFrame(filas)
        self.assertEqual(scraper_IED._columna_fecha(hoja, 0, 2), 1)
        puntual = scraper_IED.extraer_serie_desde_indice({"LP - Hog": hoja}, ("LP - Hog", 0, 2))
        self.assertEqual(puntual["valor"].tolist(), [0.262, 0.28, 0.414, 0.457, 0.426])
        self.assertEqual(scraper_IED._columna_fecha(hoja, 7, 2), 0)
        continua = scraper_IED.extraer_serie_desde_indice({"LP - Hog": hoja}, ("LP - Hog", 7, 2))
        self.assertEqual(len(continua), 8)

    def test_tablas_lado_a_lado_usan_su_propio_indice_de_tiempo(self) -> None:
        # Cuadros 59.1 (1988-1991) y 59.2 (2000-2002) de canastas, en la misma fila de ID.
        filas = [["59.1_IT_0_D_13", "59.1_CBA_0_D_26", None, "59.2_IT_0_D_13", "59.2_CBA_0_D_26"]]
        filas += [[pd.Timestamp(1988 + n, 4, 1), 100.0 + n, None, pd.Timestamp(2000 + n, 10, 1), 60.0 + n] for n in range(3)]
        hoja = pd.DataFrame(filas)
        self.assertEqual(scraper_IED._columna_fecha(hoja, 0, 4), 3)
        derecha = scraper_IED.extraer_serie_desde_indice({"CBA y CBT": hoja}, ("CBA y CBT", 0, 4))
        self.assertEqual(derecha["fecha"].dt.year.tolist(), [2000, 2001, 2002])
        self.assertEqual(derecha["valor"].tolist(), [60.0, 61.0, 62.0])
        izquierda = scraper_IED.extraer_serie_desde_indice({"CBA y CBT": hoja}, ("CBA y CBT", 0, 1))
        self.assertEqual(izquierda["fecha"].dt.year.tolist(), [1988, 1989, 1990])
