"""Orden de la corrida y registro de lo que imprime cada scraper."""

import io
import unittest

import main


class MainTests(unittest.TestCase):
    def test_el_ied_corre_ultimo_y_en_su_propio_lote(self) -> None:
        modulos = main.descubrir_scrapers()
        self.assertEqual(modulos[-1], "scraper_IED")
        self.assertEqual(sorted(modulos[:-1]), modulos[:-1])
        grupos = main.agrupar(modulos, 10)
        self.assertEqual(grupos[-1], ["scraper_IED"])
        self.assertTrue(all(len(grupo) <= 10 for grupo in grupos))
        self.assertEqual([nombre for grupo in grupos for nombre in grupo], modulos)

    def test_la_copia_deja_pasar_todo_y_guarda_los_avisos(self) -> None:
        salida = io.StringIO()
        copia = main._Copia(salida)
        copia.write("descargando...\nAVISO indec-ipc: no se pudo descargar un libro nuevo (403); ")
        copia.write("se usa el guardado\nlisto\n")
        self.assertEqual(salida.getvalue().count("\n"), 3)
        self.assertEqual(copia.avisos, ["AVISO indec-ipc: no se pudo descargar un libro nuevo (403); se usa el guardado"])

    def test_version_y_paquetes(self) -> None:
        self.assertRegex(main.version_codigo(), r"^[0-9a-f]{7}$|^desconocida$")
        self.assertEqual(main.paquetes_faltantes(), [])


if __name__ == "__main__":
    unittest.main()
