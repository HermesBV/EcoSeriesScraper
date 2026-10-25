import unittest

import pandas as pd

from scrapers import scraper_BCRA_irfcl as scraper


class ScraperBCRAIrfclTests(unittest.TestCase):
    def test_lee_partidas_de_custodia_de_la_nota_ii1(self) -> None:
        texto = (
            "(c) En la sección II.1 de la planilla correspondiente a la Autoridad Monetaria se incluyen; "
            "Cheques Cancelatorios en Otras Monedas y Certificados de Depósitos para la Inversión USD 17,99 millones, "
            "Depósitos en Otras Monedas USD 14.030,87 millones, Depósitos del Gobierno Nacional (en otras monedas) "
            "USD 91,94 millones y Otras Obligaciones en Moneda Extranjera USD 92,61 millones. "
            "(a) Los bienes que integran las reservas"
        )
        self.assertEqual(
            scraper.parse_notas(texto), {"cheques": 17.99, "otras_monedas": 14030.87, "gobierno": 91.94}
        )

    def test_tolera_palabras_cortadas_y_erratas_del_bcra(self) -> None:
        texto = (
            "(e) En la sección II.1 de la planilla correspondiente a la Aut oridad Monetaria se incluyen ; "
            "Cheques Cancelatorios en Otras Monedas y Certificados de Depósitos para la InversiónUSD 272,19 millones, "
            "Depósitos en cuentas corrien tes en otras monedas USD 2671,89 millones y Depósitos del G obierno "
            "Nacional en otras monedas USD 2.470.12 millones, Otras Obligaciones USD 800,04 y Otros Depósitos en "
            "Moneda Extranjera USD 106.17 millones. (f) En el item II.3"
        )
        self.assertEqual(
            scraper.parse_notas(texto), {"cheques": 272.19, "otras_monedas": 2671.89, "gobierno": 2470.12}
        )

    def test_sin_nota_ii1_no_inventa_partidas(self) -> None:
        self.assertEqual(scraper.parse_notas("Planilla sin notas del país"), {})

    def test_convierte_valores_del_fmi_a_millones(self) -> None:
        filas = ["DATAFLOW,COUNTRY,INDICATOR,SECTOR,FREQUENCY,TIME_PERIOD,OBS_VALUE"]
        for serie in scraper.SERIES_FMI:
            filas.append(f"IMF.STA:IRFCL(12.0.0),ARG,{serie.indicador_fmi},S1X,M,2026-M07,47599190000")
        datos = scraper.parse_fmi("\n".join(filas))
        self.assertEqual(datos.loc[0, "fecha"], pd.Timestamp("2026-07-01"))
        self.assertAlmostEqual(datos.loc[0, "I.A Activos de reserva oficiales"], 47599.19)

    def test_rechaza_dataset_fmi_sin_partidas_esenciales(self) -> None:
        contenido = "INDICATOR,TIME_PERIOD,OBS_VALUE\nIRFCLDT1_IRFCL56_USD,2026-M07,1\n"
        with self.assertRaisesRegex(ValueError, "esenciales"):
            scraper.parse_fmi(contenido)


if __name__ == "__main__":
    unittest.main()
