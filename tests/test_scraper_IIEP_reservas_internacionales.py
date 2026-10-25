import unittest

import pandas as pd

from scrapers import scraper_BCRA_cotizaciones as cotizaciones
from scrapers import scraper_IIEP_reservas_internacionales as scraper


def planilla(**cambios: float) -> pd.DataFrame:
    """Planilla IRFCL de octubre de 2025 (millones de USD, signos publicados)."""
    fila = {
        "fecha": pd.Timestamp("2025-10-01"),
        scraper.COL_BRUTAS: 39381.59, scraper.COL_ORO: 7934.40, scraper.COL_DEG: 912.26,
        scraper.COL_II1: -17417.45, scraper.COL_II2: -20795.25, scraper.COL_II2_1M: -2511.49,
        scraper.COL_II3: -3294.59,
        "Nota II.1 Cheques cancelatorios y CDI": 17.99,
        "Nota II.1 Depósitos en otras monedas": 14030.87,
        "Nota II.1 Depósitos del Gobierno Nacional": 91.94,
    }
    fila.update(cambios)
    return pd.DataFrame([fila])


class ScraperIIEPReservasTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cny = pd.Series({pd.Timestamp("2025-10-01"): 0.140443})

    def test_replica_la_base_iiep_de_octubre_2025_con_swap_de_eeuu(self) -> None:
        resultado, avisos = scraper.calcular_meses(planilla(), self.cny, pd.Timestamp("2025-09-01"))
        fila = resultado.iloc[0]
        # Valores de la base IIEP para 2025M10.
        self.assertAlmostEqual(fila["Custodia"], 14140.80, places=2)
        self.assertAlmostEqual(fila["Reservas prestadas"], 27366.4787, delta=0.02)
        self.assertAlmostEqual(fila["Reservas netas según planilla"], -2125.6887, delta=0.02)
        self.assertAlmostEqual(fila["Reservas netas"], fila["Reservas netas según planilla"], places=6)
        self.assertAlmostEqual(fila["Reservas líquidas"], 17166.7, delta=0.1)
        self.assertTrue(pd.isna(fila["RIN-EFF"]))
        self.assertTrue(any("banda hasta 1 mes" in aviso for aviso in avisos))

    def test_sin_otro_swap_descuenta_toda_la_seccion_ii2(self) -> None:
        no_activada, aviso = scraper.porcion_no_activada(-19253.56, -19253.56, 0.148107)
        self.assertIsNone(aviso)
        self.assertAlmostEqual(no_activada, 19253.56 - 35000 * 0.148107, places=6)

    def test_no_calcula_liquidas_si_no_puede_aislar_el_swap_chino(self) -> None:
        no_activada, aviso = scraper.porcion_no_activada(-30000.0, 0.0, 0.14)
        self.assertIsNone(no_activada)
        self.assertIn("no permite aislar", aviso)

    def test_mes_sin_notas_queda_sin_calcular(self) -> None:
        datos = planilla(**{"Nota II.1 Depósitos en otras monedas": float("nan")})
        resultado, avisos = scraper.calcular_meses(datos, self.cny, pd.Timestamp("2025-09-01"))
        self.assertTrue(resultado.empty)
        self.assertIn("mes sin calcular", avisos[0])

    def test_toma_el_ultimo_tipo_de_pase_del_mes(self) -> None:
        pase, cotizacion = cotizaciones.columnas("CNY")
        datos = pd.DataFrame({
            "fecha": pd.to_datetime(["2025-10-30", "2025-10-31", "2025-11-03"]),
            pase: [0.1401, 0.140443, 0.1405], cotizacion: [1, 2, 3],
        })
        mensual = scraper.cny_fin_de_mes(datos)
        self.assertEqual(mensual[pd.Timestamp("2025-10-01")], 0.140443)


class ScraperBCRACotizacionesTests(unittest.TestCase):
    def test_lee_respuesta_de_la_api(self) -> None:
        contenido = {"results": [{"fecha": "2026-10-23", "detalle": [
            {"codigoMoneda": "CNY", "tipoPase": 0.149921, "tipoCotizacion": 229.303469},
            {"codigoMoneda": "EUR", "tipoPase": 1.1, "tipoCotizacion": 1500},
        ]}]}
        datos = cotizaciones.parse_respuesta(contenido, "CNY")
        self.assertEqual(len(datos), 1)
        self.assertEqual(datos.loc[0, "tipoPase"], 0.149921)


if __name__ == "__main__":
    unittest.main()
