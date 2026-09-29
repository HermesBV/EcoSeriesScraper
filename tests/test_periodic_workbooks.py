from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from io import BytesIO

import pandas as pd

from scrapers.periodic_workbooks import Source, candidate_urls, download, extract_horizontal
from scrapers.scraper_MCH_ripte import find_pdf_url


class PeriodicWorkbooksTests(unittest.TestCase):
    def test_monthly_and_quarterly_candidates_cross_year(self):
        monthly = Source("x", "INDEC", "X", "X", "https://x/{MM}_{AA}.xls", "M", "X")
        quarterly = Source("x", "INDEC", "X", "X", "https://x/{T}_{AAAA}.xls", "T", "X")
        self.assertEqual(list(candidate_urls(monthly, date(2026, 1, 5), 3)),
                         ["https://x/01_26.xls", "https://x/12_25.xls", "https://x/11_25.xls"])
        self.assertEqual(list(candidate_urls(quarterly, date(2026, 1, 5), 3)),
                         ["https://x/I_2026.xls", "https://x/IV_2025.xls", "https://x/III_2025.xls"])

    def test_horizontal_dates_and_regions(self):
        source = Source("indec-ipc", "INDEC", "IPC", "INDEC / IPC", "https://x/{MM}_{AA}.xlsx", "M", "IPC")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "sample.xlsx"
            dates = [datetime(2025, month, 1) for month in range(1, 8)]
            raw = pd.DataFrame([
                ["Índice de precios al consumidor", *([None] * 7)],
                ["Total nacional", *dates],
                ["Nivel general", *range(100, 107)],
                ["Región GBA", *dates],
                ["Nivel general", *range(200, 207)],
                ["Total nacional", *dates],
                ["Nivel general", *range(100, 107)],
            ])
            raw.to_excel(path, index=False, header=False)
            sheets, index = extract_horizontal(path, source, "https://x/sample.xlsx")
            self.assertEqual(len(index), 2)
            self.assertEqual(index["Nombre serie"].tolist(),
                             ["Total nacional - Nivel general", "Región GBA - Nivel general"])
            frame = next(iter(sheets.values()))
            self.assertEqual(frame.iloc[-1]["Total nacional - Nivel general"], 106)
            self.assertEqual(frame.iloc[-1]["Región GBA - Nivel general"], 206)

    def test_international_accounts_keep_repeated_labels_separate(self):
        source = Source("indec-cin", "INDEC", "CuentasInternacionales", "INDEC / Economía",
                        "https://x/{T}_{AAAA}.xlsx", "T", "CIN")
        dates = [datetime(2025, month, 1) for month in range(1, 7)]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "sample.xlsx"
            with pd.ExcelWriter(path) as writer:
                pd.DataFrame([
                    ["Cuadro 20", *([None] * 6)],
                    [None, *dates],
                    ["B90. Posición neta", *([None] * 6)],
                    ["Saldo inicial", *([10] * 6)],
                    ["A. Activos", *([None] * 6)],
                    ["Saldo inicial", *([20] * 6)],
                ]).to_excel(writer, sheet_name="Cuadro 20", index=False, header=False)
                pd.DataFrame([
                    ["Cuadro 14", None, None, *([None] * 6)],
                    [None, None, None, *dates],
                    ["Q.N.AR.ACTIVOS", "3.2.2.1", "Banco central", *([30] * 6)],
                    ["Q.N.AR.PASIVOS", "3.2.2.1", "Banco central", *([40] * 6)],
                ]).to_excel(writer, sheet_name="Cuadro 14", index=False, header=False)

            sheets, index = extract_horizontal(path, source, "https://x/sample.xlsx")
            self.assertEqual(len(index), 4)
            self.assertFalse(index.duplicated(["ID"]).any())
            self.assertFalse(index.duplicated(["Pestaña BD", "Columna BD"]).any())
            by_name = dict(zip(index["Columna BD"], index["Pestaña BD"]))
            self.assertEqual(sheets[by_name["B90. Posición neta - Saldo inicial"]].iloc[-1]["B90. Posición neta - Saldo inicial"], 10)
            self.assertEqual(sheets[by_name["A. Activos - Saldo inicial"]].iloc[-1]["A. Activos - Saldo inicial"], 20)

    def test_ripte_link_with_blank_fragment(self):
        html = '<a href="blank:#/sites/default/files/ripte_julio_2026-mdch.pdf">Descargar</a>'
        self.assertEqual(find_pdf_url(html),
                         "https://www.argentina.gob.ar/sites/default/files/ripte_julio_2026-mdch.pdf")

    def test_html_response_is_skipped_even_with_http_200(self):
        source = Source("x", "INDEC", "X", "X", "https://x/{MM}_{AA}.xlsx", "M", "X")
        output = BytesIO()
        pd.DataFrame({"fecha": [datetime(2025, 1, 1)], "valor": [1]}).to_excel(output, index=False)

        class Response:
            def __init__(self, content):
                self.content = content

            def raise_for_status(self):
                pass

        with TemporaryDirectory() as directory, \
                patch("scrapers.periodic_workbooks.ROOT", Path(directory)), \
                patch("scrapers.periodic_workbooks.requests.get", side_effect=[Response(b"<html>404</html>"), Response(output.getvalue())]) as get:
            path, url = download(source, date(2026, 1, 5))
            self.assertEqual(url, "https://x/12_25.xlsx")
            self.assertTrue(path.is_file())
            self.assertEqual(get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
