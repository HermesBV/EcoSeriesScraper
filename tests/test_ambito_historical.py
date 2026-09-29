import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from scrapers.ambito_historical import download, parse_history
from scrapers.scraper_Ambito_dolar_blue import SOURCE as BLUE
from scrapers.scraper_Ambito_riesgo_pais import SOURCE as RISK


class AmbitoHistoricalTests(unittest.TestCase):
    def test_json_averages_duplicate_dates_per_column(self):
        body = '[ ["Fecha", "Compra", "Venta"], ["28/09/2026", "$ 1.200,00", "$ 1.230,00"], ["28/09/2026", "$ 1.220,00", "$ 1.250,00"], ["25/09/2026", "$ 1.190,00", "$ 1.210,00"] ]'
        frame = parse_history(body, BLUE)
        self.assertEqual(len(frame), 2)
        self.assertEqual(float(frame.iloc[-1]["Compra"]), 1210.0)
        self.assertEqual(float(frame.iloc[-1]["Venta"]), 1240.0)

    def test_html_tbody_and_single_value(self):
        body = '<table><tbody class="general-historical__tbody tbody"><tr><td>28/09/2026</td><td>500</td></tr><tr><td>28/09/2026</td><td>510</td></tr></tbody></table>'
        frame = parse_history(body, RISK)
        self.assertEqual(len(frame), 1)
        self.assertEqual(float(frame.iloc[0]["Puntos"]), 505.0)

    def test_update_uses_seven_day_overlap_and_replaces_recent_values(self):
        with TemporaryDirectory() as directory, patch("scrapers.ambito_historical.ROOT", Path(directory)):
            BLUE.directory.mkdir(parents=True)
            pd.DataFrame({"fecha": ["2026-09-24", "2026-09-25"],
                          "Compra": [100, 101], "Venta": [110, 111]}).to_csv(BLUE.file, index=False)
            fresh = pd.DataFrame({"fecha": pd.to_datetime(["2026-09-25", "2026-09-28"]),
                                  "Compra": [102, 103], "Venta": [112, 113]})
            with patch("scrapers.ambito_historical._fetch_history", return_value=(fresh, "https://example.org")) as fetch:
                download(BLUE, today=date(2026, 9, 29))
            self.assertEqual(fetch.call_args.args[1:], (date(2026, 9, 18), date(2026, 9, 29)))
            result = pd.read_csv(BLUE.file)
            self.assertEqual(result["Compra"].tolist(), [100, 102, 103])

    def test_json_rejects_unrelated_price_columns(self):
        with self.assertRaisesRegex(ValueError, "columnas esperadas"):
            parse_history('[["Fecha","Compra","Venta"],["28/09/2026","1","2"]]', RISK)


if __name__ == "__main__":
    unittest.main()
