"""Las series que una fuente deja de publicar quedan como DISCONTINUADAS mientras tengan datos."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

from openpyxl import Workbook
import pandas as pd

from tools import generar_codificacion


def fila(codigo: str, identificador: str, hoja: str) -> dict:
    return {"ID": identificador, "Código fuente": codigo, "Nombre serie": identificador, "Variable": identificador,
            "Unidad": "u", "Valoración": "No aplica", "Descripción": identificador, "Frecuencia": "M",
            "Pestaña BD": hoja, "Columna BD": identificador, "Origen": "o", "Fuente": "f",
            "Fecha inicio": pd.Timestamp("2026-01-01"), "Fecha fin": pd.Timestamp("2026-02-01"),
            "Estado": "VIGENTE", "Institución": "INDEC", "Área": "a", "Tema": "Precios"}


class DiscontinuadasTests(unittest.TestCase):
    def test_conserva_series_perdidas_con_pestana_vigente(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            raiz = Path(carpeta)
            libro = Workbook()
            libro.active.title = "Hoja viva"
            libro.create_sheet("Hoja borrada")
            libro.save(raiz / "BD.xlsx")
            actual = pd.DataFrame([fila("x", "sigue", "Hoja viva"), fila("x", "se-fue", "Hoja viva"),
                                   fila("x", "sin-hoja", "Hoja ya inexistente"), fila("otra", "ajena", "Hoja viva")])
            nuevo = pd.DataFrame([fila("x", "sigue", "Hoja viva")])
            with mock.patch.object(generar_codificacion, "DB_FILE", raiz / "BD.xlsx"):
                perdidas = generar_codificacion._discontinuadas(actual, nuevo, {"x"})
        self.assertEqual(perdidas["ID"].tolist(), ["se-fue"])
        self.assertEqual(perdidas["Estado"].tolist(), ["DISCONTINUADA"])


if __name__ == "__main__":
    unittest.main()
