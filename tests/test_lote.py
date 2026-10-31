"""El lote acumula los guardados de varios scrapers y escribe BD e índice una sola vez."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

from openpyxl import Workbook
import pandas as pd

from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools import generar_codificacion, lote


def inventario(codigo: str, hoja: str, columna: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "ID": f"{codigo}-1", "Código fuente": codigo, "Nombre serie": columna, "Variable": columna,
        "Unidad": "u", "Valoración": "No aplica", "Descripción": columna, "Frecuencia": "D",
        "Pestaña BD": hoja, "Columna BD": columna, "Archivo origen": "x", "Hoja origen": "x",
        "Origen": "o", "Fuente": "f", "Fecha inicio": pd.Timestamp("2026-01-01"),
        "Fecha fin": pd.Timestamp("2026-01-02"), "Estado": "VIGENTE", "Institución": "BCRA",
        "Área": "a", "Tema": "Precios",
    }])


class LoteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.carpeta = tempfile.TemporaryDirectory()
        self.raiz = raiz = Path(self.carpeta.name)
        self.bd = raiz / "BD.xlsx"
        libro = Workbook()
        hoja = libro.active
        hoja.title = "Hoja A"
        hoja.append(["fecha", "serie a"])
        hoja.append([pd.Timestamp("2026-01-01").to_pydatetime(), 1.0])
        libro.create_sheet("Vieja").append(["fecha", "x"])
        libro.save(self.bd)
        self.parches = [
            mock.patch.object(generar_codificacion, "DB_FILE", self.bd),
            mock.patch.object(generar_codificacion, "INDEX_FILE", raiz / "IndiceSeries.xlsx"),
            mock.patch.object(generar_codificacion, "TEMP_FILE", raiz / ".tmp.xlsx"),
        ]
        for parche in self.parches:
            parche.start()

    def tearDown(self) -> None:
        for parche in self.parches:
            parche.stop()
        self.carpeta.cleanup()

    def test_fusiona_en_memoria_y_escribe_una_vez(self) -> None:
        mtime = self.bd.stat().st_mtime_ns
        with lote.abrir(self.raiz / ".lote"):
            guardar_datos_preservando_formato(self.bd, {"Hoja A": pd.DataFrame({
                "fecha": [pd.Timestamp("2026-01-02")], "serie a": [2.0]})}, {"Hoja A": "D"}, {"Vieja"})
            generar_codificacion.generar(inventario("fuente-1", "Hoja A", "serie a"))
            guardar_datos_preservando_formato(self.bd, {"Hoja B": pd.DataFrame({
                "fecha": [pd.Timestamp("2026-01-01")], "serie b": [3.0]})}, {"Hoja B": "D"}, set())
            generar_codificacion.generar(inventario("fuente-2", "Hoja B", "serie b"))
            # Nada se escribió todavía y lo pendiente se ve fusionado.
            self.assertEqual(self.bd.stat().st_mtime_ns, mtime)
            pendiente = lote.leer_hoja(self.bd, "Hoja A")
            self.assertEqual(pendiente["serie a"].tolist(), [1.0, 2.0])
            self.assertEqual(set(generar_codificacion.cargar_indice()["Código fuente"]), {"fuente-1", "fuente-2"})
        with pd.ExcelFile(self.bd) as libro:
            self.assertEqual(sorted(libro.sheet_names), ["Hoja A", "Hoja B"])
            self.assertEqual(pd.read_excel(libro, sheet_name="Hoja A")["serie a"].tolist(), [1.0, 2.0])
        indice = pd.read_excel(generar_codificacion.INDEX_FILE, sheet_name="Codificacion")
        self.assertEqual(set(indice["Código fuente"]), {"fuente-1", "fuente-2"})
        self.assertFalse(lote.activo())

    def test_vuelca_aunque_el_bloque_falle(self) -> None:
        with self.assertRaises(RuntimeError):
            with lote.abrir(self.raiz / ".lote"):
                guardar_datos_preservando_formato(self.bd, {"Hoja B": pd.DataFrame({
                    "fecha": [pd.Timestamp("2026-01-01")], "serie b": [3.0]})}, {"Hoja B": "D"}, set())
                raise RuntimeError("un scraper falló")
        with pd.ExcelFile(self.bd) as libro:
            self.assertIn("Hoja B", libro.sheet_names)

    def test_sin_lote_escribe_en_el_acto(self) -> None:
        guardar_datos_preservando_formato(self.bd, {"Hoja B": pd.DataFrame({
            "fecha": [pd.Timestamp("2026-01-01")], "serie b": [3.0]})}, {"Hoja B": "D"}, {"Vieja"})
        with pd.ExcelFile(self.bd) as libro:
            self.assertEqual(sorted(libro.sheet_names), ["Hoja A", "Hoja B"])


if __name__ == "__main__":
    unittest.main()
