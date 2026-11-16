"""El editor del libro con LibreOffice: mismas operaciones que con Excel."""

from pathlib import Path
import tempfile
import unittest

from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill
import pandas as pd

from coyuntura.actualizador import libreoffice


def libro_de_prueba(ruta: Path) -> None:
    libro = Workbook()
    datos = libro.active
    datos.title = "Datos"
    datos.append(["fecha", "valor", "variación"])
    for fila, valor in enumerate([100.0, 110.0, 121.0], 2):
        datos.cell(fila, 1, pd.Timestamp(2026, fila - 1, 1).to_pydatetime())
        datos.cell(fila, 2, valor)
        if fila > 2:
            datos.cell(fila, 3, f"=B{fila}/B{fila - 1}-1")
    for columna in range(1, 4):   # la «barra azul» que delimita los datos
        datos.cell(5, columna).fill = PatternFill("solid", fgColor="FF156082")
    datos.cell(7, 1, "Suma")
    datos.cell(7, 2, "=SUM(B2:B4)")
    resumen = libro.create_sheet("Resumen")
    resumen["A1"] = "=Datos!B4"
    resumen["A2"] = "=VLOOKUP(121,Datos!$B$2:$C$4,2,FALSE)"
    libro.save(ruta)


@unittest.skipUnless(libreoffice.disponible(), "LibreOffice (soffice y python3-uno) no está instalado")
class LibreOfficeTests(unittest.TestCase):
    def test_inserta_copia_formulas_recalcula_y_guarda(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta) / "libro.xlsx"
            libro_de_prueba(ruta)
            with libreoffice.LibreOffice() as editor:
                editor.abrir(ruta)
                ws = editor.hoja("Datos")
                self.assertEqual(editor.ultima_columna(ws), 3)
                self.assertEqual(editor.formula(ws, 4, 3), "=B4/B3-1")
                self.assertTrue(editor.tiene_formula(ws, 4, 3))
                self.assertFalse(editor.tiene_formula(ws, 4, 2))
                self.assertEqual(editor.valor(ws, 4, 2), 121.0)
                editor.insertar_filas(ws, 5, 2)                      # dos meses nuevos sobre la barra
                editor.escribir_fecha(ws, 5, "A", pd.Timestamp(2026, 4, 1))
                editor.escribir(ws, 5, "B", 133.1)
                editor.escribir(ws, 6, "B", 146.41)
                editor.copiar_formula(ws, "C", 4, 5, 6)
                self.assertEqual(editor.formula(ws, 6, 3), "=B6/B5-1")
                self.assertEqual(editor.formula(ws, 9, 2), "=SUM(B2:B4)")   # insertar debajo no amplía el rango
                self.assertEqual(editor.ampliar_rangos("Datos", 2, 4, 2), 2)   # la suma y el VLOOKUP de la otra hoja
                editor.escribir_formula(editor.hoja("Resumen"), "A1", "=Datos!B6")
                editor.limpiar(ws, "A9")
                editor.recalcular()
                editor.guardar()
            formulas, valores = load_workbook(ruta), load_workbook(ruta, data_only=True)
            self.assertEqual(formulas["Datos"]["C6"].value, "=B6/B5-1")
            self.assertAlmostEqual(valores["Datos"]["C6"].value, 0.1)
            self.assertEqual(formulas["Datos"]["B9"].value, "=SUM(B2:B6)")
            self.assertAlmostEqual(valores["Datos"]["B9"].value, 610.51)
            self.assertIsNone(valores["Datos"]["A9"].value)
            self.assertEqual(valores["Resumen"]["A1"].value, 146.41)
            self.assertIn("$B$2:$C$6", formulas["Resumen"]["A2"].value)
            self.assertEqual(valores["Datos"]["A5"].value, pd.Timestamp(2026, 4, 1).to_pydatetime())
            # La barra bajó dos filas y las filas nuevas tomaron el formato de la de arriba.
            self.assertEqual(formulas["Datos"]["A7"].fill.fill_type, "solid")
            self.assertNotEqual(formulas["Datos"]["A5"].fill.fill_type, "solid")

    def test_borrar_una_fila_corre_las_referencias(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta) / "libro.xlsx"
            libro_de_prueba(ruta)
            with libreoffice.LibreOffice() as editor:
                editor.abrir(ruta)
                ws = editor.hoja("Datos")
                editor.borrar_fila(ws, 2)
                self.assertEqual(editor.formula(ws, 3, 3), "=B3/B2-1")
                self.assertIn("#REF!", editor.formula(ws, 2, 3))


if __name__ == "__main__":
    unittest.main()
