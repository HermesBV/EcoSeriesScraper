"""Rótulos que el INDEC escribe de dos maneras: las dos deben dar la misma columna."""

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scrapers.indec_workbooks import ROTULOS_EQUIVALENTES, extract_tables

MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre",
         "Noviembre", "Diciembre"]


def libro(carpeta: str) -> Path:
    """Cuadro 2 del índice de salarios con el sector en una fila propia, como en el libro de nov-2026."""
    filas = [["Cuadro 2. Índice de salarios. Variaciones porcentuales acumuladas, por sector"], [None],
             ["Período", None, "Sector registrado", None, None, None, None, None, "Sector privado no registrado (2)",
              None, "Total índice de salarios", None],
             [None, None, "Sector privado registrado", None, "Sector público", None, "Total registrado (1)"],
             [None, None, "Variación porcentual respecto a"],
             [None, None, *["igual mes del año anterior", "diciembre del año anterior"] * 5],
             [None, None, "%"], [None]]
    for numero, mes in enumerate(MESES):
        filas.append([2016 if numero == 0 else None, mes, *(numero + columna / 10 for columna in range(10))])
    ruta = Path(carpeta) / "latest.xlsx"
    pd.DataFrame(filas).to_excel(ruta, sheet_name="Cuadro 2", header=False, index=False)
    return ruta


class RotulosEquivalentesTests(unittest.TestCase):
    def test_el_encabezado_largo_conserva_las_columnas_publicadas(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            hojas, indice = extract_tables(libro(carpeta), "INDEC / Salarios", "indec-salarios", "INDEC Salarios")
        tabla = next(iter(hojas.values()))
        variacion = "Variación porcentual respecto a"
        esperadas = {f"{sector} - {variacion} - {periodo} - %{sufijo}"
                     for sector, sufijos in (("Sector privado registrado", ("", "")), ("Sector público", ("", "")),
                                             ("Total registrado (1)", ("", "")), ("Total registrado (1)", (" (9)", " (10)")),
                                             ("Total registrado (1)", (" (11)", " (12)")))
                     for periodo, sufijo in zip(("igual mes del año anterior", "diciembre del año anterior"), sufijos)}
        self.assertEqual(set(tabla.columns) - {"fecha"}, esperadas)
        self.assertEqual(set(indice["Columna BD"]), esperadas)
        # La columna 11 del libro (índice total, interanual) es la novena serie: valores n + 0,8.
        self.assertEqual(tabla[f"Total registrado (1) - {variacion} - igual mes del año anterior - % (11)"].tolist()[:2], [0.8, 1.8])

    def test_ninguna_equivalencia_lleva_dos_variantes_distintas_a_la_misma_columna(self) -> None:
        for tabla in ROTULOS_EQUIVALENTES.values():
            self.assertEqual(len(set(tabla.values())), len(tabla))
            self.assertFalse(set(tabla) & set(tabla.values()))


if __name__ == "__main__":
    unittest.main()
