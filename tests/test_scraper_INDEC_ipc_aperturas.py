"""Libro de aperturas del IPC: bloques por región y series que ya publica otro scraper."""

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scrapers import scraper_INDEC_ipc_aperturas as aperturas
from tools.taxonomia import Serie
from tools.taxonomia.precios_ipc_aperturas import clasificar

MESES = [pd.Timestamp(2017, 1, 1), pd.Timestamp(2017, 2, 1), pd.Timestamp(2017, 3, 1)]


def hoja(valor_base: float) -> pd.DataFrame:
    filas = [["Índice de precios al consumidor con cobertura nacional. Resultados por región"],
             ["Período de referencia: Diciembre 2016=100"], [None],
             ["Resultados regionales según principales aperturas de la canasta"], [None]]
    for numero, region in enumerate(("Región GBA", "Región Cuyo")):
        filas += [[region, *MESES], [None],
                  ["Nivel general", *(valor_base + numero + n for n in range(3))],
                  ["Alimentos y bebidas no alcohólicas", *(valor_base + numero + 10 + n for n in range(3))],
                  ["Pan y cereales", *(valor_base + numero + 20 + n for n in range(3))],
                  ["Alquiler de la vivienda", valor_base + numero + 30, None, valor_base + numero + 32],
                  [None], [None]]
    filas += [["Nota: a partir del mes de junio de 2017…"], ["Fuente: INDEC"]]
    return pd.DataFrame(filas)


class AperturasIPCTests(unittest.TestCase):
    def setUp(self) -> None:
        self.carpeta = tempfile.TemporaryDirectory()
        self.libro = Path(self.carpeta.name) / "sh_ipc_aperturas.xlsx"
        with pd.ExcelWriter(self.libro) as escritor:
            for numero, nombre in enumerate(aperturas.HOJAS):
                hoja(100.0 * (numero + 1)).to_excel(escritor, sheet_name=nombre, header=False, index=False)
        self.hojas, self.indice = aperturas.extraer(self.libro, "https://ejemplo/sh_ipc_aperturas.xls")

    def tearDown(self) -> None:
        self.carpeta.cleanup()

    def test_una_serie_por_region_y_apertura_sin_nivel_general_ni_divisiones(self) -> None:
        self.assertEqual(len(self.indice), 3 * 2 * 2)  # tres hojas, dos regiones, dos aperturas finas
        nombres = set(self.indice["Nombre serie"])
        self.assertEqual(nombres, {"Región GBA - Pan y cereales", "Región GBA - Alquiler de la vivienda",
                                   "Región Cuyo - Pan y cereales", "Región Cuyo - Alquiler de la vivienda"})
        self.assertFalse(self.indice["ID"].duplicated().any())

    def test_valores_fechas_y_unidades(self) -> None:
        tabla = self.hojas["INDEC IPC Ap Índices"]
        self.assertEqual(tabla["fecha"].tolist(), MESES)
        self.assertEqual(tabla["Región Cuyo - Pan y cereales"].tolist(), [121.0, 122.0, 123.0])
        self.assertTrue(pd.isna(tabla["Región GBA - Alquiler de la vivienda"].iloc[1]))
        unidades = dict(zip(self.indice["Hoja origen"], self.indice["Unidad"]))
        self.assertEqual(unidades["Índices aperturas"], "Índice dic-2016=100")
        self.assertEqual(unidades["Var. interanual aperturas"], "%")
        fila = self.indice[self.indice["ID"].eq("indices-aperturas|region-gba-alquiler-de-la-vivienda")].iloc[0]
        self.assertEqual((fila["Fecha inicio"], fila["Fecha fin"]), (MESES[0], MESES[2]))

    def test_las_pestanas_entran_en_excel(self) -> None:
        self.assertTrue(all(len(nombre) <= 31 for nombre in self.hojas))

    def test_taxonomia_mismo_arbol_que_el_resto_del_ipc(self) -> None:
        def serie(hoja_origen: str, columna: str) -> Serie:
            return Serie(fuente="indec-ipc-aperturas", id="x", nombre=columna, variable="", descripcion="", unidad="",
                         frecuencia="M", archivo="latest.xls", hoja=hoja_origen, columna=columna, dataset="",
                         estado="VIGENTE")
        indice = clasificar(serie("Índices aperturas", "Región GBA - Alquiler de la vivienda"))
        self.assertEqual((indice.tema, indice.indicador, indice.cobertura, indice.apertura),
                         ("Precios", "IPC", "GBA", "Principales aperturas"))
        self.assertEqual(indice.nombre, "IPC · Alquiler de la vivienda · GBA")
        mensual = clasificar(serie("Variación mensual aperturas", "Región Cuyo - Pan y cereales"))
        self.assertEqual(mensual.nombre, "IPC · Pan y cereales · Cuyo (var. % mensual)")
        self.assertIsNone(clasificar(serie("Ponderaciones", "Región GBA - Pan y cereales")))


if __name__ == "__main__":
    unittest.main()
