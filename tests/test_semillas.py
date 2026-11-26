"""Semillas históricas: el tramo sale de la semilla o de lo publicado, la carga única se registra y la corrida
diaria conserva las hojas que ningún scraper actualiza. Base e índice en una carpeta temporal."""

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd
from openpyxl import Workbook

from scrapers import scraper_IIEP_reservas_internacionales as reservas
from scrapers import scraper_IIEP_tipo_cambio_real as tcr
from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools import generar_codificacion, lote, semillas

MESES = pd.date_range("1996-10-01", periods=6, freq="MS")   # 1996-10 a 1997-03


def fila_indice(codigo: str, hoja: str, columna: str, frecuencia: str = "M") -> dict:
    return {
        "ID": f"{codigo}-{columna}", "Código fuente": codigo, "Nombre serie": columna, "Variable": columna,
        "Unidad": "u", "Valoración": "No aplica", "Descripción": columna, "Frecuencia": frecuencia,
        "Pestaña BD": hoja, "Columna BD": columna, "Archivo origen": "x", "Hoja origen": "x", "Origen": "o",
        "Fuente": "f", "Fecha inicio": pd.Timestamp("1990-01-01"), "Fecha fin": pd.Timestamp("1991-12-01"),
        "Estado": "VIGENTE", "Institución": "IIEP", "Área": "a", "Tema": "Precios",
    }


class BaseTemporal(unittest.TestCase):
    """Un BD.xlsx chico con las hojas del TCR y del ITCRM del BCRA, y el índice al lado."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.raiz = Path(self.tmp.name)
        self.bd = self.raiz / "BD.xlsx"
        libro = Workbook()
        hoja = libro.active
        hoja.title = tcr.HOJA_SALIDA
        hoja.append(["fecha", tcr.COLUMNA_SALIDA])
        # Publicado: IIEP reescalado (x0,5) hasta 1996-12 y BCRA desde 1997-01.
        for fecha, valor in zip(MESES, [40.0, 41.0, 42.0, 50.0, 51.0, 52.0]):
            hoja.append([fecha.to_pydatetime(), valor])
        bcra = libro.create_sheet(tcr.HOJA_BCRA)
        bcra.append(["fecha", tcr.COLUMNA_BCRA])
        for fecha, valor in zip(MESES[3:], [50.0, 51.0, 52.0]):
            bcra.append([fecha.to_pydatetime(), valor])
        libro.save(self.bd)
        self.parches = [
            mock.patch.object(generar_codificacion, "DB_FILE", self.bd),
            mock.patch.object(generar_codificacion, "INDEX_FILE", self.raiz / "IndiceSeries.xlsx"),
            mock.patch.object(generar_codificacion, "TEMP_FILE", self.raiz / ".tmp.xlsx"),
            mock.patch.object(semillas, "CARPETA", self.raiz / "semillas"),
            mock.patch.object(semillas, "REGISTRO", self.raiz / "semillas" / "registro.json"),
            mock.patch.object(semillas, "ARCHIVO_BD", self.bd),
            mock.patch.object(tcr, "ARCHIVO_BD", self.bd),
            mock.patch.object(tcr, "ARCHIVO_IIEP", self.raiz / "semillas" / "iiep" / "tcr.xlsx"),
        ]
        for parche in self.parches:
            parche.start()

    def tearDown(self) -> None:
        for parche in reversed(self.parches):
            parche.stop()
        self.tmp.cleanup()

    def hoja(self, nombre: str) -> pd.DataFrame:
        return pd.read_excel(self.bd, sheet_name=nombre)


class TramoHistoricoTests(BaseTemporal):
    def test_sin_semilla_toma_lo_publicado(self) -> None:
        datos, origen = semillas.tramo_historico(self.raiz / "no-esta.csv", lambda ruta: None, tcr.HOJA_SALIDA)
        self.assertEqual(origen, "BD.xlsx")
        self.assertEqual(len(datos), 6)

    def test_con_semilla_la_lee(self) -> None:
        archivo = self.raiz / "semilla.csv"
        archivo.write_text("x")
        datos, origen = semillas.tramo_historico(archivo, lambda ruta: pd.DataFrame({"fecha": [1]}), tcr.HOJA_SALIDA)
        self.assertEqual(origen, "semilla")
        self.assertEqual(len(datos), 1)

    def test_sin_semilla_ni_hoja_pide_cargarla(self) -> None:
        with self.assertRaisesRegex(FileNotFoundError, "cargarla una vez"):
            semillas.tramo_historico(self.raiz / "no-esta.csv", lambda ruta: None, "Hoja que no existe")


class TCRReproduceTests(BaseTemporal):
    def test_sin_semilla_reproduce_lo_publicado(self) -> None:
        antes = self.hoja(tcr.HOJA_SALIDA)
        tcr.procesar()
        pd.testing.assert_frame_equal(self.hoja(tcr.HOJA_SALIDA), antes)

    def test_con_la_semilla_original_tambien(self) -> None:
        tcr.ARCHIVO_IIEP.parent.mkdir(parents=True)
        libro = Workbook()
        libro.active.title = tcr.HOJA_IIEP
        libro.active.append(["Fecha", tcr.COLUMNA_IIEP])
        for fecha, valor in zip(MESES[:4], [80.0, 82.0, 84.0, 100.0]):   # sin reescalar; 1997-01 empalma x0,5
            libro.active.append([fecha.to_pydatetime(), valor])
        libro.save(tcr.ARCHIVO_IIEP)
        antes = self.hoja(tcr.HOJA_SALIDA)
        resumen = tcr.procesar()
        self.assertEqual(resumen["factor_empalme"], 0.5)
        pd.testing.assert_frame_equal(self.hoja(tcr.HOJA_SALIDA), antes)


class ReservasReproduceTests(BaseTemporal):
    def test_el_tramo_de_la_semilla_sale_igual_de_lo_publicado(self) -> None:
        fechas = pd.date_range("2026-06-01", periods=4, freq="MS")   # hasta 2026-09: una fecha pasa el corte
        valores = {clave: [float(i + n) for i in range(4)] for n, clave in enumerate(reservas.COLUMNAS)}
        archivo = self.raiz / "semilla.csv"
        pd.DataFrame({"fecha": fechas, **valores}).to_csv(archivo, index=False)
        guardar_datos_preservando_formato(
            self.bd, {reservas.HOJA: reservas.leer_semilla(archivo)}, {reservas.HOJA: "M"}, set())
        with mock.patch.object(reservas, "ARCHIVO_BD", self.bd):
            desde_semilla = reservas.leer_historico(archivo)
            desde_bd = reservas.leer_historico(self.raiz / "no-esta.csv")
        self.assertEqual(list(desde_semilla["fecha"]), list(fechas[:3]))
        pd.testing.assert_frame_equal(desde_bd, desde_semilla, check_dtype=False)


class SoloHistoricaTests(BaseTemporal):
    """Caso b: se carga una vez y la corrida diaria la conserva."""

    def setUp(self) -> None:
        super().setUp()
        modulo = types.ModuleType("scrapers.semilla_prueba")
        modulo.CODIGO, modulo.HOJA, modulo.FRECUENCIA, modulo.ARCHIVO = "prueba-hist", "Prueba historica M", "M", "vieja.csv"
        modulo.leer = lambda ruta: pd.read_csv(ruta, parse_dates=["fecha"])
        modulo.filas_indice = lambda datos: pd.DataFrame([fila_indice("prueba-hist", "Prueba historica M", "valor")])
        sys.modules["scrapers.semilla_prueba"] = modulo
        self.addCleanup(sys.modules.pop, "scrapers.semilla_prueba")
        archivo = semillas.ruta("prueba-hist", "vieja.csv")
        archivo.parent.mkdir(parents=True)
        pd.DataFrame({"fecha": pd.date_range("1990-01-01", periods=24, freq="MS"),
                      "valor": [float(i) for i in range(24)]}).to_csv(archivo, index=False)

    def test_carga_registra_y_sobrevive_a_la_corrida(self) -> None:
        entrada = semillas.cargar("semilla_prueba", paso="Hermes")
        self.assertEqual((entrada["desde"], entrada["hasta"], entrada["fechas"]), ("1990-01-01", "1991-12-01", 24))
        self.assertEqual(len(entrada["sha256"]), 64)
        self.assertEqual(json.loads(semillas.REGISTRO.read_text(encoding="utf-8")), [entrada])
        cargada = self.hoja("Prueba historica M")

        # Un lote de la corrida diaria: otro scraper reescribe su hoja y su inventario.
        with lote.abrir(self.raiz / ".lote"):
            guardar_datos_preservando_formato(self.bd, {tcr.HOJA_BCRA: pd.DataFrame({
                "fecha": [pd.Timestamp("1997-04-01")], tcr.COLUMNA_BCRA: [53.0]})}, {tcr.HOJA_BCRA: "M"}, set())
            generar_codificacion.generar(pd.DataFrame([fila_indice("bcra-itc", tcr.HOJA_BCRA, tcr.COLUMNA_BCRA)]))

        pd.testing.assert_frame_equal(self.hoja("Prueba historica M"), cargada)
        indice = pd.read_excel(generar_codificacion.INDEX_FILE, sheet_name="Codificacion")
        self.assertEqual(set(indice["Código fuente"]), {"prueba-hist", "bcra-itc"})
        self.assertEqual(len(self.hoja(tcr.HOJA_BCRA)), 4)

    def test_recargar_reemplaza_su_registro(self) -> None:
        semillas.cargar("semilla_prueba", paso="Hermes")
        semillas.cargar("semilla_prueba", paso="Hermes")
        self.assertEqual(len(semillas.leer_registro()), 1)


class RegistroVersionadoTests(unittest.TestCase):
    def test_el_registro_del_repo_es_valido(self) -> None:
        entradas = semillas.leer_registro()
        claves = {"codigo", "archivo", "sha256", "cargada", "paso", "caso", "hoja", "columnas", "desde", "hasta", "fechas"}
        self.assertGreaterEqual(len(entradas), 2)
        for entrada in entradas:
            self.assertLessEqual(claves, set(entrada))
            self.assertIn(entrada["caso"], {"a", "b"})


if __name__ == "__main__":
    unittest.main()
