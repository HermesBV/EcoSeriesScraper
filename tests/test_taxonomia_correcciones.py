"""Correcciones explícitas: exclusiones de la publicación y unidades mal informadas."""

import unittest
from unittest import mock

import pandas as pd

from tools import taxonomia


def inventario(**cambios) -> pd.DataFrame:
    fila = {"Código fuente": "mch-ripte", "ID": "ripte-mensual", "Nombre serie": "RIPTE", "Variable": "", "Unidad": "pesos",
            "Descripción": "", "Frecuencia": "M", "Archivo origen": "latest.pdf", "Hoja origen": "RIPTE",
            "Columna BD": "RIPTE", "Título dataset": "", "Estado": "VIGENTE", "Tema": "Trabajo e ingresos"}
    return pd.DataFrame([{**fila, **cambios}])


class CorreccionesTest(unittest.TestCase):
    def test_las_claves_declaradas_existen_y_tienen_motivo(self):
        excluidas, unidades = taxonomia.correcciones()
        for clave, motivo in excluidas.items():
            self.assertEqual(len(clave), 2)
            self.assertTrue(str(motivo).strip(), clave)
        for clave, unidad in unidades.items():
            self.assertEqual(len(clave), 2)
            self.assertTrue(str(unidad).strip(), clave)

    def test_excluir_marca_el_estado_y_conserva_la_fila(self):
        clave = ("mch-ripte", "ripte-mensual")
        with mock.patch.object(taxonomia, "_CORRECCIONES", ({clave: "prueba"}, {})):
            resultado = taxonomia.aplicar(inventario())
        self.assertEqual(len(resultado), 1)
        self.assertEqual(resultado.iloc[0]["Estado"], "EXCLUIDA")

    def test_la_unidad_corregida_pisa_la_de_la_fuente(self):
        clave = ("mch-ripte", "ripte-mensual")
        with mock.patch.object(taxonomia, "_CORRECCIONES", ({}, {clave: "Pesos corrientes"})):
            resultado = taxonomia.aplicar(inventario())
        self.assertEqual(resultado.iloc[0]["Unidad"], "Pesos corrientes")

    def test_sin_correcciones_no_cambia_estado_ni_unidad(self):
        with mock.patch.object(taxonomia, "_CORRECCIONES", ({}, {})):
            resultado = taxonomia.aplicar(inventario())
        # La unidad de la fuente se conserva; sólo se le pone la mayúscula inicial.
        self.assertEqual((resultado.iloc[0]["Estado"], resultado.iloc[0]["Unidad"]), ("VIGENTE", "Pesos"))


class HojasExcluidasTest(unittest.TestCase):
    def fila(self, identificador: str) -> pd.DataFrame:
        return inventario(**{"Código fuente": "datos.gob.ar", "ID": identificador, "Archivo origen": "actividad.xlsx",
                             "Hoja origen": "1.17 Productos ind.", "Variable": "urea", "Frecuencia": "M",
                             "Nombre serie": "Indicadores de producción", "Unidad": None})

    def test_la_hoja_de_produccion_industrial_no_se_publica(self):
        self.assertEqual(taxonomia.aplicar(self.fila("41.3_U_0_A_4")).iloc[0]["Estado"], "EXCLUIDA")

    def test_se_conservan_las_series_confirmadas_contra_otra_hoja(self):
        fila = self.fila("41.3_PC_0_A_14").assign(Variable="petroleo_crudo")
        self.assertEqual(taxonomia.aplicar(fila).iloc[0]["Estado"], "VIGENTE")

    def test_otra_hoja_del_mismo_libro_no_se_afecta(self):
        fila = self.fila("x").assign(**{"Hoja origen": "1.13 Ventas"})
        self.assertEqual(taxonomia.aplicar(fila).iloc[0]["Estado"], "VIGENTE")


if __name__ == "__main__":
    unittest.main()


class DejarDeExcluirTest(unittest.TestCase):
    def test_una_serie_que_ya_no_esta_excluida_recupera_su_estado(self):
        import pandas as pd
        from tools import taxonomia
        filas = pd.DataFrame([
            {"Código fuente": "datos.gob.ar", "ID": "64.1_T_0_0_5", "Nombre serie": "x", "Estado": "EXCLUIDA",
             "Hoja origen": "LP - Pers", "Archivo origen": "empleo_ingresos.xlsx", "Fecha fin": "2003-01-01",
             "Descripción": "Población con ingresos debajo de línea de pobreza (%) de 2001 a 2003. TOTAL. EPH puntual."},
            {"Código fuente": "datos.gob.ar", "ID": "41.1_PPSOU_0_M_33", "Nombre serie": "x", "Estado": "CERRADA",
             "Hoja origen": "1.17 Productos ind.", "Archivo origen": "actividad.xlsx", "Fecha fin": "2020-05-01"},
        ])
        estados = list(taxonomia.aplicar(filas)["Estado"])
        self.assertEqual(estados, ["CERRADA", "EXCLUIDA"])
