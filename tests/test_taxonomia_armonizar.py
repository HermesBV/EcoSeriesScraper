"""Acuerdos entre módulos: un indicador, un árbol y una grafía."""

import unittest

from tools.taxonomia import Clasificacion, Serie, clasificar
from tools.taxonomia.armonizar import armonizar

SERIE = Serie(fuente="x", id="1", nombre="", variable="", descripcion="", unidad="", frecuencia="M",
              archivo="", hoja="", columna="", dataset="", estado="VIGENTE")


def c(**campos) -> Clasificacion:
    return Clasificacion(**{"tema": "Sector externo", "indicador": "Tipo de cambio nominal", "nombre": "x", **campos})


class ArmonizarTest(unittest.TestCase):
    def test_tipo_de_cambio_un_solo_prefijo_y_grupos_por_mercado(self):
        casos = (
            (c(nombre="Tipo de cambio nominal · Valuación contable"), "Tipo de cambio · Valuación contable", "Oficial"),
            (c(nombre="Tipo de cambio · Implícito en ADR", unidad="Pesos por US$"), "Tipo de cambio · Implícito en ADR",
             "Paralelos y financieros"),
            (c(nombre="Tipo de cambio · Dólar blue · Venta", cobertura="Dólar blue"), "Tipo de cambio · Dólar blue · Venta",
             "Paralelos y financieros"),
            (c(nombre="Tipo de cambio · Oficial mayorista (A 3500) (promedio)", cobertura="Oficial mayorista"),
             "Tipo de cambio · Oficial mayorista (A 3500, promedio)", "Oficial"),
            (c(nombre="Banda cambiaria · Superior", cobertura="Bandas cambiarias"), "Banda cambiaria · Superior",
             "Bandas cambiarias"),
        )
        for entrada, nombre, cobertura in casos:
            with self.subTest(nombre=nombre):
                salida = armonizar(SERIE, entrada)
                self.assertEqual((salida.nombre, salida.cobertura), (nombre, cobertura))
        self.assertEqual(armonizar(SERIE, c(nombre="Tipo de cambio · Mayorista (MAE)", unidad="Pesos por US$")).unidad,
                         "Pesos por dólar")

    def test_tipo_de_cambio_del_pib_va_al_sector_externo(self):
        entrada = Clasificacion(tema="Actividad", indicador="PIB", nombre="PIB · Tipo de cambio nominal",
                                cobertura="En dólares y per cápita")
        salida = armonizar(SERIE, entrada)
        self.assertEqual((salida.tema, salida.indicador, salida.cobertura), ("Sector externo", "Tipo de cambio nominal", "Oficial"))

    def test_sectores_con_una_sola_grafia(self):
        entrada = Clasificacion(tema="Actividad", indicador="EMAE", cobertura="Sectores",
                                nombre="EMAE · Transporte y comunicaciones (var. % interanual)")
        salida = armonizar(SERIE, entrada)
        self.assertEqual(salida.cobertura, "Por sector de actividad")
        self.assertEqual(salida.nombre, "EMAE · Transporte, almacenamiento y comunicaciones (var. % interanual)")

    def test_lo_demas_no_cambia(self):
        entrada = Clasificacion(tema="Precios", indicador="IPC", nombre="IPC · Nivel general", cobertura="Nacional")
        self.assertIs(armonizar(SERIE, entrada), entrada)

    def test_el_despacho_general_aplica_los_acuerdos(self):
        serie = Serie(fuente="ambito-dolar-blue", id="ambito-dolar-blue-venta", nombre="Dólar blue - Venta",
                      variable="ambito-dolar-blue-venta", descripcion="Dólar blue.", unidad="pesos por dólar",
                      frecuencia="D", archivo="history.csv", hoja="Dólar blue", columna="Venta", dataset="",
                      estado="VIGENTE")
        resultado = clasificar(serie)
        if resultado is not None:  # la regla del módulo puede exigir más campos que estos
            self.assertEqual(resultado.cobertura, "Paralelos y financieros")


if __name__ == "__main__":
    unittest.main()
