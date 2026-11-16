"""Pruebas de las reglas de la Encuesta de supermercados (INDEC)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.actividad_supermercados import EXCLUIDAS, clasificar

TITULOS = {
    "Cuadro 1": "Encuesta de Supermercados. Índice de ventas totales a precios constantes. Serie original, desestacionalizada y tendencia-ciclo, base año 2017=100, en números índice y variación porcentual. {rango}",
    "Cuadro 2": "Encuesta de supermercados. Índice de ventas totales a precios corrientes, constantes e índice de precios implícitos, base año 2017=100, en números índice y variación porcentual. {rango}",
    "Cuadro 3.": "Encuesta de Supermercados. Ventas totales a precios corrientes y a precios constantes, en millones de pesos y variación porcentual. {rango}",
    "Cuadro 4.": "Encuesta de Supermercados. Ventas totales a precios corrientes por canal de venta y medio de pago, en miles de pesos. {rango}",
    "Cuadro 5.": "Encuesta de supermercados. Ventas totales a precios corrientes por grupo de artículos y jurisdicción, en miles de pesos. {rango}",
    "Cuadro 6.": "Encuesta de Supermercados. Ventas totales a precios corrientes por boca de expendio, metro cuadro y por operación según jurisdicción, en miles de pesos, unidades, metro cuadrado y pesos. {rango}",
}
RANGO_A = "Enero 2017 – agosto 2026"
RANGO_B = "Enero 2017 – septiembre 2027"


def serie(hoja: str, columna: str, rango: str = RANGO_A, fuente: str = "indec-supermercados") -> Serie:
    titulo = TITULOS[hoja].format(rango=rango)
    return Serie(
        fuente=fuente, id=f"supermercados.xlsx|{hoja}|{columna.lower().replace(' ', '-')}",
        nombre=f"{titulo} - {columna}", variable=columna, descripcion=titulo, unidad="",
        frecuencia="M", archivo="supermercados.xlsx", hoja=hoja, columna=columna,
        dataset=titulo, estado="VIGENTE",
    )


def esperado(cobertura, apertura, nombre, unidad):
    return Clasificacion(tema="Actividad", indicador="Ventas en supermercados", cobertura=cobertura,
                         apertura=apertura, nombre=nombre, unidad=unidad)


V = "Ventas en supermercados"
CASOS = (
    # Cuadro 1: serie a precios constantes, original, desestacionalizada y tendencia-ciclo
    ("Cuadro 1", "Serie original - Índice de ventas totales a precios constantes - Números índice",
     esperado("Total", "", f"{V} · Total (precios constantes)", "Índice 2017=100")),
    ("Cuadro 1", "Serie original - Variación porcentual - respecto al mismo mes del año anterior - %",
     esperado("Total", "", f"{V} · Total (precios constantes, var. % interanual)", "%")),
    ("Cuadro 1", "Serie original - Variación porcentual - acumulada del año respecto a igual acumulado del año anterior - %",
     esperado("Total", "", f"{V} · Total (precios constantes, var. % acumulada)", "%")),
    ("Cuadro 1", "Serie desestacionalizada (1) - Índice de ventas totales a precios constantes - acumulada del año respecto a igual acumulado del año anterior - Números índice",
     esperado("Total", "", f"{V} · Total (precios constantes, desestacionalizado)", "Índice 2017=100")),
    ("Cuadro 1", "Serie desestacionalizada (1) - Variación porcentual respecto al mes anterior - acumulada del año respecto a igual acumulado del año anterior - %",
     esperado("Total", "", f"{V} · Total (precios constantes, desestacionalizado, var. % mensual)", "%")),
    ("Cuadro 1", "Serie tendencia-ciclo - Índice de ventas totales a precios constantes - acumulada del año respecto a igual acumulado del año anterior - Números índice",
     esperado("Total", "", f"{V} · Total (precios constantes, tendencia-ciclo)", "Índice 2017=100")),
    # Cuadro 2: corrientes, constantes y precios implícitos
    ("Cuadro 2", "Índice ventas totales a precios corrientes - Números índice",
     esperado("Total", "", f"{V} · Total", "Índice 2017=100")),
    ("Cuadro 2", "Variación porcentual - respecto al mismo mes del año anterior - %",
     esperado("Total", "", f"{V} · Total (var. % interanual)", "%")),
    ("Cuadro 2", "Variación porcentual respecto al mismo mes del año anterior - acumulada del año respecto a igual acumulado del año anterior - %",
     esperado("Total", "", f"{V} · Total (precios constantes, var. % interanual)", "%")),
    ("Cuadro 2", "Índice de precios implícitos - acumulada del año respecto a igual acumulado del año anterior - Números índice",
     esperado("Total", "", f"{V} · Precios implícitos", "Índice 2017=100")),
    ("Cuadro 2", "Variación porcentual - respecto al mismo mes del año anterior - % (2)",
     esperado("Total", "", f"{V} · Precios implícitos (var. % interanual)", "%")),
    ("Cuadro 2", "Variación porcentual - respecto al mes anterior - %",
     esperado("Total", "", f"{V} · Precios implícitos (var. % mensual)", "%")),
    # Cuadro 3: millones de pesos
    ("Cuadro 3.", "Ventas totales - a precios corrientes - Millones de pesos",
     esperado("Total", "", f"{V} · Total", "Millones de pesos")),
    ("Cuadro 3.", "Ventas totales - a precios constantes - Millones de pesos",
     esperado("Total", "", f"{V} · Total (precios constantes)", "Millones de pesos")),
    ("Cuadro 3.", "Variación porcentual respecto al mismo período del año anterior - a precios constantes - %",
     esperado("Total", "", f"{V} · Total (precios constantes, var. % interanual)", "%")),
    # Cuadro 4: canal y medio de pago
    ("Cuadro 4.", "Ventas totales - Miles de pesos",
     esperado("Total", "", f"{V} · Total", "Miles de pesos")),
    ("Cuadro 4.", "Canal de venta - Canales online - Miles de pesos",
     esperado("Por canal de venta", "", f"{V} · Canales online", "Miles de pesos")),
    ("Cuadro 4.", "Medio de pago - Tarjetas de crédito - Miles de pesos",
     esperado("Por medio de pago", "", f"{V} · Tarjetas de crédito", "Miles de pesos")),
    # Cuadro 5: grupo de artículos por jurisdicción
    ("Cuadro 5.", "Córdoba - Almacén - Miles de pesos",
     esperado("Por grupo de artículos", "Almacén", f"{V} · Almacén · Córdoba", "Miles de pesos")),
    ("Cuadro 5.", "24 partidos del Gran Buenos Aires - Alimentos preparados y rotisería - Miles de pesos",
     esperado("Por grupo de artículos", "Alimentos preparados y rotisería",
              f"{V} · Alimentos preparados y rotisería · 24 partidos del Gran Buenos Aires", "Miles de pesos")),
    ("Cuadro 5.", "Total del país - Indumentaria, calzado y textiles para el hogar - Miles de pesos",
     esperado("Por grupo de artículos", "Indumentaria, calzado y textiles para el hogar",
              f"{V} · Indumentaria, calzado y textiles para el hogar", "Miles de pesos")),
    ("Cuadro 5.", "Ciudad Autónoma de Buenos Aires - Verdulería y frutería - Miles de pesos",
     esperado("Por grupo de artículos", "Verdulería y frutería",
              f"{V} · Verdulería y frutería · Ciudad Autónoma de Buenos Aires", "Miles de pesos")),
    ("Cuadro 5.", "Total del país - Total - Miles de pesos",
     esperado("Total", "", f"{V} · Total", "Miles de pesos")),
    ("Cuadro 5.", "Tucumán - Total - Miles de pesos",
     esperado("Por jurisdicción", "Ventas totales", f"{V} · Total · Tucumán", "Miles de pesos")),
    # Cuadro 6: estructura por jurisdicción
    ("Cuadro 6.", "Santa Fe - Cantidad de bocas - Unidades",
     esperado("Por jurisdicción", "Bocas de expendio", "Supermercados · Bocas de expendio · Santa Fe", "Unidades")),
    ("Cuadro 6.", "Total del país - Superficie del área de ventas - m2",
     esperado("Por jurisdicción", "Superficie del área de ventas",
              "Supermercados · Superficie del área de ventas", "Metros cuadrados")),
    ("Cuadro 6.", "Resto de Buenos Aires - Cantidad de operaciones - Unidades",
     esperado("Por jurisdicción", "Operaciones", "Supermercados · Operaciones · Resto de Buenos Aires", "Unidades")),
    ("Cuadro 6.", "Neuquén - Ventas por boca - Miles de pesos",
     esperado("Por jurisdicción", "Ventas por boca de expendio",
              "Supermercados · Ventas por boca de expendio · Neuquén", "Miles de pesos")),
    ("Cuadro 6.", "Entre Ríos - Ventas por m2 - Pesos",
     esperado("Por jurisdicción", "Ventas por metro cuadrado",
              "Supermercados · Ventas por metro cuadrado · Entre Ríos", "Pesos")),
    ("Cuadro 6.", "Tierra del Fuego - Ventas por operación - Pesos",
     esperado("Por jurisdicción", "Ventas por operación",
              "Supermercados · Ventas por operación · Tierra del Fuego", "Pesos")),
    ("Cuadro 6.", "Mendoza - Ventas totales - Miles de pesos",
     esperado("Por jurisdicción", "Ventas totales", f"{V} · Total · Mendoza", "Miles de pesos")),
    ("Cuadro 6.", "Total del país - Ventas totales - Miles de pesos",
     esperado("Total", "", f"{V} · Total", "Miles de pesos")),
)


class ClasificarSupermercadosTest(unittest.TestCase):
    def test_casos_reales(self):
        for hoja, columna, bueno in CASOS:
            with self.subTest(hoja=hoja, columna=columna):
                self.assertEqual(clasificar(serie(hoja, columna)), bueno)

    def test_no_depende_del_periodo_del_titulo(self):
        for hoja, columna, bueno in CASOS:
            with self.subTest(hoja=hoja, columna=columna):
                self.assertEqual(clasificar(serie(hoja, columna, RANGO_B)), bueno)

    def test_rotulo_con_el_titulo_antepuesto(self):
        titulo = TITULOS["Cuadro 5."].format(rango=RANGO_B)
        resultado = clasificar(serie("Cuadro 5.", f"{titulo} - Córdoba - Almacén - Miles de pesos"))
        self.assertEqual(resultado.nombre, f"{V} · Almacén · Córdoba")
        titulo = TITULOS["Cuadro 2"].format(rango=RANGO_B)
        resultado = clasificar(serie("Cuadro 2", f"{titulo} - Variación porcentual - respecto al mes anterior - %"))
        self.assertEqual(resultado.nombre, f"{V} · Precios implícitos (var. % mensual)")

    def test_otra_fuente_devuelve_none(self):
        self.assertIsNone(clasificar(serie("Cuadro 5.", "Córdoba - Almacén - Miles de pesos", fuente="indec-ipc")))

    def test_hoja_o_rotulo_desconocidos_devuelven_none(self):
        self.assertIsNone(clasificar(serie("Cuadro 5.", "Atlántida - Almacén - Miles de pesos")))
        self.assertIsNone(clasificar(serie("Cuadro 5.", "Córdoba - Pescadería - Miles de pesos")))
        self.assertIsNone(clasificar(serie("Cuadro 6.", "Córdoba - Ventas por boca - Pesos")))
        self.assertIsNone(clasificar(serie("Cuadro 4.", "Medio de pago - Cheques - Miles de pesos")))
        otra = serie("Cuadro 4.", "Ventas totales - Miles de pesos")
        self.assertIsNone(clasificar(Serie(**{**otra.__dict__, "hoja": "Cuadro 9"})))

    def test_distinguir_precios_corrientes_y_constantes(self):
        corriente = clasificar(serie("Cuadro 3.", "Ventas totales - a precios corrientes - Millones de pesos"))
        constante = clasificar(serie("Cuadro 3.", "Ventas totales - a precios constantes - Millones de pesos"))
        self.assertNotEqual(corriente.nombre, constante.nombre)
        self.assertIn("precios constantes", constante.nombre)
        self.assertNotIn("precios", corriente.nombre)

    def test_excluidas_son_copias_exactas(self):
        self.assertTrue(all(f == "indec-supermercados" for f, _ in EXCLUIDAS))
        ids = {i for _, i in EXCLUIDAS}
        self.assertEqual(len(ids), 31)  # 6 columnas repetidas entre cuadros + 25 jurisdicciones del Cuadro 6
        for esperado_id in (
                "supermercados.xlsx|Cuadro 6.|24-partidos-del-gran-buenos-aires-ventas-totales-miles-de-pesos",
                "supermercados.xlsx|Cuadro 6.|ciudad-autonoma-de-buenos-aires-ventas-totales-miles-de-pesos",
                "supermercados.xlsx|Cuadro 6.|total-del-pais-ventas-totales-miles-de-pesos",
                "supermercados.xlsx|Cuadro 5.|total-del-pais-total-miles-de-pesos",
                "supermercados.xlsx|Cuadro 3.|variacion-porcentual-respecto-al-mismo-periodo-del-ano-anterior-a-precios-corrientes"):
            self.assertIn(esperado_id, ids)
        # Se conservan los totales de jurisdicción del Cuadro 5 y el total del Cuadro 4.
        self.assertNotIn("supermercados.xlsx|Cuadro 5.|cordoba-total-miles-de-pesos", ids)
        self.assertEqual(sum(1 for i in ids if "|Cuadro 6.|" in i), 26)

    def test_lo_excluido_sigue_teniendo_el_mismo_nombre_que_su_original(self):
        original = clasificar(serie("Cuadro 4.", "Ventas totales - Miles de pesos"))
        for hoja, columna in (("Cuadro 5.", "Total del país - Total - Miles de pesos"),
                              ("Cuadro 6.", "Total del país - Ventas totales - Miles de pesos")):
            self.assertEqual(clasificar(serie(hoja, columna)), original)


if __name__ == "__main__":
    unittest.main()
