"""Pruebas del actualizador del Excel de coyuntura (sin Excel ni BD.xlsx)."""

from datetime import date
from pathlib import Path
import tempfile
import unittest
import zipfile

from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.styles.colors import Color
import pandas as pd

from coyuntura.actualizador import respaldo
from coyuntura.actualizador.estructura import Libro
from coyuntura.actualizador.fuentes import anualizar
from coyuntura.actualizador.mapeo import Anual, BloqueAnual, Columna, Hoja, Serie
from coyuntura.actualizador.plan import clave, fecha_desde_clave, planificar_bloque, planificar_hoja

AZUL = PatternFill(fill_type="solid", fgColor=Color(theme=4))


class FuentesFalsas:
    def __init__(self, series: dict[str, pd.Series]) -> None:
        self.series = series

    def evaluar(self, fuente):
        serie = fuente.serie if isinstance(fuente, Anual) else fuente
        valores = self.series[serie.id]
        return anualizar(valores, fuente.metodo) if isinstance(fuente, Anual) else valores


def libro_en_memoria(wb: Workbook) -> Libro:
    libro = Libro.__new__(Libro)
    libro.formulas = libro.valores = wb
    libro.ruta = Path("memoria.xlsx")
    return libro


def hoja_diaria() -> Workbook:
    wb = Workbook()
    ws = wb.active
    ws.title = "Reservas diario"
    ws.append(["Mes", "fecha", "Reservas brutas", "m/m"])
    fechas = pd.bdate_range("2026-09-21", "2026-09-28")
    for fila, fecha in enumerate(fechas, start=2):
        ws.cell(fila, 1, f"=+DATE(YEAR(B{fila}),MONTH(B{fila}),1)")
        ws.cell(fila, 2, fecha.to_pydatetime())
        ws.cell(fila, 3, 1000 + fila)
        ws.cell(fila, 4, f"=+C{fila}/C{fila - 1}-1")
    ws.cell(5, 3, 7777)  # 2026-09-24: valor que la fuente no publica (imputación)
    barra = 2 + len(fechas)
    ws.row_dimensions[barra].fill = AZUL
    ws.cell(barra + 2, 3, "=SUM(C2:C5)")
    return wb


class PlanHojaDiariaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hoja = Hoja("Reservas diario", "D", "B", 2, {"C": Columna(Serie("bcra-dmd", "74"))})
        self.wb = hoja_diaria()
        self.estructura = libro_en_memoria(self.wb).analizar(self.hoja)

    def test_estructura_encuentra_barra_fechas_y_formulas(self) -> None:
        self.assertEqual(self.estructura.ultima_fila, 7)
        self.assertEqual(self.estructura.fila_azul, 8)
        self.assertEqual(self.estructura.ultima_fecha, pd.Timestamp("2026-09-28"))
        self.assertEqual(self.estructura.formulas["A"][0], 7)
        self.assertEqual(self.estructura.formulas["D"][0], 7)
        self.assertNotIn("C", self.estructura.formulas)

    def test_plan_agrega_filas_reemplaza_y_conserva_imputaciones(self) -> None:
        fuente = pd.Series(
            {pd.Timestamp("2026-09-21"): 1002.0, pd.Timestamp("2026-09-22"): 1003.5,
             pd.Timestamp("2026-09-29"): 2000.0, pd.Timestamp("2026-09-30"): 2001.0},
        )
        plan = planificar_hoja(self.hoja, self.estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual([(fila, fecha.date()) for fila, fecha in plan.filas_nuevas],
                         [(8, date(2026, 9, 29)), (9, date(2026, 9, 30))])
        motivos = {(x.col, x.fila): (x.motivo, x.valor, x.anterior) for x in plan.escrituras}
        self.assertEqual(motivos[("C", 3)], ("reemplazo", 1003.5, 1003))
        self.assertNotIn(("C", 2), motivos)  # igual que la fuente: no se toca
        self.assertEqual(motivos[("C", 8)], ("nuevo", 2000.0, None))
        self.assertEqual(motivos[("C", 9)], ("nuevo", 2001.0, None))
        self.assertIn((5, "C", 7777), plan.conservadas)
        self.assertEqual(set(plan.formulas), {"A", "D"})
        self.assertEqual(plan.fechas_finales[-1], pd.Timestamp("2026-09-30"))

    def test_las_formulas_del_economista_no_se_pisan_salvo_pedido(self) -> None:
        self.wb["Reservas diario"]["C3"] = "=+C2*1.01"
        estructura = libro_en_memoria(self.wb).analizar(self.hoja)
        fuente = pd.Series({pd.Timestamp("2026-09-22"): 999.0})
        plan = planificar_hoja(self.hoja, estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual(plan.escrituras, [])
        self.assertEqual(plan.formulas_respetadas, 1)
        hoja = Hoja("Reservas diario", "D", "B", 2, {"C": Columna(Serie("bcra-dmd", "74"), pisar_formulas=True)})
        plan = planificar_hoja(hoja, estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual([(x.fila, x.motivo, x.anterior) for x in plan.escrituras], [(3, "reemplazo", "=+C2*1.01")])

    def test_un_cero_de_la_fuente_no_pisa_una_imputacion(self) -> None:
        fuente = pd.Series({pd.Timestamp("2026-09-24"): 0.0, pd.Timestamp("2026-09-25"): 1006.0})
        plan = planificar_hoja(self.hoja, self.estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual([(x.fila, x.motivo) for x in plan.escrituras], [])  # C6 ya vale 1006
        self.assertIn((5, "C", 7777), plan.conservadas)

    def test_un_cero_de_la_fuente_no_completa_celdas_vacias(self) -> None:
        self.wb["Reservas diario"]["C3"] = None
        self.wb["Reservas diario"]["C4"] = None
        estructura = libro_en_memoria(self.wb).analizar(self.hoja)
        fuente = pd.Series({pd.Timestamp("2026-09-22"): 0.0, pd.Timestamp("2026-09-23"): 4.0})
        plan = planificar_hoja(self.hoja, estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual([(x.fila, x.motivo, x.valor) for x in plan.escrituras], [(4, "completa", 4.0)])

    def test_modo_agregar_no_pisa_valores_existentes(self) -> None:
        hoja = Hoja("Reservas diario", "D", "B", 2, {"C": Columna(Serie("ambito", "x"), "agregar")})
        fuente = pd.Series({pd.Timestamp("2026-09-21"): 5.0, pd.Timestamp("2026-09-29"): 6.0})
        plan = planificar_hoja(hoja, self.estructura, FuentesFalsas({"x": fuente}), {})
        self.assertEqual([(x.fila, x.motivo) for x in plan.escrituras], [(8, "nuevo")])


class PlanComexTests(unittest.TestCase):
    def libro(self) -> tuple[Workbook, Hoja]:
        wb = Workbook()
        ws = wb.active
        ws.title = "COMEX"
        ws.append(["Año", "fecha", "Expo"])
        ws.append([None, None, None])
        fila = 3
        for mes in pd.date_range("2025-01-01", "2026-12-01", freq="MS"):
            ws.cell(fila, 1, f"=+YEAR(B{fila})")
            ws.cell(fila, 2, mes.to_pydatetime())
            if mes <= pd.Timestamp("2026-08-01"):
                ws.cell(fila, 3, 100.0 + fila)
            fila += 1
        barra = fila
        ws.row_dimensions[barra].fill = AZUL
        ws.cell(barra + 2, 2, 2024)
        ws.cell(barra + 2, 1, 2024)
        ws.cell(barra + 2, 3, f"=+SUMIF($A$3:$A${barra},$B{barra + 2},C$3:C${barra})")
        ws.cell(barra + 2, 5, 11.0)
        ws.cell(barra + 3, 2, 2025)
        ws.cell(barra + 3, 1, 2025)
        ws.cell(barra + 3, 3, f"=+SUMIF($A$3:$A${barra},$B{barra + 3},C$3:C${barra})")
        hoja = Hoja("COMEX", "M", "B", 3, {"C": Columna(Serie("indec-ica", "expo"))}, especial="comex")
        return wb, hoja

    def test_rellena_hasta_diciembre_y_mueve_la_barra_solo_con_anio_nuevo(self) -> None:
        wb, hoja = self.libro()
        estructura = libro_en_memoria(wb).analizar(hoja, "B")
        self.assertEqual(estructura.ultima_fecha, pd.Timestamp("2026-12-01"))
        self.assertEqual(estructura.bloques_anuales, [(estructura.fila_azul + 2, estructura.fila_azul + 3, [2024, 2025])])
        fuente = pd.Series({pd.Timestamp("2026-09-01"): 9.0})
        plan = planificar_hoja(hoja, estructura, FuentesFalsas({"expo": fuente}), {})
        self.assertEqual(plan.filas_nuevas, [])
        self.assertEqual([(x.fila, x.motivo) for x in plan.escrituras], [(11 + 12, "completa")])

        fuente = pd.Series({pd.Timestamp("2027-01-01"): 9.0})
        plan = planificar_hoja(hoja, estructura, FuentesFalsas({"expo": fuente}), {})
        self.assertEqual(len(plan.filas_nuevas), 12)
        self.assertEqual(plan.filas_nuevas[0][1], pd.Timestamp("2027-01-01"))
        self.assertEqual(plan.filas_nuevas[-1][1], pd.Timestamp("2027-12-01"))
        self.assertEqual(plan.filas_nuevas[0][0], estructura.fila_azul)
        self.assertEqual(plan.formulas, {"A": estructura.ultima_fila})

    def test_bloque_anual_suma_el_anio_cuando_hay_diciembre(self) -> None:
        wb, hoja = self.libro()
        libro = libro_en_memoria(wb)
        estructura = libro.analizar(hoja, "B")
        trimestral = pd.Series({pd.Timestamp(f"{anio}-{mes:02d}-01"): float(anio) for anio in (2024, 2025, 2026) for mes in (1, 4, 7, 10)
                                if not (anio == 2026 and mes == 10)})
        bloque = BloqueAnual("COMEX", "B", 1, "COMEX", "C", {"E": Columna(Anual(Serie("indec-pib", "pib"), "promedio"))})
        fuentes = FuentesFalsas({"expo": pd.Series({pd.Timestamp("2026-12-01"): 1.0}), "pib": trimestral})
        plan_mensual = planificar_hoja(hoja, estructura, fuentes, {})
        plan = planificar_bloque(bloque, libro, estructura, plan_mensual, fuentes)
        self.assertEqual(plan.nuevos, [2026])
        self.assertEqual(plan.columnas_anio, ["A", "B"])
        self.assertEqual(plan.formulas, ["C"])
        escrituras = {(x.col, x.fila): (x.motivo, x.valor) for x in plan.escrituras}
        self.assertEqual(escrituras[("E", plan.fila_inicio)], ("reemplazo", 2024.0))
        self.assertEqual(escrituras[("E", plan.fila_inicio + 1)], ("completa", 2025.0))
        self.assertNotIn(("E", plan.fila_fin + 1), escrituras)  # 2026 no tiene cuatro trimestres


class ClavesTests(unittest.TestCase):
    def test_claves_y_convencion_de_fecha(self) -> None:
        self.assertEqual(clave(pd.Timestamp("2026-03-15"), "M"), (2026, 3))
        self.assertEqual(fecha_desde_clave((2026, 2), "M", pd.Timestamp("2026-01-01")), pd.Timestamp("2026-02-01"))
        self.assertEqual(fecha_desde_clave((2026, 2), "M", pd.Timestamp("2026-01-31")), pd.Timestamp("2026-02-28"))

    def test_anualizar_exige_cuatro_trimestres(self) -> None:
        serie = pd.Series({pd.Timestamp("2024-01-01"): 1.0, pd.Timestamp("2024-04-01"): 2.0,
                           pd.Timestamp("2024-07-01"): 3.0, pd.Timestamp("2024-10-01"): 4.0,
                           pd.Timestamp("2025-01-01"): 9.0})
        self.assertEqual(anualizar(serie, "suma").to_dict(), {pd.Timestamp("2024-01-01"): 10.0})
        self.assertEqual(anualizar(serie, "promedio").to_dict(), {pd.Timestamp("2024-01-01"): 2.5})


class RespaldoTests(unittest.TestCase):
    def test_rotacion_comprime_meses_cerrados_y_conserva_dos_zip(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            carpeta = Path(carpeta)
            for sufijo in ("260801", "260815", "260903", "261030", "261031"):
                (carpeta / respaldo.nombre_respaldo(date(2000 + int(sufijo[:2]), int(sufijo[2:4]), int(sufijo[4:])))).write_text("x")
            resultado = respaldo.rotar(date(2026, 10, 31), carpeta, conservar=2)
            self.assertEqual(resultado["comprimidos"], ["coyuntura iiep reemplazar 2608.zip", "coyuntura iiep reemplazar 2609.zip"])
            self.assertEqual(resultado["borrados"], [])
            with zipfile.ZipFile(carpeta / "coyuntura iiep reemplazar 2608.zip") as archivo:
                self.assertEqual(sorted(archivo.namelist()), ["coyuntura iiep reemplazar 260801.xlsx", "coyuntura iiep reemplazar 260815.xlsx"])
            self.assertEqual(sorted(p.name for p in carpeta.glob("*.xlsx")),
                             ["coyuntura iiep reemplazar 261030.xlsx", "coyuntura iiep reemplazar 261031.xlsx"])
            resultado = respaldo.rotar(date(2026, 11, 2), carpeta, conservar=2)
            self.assertEqual(resultado["comprimidos"], ["coyuntura iiep reemplazar 2610.zip"])
            self.assertEqual(resultado["borrados"], ["coyuntura iiep reemplazar 2608.zip"])
            self.assertEqual(sorted(p.name for p in carpeta.glob("*.zip")),
                             ["coyuntura iiep reemplazar 2609.zip", "coyuntura iiep reemplazar 2610.zip"])


if __name__ == "__main__":
    unittest.main()
