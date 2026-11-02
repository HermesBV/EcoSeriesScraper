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
from coyuntura.actualizador.formulas import ampliar_rangos, planificar_formulas, referencias, tramos
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
        # 23 al 28/9 no están en la fuente: promedio del dato anterior (22/9) y el posterior (29/9).
        self.assertEqual(motivos[("C", 5)], ("imputada", (1003.5 + 2000.0) / 2, 7777))
        self.assertEqual({fila for fila, *_ in plan.imputadas}, {4, 5, 6, 7})
        planificar_formulas(libro_en_memoria(self.wb), [plan])
        self.assertEqual(plan.formulas, {"A": (7, [8, 9]), "D": (7, [8, 9])})
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

    def test_un_cero_de_la_fuente_se_imputa_con_el_promedio(self) -> None:
        fuente = pd.Series({pd.Timestamp("2026-09-23"): 1004.0, pd.Timestamp("2026-09-24"): 0.0,
                            pd.Timestamp("2026-09-25"): 1006.0})
        plan = planificar_hoja(self.hoja, self.estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual([(x.fila, x.motivo, x.valor) for x in plan.escrituras], [(5, "imputada", 1005.0)])
        self.assertIn((7, "C", 1007), plan.conservadas)  # sin dato posterior: no hay promedio posible

    def test_un_cero_no_es_dato_y_el_hueco_se_imputa(self) -> None:
        self.wb["Reservas diario"]["C3"] = None
        self.wb["Reservas diario"]["C4"] = None
        estructura = libro_en_memoria(self.wb).analizar(self.hoja)
        fuente = pd.Series({pd.Timestamp("2026-09-22"): 0.0, pd.Timestamp("2026-09-23"): 4.0})
        plan = planificar_hoja(self.hoja, estructura, FuentesFalsas({"74": fuente}), {})
        # El 0 no completa C3, pero C3 queda entre dos datos: se imputa con el promedio.
        self.assertEqual([(x.fila, x.motivo, x.valor) for x in plan.escrituras],
                         [(4, "completa", 4.0), (3, "imputada", (1002 + 4.0) / 2)])

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
        planificar_formulas(libro_en_memoria(wb), [plan])
        self.assertEqual(plan.formulas, {"A": (estructura.ultima_fila, [fila for fila, _ in plan.filas_nuevas])})

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


def hoja_mensual(wb: Workbook, nombre: str, meses: int, datos: dict[str, int]) -> None:
    """Hoja mensual desde 2026-01 con ``meses`` fechas; ``datos`` = columna -> meses con valor."""
    ws = wb.create_sheet(nombre)
    ws.append(["fecha"])
    for i, mes in enumerate(pd.date_range("2026-01-01", periods=meses, freq="MS"), start=2):
        ws.cell(i, 1, mes.to_pydatetime())
        for col, hasta in datos.items():
            if i - 1 <= hasta:
                ws[f"{col}{i}"] = 100.0 + i
    ws.row_dimensions[meses + 2].fill = AZUL


class FormulasTests(unittest.TestCase):
    """Qué fórmulas se extienden y dónde (los errores del 31/10 en IPC, Salarios y COMEX)."""

    def libro(self) -> Workbook:
        wb = Workbook()
        wb.remove(wb.active)
        # Salarios: B con 7 meses, C (SIPA) con 6; D viva sobre C; E abandonada en el mes 3.
        hoja_mensual(wb, "Salarios", 7, {"B": 7, "C": 6})
        ws = wb["Salarios"]
        for fila in range(3, 8):
            ws[f"D{fila}"] = f"=+C{fila}/C{fila - 1}-1"
        for fila in range(3, 5):
            ws[f"E{fila}"] = f"=+C{fila}/C{fila - 1}-1"
        for fila in range(2, 8):
            ws[f"F{fila}"] = f"=+B{fila}/IPC!B{fila}"   # deflactado con IPC, misma fila
        # IPC: 6 meses (la fila 8 es la barra).
        hoja_mensual(wb, "IPC", 6, {"B": 6})
        return wb

    def planes(self, wb: Workbook, salarios: dict, ipc: dict):
        libro = libro_en_memoria(wb)
        hojas = [Hoja("Salarios", "M", "A", 2, {"B": Columna(Serie("x", "B")), "C": Columna(Serie("x", "C"))}),
                 Hoja("IPC", "M", "A", 2, {"B": Columna(Serie("x", "IPC"))}, calendario=["B"])]
        series = {k: pd.Series(v, dtype=float) for k, v in {**salarios, **ipc}.items()}
        fuentes = FuentesFalsas(series)
        planes = {}
        for hoja in hojas:
            planes[hoja.nombre] = planificar_hoja(hoja, libro.analizar(hoja), fuentes, planes)
        planificar_formulas(libro, list(planes.values()))
        return planes

    def test_completa_formulas_en_filas_existentes_y_no_revive_abandonadas(self) -> None:
        wb = self.libro()
        mes = lambda m: pd.Timestamp(f"2026-{m:02d}-01")
        planes = self.planes(wb, {"B": {mes(8): 5.0}, "C": {mes(7): 9.0}}, {"IPC": {mes(7): 1.0}})
        salarios = planes["Salarios"]
        self.assertEqual([fila for fila, _ in salarios.filas_nuevas], [9])
        # D: julio (fila 8) se completa porque llegó C; agosto (9) no tiene C.
        self.assertEqual(salarios.formulas["D"], (7, [8]))
        self.assertNotIn("E", salarios.formulas)  # abandonada en la fila 4
        # F: julio tiene IPC (fila nueva 8 de IPC); agosto no.
        self.assertEqual(salarios.formulas["F"], (7, [8]))

    def test_sin_ipc_no_se_copia_la_formula_que_lo_usa(self) -> None:
        wb = self.libro()
        mes = lambda m: pd.Timestamp(f"2026-{m:02d}-01")
        planes = self.planes(wb, {"B": {mes(8): 5.0}, "C": {}}, {"IPC": {}})
        self.assertEqual(planes["IPC"].filas_nuevas, [])
        self.assertNotIn("F", planes["Salarios"].formulas)
        self.assertNotIn("D", planes["Salarios"].formulas)

    def test_dias_no_habiles_no_abren_filas(self) -> None:
        hoja = Hoja("Reservas diario", "D", "B", 2, {"C": Columna(Serie("bcra-dmd", "74"))})
        estructura = libro_en_memoria(hoja_diaria()).analizar(hoja)
        fuente = pd.Series({pd.Timestamp("2026-09-29"): 1.0, pd.Timestamp("2026-10-03"): 2.0,
                            pd.Timestamp("2026-10-05"): 0.0})
        plan = planificar_hoja(hoja, estructura, FuentesFalsas({"74": fuente}), {})
        self.assertEqual([f.date() for _, f in plan.filas_nuevas], [date(2026, 9, 29), date(2026, 10, 5)])
        self.assertEqual([(x.fila, x.valor) for x in plan.escrituras], [(8, 1.0)])  # el 0 no es dato

    def test_tcr_diario_abre_filas_en_dias_corridos(self) -> None:
        hoja = Hoja("Reservas diario", "D", "B", 2, {"C": Columna(Serie("bcra-itc", "ITCRM"))}, dias_corridos=True)
        estructura = libro_en_memoria(hoja_diaria()).analizar(hoja)
        fuente = pd.Series({pd.Timestamp("2026-09-29"): 1.0, pd.Timestamp("2026-10-03"): 2.0})
        plan = planificar_hoja(hoja, estructura, FuentesFalsas({"ITCRM": fuente}), {})
        self.assertEqual([f.date() for _, f in plan.filas_nuevas], [date(2026, 9, 29), date(2026, 10, 3)])

    def test_dias_habiles_segun_el_calendario_del_a3500(self) -> None:
        from coyuntura.actualizador.plan import dia_habil
        habiles = {pd.Timestamp("2024-03-28"), pd.Timestamp("2024-04-02")}  # 01/04/2024: feriado puente
        self.assertFalse(dia_habil(pd.Timestamp("2024-04-01"), habiles))
        self.assertTrue(dia_habil(pd.Timestamp("2024-04-02"), habiles))
        self.assertTrue(dia_habil(pd.Timestamp("2026-11-02"), habiles))   # fuera de cobertura: lunes
        self.assertFalse(dia_habil(pd.Timestamp("2026-12-25"), habiles))  # feriado fijo

    def test_referencias(self) -> None:
        refs = referencias("=+LOG10(C5)/'TCR Mensual'!$S$2+IPC!$B1002-SUM(Z430:Z441)")
        self.assertEqual([(r.hoja, r.col, r.fila, r.fila_relativa) for r in refs],
                         [(None, "C", 5, True), ("TCR Mensual", "S", 2, False), ("IPC", "B", 1002, True),
                          (None, "Z", 430, True), (None, "Z", 441, True)])
        self.assertEqual(tramos([5, 3, 4, 9]), [(3, 5), (9, 9)])

    def test_ampliar_rangos_del_cuadro_anual(self) -> None:
        formula = "=+Y441/VLOOKUP(A441,$A$449:$AB$481,28,FALSE)+SUM($A$481:$A$481)"
        self.assertEqual(ampliar_rangos(formula, "COMEX", "COMEX", 449, 481, 1),
                         "=+Y441/VLOOKUP(A441,$A$449:$AB$482,28,FALSE)+SUM($A$481:$A$481)")
        serie = "=SERIES(,COMEX!$B$459:$B$481,COMEX!$AQ$459:$AQ$481,1)"
        self.assertEqual(ampliar_rangos(serie, "Gráficos", "COMEX", 449, 481, 2),
                         "=SERIES(,COMEX!$B$459:$B$483,COMEX!$AQ$459:$AQ$483,1)")
        self.assertEqual(ampliar_rangos("=SUM(C449:C481)", "Fiscal", "COMEX", 449, 481, 1), "=SUM(C449:C481)")


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
