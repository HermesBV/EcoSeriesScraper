from datetime import date, datetime, time
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

import pandas as pd

from tools.calendario import a_ics, actualizar, estado, fusionar
from tools.calendario.bcra import leer_pagina
from tools.calendario.bls import leer_ics
from tools.calendario.indec import leer_mes
from tools.calendario.modelo import Lectura, Publicacion, periodo_del_titulo

INDEC_OCTUBRE = """
<div>Octubre 2026</div>
<div>13</div><div>MARTES</div>
<p>&#205;ndice de precios al consumidor (IPC). Cobertura nacional. Septiembre de 2026</p><span>16:00hs</span>
<p>Valorizaci&#243;n mensual de la canasta b&#225;sica alimentaria (CBA). Septiembre de 2026</p><span>16:00hs</span>
<div>20</div><div>MARTES</div>
<p>Encuesta de tendencia de negocios. Supermercados y autoservicios mayoristas. Expectativas octubre-diciembre de 2026</p>
<div>22</div><div>JUEVES</div>
<p>Encuesta de supermercados. Agosto de 2026</p><span>16:00hs</span>
<script>var x = "1";</script>
"""

BCRA = """
<h2>Calendario Anticipado de Publicaciones del BCRA | 2026</h2>
<h3>Publicaciones diarias</h3><p><a>Tasas de inter&eacute;s pasivas</a>*</p>
<p>* La informaci&oacute;n se publica dentro de las 24 horas.</p>
<h3>Publicaciones semanales</h3><p><a>Estado resumido de activos y pasivos del BCRA</a></p>
<table><thead><tr><td>Fecha a la que se refiere la informaci&oacute;n</td><td>Fecha Publicaci&oacute;n</td></tr></thead>
<tr><td>23/10/2026</td><td>28/10/2026</td><td>31/10/2026</td><td>04/11/2026</td></tr></table>
<h3>Publicaciones mensuales</h3>
<p>Planilla de reservas internacionales y liquidez en moneda extranjera del BCRA</p>
<p><a>Bolet&iacute;n estad&iacute;stico del BCRA</a><sup>*</sup></p>
<table><tr><td>30/09/2026</td><td>23/10/2026</td></tr><tr><td>31/10/2026</td><td>20/11/2026</td></tr></table>
<table><tr><td>sept-26</td><td>13/11/2026</td><td>oct-26</td><td>14/12/2026 </td></tr></table>
<p>* Se hace uso de la opci&oacute;n de flexibilidad en materia de puntualidad.</p>
<h3>Referencias</h3><p>Resumen de Datos (INDEC)</p>
"""

BLS = (
    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART;TZID=US-Eastern:20261014T083000\r\n"
    "SUMMARY:Consumer Price Index\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nDTSTART;TZID=US-Eastern:20261110T08300\r\n"
    " 0\r\nSUMMARY:Consumer Price Index\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\n"
    "DTSTART;TZID=US-Eastern:20261015T100000\r\nSUMMARY:Producer Price Index\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
)


def _publicacion(titulo, fecha, periodo=None, codigos=(), institucion="INDEC", hora=time(16)):
    return Publicacion(institucion, titulo, fecha, hora, pd.Timestamp(periodo) if periodo else None, codigos, "x")


class CalendarioTests(unittest.TestCase):
    def test_periodo_mensual_y_trimestral(self):
        self.assertEqual(periodo_del_titulo("Índice de salarios. Agosto de 2026"), pd.Timestamp(2026, 8, 1))
        self.assertEqual(periodo_del_titulo("Informe de avance del nivel de actividad. Tercer trimestre de 2026"),
                         pd.Timestamp(2026, 7, 1))
        self.assertIsNone(periodo_del_titulo("INDEC Informa"))

    def test_indec_lee_todas_las_difusiones_con_hora_y_codigos(self):
        publicaciones = leer_mes(INDEC_OCTUBRE, 2026, 10)
        self.assertEqual(len(publicaciones), 4)
        ipc, canasta, expectativas, supermercados = publicaciones
        self.assertEqual((ipc.codigos, ipc.fecha, ipc.hora, ipc.periodo),
                         (("indec-ipc",), date(2026, 10, 13), time(16), pd.Timestamp(2026, 9, 1)))
        self.assertEqual(canasta.codigos, ())
        self.assertIsNone(expectativas.hora)
        self.assertEqual(expectativas.codigos, ())  # No es la encuesta de supermercados.
        self.assertEqual(supermercados.codigos, ("indec-supermercados",))

    def test_bcra_empareja_cada_tabla_con_su_titulo(self):
        publicaciones = leer_pagina(BCRA)
        titulos = [p.titulo for p in publicaciones]
        self.assertEqual(titulos, [
            "Estado resumido de activos y pasivos del BCRA. Datos al 23/10/2026",
            "Estado resumido de activos y pasivos del BCRA. Datos al 31/10/2026",
            "Planilla de reservas internacionales y liquidez en moneda extranjera del BCRA. Septiembre de 2026",
            "Planilla de reservas internacionales y liquidez en moneda extranjera del BCRA. Octubre de 2026",
            "Boletín estadístico del BCRA. Septiembre de 2026",
            "Boletín estadístico del BCRA. Octubre de 2026",
        ])
        planilla = publicaciones[2]
        self.assertEqual((planilla.fecha, planilla.periodo, planilla.codigos),
                         (date(2026, 10, 23), pd.Timestamp(2026, 9, 1), ("bcra-irfcl", "iiep-rin")))
        self.assertEqual(publicaciones[0].periodo, pd.Timestamp(2026, 10, 23))
        self.assertEqual(publicaciones[-1].fecha, date(2026, 12, 14))

    def test_bls_conserva_el_cpi_en_hora_argentina(self):
        publicaciones = leer_ics(BLS)
        self.assertEqual([(p.fecha, p.hora, p.periodo, p.codigos) for p in publicaciones], [
            (date(2026, 10, 14), time(9, 30), pd.Timestamp(2026, 9, 1), ("fred-cpiaucsl",)),
            (date(2026, 11, 10), time(10, 30), pd.Timestamp(2026, 10, 1), ("fred-cpiaucsl",)),
        ])

    def test_fusionar_reemplaza_solo_la_ventana_leida(self):
        vieja = _publicacion("Vieja", date(2026, 7, 10))
        reprogramada = _publicacion("IPC", date(2026, 11, 11))
        bcra = _publicacion("Planilla", date(2026, 11, 20), institucion="BCRA", hora=None)
        nueva = _publicacion("IPC", date(2026, 11, 12))
        lectura = Lectura("INDEC", "x", [nueva], date(2026, 8, 1), date(2027, 3, 1))
        self.assertEqual(fusionar([vieja, reprogramada, bcra], [lectura]), [vieja, nueva, bcra])

    def test_estado_por_calendario_y_por_regla(self):
        publicaciones = [
            _publicacion("IPC. Septiembre de 2026", date(2026, 10, 13), "2026-09-01", ("indec-ipc",)),
            _publicacion("IPC. Octubre de 2026", date(2026, 11, 12), "2026-10-01", ("indec-ipc",)),
        ]
        indice = pd.DataFrame({
            "Código fuente": ["indec-ipc", "indec-ipc", "bcra-itc", "ambito-dolar-blue", "datos.gob.ar"],
            "Frecuencia": ["M", "M", "D", "D", "M"],
            "Institución": ["INDEC", "INDEC", "BCRA", "Ámbito", "INDEC"],
            "Estado": ["VIGENTE", "DISCONTINUADA", "VIGENTE", "VIGENTE", "VIGENTE"],
            "Fecha fin": [datetime(2026, 8, 1), datetime(2026, 9, 1), datetime(2026, 9, 14),
                          datetime(2026, 10, 30), datetime(2020, 1, 1)],
        })
        filas = {f["codigo"]: f for f in estado(publicaciones, indice, datetime(2026, 11, 2, 11))}
        self.assertNotIn("datos.gob.ar", filas)
        self.assertEqual(filas["indec-ipc"]["situacion"], "falta")
        self.assertEqual(filas["indec-ipc"]["proxima"]["fecha"], "2026-11-12")
        self.assertEqual((filas["bcra-itc"]["metodo"], filas["bcra-itc"]["situacion"]), ("regla", "posible atraso"))
        self.assertEqual(filas["ambito-dolar-blue"]["situacion"], "al día")
        antes = {f["codigo"]: f for f in estado(publicaciones, indice, datetime(2026, 10, 13, 15))}
        self.assertEqual(antes["indec-ipc"]["metodo"], "regla")  # Todavía no salió ninguna.

    def test_ics_pliega_escapa_y_usa_utc(self):
        largo = _publicacion("Índice de precios al consumidor (IPC). Cobertura nacional; prueba, " * 2,
                             date(2026, 11, 12), "2026-10-01", ("indec-ipc",))
        sin_hora = _publicacion("Boletín", date(2026, 11, 13), institucion="BCRA", hora=None)
        texto = a_ics([largo, sin_hora], datetime(2026, 11, 2, 11))
        self.assertTrue(all(len(linea.encode("utf-8")) <= 75 for linea in texto.split("\r\n")))
        desplegado = texto.replace("\r\n ", "")
        self.assertIn("DTSTART:20261112T190000Z", desplegado)
        self.assertIn("DTSTART;VALUE=DATE:20261113", desplegado)
        self.assertIn("Cobertura nacional\\; prueba\\,", desplegado)
        self.assertIn("DTSTAMP:20261102T140000Z", desplegado)

    def test_actualizar_guarda_historia_y_sobrevive_a_un_calendario_caido(self):
        indice = pd.DataFrame({"Código fuente": ["indec-ipc"], "Frecuencia": ["M"], "Institución": ["INDEC"],
                               "Estado": ["VIGENTE"], "Fecha fin": [datetime(2026, 9, 1)]})
        publicacion = _publicacion("IPC. Septiembre de 2026", date(2026, 10, 13), "2026-09-01", ("indec-ipc",))

        def caido(_):
            raise ConnectionError("sin red")

        with TemporaryDirectory() as directorio:
            carpeta = Path(directorio)
            lectores = {"INDEC": lambda _: Lectura("INDEC", "x", [publicacion], date(2026, 8, 1), date(2027, 1, 1)),
                        "BCRA": caido}
            estados = actualizar(datetime(2026, 11, 2), carpeta, lectores, indice)
            self.assertEqual(estados[0]["situacion"], "al día")
            guardado = json.loads((carpeta / "publicaciones.json").read_text(encoding="utf-8"))
            self.assertIn("BCRA", guardado["errores"])
            # Sin red al día siguiente, la historia guardada se conserva.
            actualizar(datetime(2026, 11, 3), carpeta, {"INDEC": caido}, indice)
            guardado = json.loads((carpeta / "publicaciones.json").read_text(encoding="utf-8"))
            self.assertEqual(len(guardado["publicaciones"]), 1)
            self.assertIn("BEGIN:VEVENT", (carpeta / "publicaciones.ics").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
