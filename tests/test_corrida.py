"""Retomar una corrida cortada, el candado y el tiempo máximo, con scrapers falsos."""

import json
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock

import main
from tools import corrida

FALSOS = ["scraper_a", "scraper_b", "scraper_c", "scraper_d", "scraper_e"]


class Corte(BaseException):
    """El coordinador muere (un reinicio) en medio de un lote."""


class LanzadorFalso:
    """Hace lo que ``correr_lote``: anota cada scraper y, al escribir el lote, saca ``_sin_escribir``."""

    def __init__(self, cortar_despues_de: str | None = None) -> None:
        self.cortar_despues_de = cortar_despues_de
        self.lanzados: list[list[str]] = []

    def __call__(self, grupo, ruta_estado, args=None) -> int:
        self.lanzados.append(list(grupo))
        estado = corrida.leer(ruta_estado)
        for indice, nombre in enumerate(grupo):
            estado[nombre] = "OK (1 s)"
            corrida.anotar(ruta_estado, {**estado, "_sin_escribir": ", ".join(grupo[:indice + 1])})
            if nombre == self.cortar_despues_de:
                raise Corte()
        corrida.anotar(ruta_estado, estado)
        return 0


def argumentos(**cambios) -> Namespace:
    base = dict(reintentos=0, espera=0, volcar_cada=2, sin_coyuntura=True, de_cero=False,
                retomar_horas=corrida.HORAS_RETOMAR, tiempo_max=1)
    return Namespace(**{**base, **cambios})


class RetomarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        carpeta = Path(self.tmp.name)
        for parche in (mock.patch.object(corrida, "CARPETA", carpeta / ".corrida"),
                       mock.patch("tools.lote.CARPETA_LOTE", carpeta / ".lote"),
                       mock.patch.object(main.corrida, "correr_con_tiempo", return_value=0)):
            parche.start()
            self.addCleanup(parche.stop)
        corrida.CARPETA.mkdir()
        self.ruta = corrida.CARPETA / "estado.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def correr(self, lanzador, **cambios) -> None:
        with mock.patch.object(main, "lanzar_lote", lanzador):
            main.correr_todo(FALSOS, argumentos(**cambios))

    def test_un_corte_a_mitad_de_lote_se_retoma_con_lo_que_falto(self) -> None:
        with self.assertRaises(Corte):
            self.correr(LanzadorFalso(cortar_despues_de="scraper_c"))
        cortada = corrida.leer(self.ruta)
        self.assertEqual(cortada["_sin_escribir"], "scraper_c")

        segunda = LanzadorFalso()
        self.correr(segunda)
        # a y b ya estaban escritos; c corrió pero su lote no llegó a la base.
        self.assertEqual(segunda.lanzados, [["scraper_c", "scraper_d"], ["scraper_e"]])
        ultima = json.loads((corrida.CARPETA / "ultima.json").read_text(encoding="utf-8"))
        self.assertTrue(all(ultima[n].startswith("OK") for n in FALSOS))
        self.assertEqual(ultima["Inicio"], cortada["Inicio"])
        self.assertFalse(self.ruta.exists())

    def test_de_cero_corre_todo(self) -> None:
        with self.assertRaises(Corte):
            self.correr(LanzadorFalso(cortar_despues_de="scraper_c"))
        segunda = LanzadorFalso()
        self.correr(segunda, de_cero=True)
        self.assertEqual([n for grupo in segunda.lanzados for n in grupo], FALSOS)

    def test_una_corrida_terminada_no_se_retoma(self) -> None:
        self.correr(LanzadorFalso())
        segunda = LanzadorFalso()
        self.correr(segunda)
        self.assertEqual([n for grupo in segunda.lanzados for n in grupo], FALSOS)

    def test_una_corrida_cortada_hace_mucho_no_se_retoma(self) -> None:
        corrida.anotar(self.ruta, {"Inicio": "2026-01-01 03:00:00", "scraper_a": "OK (1 s)"})
        self.assertEqual(corrida.retomable(self.ruta, 20), {})
        self.assertEqual(corrida.retomable(self.ruta, 10 ** 6)["scraper_a"], "OK (1 s)")


class TiempoMaximoTests(unittest.TestCase):
    def test_corta_el_lote_y_sus_hijos(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            nieto = Path(carpeta) / "nieto.pid"
            codigo = ("import subprocess, sys, time\n"
                      "hijo = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                      f"open({str(nieto)!r}, 'w').write(str(hijo.pid))\n"
                      "time.sleep(60)\n")
            inicio = time.time()
            resultado = corrida.correr_con_tiempo([sys.executable, "-c", codigo], 2, Path(carpeta))
            self.assertEqual(resultado, corrida.AGOTADO)
            self.assertLess(time.time() - inicio, 20)
            pid = int(nieto.read_text())
            for _ in range(50):
                if not corrida.vive(pid) or _zombi(pid):
                    break
                time.sleep(0.1)
            self.assertTrue(not corrida.vive(pid) or _zombi(pid))

    def test_un_lote_que_se_pasa_se_repite_de_a_uno(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta) / "estado.json"
            corrida.anotar(ruta, {"Inicio": time.strftime(corrida.FORMATO_HORA)})
            lanzados = []

            def lanzar(grupo, ruta_estado):
                lanzados.append(list(grupo))
                if "scraper_b" in grupo:
                    return corrida.AGOTADO
                return LanzadorFalso()(grupo, ruta_estado)

            estado = main.correr_lotes([["scraper_a", "scraper_b"], ["scraper_c"]], ruta, lanzar)
            self.assertEqual(lanzados, [["scraper_a", "scraper_b"], ["scraper_a"], ["scraper_b"], ["scraper_c"]])
            self.assertTrue(estado["scraper_a"].startswith("OK"))
            self.assertIn("tiempo máximo", estado["scraper_b"])


def _zombi(pid: int) -> bool:
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] == "Z"
    except OSError:
        return False


class CandadoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.carpeta = Path(self.tmp.name)
        self.ruta = self.carpeta / "candado"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def dejar(self, **datos) -> None:
        base = {"maquina": socket.gethostname(), "desde": "2026-11-26 03:00:00",
                "sistema": corrida._arranque_sistema(), "arranque": None}
        self.ruta.write_text(json.dumps({**base, **datos}), encoding="utf-8")

    def test_toma_y_suelta(self) -> None:
        ruta = corrida.tomar_candado(self.carpeta)
        with self.assertRaises(corrida.CorridaEnCurso):
            corrida.tomar_candado(self.carpeta)
        corrida.soltar_candado(ruta)
        self.assertFalse(ruta.exists())

    def test_un_candado_de_un_proceso_muerto_se_libera(self) -> None:
        muerto = subprocess.Popen([sys.executable, "-c", "pass"])
        muerto.wait()
        self.dejar(pid=muerto.pid)
        ruta = corrida.tomar_candado(self.carpeta)
        self.assertEqual(json.loads(ruta.read_text())["pid"], __import__("os").getpid())

    def test_un_candado_de_antes_de_reiniciar_se_libera(self) -> None:
        self.dejar(pid=__import__("os").getpid(), sistema="otro-arranque")
        corrida.tomar_candado(self.carpeta)

    def test_un_candado_vivo_se_respeta_aunque_viva_solo_su_lote(self) -> None:
        vivo = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        self.addCleanup(vivo.kill)
        muerto = subprocess.Popen([sys.executable, "-c", "pass"])
        muerto.wait()
        for datos in ({"pid": vivo.pid}, {"pid": muerto.pid, "lote": vivo.pid}):
            self.dejar(**datos)
            with self.assertRaises(corrida.CorridaEnCurso):
                corrida.tomar_candado(self.carpeta)


class MainOcupadoTests(unittest.TestCase):
    def test_con_otra_corrida_en_curso_sale_con_75_sin_correr(self) -> None:
        with tempfile.TemporaryDirectory() as carpeta, \
                mock.patch.object(corrida, "CARPETA", Path(carpeta)), \
                mock.patch.object(main, "correr_todo") as correr_todo:
            ruta = corrida.tomar_candado(Path(carpeta))
            with self.assertRaises(SystemExit) as salida:
                main.main([])
            self.assertEqual(salida.exception.code, main.SALIDA_OCUPADO)
            correr_todo.assert_not_called()
            self.assertTrue(ruta.exists())


if __name__ == "__main__":
    unittest.main()
