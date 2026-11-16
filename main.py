"""Punto de entrada de todos los scrapers del proyecto."""

from __future__ import annotations

import argparse
import contextlib
import importlib
import io
import json
import pkgutil
import platform
import subprocess
import sys
import tempfile
import time
from datetime import date
from pathlib import Path

import scrapers


# Los más pesados van al final y en su propio lote: si el proceso muere ahí (memoria,
# tiempo), lo que corrió antes ya quedó escrito. El IED carga ocho libros y 500 hojas.
AL_FINAL = ("scraper_IED",)
HOJA_ESTADO = "Estado corrida"
PAQUETES = ("numpy", "openpyxl", "pandas", "pypdf", "requests", "certifi", "tqdm", "xlrd")


def descubrir_scrapers() -> list[str]:
    """Devuelve todos los módulos ``scraper_*.py`` del paquete scrapers, los pesados al final."""
    nombres = sorted(
        modulo.name
        for modulo in pkgutil.iter_modules(scrapers.__path__)
        if modulo.name.startswith("scraper_") and modulo.name not in {
            "scraper_BCRA_comunicaciones", "scraper_MECON_hacienda",
        }
    )
    return [n for n in nombres if n not in AL_FINAL] + [n for n in AL_FINAL if n in nombres]


def agrupar(modulos: list[str], tamano: int) -> list[list[str]]:
    """Lotes de ``tamano`` scrapers; cada uno de ``AL_FINAL`` va solo en el suyo."""
    livianos = [n for n in modulos if n not in AL_FINAL]
    grupos = [livianos[i:i + tamano] for i in range(0, len(livianos), tamano)]
    return grupos + [[n] for n in modulos if n in AL_FINAL]


def paquetes_faltantes() -> list[str]:
    # Sin importarlos: este proceso sólo coordina y tiene que ocupar poca memoria.
    from importlib.util import find_spec
    return [nombre for nombre in PAQUETES if find_spec(nombre) is None]


def version_codigo() -> str:
    """Commit en uso, leído de ``.git`` sin depender de tener git instalado."""
    try:
        raiz = Path(__file__).resolve().parent / ".git"
        cabeza = (raiz / "HEAD").read_text(encoding="utf-8").strip()
        if cabeza.startswith("ref:"):
            referencia = raiz / cabeza.split(" ", 1)[1]
            if referencia.is_file():
                cabeza = referencia.read_text(encoding="utf-8").strip()
            else:
                empaquetadas = (raiz / "packed-refs").read_text(encoding="utf-8")
                cabeza = next(l.split()[0] for l in empaquetadas.splitlines() if l.endswith(cabeza.split(" ", 1)[1]))
        return cabeza[:7]
    except Exception:
        return "desconocida"


class _Copia(io.TextIOBase):
    """Deja pasar lo que imprime un scraper y guarda sus líneas de aviso."""

    MARCAS = ("AVISO", "se reutiliza", "se usa el guardado", "Reintento")

    def __init__(self, salida) -> None:
        self.salida, self.avisos, self._resto = salida, [], ""

    def write(self, texto: str) -> int:
        self.salida.write(texto)
        *lineas, self._resto = (self._resto + texto).split("\n")
        self.avisos += [l.strip() for l in lineas if any(m in l for m in self.MARCAS)]
        return len(texto)

    def flush(self) -> None:
        self.salida.flush()


def guardar_estado(estado: dict[str, str]) -> None:
    """Una fila por día en la hoja «Estado corrida» de BD.xlsx, con el resultado de cada scraper.

    Viaja con la base hasta la web: bajando «base completa» se ve qué pasó en el
    servidor sin entrar a él. No tiene series en el índice, así que no se grafica.
    """
    import pandas as pd
    from scrapers.scraper_IED import ARCHIVO_BD, guardar_datos_preservando_formato
    fila = {"fecha": pd.Timestamp(date.today()), **estado}
    guardar_datos_preservando_formato(ARCHIVO_BD, {HOJA_ESTADO: pd.DataFrame([fila])}, {HOJA_ESTADO: "D"})


def ejecutar_modulo(nombre_completo: str, reintentos: int, espera: float) -> None:
    """Corre ``ejecutar()`` del módulo; ante un error reintenta tras una pausa."""
    for intento in range(reintentos + 1):
        try:
            modulo = importlib.import_module(nombre_completo)
            ejecutar = getattr(modulo, "ejecutar", None)
            if not callable(ejecutar):
                raise AttributeError(f"{nombre_completo} no define una función ejecutar()")
            ejecutar()
            return
        except Exception as exc:
            if intento == reintentos:
                raise
            print(f"Reintento {intento + 1} de {reintentos} para {nombre_completo} en {espera:.0f} s: {exc}", flush=True)
            time.sleep(espera)


def _leer_estado(ruta: Path) -> dict[str, str]:
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _anotar(ruta: Path, estado: dict[str, str]) -> None:
    temporal = ruta.with_name(ruta.name + ".tmp")
    temporal.write_text(json.dumps(estado, ensure_ascii=False), encoding="utf-8")
    temporal.replace(ruta)


def correr_lote(nombres: list[str], ruta_estado: Path, reintentos: int, espera: float) -> None:
    """Un lote completo en este proceso: corre sus scrapers y escribe la base una vez.

    El resultado de cada scraper se anota en ``ruta_estado`` apenas termina: si el
    proceso muere, quien lo lanzó sabe hasta dónde llegó.
    """
    from tools import lote
    estado = _leer_estado(ruta_estado)
    with lote.abrir():
        for nombre in nombres:
            nombre_completo = f"scrapers.{nombre}"
            print(f"\n=== Ejecutando {nombre_completo} ({time.strftime('%H:%M:%S')}) ===", flush=True)
            inicio = time.time()
            copia = _Copia(sys.stdout)
            try:
                with contextlib.redirect_stdout(copia):
                    ejecutar_modulo(nombre_completo, reintentos, espera)
                resultado = f"OK ({time.time() - inicio:.0f} s)"
            except Exception as exc:
                print(f"ERROR en {nombre_completo}: {exc}", flush=True)
                resultado = f"ERROR ({time.time() - inicio:.0f} s): {type(exc).__name__}: {exc}"
            if copia.avisos:
                resultado += " · " + " | ".join(dict.fromkeys(copia.avisos))
            estado[nombre] = resultado[:900]
            _anotar(ruta_estado, {**estado, "_sin_escribir": ", ".join(nombres[:nombres.index(nombre) + 1])})
            print(f"--- {nombre_completo}: {time.time() - inicio:.0f} s", flush=True)
        estado["Último lote escrito"] = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            guardar_estado({clave: valor for clave, valor in estado.items() if not clave.startswith("_")})
        except Exception as exc:
            print(f"AVISO: no se pudo anotar el estado de la corrida: {exc}", flush=True)
    # Sólo se llega acá si el lote se escribió.
    _anotar(ruta_estado, estado)


def lanzar_lote(nombres: list[str], ruta_estado: Path, args) -> int:
    """Corre el lote en un proceso aparte y devuelve su código de salida.

    Cada lote libera su memoria al terminar y, si el sistema lo mata (falta de
    memoria), la corrida sigue con el lote siguiente.
    """
    orden = [sys.executable, str(Path(__file__).resolve()), "--estado", str(ruta_estado),
             "--reintentos", str(args.reintentos), "--espera", str(args.espera), "--lote", *nombres]
    return subprocess.run(orden, cwd=Path(__file__).resolve().parent).returncode


def actualizar_coyuntura() -> int | None:
    """En un servidor Linux con LibreOffice, deja al día el Excel de coyuntura.

    En Windows sigue siendo un paso aparte (``python -m coyuntura.actualizador``),
    porque abre Excel en la máquina de quien lo corre. Devuelve el código de salida
    o ``None`` si se omitió.
    """
    import shutil
    if sys.platform == "win32":
        return None
    if not (shutil.which("soffice") or shutil.which("libreoffice")):
        print("\nExcel de coyuntura: se omite (falta LibreOffice: apt install libreoffice-calc python3-uno)", flush=True)
        return None
    print(f"\n=== Excel de coyuntura ({time.strftime('%H:%M:%S')}) ===", flush=True)
    return subprocess.run([sys.executable, "-m", "coyuntura.actualizador"], cwd=Path(__file__).resolve().parent).returncode


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ejecuta todos los scrapers y publica BD.xlsx e IndiceSeries.xlsx")
    parser.add_argument("--reintentos", type=int, default=2, help="reintentos por scraper ante un error (2)")
    parser.add_argument("--espera", type=float, default=60.0, help="segundos entre reintentos (60)")
    parser.add_argument("--volcar-cada", type=int, default=3,
                        help="scrapers por lote: cada lote corre en un proceso propio y escribe la base "
                             "al terminar (3). Lotes chicos: más escrituras, menos que perder si uno muere")
    parser.add_argument("--sin-coyuntura", action="store_true",
                        help="no actualizar el Excel de coyuntura al terminar (en Linux, con LibreOffice)")
    parser.add_argument("--lote", nargs="*", metavar="SCRAPER", help=argparse.SUPPRESS)   # uso interno
    parser.add_argument("--estado", type=Path, help=argparse.SUPPRESS)                    # uso interno
    args = parser.parse_args(argv)
    if args.lote is not None:
        correr_lote(args.lote, args.estado, args.reintentos, args.espera)
        return

    modulos = descubrir_scrapers()
    if not modulos:
        raise RuntimeError("No se encontraron módulos scraper_*.py en la carpeta scrapers")
    inicio_total = time.time()
    grupos = agrupar(modulos, max(1, args.volcar_cada))
    faltan = paquetes_faltantes()
    if faltan:
        print(f"AVISO: faltan paquetes de Python ({', '.join(faltan)}); los scrapers que los usan van a fallar. "
              "Instalar con: pip install -r requirements.txt", flush=True)
    estado = {
        "Inicio": time.strftime("%Y-%m-%d %H:%M:%S"),
        "Código": version_codigo(),
        "Python": f"{platform.python_version()} en {sys.platform}",
        "Paquetes faltantes": ", ".join(faltan) or "ninguno",
        "Último lote escrito": "",
    }
    with tempfile.TemporaryDirectory(prefix="corrida-") as carpeta:
        ruta_estado = Path(carpeta) / "estado.json"
        _anotar(ruta_estado, estado)
        pendientes = list(grupos)
        sin_anotar = False   # hay novedades que todavía no llegaron a la hoja de estado
        while pendientes:
            grupo = pendientes.pop(0)
            print(f"\n##### Lote: {', '.join(grupo)}", flush=True)
            codigo = lanzar_lote(grupo, ruta_estado, args)
            estado = _leer_estado(ruta_estado)
            if codigo == 0 and "_sin_escribir" not in estado:
                sin_anotar = False
                continue
            # El proceso murió o el volcado falló: nada de este lote llegó a la base.
            motivo = (f"el proceso del lote murió (señal {-codigo}; si es 9, falta de memoria)" if codigo < 0
                      else f"falló la escritura del lote (código {codigo})")
            print(f"ERROR: {motivo}: {', '.join(grupo)}", flush=True)
            estado.pop("_sin_escribir", None)
            if len(grupo) > 1:
                # De a uno, para perder sólo el que causa el problema.
                for nombre in grupo:
                    estado.pop(nombre, None)
                pendientes = [[nombre] for nombre in grupo] + pendientes
            else:
                previo = estado.get(grupo[0], "")
                estado[grupo[0]] = (f"NO SE GUARDÓ: {motivo}" + (f" · antes: {previo}" if previo else ""))[:900]
                sin_anotar = True
            _anotar(ruta_estado, estado)
        if sin_anotar:
            lanzar_lote([], ruta_estado, args)   # un lote vacío: sólo deja la hoja de estado al día
        estado = _leer_estado(ruta_estado)
    print(f"\nTiempo total: {(time.time() - inicio_total) / 60:.1f} min", flush=True)

    errores = [f"{nombre}: {estado[nombre]}" for nombre in modulos
               if not str(estado.get(nombre, "sin resultado")).startswith("OK")]
    # Un libro viejo no produce error: sólo el calendario muestra el atraso.
    if subprocess.run([sys.executable, "-m", "tools.calendario"], cwd=Path(__file__).resolve().parent).returncode:
        print("AVISO: no se pudo actualizar el calendario de publicaciones", flush=True)
    if not args.sin_coyuntura:
        codigo = actualizar_coyuntura()
        if codigo:
            errores.append(f"Excel de coyuntura: el actualizador terminó con código {codigo}; el Excel vigente no se modificó")

    if errores:
        raise RuntimeError(f"Fallaron {len(errores)} de {len(modulos)} scrapers: " + "; ".join(errores))


if __name__ == "__main__":
    main()
