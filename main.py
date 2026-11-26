"""Punto de entrada de todos los scrapers del proyecto."""

from __future__ import annotations

import argparse
import contextlib
import importlib
import io
import pkgutil
import platform
import sys
import time
from datetime import date
from pathlib import Path

import scrapers
from tools import corrida


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


_leer_estado = corrida.leer
_anotar = corrida.anotar


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


def tiempo_lote(nombres: list[str], args) -> float:
    """Segundos que puede durar un lote: ``--tiempo-max`` minutos por scraper, más el volcado."""
    return (args.tiempo_max * max(1, len(nombres)) + 15) * 60


def lanzar_lote(nombres: list[str], ruta_estado: Path, args) -> int:
    """Corre el lote en un proceso aparte y devuelve su código de salida.

    Cada lote libera su memoria al terminar y, si el sistema lo mata (falta de
    memoria) o pasa su tiempo máximo, la corrida sigue con el lote siguiente.
    """
    orden = [sys.executable, str(Path(__file__).resolve()), "--estado", str(ruta_estado),
             "--reintentos", str(args.reintentos), "--espera", str(args.espera), "--lote", *nombres]
    return corrida.correr_con_tiempo(orden, tiempo_lote(nombres, args), Path(__file__).resolve().parent,
                                     candado=corrida.CARPETA / "candado")


def correr_lotes(grupos: list[list[str]], ruta_estado: Path, lanzar) -> dict[str, str]:
    """Corre los lotes con ``lanzar(grupo, ruta_estado) -> código`` y devuelve el estado final.

    Un lote que muere o pasa su tiempo se repite de a un scraper, para perder
    sólo el que causa el problema.
    """
    pendientes = list(grupos)
    sin_anotar = False   # hay novedades que todavía no llegaron a la hoja de estado
    while pendientes:
        grupo = pendientes.pop(0)
        print(f"\n##### Lote: {', '.join(grupo)}", flush=True)
        codigo = lanzar(grupo, ruta_estado)
        estado = _leer_estado(ruta_estado)
        if codigo == 0 and "_sin_escribir" not in estado:
            sin_anotar = False
            continue
        # El proceso murió o el volcado falló: nada de este lote llegó a la base.
        if codigo == corrida.AGOTADO:
            motivo = "el lote pasó su tiempo máximo (--tiempo-max) y se cortó"
        elif codigo < 0:
            motivo = f"el proceso del lote murió (señal {-codigo}; si es 9, falta de memoria)"
        else:
            motivo = f"falló la escritura del lote (código {codigo})"
        print(f"ERROR: {motivo}: {', '.join(grupo)}", flush=True)
        estado.pop("_sin_escribir", None)
        if len(grupo) > 1:
            for nombre in grupo:
                estado.pop(nombre, None)
            pendientes = [[nombre] for nombre in grupo] + pendientes
        else:
            previo = estado.get(grupo[0], "")
            estado[grupo[0]] = (f"NO SE GUARDÓ: {motivo}" + (f" · antes: {previo}" if previo else ""))[:900]
            sin_anotar = True
        _anotar(ruta_estado, estado)
    if sin_anotar:
        lanzar([], ruta_estado)   # un lote vacío: sólo deja la hoja de estado al día
    return _leer_estado(ruta_estado)


def actualizar_coyuntura(tiempo: float | None = None) -> int | None:
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
    codigo = corrida.correr_con_tiempo([sys.executable, "-m", "coyuntura.actualizador"], tiempo, Path(__file__).resolve().parent)
    if codigo == corrida.AGOTADO:
        print(f"ERROR: el Excel de coyuntura pasó su tiempo máximo ({tiempo / 60:.0f} min) y se cortó", flush=True)
    return codigo


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ejecuta todos los scrapers y publica BD.xlsx e IndiceSeries.xlsx")
    parser.add_argument("--reintentos", type=int, default=2, help="reintentos por scraper ante un error (2)")
    parser.add_argument("--espera", type=float, default=60.0, help="segundos entre reintentos (60)")
    parser.add_argument("--volcar-cada", type=int, default=3,
                        help="scrapers por lote: cada lote corre en un proceso propio y escribe la base "
                             "al terminar (3). Lotes chicos: más escrituras, menos que perder si uno muere")
    parser.add_argument("--sin-coyuntura", action="store_true",
                        help="no actualizar el Excel de coyuntura al terminar (en Linux, con LibreOffice)")
    parser.add_argument("--de-cero", action="store_true",
                        help="no retomar una corrida cortada: correr todos los scrapers")
    parser.add_argument("--retomar-horas", type=float, default=corrida.HORAS_RETOMAR,
                        help=f"retomar una corrida cortada si empezó hace menos de estas horas ({corrida.HORAS_RETOMAR})")
    parser.add_argument("--tiempo-max", type=float, default=60.0,
                        help="minutos por scraper antes de cortar su lote (60); el lote suma 15 para escribir "
                             "la base. También es el tope del Excel de coyuntura")
    parser.add_argument("--lote", nargs="*", metavar="SCRAPER", help=argparse.SUPPRESS)   # uso interno
    parser.add_argument("--estado", type=Path, help=argparse.SUPPRESS)                    # uso interno
    args = parser.parse_args(argv)
    if args.lote is not None:
        correr_lote(args.lote, args.estado, args.reintentos, args.espera)
        return

    modulos = descubrir_scrapers()
    if not modulos:
        raise RuntimeError("No se encontraron módulos scraper_*.py en la carpeta scrapers")
    try:
        candado = corrida.tomar_candado()
    except corrida.CorridaEnCurso as exc:
        print(f"AVISO: {exc}; no se arranca otra.", flush=True)
        raise SystemExit(SALIDA_OCUPADO)
    try:
        correr_todo(modulos, args)
    finally:
        corrida.soltar_candado(candado)


SALIDA_OCUPADO = 75   # otra corrida tiene el candado (EX_TEMPFAIL); corrida_diaria.sh no copia nada


def correr_todo(modulos: list[str], args) -> None:
    from tools.lote import CARPETA_LOTE
    corrida.limpiar_lotes_viejos(CARPETA_LOTE)
    inicio_total = time.time()
    faltan = paquetes_faltantes()
    if faltan:
        print(f"AVISO: faltan paquetes de Python ({', '.join(faltan)}); los scrapers que los usan van a fallar. "
              "Instalar con: pip install -r requirements.txt", flush=True)
    ruta_estado = corrida.CARPETA / "estado.json"
    estado = {} if args.de_cero else corrida.retomable(ruta_estado, args.retomar_horas)
    hechos = [nombre for nombre in modulos if nombre in estado]
    if estado:
        print(f"Retomando la corrida del {estado['Inicio']}: ya corrieron {len(hechos)} de {len(modulos)} scrapers "
              "(--de-cero para empezar de nuevo)", flush=True)
    else:
        estado = {
            "Inicio": time.strftime(corrida.FORMATO_HORA),
            "Código": version_codigo(),
            "Python": f"{platform.python_version()} en {sys.platform}",
            "Paquetes faltantes": ", ".join(faltan) or "ninguno",
            "Último lote escrito": "",
        }
    _anotar(ruta_estado, estado)
    grupos = agrupar([nombre for nombre in modulos if nombre not in hechos], max(1, args.volcar_cada))
    estado = correr_lotes(grupos, ruta_estado, lambda grupo, ruta: lanzar_lote(grupo, ruta, args))
    print(f"\nTiempo total: {(time.time() - inicio_total) / 60:.1f} min", flush=True)

    errores = [f"{nombre}: {estado[nombre]}" for nombre in modulos
               if not str(estado.get(nombre, "sin resultado")).startswith("OK")]
    # Un libro viejo no produce error: sólo el calendario muestra el atraso.
    if corrida.correr_con_tiempo([sys.executable, "-m", "tools.calendario"], args.tiempo_max * 60,
                                 Path(__file__).resolve().parent):
        print("AVISO: no se pudo actualizar el calendario de publicaciones", flush=True)
    if not args.sin_coyuntura:
        codigo = actualizar_coyuntura(args.tiempo_max * 60)
        if codigo:
            errores.append(f"Excel de coyuntura: el actualizador terminó con código {codigo}; el Excel vigente no se modificó")
    corrida.cerrar(ruta_estado)

    if errores:
        raise RuntimeError(f"Fallaron {len(errores)} de {len(modulos)} scrapers: " + "; ".join(errores))


if __name__ == "__main__":
    main()
