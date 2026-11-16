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
from tools import calendario, lote


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
    faltan = []
    for nombre in PAQUETES:
        try:
            importlib.import_module(nombre)
        except Exception:
            faltan.append(nombre)
    return faltan


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


def actualizar_coyuntura() -> None:
    """En un servidor Linux con LibreOffice, deja al día el Excel de coyuntura.

    En Windows sigue siendo un paso aparte (``python -m coyuntura.actualizador``),
    porque abre Excel en la máquina de quien lo corre.
    """
    import sys
    if sys.platform == "win32":
        return
    from coyuntura.actualizador import libreoffice
    if not libreoffice.disponible():
        print("\nExcel de coyuntura: se omite (falta LibreOffice: apt install libreoffice-calc python3-uno)", flush=True)
        return
    from coyuntura.actualizador import actualizar
    print(f"\n=== Excel de coyuntura ({time.strftime('%H:%M:%S')}) ===", flush=True)
    codigo = actualizar.main([])
    if codigo:
        raise RuntimeError(f"el actualizador terminó con código {codigo}; el Excel vigente no se modificó")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ejecuta todos los scrapers y publica BD.xlsx e IndiceSeries.xlsx")
    parser.add_argument("--reintentos", type=int, default=2, help="reintentos por scraper ante un error (2)")
    parser.add_argument("--espera", type=float, default=60.0, help="segundos entre reintentos (60)")
    parser.add_argument("--volcar-cada", type=int, default=10,
                        help="escribir la base cada N scrapers en vez de una sola vez al final; "
                             "acota la memoria en el servidor (10; 0 = un solo volcado)")
    parser.add_argument("--sin-coyuntura", action="store_true",
                        help="no actualizar el Excel de coyuntura al terminar (en Linux, con LibreOffice)")
    args = parser.parse_args(argv)
    errores: list[tuple[str, Exception]] = []
    modulos = descubrir_scrapers()
    if not modulos:
        raise RuntimeError("No se encontraron módulos scraper_*.py en la carpeta scrapers")

    inicio_total = time.time()
    tamano = args.volcar_cada if args.volcar_cada > 0 else len(modulos)
    grupos = agrupar(modulos, tamano)
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
    # Cada grupo es un lote: los scrapers acumulan sus hojas y BD.xlsx e
    # IndiceSeries.xlsx se escriben una vez por grupo, también si uno falló.
    for grupo in grupos:
        # Si falla el volcado (BD.xlsx bloqueado, validación) se pierde ese
        # grupo, pero los siguientes corren y escriben sobre la base en disco.
        try:
            with lote.abrir():
                for nombre in grupo:
                    nombre_completo = f"scrapers.{nombre}"
                    print(f"\n=== Ejecutando {nombre_completo} ({time.strftime('%H:%M:%S')}) ===", flush=True)
                    inicio = time.time()
                    copia = _Copia(sys.stdout)
                    try:
                        with contextlib.redirect_stdout(copia):
                            ejecutar_modulo(nombre_completo, args.reintentos, args.espera)
                        resultado = f"OK ({time.time() - inicio:.0f} s)"
                    except Exception as exc:
                        errores.append((nombre_completo, exc))
                        print(f"ERROR en {nombre_completo}: {exc}", flush=True)
                        resultado = f"ERROR ({time.time() - inicio:.0f} s): {type(exc).__name__}: {exc}"
                    if copia.avisos:
                        resultado += " · " + " | ".join(dict.fromkeys(copia.avisos))
                    estado[nombre] = resultado[:900]
                    print(f"--- {nombre_completo}: {time.time() - inicio:.0f} s", flush=True)
                # El estado entra en el mismo lote: si el proceso muere más adelante, en la
                # base queda hasta dónde llegó.
                estado["Último lote escrito"] = f"{grupos.index(grupo) + 1} de {len(grupos)} ({time.strftime('%H:%M:%S')})"
                try:
                    guardar_estado(estado)
                except Exception as exc:
                    print(f"AVISO: no se pudo anotar el estado de la corrida: {exc}", flush=True)
        except Exception as exc:
            errores.append((f"volcado de {', '.join(grupo)}", exc))
            print(f"ERROR al volcar el lote: {exc}", flush=True)
    print(f"\nTiempo total: {(time.time() - inicio_total) / 60:.1f} min", flush=True)
    # Un libro viejo no produce error: sólo el calendario muestra el atraso.
    try:
        calendario.actualizar()
    except Exception as exc:
        print(f"AVISO: no se pudo actualizar el calendario de publicaciones: {exc}", flush=True)

    if not args.sin_coyuntura:
        try:
            actualizar_coyuntura()
        except Exception as exc:
            errores.append(("Excel de coyuntura", exc))
            print(f"ERROR al actualizar el Excel de coyuntura: {exc}", flush=True)

    if errores:
        detalle = "; ".join(f"{nombre}: {error}" for nombre, error in errores)
        raise RuntimeError(f"Fallaron {len(errores)} de {len(modulos)} scrapers: {detalle}")


if __name__ == "__main__":
    main()
