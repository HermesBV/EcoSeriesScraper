"""Punto de entrada de todos los scrapers del proyecto."""

from __future__ import annotations

import argparse
import importlib
import pkgutil
import time

import scrapers
from tools import calendario, lote


def descubrir_scrapers() -> list[str]:
    """Devuelve todos los módulos ``scraper_*.py`` del paquete scrapers."""
    return sorted(
        modulo.name
        for modulo in pkgutil.iter_modules(scrapers.__path__)
        if modulo.name.startswith("scraper_") and modulo.name not in {
            "scraper_BCRA_comunicaciones", "scraper_MECON_hacienda",
        }
    )


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
    grupos = [modulos[i:i + tamano] for i in range(0, len(modulos), tamano)]
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
                    try:
                        ejecutar_modulo(nombre_completo, args.reintentos, args.espera)
                    except Exception as exc:
                        errores.append((nombre_completo, exc))
                        print(f"ERROR en {nombre_completo}: {exc}", flush=True)
                    print(f"--- {nombre_completo}: {time.time() - inicio:.0f} s", flush=True)
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
