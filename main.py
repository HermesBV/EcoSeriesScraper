"""Punto de entrada de todos los scrapers del proyecto."""

from __future__ import annotations

import importlib
import pkgutil

import scrapers


def descubrir_scrapers() -> list[str]:
    """Devuelve todos los módulos ``scraper_*.py`` del paquete scrapers."""
    return sorted(
        modulo.name
        for modulo in pkgutil.iter_modules(scrapers.__path__)
        if modulo.name.startswith("scraper_")
    )


def main() -> None:
    errores: list[tuple[str, Exception]] = []
    modulos = descubrir_scrapers()
    if not modulos:
        raise RuntimeError("No se encontraron módulos scraper_*.py en la carpeta scrapers")

    for nombre in modulos:
        nombre_completo = f"scrapers.{nombre}"
        print(f"\n=== Ejecutando {nombre_completo} ===", flush=True)
        try:
            modulo = importlib.import_module(nombre_completo)
            ejecutar = getattr(modulo, "ejecutar", None)
            if not callable(ejecutar):
                raise AttributeError(f"{nombre_completo} no define una función ejecutar()")
            ejecutar()
        except Exception as exc:
            errores.append((nombre_completo, exc))
            print(f"ERROR en {nombre_completo}: {exc}", flush=True)

    if errores:
        detalle = "; ".join(f"{nombre}: {error}" for nombre, error in errores)
        raise RuntimeError(f"Fallaron {len(errores)} de {len(modulos)} scrapers: {detalle}")


if __name__ == "__main__":
    main()
