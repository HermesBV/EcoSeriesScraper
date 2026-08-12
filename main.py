"""Punto de entrada de todos los scrapers del proyecto."""

from scrapers.scraper_IED import ejecutar as ejecutar_ied
from scrapers.scraper_BCRA_comunicaciones import ejecutar as ejecutar_bcra_comunicaciones


def main() -> None:
    ejecutar_ied()
    ejecutar_bcra_comunicaciones()


if __name__ == "__main__":
    main()
