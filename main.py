"""Punto de entrada de todos los scrapers del proyecto."""

from scrapers.scraper_IED import ejecutar as ejecutar_ied


def main() -> None:
    ejecutar_ied()


if __name__ == "__main__":
    main()
