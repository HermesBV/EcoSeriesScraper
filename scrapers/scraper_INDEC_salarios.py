"""Índice de salarios."""

from scrapers.periodic_workbooks import Source, process

SOURCE = Source("indec-salarios", "INDEC", "Salarios", "INDEC / Sociedad / Trabajo e ingresos / Salarios",
                "https://www.indec.gob.ar/ftp/cuadros/sociedad/variaciones_salarios_{MM}_{AA}.xls", "M", "INDEC Salarios")


def ejecutar() -> None:
    process(SOURCE)
