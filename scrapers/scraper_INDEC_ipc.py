"""Índice de precios al consumidor."""

from scrapers.periodic_workbooks import Source, process

SOURCE = Source("indec-ipc", "INDEC", "IPC", "INDEC / Economía / Precios / IPC",
                "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_{MM}_{AA}.xls", "M", "INDEC IPC")


def ejecutar() -> None:
    process(SOURCE)
