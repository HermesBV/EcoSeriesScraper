"""Índice de precios al consumidor.

Desde la publicación de octubre de 2026 el INDEC dejó de subir el libro con
fecha (``sh_ipc_MM_AA.xls``, nombrado por mes de publicación) y actualiza en
el lugar ``sh_ipc_divisiones.xls``, que tiene las mismas hojas.
"""

from scrapers.periodic_workbooks import Source, process

SOURCE = Source("indec-ipc", "INDEC", "IPC", "INDEC / Economía / Precios / IPC",
                "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_divisiones.xls", "M", "INDEC IPC")


def ejecutar() -> None:
    process(SOURCE)
