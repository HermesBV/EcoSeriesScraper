"""Agregados macroeconómicos, oferta y demanda."""

from scrapers.periodic_workbooks import Source, process

SOURCE = Source("indec-pib", "INDEC", "PIB", "INDEC / Economía / Cuentas nacionales / PIB",
                "https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_{MM}_{AA}.xls", "M", "INDEC PIB")


def ejecutar() -> None:
    process(SOURCE)
