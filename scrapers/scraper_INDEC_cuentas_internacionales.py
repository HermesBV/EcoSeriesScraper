"""Balanza de pagos, posición de inversión internacional y deuda externa."""

from scrapers.periodic_workbooks import Source, process

SOURCE = Source("indec-cin", "INDEC", "CuentasInternacionales",
                "INDEC / Economía / Cuentas internacionales",
                "https://www.indec.gob.ar/ftp/cuadros/economia/cin_{T}_{AAAA}.xls", "T", "INDEC CIN")


def ejecutar() -> None:
    process(SOURCE)
