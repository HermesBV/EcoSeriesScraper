"""Estadísticas mensuales de trabajo registrado (SIPA)."""

from scrapers.periodic_workbooks import Source, process

SOURCE = Source("mch-sipa", "MCH", "SIPA", "MCH / Trabajo / SIPA",
                "https://www.argentina.gob.ar/sites/default/files/trabajoregistrado_{AA}{MM}_estadisticas.xlsx",
                "M", "MCH SIPA", "Ministerio de Capital Humano")


def ejecutar() -> None:
    process(SOURCE)
