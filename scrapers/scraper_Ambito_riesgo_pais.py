"""Riesgo país EMBI publicado por Ámbito."""

from scrapers.ambito_historical import Source, process

SOURCE = Source("ambito-riesgo-pais", "RiesgoPais", "Riesgo país (EMBI)",
                "https://www.ambito.com/contenidos/riesgo-pais-historico.html",
                ("riesgopais",), ("Puntos",), "JP Morgan Chase",
                "puntos básicos", "Riesgo país")


def ejecutar() -> None:
    process(SOURCE)
