"""Dólar MEP histórico publicado por Ámbito."""

from datetime import date

from scrapers.ambito_historical import Source, process

SOURCE = Source("ambito-dolar-mep", "DolarMEP", "Dólar MEP",
                "https://www.ambito.com/contenidos/dolar-mep-historico.html",
                ("dolarrava/mep",), ("Referencia",), "Ámbito Financiero",
                "pesos por dólar", "Tipo de cambio", date(2020, 1, 1))


def ejecutar() -> None:
    process(SOURCE)
