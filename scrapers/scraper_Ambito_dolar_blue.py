"""Dólar blue histórico publicado por Ámbito."""

from scrapers.ambito_historical import Source, process

SOURCE = Source("ambito-dolar-blue", "DolarBlue", "Dólar blue",
                "https://www.ambito.com/contenidos/dolar-informal-historico.html",
                ("dolar/informal",), ("Compra", "Venta"), "Ámbito Financiero",
                "pesos por dólar", "Tipo de cambio")


def ejecutar() -> None:
    process(SOURCE)
