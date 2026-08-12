# EcoSeriesScraper

Descarga series económicas desde distintas fuentes y actualiza `BD.xlsx`. Actualmente contiene la fuente IED (Informe Económico al Día).

## Organización

```text
main.py                    Inicia todos los scrapers
scrapers/
  scraper_IED.py           Descarga y procesa el IED
fuentes_BD/
  IED/                     Excel descargados (ignorada por Git)
BD.xlsx                    Base consolidada, codificación y diccionarios
Codigos.xlsx               Lista operativa temporal de IDs que busca el scraper IED
logs/                      Registro de cada ejecución
```

Cada fuente nueva debe sumar `scrapers/scraper_<FUENTE>.py` y `fuentes_BD/<FUENTE>/`, conectarse desde `main.py` y quedar documentada aquí.

Los IDs se administran en `Codificacion`. `Referencia_Codigos` contiene solo catálogos globales y `Mapa_Tematico` representa la jerarquía padre-hijo. El formato es `IITTTTTTTTNNNVBBUEF` (19 caracteres). El período y la vigencia son metadatos, por lo que cerrar o sustituir una serie no modifica su ID.

`Codigos.xlsx` se mantiene separado porque actualmente guía la extracción selectiva del IED. Se retirará cuando el scraper descargue y reorganice automáticamente todas las series con la codificación propia.

## Uso

Instalar dependencias con `pip install -r requirements.txt` y ejecutar `python main.py`.
