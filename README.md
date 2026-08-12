# EcoSeriesScraper

Descarga series y documentos económicos desde distintas fuentes y actualiza `BD.xlsx`. Actualmente contiene el IED y las comunicaciones del BCRA.

## Organización

```text
main.py                    Inicia todos los scrapers
scrapers/
  scraper_IED.py           Descarga y procesa el IED
  scraper_BCRA_comunicaciones.py  Descarga comunicaciones del BCRA
fuentes_BD/
  MECON/
    IED/                   Excel descargados del IED
  BCRA/
    Comunicaciones/
      A/, B/, C/, P/       Textos de las comunicaciones por tipo
BD.xlsx                    Base consolidada, codificación y diccionarios
Codigos.xlsx               Lista operativa temporal de IDs que busca el scraper IED
logs/                      Registro de cada ejecución
```

Cada fuente nueva debe sumar `scrapers/scraper_<FUENTE>.py` y `fuentes_BD/<INSTITUCION>/<FUENTE>/`, conectarse desde `main.py` y quedar documentada aquí.

`scraper_BCRA_comunicaciones.py` consulta el período, los tipos y las circulares definidos en su bloque de configuración. Consolida los registros en `Comunicaciones BCRA`, guarda fechas comparables, enlaces y la circular temática cuando existe. Descarga cada PDF de forma temporal, extrae su texto a un TXT y elimina el PDF validado. En corridas posteriores consulta el índice, pero descarga únicamente comunicaciones nuevas o archivos faltantes. Todo el conjunto tiene una sola entrada en `Codificacion`: `BC10100000001X00O0I`.

Los parámetros editables están al principio de `scraper_BCRA_comunicaciones.py`: `FECHA_DESDE`, `FECHA_HASTA`, `TIPOS_BUSQUEDA` y `CIRCULARES_BUSQUEDA`. Usar `("ALL",)` para todos, `("NONE",)` para registros sin ese dato o una tupla explícita, por ejemplo `("A", "B")` o `("CAMEX", "OPASI")`.

Los IDs se administran en `Codificacion`. `Referencia_Codigos` contiene catálogos globales y `Parentesco_Codigos` representa la jerarquía institución → tema → subtema. `ID fuente` conserva el identificador original y `Fuente` su URL. El formato es `IITTTTTTTTNNNVBBUEF` (19 caracteres). El período y la vigencia son metadatos, por lo que cerrar o sustituir una serie no modifica su ID.

Las instituciones registradas son `ME`, Ministerio de Economía de la Nación (MECON), y `BC`, Banco Central de la República Argentina (BCRA). Los códigos de futuras instituciones se incorporan únicamente cuando sean definidos.

`Codigos.xlsx` se mantiene separado porque actualmente guía la extracción selectiva del IED. Se retirará cuando el scraper descargue y reorganice automáticamente todas las series con la codificación propia.

## Uso

Instalar dependencias con `pip install -r requirements.txt` y ejecutar `python main.py`.
