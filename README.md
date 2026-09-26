# EcoSeriesScraper

Descarga series y documentos económicos desde distintas fuentes y actualiza `BD.xlsx`. IED es la primera fuente de series incorporada con el inventario multi-fuente.

## Organización

```text
main.py                         Ejecuta todos los scrapers
scrapers/scraper_IED.py         Procesa los ocho Excel del IED
scrapers/ied_inventory.py       Descubre series, metadatos y respaldo API
scrapers/scraper_BCRA_indices_tipo_cambio.py
scrapers/scraper_BCRA_datos_monetarios_diarios.py
scrapers/scraper_BCRA_tasas_depositos.py
scrapers/scraper_BCRA_com3500.py
scrapers/scraper_BCRA_bandas_cambiarias.py
scrapers/scraper_BCRA_mercado_cambios.py
scrapers/scraper_IIEP_tipo_cambio_real.py
scrapers/scraper_INDEC_emae.py
scrapers/scraper_INDEC_sipm.py
scrapers/scraper_INDEC_isac.py
scrapers/scraper_INDEC_ipi_manufacturero.py
scrapers/scraper_INDEC_supermercados.py
scrapers/scraper_INDEC_comercio_exterior.py
scrapers/scraper_MECON_hacienda.py
fuentes_BD/MECON/IED/           Excel fuente del IED
fuentes_BD/BCRA/IndicesTipoCambio/ Excel fuente de ITCRM e ITCNM
fuentes_BD/BCRA/DatosMonetariosDiarios/ Libro oficial de datos monetarios diarios
fuentes_BD/IIEP/TipoCambioReal/    Serie histórica utilizada para el empalme IIEP
series-tiempo-metadatos.csv     Catálogo de IDs y metadatos de datos.gob.ar
BD.xlsx                         Datos consolidados
IndiceSeries.xlsx               Inventario maestro multi-fuente
logs/                           Registro de ejecuciones
```

Cada fuente nueva suma `scrapers/scraper_<FUENTE>.py`, expone `ejecutar()` y guarda sus archivos en `fuentes_BD/<INSTITUCION>/<FUENTE>/`. `main.py` descubre y ejecuta esos módulos.

## Identidad e inventario

`Codificacion`, dentro de `IndiceSeries.xlsx`, es el inventario maestro. La identidad lógica es el par (`Código fuente`, `ID`). `ID` conserva el identificador nativo de la fuente; cuando la fuente no publica uno se asigna un identificador estable y descriptivo, nunca un correlativo. La combinación con `Código fuente` evita colisiones entre proveedores sin agregar un ID compuesto visible.

El inventario registra nombre, variable, unidad, valoración, descripción, frecuencia, ubicación física, procedencia, dataset, distribución, período desde/hasta y estado. También conserva institución, área, tres niveles de subárea y tema. La clasificación existente se mantiene al regenerar el índice y se hereda para nuevas series del mismo dataset. `Valoración` sólo clasifica precios corrientes o constantes cuando los metadatos o el recurso oficial aportan una señal inequívoca. Usa `No aplica` para unidades físicas, tasas e índices, y `No informado` cuando la unidad es monetaria o ambigua pero no se conoce si está a precios corrientes o constantes. Las canastas de los datasets 444, 445 y 446 y los rubros en pesos del recurso 458.1 se verificaron como valores corrientes; la descripción general de un dataset no se aplica a todos sus recursos porque puede mezclar valores corrientes, constantes e índices.

## IED

El scraper descubre todas las series de los ocho libros IED cruzando sus IDs con `series-tiempo-metadatos.csv`. Extrae los valores prioritariamente de los Excel. Si un bloque existe pero su formato no puede interpretarse, consulta la API para esa serie. Nombre, unidades y descripción provienen del catálogo API.

Las hojas de salida se separan por archivo, hoja fuente y frecuencia. Las fechas se normalizan al inicio del período y se guardan como fechas reales. Antes de publicar `BD.xlsx`, el proceso reabre el temporal y comprueba dimensiones, encabezados, formatos, fechas presentes y ausencia de filas completamente vacías.

## Índices de tipo de cambio BCRA

`scraper_BCRA_indices_tipo_cambio.py` descarga los libros oficiales ITCRM e ITCNM e incorpora los índices multilaterales, todos los bilaterales y los ponderadores disponibles. Conserva frecuencia diaria y promedios mensuales en hojas separadas; las fechas mensuales se normalizan al primer día del mes.

## Datos monetarios diarios BCRA

`scraper_BCRA_datos_monetarios_diarios.py` descarga `series.xlsm` e incorpora todas las columnas publicadas de base monetaria, reservas, depósitos, préstamos, tasas de mercado e instrumentos del BCRA. Usa los IDs nativos incluidos en `API_Series` y asigna una identidad estable por hoja y columna sólo cuando el libro no publica una correspondencia.

## Tasas y depósitos diarios BCRA

`scraper_BCRA_tasas_depositos.py` descarga `pas2026.xls` e incorpora las 19 hojas de series de tasas, montos colocados y saldos de depósitos. Como el libro repite códigos entre aperturas, cada ID combina la hoja y el código publicado.

## Tipo de cambio real histórico IIEP

`scraper_IIEP_tipo_cambio_real.py` empalma la serie mensual `Importación (implícito)` del IIEP con el promedio mensual oficial del ITCRB Estados Unidos. Reescala el tramo histórico en enero de 1997 y conserva sin cambios la serie BCRA desde ese mes. Esta serie alimenta la vista Daniel Heymann de SeriesMacro.

El inventario publica `ITCRB Estados Unidos (mensual)` como serie adicional, con unidad `Índice 17-dic-2015=100`. Su detalle es: «ITCRB Estados Unidos, empalme IIEP (hasta 1996-12) y BCRA (desde 1997-01) índice Laspeyres geométrico encadenado». Desde 1958-01 hasta 1996-12 usa `Importación (implícito)` del archivo IIEP, reescalada en 1997-01; desde 1997-01 usa el promedio mensual oficial ITCRB Estados Unidos del BCRA. La serie BCRA `ITCRB Estados Unidos` conserva su identidad y cobertura propias.

## Otras fuentes

Los scrapers BCRA de tipo de cambio A3500, bandas cambiarias y mercado de cambios procesan sus libros oficiales. Los scrapers INDEC incorporan EMAE, SIPM, ISAC, IPI manufacturero, supermercados y comercio exterior. El scraper MECON Hacienda permanece en pausa y fuera de la ejecución general hasta recibir nuevas instrucciones. Cada módulo guarda sus descargas en `fuentes_BD/` y actualiza sólo las hojas que administra.

## Uso

Instalar dependencias con `pip install -r requirements.txt` y ejecutar `python main.py`.

El scraper IED valida cada descarga antes de reemplazar la copia local. Si una actualización falla por red o TLS, reutiliza el último Excel válido y lo registra en el log.
