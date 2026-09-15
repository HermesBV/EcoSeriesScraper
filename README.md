# EcoSeriesScraper

Descarga series y documentos económicos desde distintas fuentes y actualiza `BD.xlsx`. IED es la primera fuente de series incorporada con el inventario multi-fuente; también se procesan las comunicaciones del BCRA.

## Organización

```text
main.py                         Ejecuta todos los scrapers
scrapers/scraper_IED.py         Procesa los ocho Excel del IED
scrapers/ied_inventory.py       Descubre series, metadatos y respaldo API
scrapers/scraper_BCRA_comunicaciones.py
scrapers/scraper_BCRA_indices_tipo_cambio.py
scrapers/scraper_IIEP_tipo_cambio_real.py
fuentes_BD/MECON/IED/           Excel fuente del IED
fuentes_BD/BCRA/Comunicaciones/ Textos de comunicaciones
fuentes_BD/BCRA/IndicesTipoCambio/ Excel fuente de ITCRM e ITCNM
fuentes_BD/IIEP/TipoCambioReal/    Serie histórica utilizada para el empalme IIEP
series-tiempo-metadatos.csv     Catálogo de IDs y metadatos de datos.gob.ar
BD.xlsx                         Datos consolidados e inventario maestro
logs/                           Registro de ejecuciones
```

Cada fuente nueva suma `scrapers/scraper_<FUENTE>.py`, expone `ejecutar()` y guarda sus archivos en `fuentes_BD/<INSTITUCION>/<FUENTE>/`. `main.py` descubre y ejecuta esos módulos.

## Identidad e inventario

`Codificacion`, dentro de `BD.xlsx`, es el inventario maestro. La identidad lógica es el par (`Código fuente`, `ID`). `ID` conserva el identificador nativo de la fuente; cuando la fuente no publica uno se asigna un identificador estable y descriptivo, nunca un correlativo. La combinación con `Código fuente` evita colisiones entre proveedores sin agregar un ID compuesto visible.

El inventario registra nombre, variable, unidades, valoración, descripción, frecuencia, ubicación física en la base, procedencia, dataset, distribución, rango temporal, estado y si los valores provinieron del Excel o del respaldo API. `Valoración` sólo clasifica precios corrientes o constantes cuando los metadatos aportan una señal inequívoca; en los demás casos indica que no aplica o no está informado.

## IED

El scraper descubre todas las series de los ocho libros IED cruzando sus IDs con `series-tiempo-metadatos.csv`. Extrae los valores prioritariamente de los Excel. Si un bloque existe pero su formato no puede interpretarse, consulta la API para esa serie. Nombre, unidades y descripción provienen del catálogo API.

Las hojas de salida se separan por archivo, hoja fuente y frecuencia. Las fechas se normalizan al inicio del período y se guardan como fechas reales. Antes de publicar `BD.xlsx`, el proceso reabre el temporal y comprueba dimensiones, encabezados, formatos, fechas presentes y ausencia de filas completamente vacías.

## Comunicaciones BCRA

`scraper_BCRA_comunicaciones.py` consulta el período, tipos y circulares configurados al inicio del módulo. Guarda texto en `fuentes_BD/BCRA/Comunicaciones/<TIPO>/`, reutiliza archivos existentes y conserva una sola entrada agregada en `Codificacion`.

## Índices de tipo de cambio BCRA

`scraper_BCRA_indices_tipo_cambio.py` descarga los libros oficiales ITCRM e ITCNM e incorpora los índices multilaterales, todos los bilaterales y los ponderadores disponibles. Conserva frecuencia diaria y promedios mensuales en hojas separadas; las fechas mensuales se normalizan al primer día del mes.

## Tipo de cambio real histórico IIEP

`scraper_IIEP_tipo_cambio_real.py` empalma la serie mensual `Importación (implícito)` del IIEP con el promedio mensual oficial del ITCRB Estados Unidos. Reescala el tramo histórico en enero de 1997 y conserva sin cambios la serie BCRA desde ese mes. Esta serie alimenta la vista Daniel Heymann de SeriesMacro.

## Uso

Instalar dependencias con `pip install -r requirements.txt` y ejecutar `python main.py`.

El scraper IED valida cada descarga antes de reemplazar la copia local. Si una actualización falla por red o TLS, reutiliza el último Excel válido y lo registra en el log.
