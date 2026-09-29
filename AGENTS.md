# Instrucciones de organización

- Cada fuente tiene un único módulo `scrapers/scraper_<FUENTE>.py`, expone `ejecutar()` y guarda entradas en `fuentes_BD/<INSTITUCION>/<FUENTE>/`.
- `main.py` sólo orquesta scrapers; no contiene lógica de una fuente.
- No dejar scripts de depuración en la raíz. Las validaciones reutilizables pertenecen a `tests/` o al módulo correspondiente.
- Al agregar, renombrar o quitar una fuente, actualizar este archivo y `README.md`.

## Inventario multi-fuente

- `Codificacion` en `IndiceSeries.xlsx` es el inventario maestro; `BD.xlsx` contiene sólo datos.
- La clave natural es (`Código fuente`, `ID`). `ID` conserva el identificador nativo; si la fuente no publica uno, se asigna un identificador estable y descriptivo, nunca un correlativo.
- Nunca inventar correlativos ni modificar el ID nativo para clasificar una serie.
- Cada scraper sólo reemplaza las filas y hojas que administra; debe preservar fuentes ajenas.
- Registrar como mínimo nombre, variable, unidades, valoración, descripción, frecuencia, hoja y columna de datos, origen, URL, rango temporal, estado y método usado para obtener valores.
- Una modificación del esquema exige actualizar el generador, la web, las pruebas y la documentación.

## IED

- IED comprende los ocho libros definidos en `EXCEL_URLS`.
- Las series se descubren cruzando IDs presentes en los libros con `series-tiempo-metadatos.csv`; no usar una lista manual tipo `Codigos.xlsx`.
- Los valores vienen del Excel IED y la API se usa sólo como respaldo ante un fallo de interpretación.
- Separar hojas de salida por libro, hoja fuente y frecuencia.
- Las fechas son valores comparables, nunca strings: inicio del año, semestre, trimestre o mes; fecha exacta para datos diarios.
- Guardar de forma atómica y reabrir el temporal para validar dimensiones, encabezados, fechas, formato y filas vacías.

## BCRA

- `scraper_BCRA_indices_tipo_cambio.py` administra ITCRM, ITCNM, sus bilaterales y ponderadores desde los dos Excel oficiales.
- La vista Heymann de SeriesMacro consume el promedio mensual de `ITCRB Estados Unidos`; conservar estable su identidad nativa y la hoja mensual.
- `scraper_BCRA_datos_monetarios_diarios.py` administra todas las series de `series.xlsm`.
- Conservar los IDs nativos de `API_Series`; para columnas sin correspondencia usar un ID estable basado en hoja y columna.
- `scraper_BCRA_tasas_depositos.py` administra las 19 hojas de datos de `pas2026.xls`.
- En `pas2026.xls`, identificar cada serie con `hoja|código publicado`, porque los códigos se repiten entre hojas.
- `scraper_BCRA_com3500.py`, `scraper_BCRA_bandas_cambiarias.py` y `scraper_BCRA_mercado_cambios.py` administran sus respectivos libros oficiales.
- El generador conserva la clasificación de series existentes por (`Código fuente`, `ID`) y la hereda para nuevas series del mismo dataset.

## INDEC y MECON

- `scraper_INDEC_emae.py`, `scraper_INDEC_sipm.py`, `scraper_INDEC_isac.py`, `scraper_INDEC_ipi_manufacturero.py`, `scraper_INDEC_supermercados.py` y `scraper_INDEC_comercio_exterior.py` administran sus respectivas tablas INDEC.
- `scraper_MECON_hacienda.py` está en pausa: no incluirlo en la ejecución general ni publicar sus series hasta recibir nuevas instrucciones.
- `scraper_INDEC_ipc.py`, `scraper_INDEC_pib.py`, `scraper_INDEC_cuentas_internacionales.py` y `scraper_INDEC_salarios.py` incorporan los libros de la segunda iteración. Para nombres con mes o trimestre, prueban períodos anteriores hasta encontrar un Excel válido; los datos pueden venir en filas o columnas.

## Ministerio de Capital Humano

- `scraper_MCH_sipa.py` incorpora las estadísticas SIPA del último libro mensual válido.
- `scraper_MCH_ripte.py` localiza el enlace PDF vigente en la página oficial y extrae la serie mensual histórica RIPTE.

## Estados Unidos

- `scraper_FRED_cpiaucsl.py` descarga el CSV CPIAUCSL de FRED, conserva el ID nativo y atribuye la serie a U.S. Bureau of Labor Statistics.

## IIEP

- `scraper_IIEP_tipo_cambio_real.py` administra la serie mensual histórica de TCR bilateral con Estados Unidos.
- El tramo `Importación (implícito)` se reescala en enero de 1997 y sólo se usa antes de esa fecha; desde enero de 1997 se conserva el promedio mensual BCRA sin modificaciones.
- La vista Heymann consume la serie empalmada con código fuente `iiep` e ID `itcrb-eeuu-empalmado-importacion-m`.
