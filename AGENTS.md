# Instrucciones de organización

- Cada fuente tiene un único módulo `scrapers/scraper_<FUENTE>.py`, expone `ejecutar()` y guarda entradas en `fuentes_BD/<INSTITUCION>/<FUENTE>/`.
- `main.py` sólo orquesta scrapers; no contiene lógica de una fuente.
- No dejar scripts de depuración en la raíz. Las validaciones reutilizables pertenecen a `tests/` o al módulo correspondiente.
- Al agregar, renombrar o quitar una fuente, actualizar este archivo y `README.md`.

## Inventario multi-fuente

- `Codificacion` en `BD.xlsx` es el inventario maestro.
- La clave natural es (`Código fuente`, `ID origen`). `ID` se construye como `Código fuente::ID origen`.
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

- `scraper_BCRA_comunicaciones.py` guarda textos en `fuentes_BD/BCRA/Comunicaciones/<TIPO>/` y reutiliza los existentes.
- Los PDF son temporales; si no contienen texto extraíble, conservar el PDF e informar el caso.
- `Comunicaciones BCRA` tiene una única fila agregada en el inventario, no una por documento.
