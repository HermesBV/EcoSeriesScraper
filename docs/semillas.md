# Semillas históricas

Una **semilla** es un archivo que alguien pasa a mano (un Excel, un CSV) con datos que no están en internet. Se carga una
sola vez en `BD.xlsx`; desde ahí la fuente de ese tramo es la base y el archivo ya no hace falta para la corrida diaria.
Lo que se actualiza todos los días viene siempre de una fuente de internet, nunca de una semilla.

## Dónde va cada cosa

| Qué | Dónde | ¿En git? |
| --- | --- | --- |
| El archivo crudo | `semillas/<código fuente>/<archivo>` | No (pueden ser privados o pesados) |
| Lo que bajan los scrapers | `fuentes_BD/<INSTITUCION>/<FUENTE>/` | No |
| El registro de lo cargado | `semillas/registro.json` | **Sí** |
| El código que la lee | el scraper (caso a) o `scrapers/semilla_<FUENTE>.py` (caso b) | Sí |

Dónde guardar los originales fuera de la máquina queda pendiente, sin apuro: por ahora quedan en `semillas/` de la PC
de Hermes, fuera de git, y la base ya los tiene integrados. El `sha256` del registro permite comprobar que un archivo
es el que se cargó.

## Caso a: un tramo histórico de una serie que se sigue actualizando

Ejemplos: el TCR bilateral con Estados Unidos del IIEP (antes de 1997) y las reservas del DT-IIEP N° 126 (hasta 2026-08).

1. Poner el archivo en `semillas/<código fuente>/`.
2. En el scraper, leer el tramo con `semillas.tramo_historico(archivo, leer, hoja)`: con el archivo presente usa `leer`;
   sin él, lo ya publicado en la hoja. Recortar el tramo (hasta la fecha de corte o hasta donde empieza la fuente viva) y
   seguir con lo que baja de internet. Si la semilla necesita un ajuste (un empalme), aplicarlo sólo cuando el origen es
   `"semilla"`: lo publicado ya está ajustado.
3. Correr ese scraper una vez (`python -m scrapers.scraper_<FUENTE>`) y revisar la hoja.
4. Registrarla: `python -m tools.semillas registrar --codigo <código> --hoja <hoja> --columnas <columnas…>
   --archivo semillas/<código>/<archivo> --hasta <última fecha del tramo> --paso <quién la pasó>`.
5. Commit del código, de `BD.xlsx`, de `IndiceSeries.xlsx` y del registro. El archivo puede borrarse de la máquina.

## Caso b: una serie sólo histórica, sin fuente que se actualice

1. Poner el archivo en `semillas/<código fuente>/`.
2. Escribir `scrapers/semilla_<FUENTE>.py` con `CODIGO`, `HOJA`, `FRECUENCIA`, `ARCHIVO`, `leer(ruta)` (devuelve
   `fecha` y una columna por serie) y `filas_indice(datos)` (las filas de `Codificacion`, como cualquier scraper).
   `main.py` no lo corre: sólo corre `scraper_*`.
3. Cargarla y registrarla: `python -m tools.semillas cargar semilla_<FUENTE> --paso <quién la pasó>`.
4. Si el código fuente es nuevo, sumar sus reglas de taxonomía y correr
   `python -m tools.taxonomia.revisar --fuente <código>` (AGENTS.md, «Taxonomía»).
5. Commit del módulo, de `BD.xlsx`, de `IndiceSeries.xlsx` y del registro.

La corrida diaria conserva la hoja y sus filas del índice: cada scraper reescribe sólo sus hojas y sus códigos
(`tests/test_semillas.py` lo prueba).

## Reglas

- Una semilla nunca reemplaza datos que llegan de internet: llena sólo lo que la fuente viva no tiene.
- Para corregir una semilla ya cargada, se pone el archivo corregido y se repite su caso (el registro se reemplaza).
- `python -m tools.semillas ver` lista lo cargado.
