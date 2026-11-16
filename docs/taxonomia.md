# Taxonomía del inventario

Cómo se anida cada serie y cómo se llama. Las reglas viven en `tools/taxonomia/` y se aplican al final de `_normalize_inventory` (`tools/generar_codificacion.py`).

## Niveles

Cada nivel significa siempre lo mismo. La institución no es un nivel: es una columna y un filtro.

| Nivel | Columna | Qué es | Ejemplo |
| --- | --- | --- | --- |
| 1 | `Tema` | Uno de los ocho temas | Precios |
| 2 | `Área` | Indicador u operativo estadístico | IPC |
| 3 | `Subárea 1` | Cobertura o versión del indicador | Nacional |
| 4 | `Subárea 2` | Apertura | Divisiones |
| 5 | `Subárea 3` | Segunda apertura o medida, sólo si hace falta | Variación mensual |

Reglas del árbol:

1. **Un indicador, un grupo.** Todo lo que mide el mismo fenómeno con el mismo operativo cuelga del mismo indicador, sin importar la fuente que lo publica, la base o si está vigente. El IPC del INDEC que llega por datos.gob.ar y el que llega por el libro del INDEC van al mismo lugar.
2. **Los hermanos son de la misma clase.** Bajo un indicador, todos los grupos del nivel 3 son coberturas o versiones; bajo una cobertura, todos son aperturas. No se mezclan «GBA» con «Divisiones» en el mismo nivel.
3. **Series históricas.** Las versiones que ya no se publican (bases viejas, metodologías reemplazadas) van en una rama «Series históricas» del nivel 3, dentro de su indicador, con la versión como nivel 4 («GBA base abr-2008»). La versión vigente no lleva la base en el nombre del grupo.
4. **Sin niveles de un solo hijo.** Un nivel se informa sólo si separa: si un indicador tiene una única cobertura, las series cuelgan directamente del indicador.
5. **Grupos finales manejables.** Si un grupo final supera las 150 series, falta una apertura.
6. **La medida no es un nivel por defecto.** Índice, variación mensual e interanual de un mismo concepto conviven en el mismo grupo y se distinguen por el nombre y la unidad. Se usa como nivel 5 sólo cuando el grupo quedaría demasiado grande.
7. **Vocabulario controlado.** El mismo grupo se escribe igual en todo el árbol («Nivel general», «Divisiones», «Categorías», «Bienes y servicios», «Series históricas»). Los nombres de grupo van en singular o plural según el uso corriente, con mayúscula sólo en la primera palabra y en nombres propios.

## Nombres de serie

Forma general: **`Indicador · apertura · cobertura (medida)`**, con « · » como separador.

- El nombre dice **qué mide** la serie. No la identifica: la identidad es (`Código fuente`, `ID`).
- **Dos series que miden lo mismo llevan el mismo nombre**, aunque difieran en institución, frecuencia, período o base. El ITCRB con Estados Unidos del BCRA y el empalmado del IIEP se llaman igual; el IPC mensual y su versión trimestral también.
- **No van en el nombre**: frecuencia, unidad, base, número de cuadro, rango de años, estado («SERIE DISCONTINUADA»), institución, llamadas de nota («(1)», «1/»). Todo eso tiene su columna.
- **Cobertura**: se omite cuando es la cobertura natural del indicador (el total nacional). «IPC · Nivel general» es el nacional; «IPC · Nivel general · Cuyo» el regional.
- **Medida**, entre paréntesis y en minúsculas, sólo cuando la serie no es el nivel del indicador: «(var. % mensual)», «(var. % interanual)», «(var. % acumulada)», «(incidencia)», «(desestacionalizado)», «(tendencia-ciclo)». Se combinan con coma: «(desestacionalizado, var. % mensual)».
- **Indicador**: la sigla cuando es la forma corriente (IPC, IPIM, IPIB, IPP, ICC, EMAE, PIB); si no, el nombre corto.
- **Escritura**: mayúscula inicial y minúsculas, con tildes; siglas y nombres propios con su grafía. Nada EN MAYÚSCULAS ni Con Mayúsculas En Cada Palabra; sin guiones bajos ni códigos de variable.
- **Aperturas con clasificador**: se usan los nombres del clasificador oficial tal como los publica el organismo (divisiones del IPC del INDEC, que siguen la COICOP; CIIU para ramas de actividad), escritos siempre igual.
- **Unidad**: si la fuente no la informa y surge del cuadro, la regla la completa («Índice dic-2016=100», «%», «Pesos»).

| Fuente | Nombre |
| --- | --- |
| Índice de Precios al Consumidor, por capítulos. Base  diciembre 2016. Valores mensuales (IPC. Alimentos y bebidas no alcohólicas. Región pampeana) | IPC · Alimentos y bebidas no alcohólicas · Pampeana |
| Región Cuyo - Alimentos y bebidas no alcohólicas (hoja de variación mensual) | IPC · Alimentos y bebidas no alcohólicas · Cuyo (var. % mensual) |
| CPIAUCSL | IPC · Nivel general · Estados Unidos (desestacionalizado) |
| IPIM: Agrícolas | IPIM · Agrícolas |

## Organización de las reglas

Un módulo por familia de fuentes en `tools/taxonomia/` (`precios_ipc.py`, `finanzas_publicas_caja.py`, `externo_balance_pagos.py`…), cada uno con `clasificar(serie)` y su archivo de pruebas. `armonizar.py` reúne los acuerdos que cruzan más de un módulo: un solo árbol y un solo prefijo para los tipos de cambio y una sola grafía para los sectores de actividad.

Para sumar una fuente: agregar su módulo a `MODULOS` en `tools/taxonomia/__init__.py`, definir su alcance por `fuente`, `archivo` y `hoja`, y correr la revisión hasta que no queden series sin regla.

### Correcciones explícitas

Cada módulo puede declarar, con su evidencia en un comentario:

- `UNIDADES = {(fuente, id): unidad}`: reemplaza una unidad que la fuente informa mal.
- `EXCLUIDAS = {(fuente, id): motivo}`: la serie queda en el índice con `Estado = EXCLUIDA` y no se publica. Sólo para columnas que no son series, copias exactas de otra serie de la misma fuente, y series cuyo contenido no corresponde a lo que la fuente declara y no se puede renombrar con certeza.
- `HOJAS_EXCLUIDAS = {(fuente, archivo, hoja): (motivo, ID que se conservan)}`: lo mismo para una hoja entera.

Toda corrección exige al menos dos confirmaciones independientes (una identidad que cierra en todo el período y la coincidencia con otra fuente, por ejemplo). No ubicar una serie en una identidad es una duda, no una prueba de error. Los datos de `BD.xlsx` nunca se modifican.

La columna `Clasificación` del índice dice de dónde salen los niveles y el nombre de cada serie: `taxonomía` (una regla) o `fuente` (heredados de la fuente, para series que todavía no tienen regla). La web arma el árbol con los niveles de la taxonomía y antepone el tema o la institución según cómo se elija agrupar.

Los problemas de datos que aparecieron al escribir las reglas están en `docs/problemas_de_datos.md`.

## Campos que leen las reglas

`Nombre fuente` (el título tal como lo entrega la fuente, registrado antes de renombrar), `ID`, `Variable`, `Descripción`, `Unidad`, `Frecuencia`, `Archivo origen`, `Hoja origen`, `Columna BD`, `Título dataset` y `Estado`. Nunca `Nombre serie` ni la clasificación anterior: aplicar la taxonomía dos veces da el mismo resultado, y un scraper que vuelve a correr no la deshace.

Las reglas no cambian `ID`, `Código fuente`, `Institución`, `Pestaña BD` ni `Columna BD`.

## Revisión

```
python -m tools.taxonomia.revisar --tema Precios
```

Muestra la cobertura de las reglas, el árbol con ejemplos y los problemas detectables: series sin regla, restos de frecuencia, base o estado en el nombre, grupos escritos de dos maneras, grupos finales demasiado grandes y series distintas de una misma hoja que quedaron con el mismo nombre y unidad.

## Referencias

El criterio sigue la práctica de los portales de series más usados:

- **FRED**: raíz por tema, la fuente como atributo, títulos «Indicador: apertura en cobertura», y unidad, frecuencia y ajuste estacional como campos separados. <https://fred.stlouisfed.org/categories>, <https://fred.stlouisfed.org/docs/api/fred/series.html>
- **SDMX (BCE, Eurostat)**: cada serie es una combinación de dimensiones (área, ítem, ajuste, transformación); la transformación es una dimensión, no una rama. <https://data.ecb.europa.eu/data/datasets/ICP/ICP.M.U2.N.000000.4.ANR>, <https://ec.europa.eu/eurostat/databrowser/product/view/PRC_HICP_MIDX>
- **Clasificadores**: COICOP para consumo, CIIU para actividad, MBP6 para el sector externo y MEFP 2014 para finanzas públicas.
