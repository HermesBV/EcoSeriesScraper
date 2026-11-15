# Problemas de datos detectados al clasificar

Al escribir las reglas de la taxonomía (noviembre de 2026) se compararon los valores de `BD.xlsx` contra los rótulos y contra otras fuentes. Aparecieron estos problemas, que no son de clasificación sino de los datos o de su extracción. La taxonomía nombra cada serie según lo que la fuente declara; ninguno está corregido.

## Columnas corridas o rótulos cruzados (datos.gob.ar)

**Origen verificado (15/11/2026):** los valores de `BD.xlsx` coinciden al decimal con los que devuelve la API pública de series de tiempo (`apis.datos.gob.ar/series`) para los mismos ID, en las tres hojas que se cotejaron (4.2.4, 1.17 y 8.2). El scraper lee bien: localiza cada serie por su ID dentro del libro y toma la columna de abajo. El error viene del Ministerio, que publica esos valores bajo ese ID. Las otras hojas de la tabla no se cotejaron contra la API.

En la hoja 4.2.4 la causa probable es un empalme por posición de dos clasificaciones: hasta diciembre de 2016 los nueve capítulos de la base anterior y desde enero de 2017 las doce divisiones nuevas, en otro orden. Los mnemónicos de los ID (`I2T`, `I2EH`) tampoco coinciden con las descripciones (`I2T` figura como «Educación»). La API da esa distribución por cerrada en julio de 2020, aunque el libro sigue trayendo datos.

| Dónde | Qué pasa | Cómo se detectó |
| --- | --- | --- |
| `precios.xlsx`, hoja 4.2.4 (IPC GBA por divisiones) | Desde enero de 2017, once de las doce divisiones traen los valores de otra división («Salud» trae los de «Equipamiento y mantenimiento del hogar», etc.). Afecta 22 ID, listados en `tools/taxonomia/precios_ipc.py`. | Comparación con la hoja «IPC GBA Base dic 2016» del libro del INDEC |
| `actividad.xlsx`, hoja 1.17 (producción industrial por producto) | Las magnitudes no corresponden a las unidades declaradas (cemento en «miles de toneladas» con valores de 14.900) y hay un valor idéntico al de otra hoja. Sin unidad completada en 267 series. | Orden de magnitud y cruce con la hoja 1.13 |
| `dinero_bancos.xlsx`, hoja 8.2 (agregados monetarios) | El total es menor que sus partes; «Total en dólares» da 4 millones contra 63 millones del sector privado. | Identidades de suma |
| `empleo_ingresos.xlsx`, hoja «Salario real Ind85-90» anual | Valores idénticos a los de «Salario oficial60-75», rubro Alimentación. | Cruce entre hojas |
| `sector_externo.xlsx`, hojas 39 y 40 | Rótulos de los rangos de exportación corridos por celdas combinadas. Las reglas ya los reasignan. | Las cantidades por rango suman el total |
| `sector_externo.xlsx`, hoja 16.a | Rótulos por sector corridos («activos del gobierno general» trae los de las sociedades captadoras de depósitos). Las reglas identifican cada serie por sus valores. | Cruce con los cuadros 15 y 16 del INDEC |
| `sector_externo.xlsx`, hoja 14.a | «Otra inversión · Pasivos» con signo invertido respecto del cuadro 8 del INDEC desde 2017T1. | Cruce con `indec-cin` |
| `sector_externo.xlsx`, hoja 8 | Cuatro series rotuladas «Participación de importaciones … en el total» están en millones de dólares. Se nombraron por lo que miden. | Orden de magnitud |
| Varias hojas (`TE 03-`, `TSD 03-`, 4.3 a 4.5, 4.11, 4.12, 4.13.1, 1.2, 1.5.1, hoja 12) | Descripciones o siglas del ID cruzadas entre series o frecuencias. Las reglas usan el campo que los valores confirman. | Identidades y recálculo de variaciones |

## Columnas que no son series (extracción)

| Fuente | Qué es |
| --- | --- |
| `indec-ipi-manufacturero`, cinco columnas «… Código (CLaNAE 2004) - Período» | Número de fila de la hoja de Excel. Quedaron en «IPI manufacturero › Auxiliares». |
| `bcra-mc-bc`, hoja «Balance Cambiario», columna sin rótulo | Un contador. |
| `bcra-mc-bc`, «Datos Mercado de Cambios», columna «Monto» | El valor de la última fila de cada mes de una tabla de datos, no un agregado. |
| `indec-cin`, cuadro 20, una columna de ceros sin rótulo | Columna espuria. |

Conviene que los scrapers dejen de publicarlas.

## Series duplicadas en la fuente

- `empleo_ingresos.xlsx`, hojas «LP - Hog» y «LP - Pers»: los cuadros 63.1 y 64.1 son copias exactas de 63.2 y 64.2 (78 series).
- `empleo_ingresos.xlsx`, hoja «HaberMin»: la misma serie dos veces.
- `sector_externo.xlsx`, hoja 41: «Total general» y «Empresas exportadoras» son la misma serie.
- `sector_externo.xlsx`, hoja 16.a: cuatro agregados de la posición de inversión internacional aparecen en dos subtablas.
- `dinero_bancos.xlsx`, hoja 8.2: plazo fijo del sector privado en dólares, dos veces.

Llevan el mismo nombre a propósito.

## Unidades

- Parte de las variaciones y tasas de datos.gob.ar viene como fracción (0,03 = 3 %). Se rotulan «Proporción (1 = 100%)» en lugar de «%».
- El inventario trae «%» como unidad en series que están en millones (cuadro 20 y dos series del cuadro 14 de `indec-cin`; «Variación de existencias» de los cuadros 1 y 8 de `indec-pib`). La taxonomía sólo completa unidades vacías, así que siguen mal.
- Valores atípicos sin tocar: variaciones de 11.000 a 14.000 en el IPIM (hoja 4.3, dic-1999 y ene-2000) y valores aislados en otra escala en el PIB trimestral del REM.

## Definiciones que no se pudieron confirmar

- «Eficiencia» del sistema financiero (hoja 8.12): sus valores no cierran con ninguna fórmula habitual.
- Descripciones de los capítulos de la NCM: se escribieron sin cotejar contra el nomenclador oficial y se abreviaron con una regla uniforme.
- El detalle por módulo está en los comentarios de cada archivo de `tools/taxonomia/`.
