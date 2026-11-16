# Problemas de datos detectados al clasificar y auditar

En noviembre de 2026 se escribieron las reglas de la taxonomía y después cada módulo pasó por una auditoría independiente, que contrastó los valores de `BD.xlsx` contra los rótulos, contra otras series del inventario, contra la API pública de series de tiempo y contra los libros originales de INDEC, BCRA, Hacienda y ARCA. Este documento resume lo que apareció. El detalle y la evidencia de cada corrección están en los comentarios del módulo correspondiente de `tools/taxonomia/`.

## Qué se corrigió y cómo

Ninguna corrección cambia valores de `BD.xlsx` ni identificadores. Hay tres mecanismos, todos explícitos:

- **Reglas**: la serie se nombra y se ubica por lo que realmente contiene, cuando eso quedó identificado para todo su período.
- **`UNIDADES`**: reemplaza una unidad que la fuente informa mal (1.454 series).
- **`EXCLUIDAS` y `HOJAS_EXCLUIDAS`**: la serie queda en el índice con `Estado = EXCLUIDA` y la web no la publica (486 series). Se usa sólo para columnas que no son series, copias exactas, y series cuyo contenido no corresponde a lo que declara la fuente y no se puede reparar.

Estándar de evidencia: al menos dos confirmaciones independientes por corrección. Lo que no lo alcanzó quedó como duda (ver el final).

## Exclusiones

| Qué | Series | Por qué |
| --- | --- | --- |
| Hoja 1.17, producción industrial por producto | 248 | Contenido erróneo comprobado en cuatro productos (papel sanitario es copia de ventas de gaseosas; cemento y naftas imposibles) y valores incompatibles con la unidad declarada en muchos otros. Se conservan petróleo crudo y gas natural, confirmados contra las hojas 1.24 y 1.25. |
| Hoja `CGI Puestos` anual | 61 | Cada valor «anual» es una observación trimestral fechada como año. El dato correcto está en la trimestral. |
| Hoja 4.2.4, IPC GBA principales aperturas | 52 | Cada columna empalma dos conceptos: su rótulo hasta dic-2016 y la fila de igual posición del libro de aperturas del INDEC desde ene-2017. |
| Supermercados y mayoristas | 35 | Copias exactas de totales entre cuadros. |
| Empresas exportadoras, hoja 41 | 16 | Copias exactas de la hoja 39. |
| Cuentas internacionales (`indec-cin` y hojas 14.a y 16.a) | 17 | Una columna espuria, dos series con signo invertido desde 2017, ocho copias, cuatro desgloses en cero con total distinto de cero y dos derivados atribuidos al sector equivocado. |
| Servicios públicos, hoja 1.10 | 9 | Cinco índices anuales con los rótulos cruzados entre 2001 y 2013 y cuatro stocks de líneas sumados como flujo. |
| IMIG | 8 | Series que cambian de definición a mitad de camino (por ejemplo «Resto de ingresos tributarios», que incorpora Combustibles desde 2019 o 2020 según la fuente). |
| Salario real industrial 1985-90, anual | 7 | Copia de la tabla de salarios de 1960-64 con las fechas cambiadas. |
| Mercado de cambios del BCRA | 7 | Un contador, un valor suelto de una tabla y cinco copias exactas. |
| Columnas «Período» del IPI manufacturero | 5 | Traen el número de fila de la hoja de Excel. |
| Otras | 21 | Carbón de Sudáfrica (un índice rotulado como precio), tipo de cambio Banco Nación (empalma mayorista y minorista), copias sueltas. |

## Rótulos equivocados en la fuente, ya resueltos por las reglas

- **Depósitos de la hoja 8.2**: la columna «público» trae el total y la «total» trae el sector público (24 series, confirmado contra los datos diarios del BCRA en 285 meses).
- **Unión Europea en las hojas 7, 8 y 9**: «total» y «resto» están intercambiados (27 series, confirmado contra el ICA del INDEC).
- **Hoja 4.9.4**: rotulada «base 1993», está en base 2004.
- **Importaciones, hoja 1**: la serie de bienes de consumo y vehículos incluye además el resto.
- **Balance de pagos MBP5, hoja 14**: cuenta financiera con signo contrario y sin reservas; «otras rentas» y «transferencias corrientes» no equivalen a sus pares del MBP6.
- **Canasta de la Ciudad de Buenos Aires**: es por hogar tipo, no por adulto equivalente.
- **Capítulos de la NCM**: 36 de 96 descripciones corregidas contra el Sistema Armonizado.
- **Títulos públicos**: año de vencimiento y tipo de bono mal informados en varias especies; identificados por su código.
- **Monedas**: la cuenta devengado 1961-1992, el salario mínimo, el haber mínimo y los tipos de cambio históricos están en la moneda de cada época.

## Errores de extracción del scraper (corregidos)

Casos en que `BD.xlsx` no coincidía con la API pública para el mismo ID. Eran tres errores de `scrapers/scraper_IED.py`, corregidos y cubiertos por `tests/test_scraper_IED.py`:

- **Series trimestrales con un solo dato.** El rótulo del trimestre escrito sin separador («II14») cortaba el bloque. Afectaba a las hojas trimestrales de recaudación (`RECA`), de servicios públicos (1.10), de producción industrial (1.17) y a 1.13, 1.14, 1.14b, 1.16, 1.18, 4.15.1 y 4.15.2.
- **Columna de fechas de otro bloque.** Cuando una hoja apila dos cuadros, el primero tomaba las fechas (y con ellas los datos) del segundo: cuadros 61.1 a 64.1 de pobreza e indigencia (hojas `LI` y `LP`). Las hojas de tasas del mercado de trabajo (`TA 03-`, `TD 03-`, `TE 03-`, `TS 03-`, `TSD 03-`, `TSND 03-`) y de distribución del ingreso (`DP`) usaban una columna auxiliar de fechas que termina en 2013 y perdían todo lo posterior; ahora usan el índice de tiempo que declara el cuadro y sus fechas son las de fin de trimestre, como en el resto de las series trimestrales.
- **Cuadros lado a lado.** El segundo cuadro heredaba las fechas del primero: cuadro 59.2 de canastas (valores de 2000-2003 fechados como 1988-1991), `CBA y CBT Reg`, `OEDE Asal prov` y `EIL - Sector`.

Con la extracción corregida volvieron a publicarse las 78 series de pobreza de los cuadros 63.1 y 64.1 (antes copias de 63.2 y 64.2) y las cuatro canastas del cuadro 59.2. Los cuadros 61.1 a 64.1 son la EPH puntual de 2001 a 2003 y van a «Series históricas».

Tres cosas más que dejó a la vista la corrida completa de noviembre de 2026:

- **`BD.xlsx` no borra fechas.** Cada corrida suma y pisa, así que un arreglo de fechas deja las viejas. Para las pestañas afectadas (`EMP-LI - Hog-S-f2587`, `EMP-LI - Pers-S-0b74f`, `EMP-LP - Hog-S-17d17`, `EMP-LP - Pers-S-42c59` y `EMP-CBA y CBT-T-318e0`) hay que correr una vez `python -m tools.rehacer_pestanas <pestañas>` antes del scraper IED, en cada instalación que tenga la base anterior.
- **Rótulos del INDEC que cambian de un mes a otro.** El índice de salarios y el cuadro 1 del ISAC salen con el encabezado combinado escrito de dos maneras; cada una daba un ID distinto y dejaba a la serie publicada como discontinuada. `ROTULOS_EQUIVALENTES` (`scrapers/indec_workbooks.py`) lleva cada variante al rótulo publicado. Si aparece una variante nueva se la ve como series nuevas sin regla de taxonomía en `python -m tools.taxonomia.revisar`.
- **IPI manufacturero.** El libro de septiembre de 2026 hacía fallar la lectura de encabezados (columna numérica en la zona del título). Corregido; además el scraper ya no emite las columnas «Período», que traen el número de fila.

Queda afuera la **indigencia semestral 2003-2006** (cuadros 61.2 y 62.2 de las hojas `LI`): los ID del libro no figuran en el catálogo de la API y el scraper sólo toma ID catalogados. Antes esos datos aparecían, por el error de fechas, bajo los ID de 61.1 y 62.1.

No era un error: **`indec-cin`, cuadro 20**. El libro trae 2006-2019 en columnas anuales y desde 2020 en trimestrales; la base publica la serie trimestral y no le mezcla los años.

Las aperturas finas del IPC por región salen ahora de `sh_ipc_aperturas.xls` del INDEC (`scrapers/scraper_INDEC_ipc_aperturas.py`, fuente `indec-ipc-aperturas`), que es la fuente correcta de lo que la hoja 4.2.4 mezcla.

## Verificado contra el original, sin errores

Coinciden con el libro del organismo: IPC (`sh_ipc_divisiones.xls`), PIB (`sh_oferta_demanda`), EMAE, IPI manufacturero, ISAC, supermercados, servicios públicos, intercambio comercial (`balanmensual.xls`), cuentas internacionales (`cin_II_2026.xls`), índice de salarios, depósitos y tasas del BCRA (`pas2026.xls`), datos monetarios diarios, mercado de cambios, IRFCL, REM, cuenta ahorro-inversión-financiamiento (cuadros de Hacienda de dic-2019 y dic-2025) y recaudación (informe de ARCA de dic-2024).

## Detalle de los primeros hallazgos

### Columnas corridas o rótulos cruzados (datos.gob.ar)

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

### Columnas que no son series (extracción)

| Fuente | Qué es |
| --- | --- |
| `indec-ipi-manufacturero`, cinco columnas «… Código (CLaNAE 2004) - Período» | Número de fila de la hoja de Excel. Quedaron en «IPI manufacturero › Auxiliares». |
| `bcra-mc-bc`, hoja «Balance Cambiario», columna sin rótulo | Un contador. |
| `bcra-mc-bc`, «Datos Mercado de Cambios», columna «Monto» | El valor de la última fila de cada mes de una tabla de datos, no un agregado. |
| `indec-cin`, cuadro 20, una columna de ceros sin rótulo | Columna espuria. |

Conviene que los scrapers dejen de publicarlas.

### Series duplicadas en la fuente

- `empleo_ingresos.xlsx`, hoja «HaberMin»: la misma serie dos veces.
- `sector_externo.xlsx`, hoja 41: «Total general» y «Empresas exportadoras» son la misma serie.
- `sector_externo.xlsx`, hoja 16.a: cuatro agregados de la posición de inversión internacional aparecen en dos subtablas.
- `dinero_bancos.xlsx`, hoja 8.2: plazo fijo del sector privado en dólares, dos veces.

Llevan el mismo nombre a propósito.

### Unidades

- Parte de las variaciones y tasas de datos.gob.ar viene como fracción (0,03 = 3 %). Se rotulan «Proporción (1 = 100%)» en lugar de «%».
- El inventario trae «%» como unidad en series que están en millones (cuadro 20 y dos series del cuadro 14 de `indec-cin`; «Variación de existencias» de los cuadros 1 y 8 de `indec-pib`). La taxonomía sólo completa unidades vacías, así que siguen mal.
- Valores atípicos sin tocar: variaciones de 11.000 a 14.000 en el IPIM (hoja 4.3, dic-1999 y ene-2000) y valores aislados en otra escala en el PIB trimestral del REM.

## Dudas que quedan

- Legislación (Nueva York o argentina) de diez bonos en dólares y bases de los índices bursátiles.
- IPC nacional urbano 2013-2015: los cuadros 4.15.1 y 4.15.2 comparten nombres de capítulo con valores distintos.
- «Bienes» y «Servicios» de la hoja 1.9 (cont. 2) de ingreso nacional: no cierran con ninguna identidad.
- Materias primas, hoja 4.10: entre 1980 y 1990 varias series traen valores idénticos entre sí (relleno de la fuente).
- Valores atípicos puntuales que no se tocan: IPIM de dic-1999 a feb-2000, RIPTE de ago-2026 en datos.gob.ar, resultado de bancos privados 2010-2011, tramos congelados del tipo de cotización del dólar en 2026.
- El detalle por módulo está en los comentarios de cada archivo de `tools/taxonomia/`.
