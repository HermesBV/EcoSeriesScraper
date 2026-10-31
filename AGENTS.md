# Instrucciones de organización

- Cada fuente tiene un único módulo `scrapers/scraper_<FUENTE>.py`, expone `ejecutar()` y guarda entradas en `fuentes_BD/<INSTITUCION>/<FUENTE>/`.
- `main.py` sólo orquesta scrapers; no contiene lógica de una fuente. Corre los módulos dentro de `tools.lote.abrir()`: `guardar_datos_preservando_formato` y `generar` acumulan hojas e inventario en memoria y `BD.xlsx` e `IndiceSeries.xlsx` se escriben una sola vez al final (también si un scraper falló). Un scraper que lee hojas de otro debe usar `tools.lote.leer_hoja` para ver lo pendiente. Suelto, cada scraper sigue escribiendo en el acto.
- No dejar scripts de depuración en la raíz. Las validaciones reutilizables pertenecen a `tests/` o al módulo correspondiente.
- Al agregar, renombrar o quitar una fuente, actualizar este archivo y `README.md`.

## Inventario multi-fuente

- `Codificacion` en `IndiceSeries.xlsx` es el inventario maestro; `BD.xlsx` contiene sólo datos.
- La clave natural es (`Código fuente`, `ID`). `ID` conserva el identificador nativo; si la fuente no publica uno, se asigna un identificador estable y descriptivo, nunca un correlativo.
- Nunca inventar correlativos ni modificar el ID nativo para clasificar una serie.
- Cada scraper sólo reemplaza las filas y hojas que administra; debe preservar fuentes ajenas. Las series de un código que la entrega nueva ya no trae quedan en el índice con `Estado = DISCONTINUADA` mientras exista su pestaña (`_discontinuadas` en `tools/generar_codificacion.py`); nunca se borran datos de `BD.xlsx` por una serie que la fuente dejó de publicar.
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
- `scraper_BCRA_comunicaciones.py` no se usa en este repositorio: `main.py` lo excluye y sus documentos no se publican en SeriesMacro. Se conserva porque lo usa otro repositorio; no eliminarlo.
- `scraper_BCRA_cotizaciones.py` descarga de la API cambiaria del BCRA el tipo de pase y el tipo de cotización diarios de las monedas en `MONEDAS` (hoy CNY), por año y con siete días de solapamiento.
- `scraper_BCRA_irfcl.py` administra la planilla IRFCL: las partidas numéricas vienen del dataset IRFCL del FMI (sector autoridad monetaria, declarado por el BCRA, historia revisada desde 2000-09) y las partidas de custodia de la nota del país sobre II.1, leídas de los PDF mensuales del BCRA (desde 2007-10). Conserva los signos publicados.
- Las series que miden saldo, composición o variación de reservas llevan Tema «Sector externo» para todas las instituciones (`unificar_tema_reservas` en `tools/temas.py`); los pasivos y contrapartidas del balance del BCRA siguen en «Moneda y sistema financiero». Área y subáreas no se modifican.

## INDEC y MECON

- `scraper_INDEC_emae.py`, `scraper_INDEC_sipm.py`, `scraper_INDEC_isac.py`, `scraper_INDEC_ipi_manufacturero.py`, `scraper_INDEC_supermercados.py` y `scraper_INDEC_comercio_exterior.py` administran sus respectivas tablas INDEC.
- `scraper_MECON_hacienda.py` está en pausa: no incluirlo en la ejecución general ni publicar sus series hasta recibir nuevas instrucciones. `scraper_MECON_imig.py` reutiliza su descarga y publica sólo el Informe Mensual de Ingresos y Gastos (hoja `MECON IMIG M`, código `mecon-imig`, ID = slug del concepto): el informe más reciente revisa los meses anteriores y «Mensualizacion» cubre el año en curso.
- `scraper_BCRA_datos_monetarios_diarios.py` conserva sólo las filas con «Tipo de serie» D; las filas PM/VFM/VPM del libro oficial pisaban los saldos del primer día de cada mes.
- Las columnas de fechas de INDEC/MCH admiten texto («feb-15», «dic-25*») y trimestres provisorios («2025T4*»); sin eso SIPA A.4 se cortaba en 2015 y CIN Cuadro 1 en 2023.
- `scraper_INDEC_ipc.py`, `scraper_INDEC_pib.py`, `scraper_INDEC_cuentas_internacionales.py` y `scraper_INDEC_salarios.py` incorporan los libros de la segunda iteración. Para nombres con mes o trimestre, prueban períodos anteriores hasta encontrar un Excel válido; los datos pueden venir en filas o columnas.

## Ministerio de Capital Humano

- `scraper_MCH_sipa.py` incorpora las estadísticas SIPA del último libro mensual válido.
- `scraper_MCH_ripte.py` localiza el enlace PDF vigente en la página oficial y extrae la serie mensual histórica RIPTE.

## Estados Unidos

- `scraper_FRED_cpiaucsl.py` descarga el CSV CPIAUCSL de FRED, conserva el ID nativo y atribuye la serie a U.S. Bureau of Labor Statistics.

## Ámbito

- `scraper_Ambito_riesgo_pais.py`, `scraper_Ambito_dolar_blue.py` y `scraper_Ambito_dolar_mep.py` descargan sus históricos de la API de Ámbito con Chrome oculto cuando la consulta directa recibe 403.
- La primera descarga conserva todo el histórico disponible. Las siguientes reanudan desde siete días antes de la última observación y reemplazan las fechas repetidas; dos cotizaciones de la misma fecha se promedian.
- El endpoint de referencia MEP empieza en 2020 y puede fallar en tramos con datos faltantes. La descarga inicial separa años y luego tramos más chicos; las fechas que la API rechaza se registran sin inventar valores.

## IIEP

- `scraper_IIEP_tipo_cambio_real.py` administra la serie mensual histórica de TCR bilateral con Estados Unidos.
- El tramo `Importación (implícito)` se reescala en enero de 1997 y sólo se usa antes de esa fecha; desde enero de 1997 se conserva el promedio mensual BCRA sin modificaciones.
- La vista Heymann consume la serie empalmada con código fuente `iiep` e ID `itcrb-eeuu-empalmado-importacion-m`.
- `scraper_IIEP_reservas_internacionales.py` (código `iiep-rin`) publica las nueve series del DT-IIEP N° 126. Hasta `CORTE_HISTORICO` usa la base de los autores; después calcula con su método desde las hojas `BCRA IRFCL M` y `BCRA Cotizaciones D`, por lo que debe correr después de los scrapers BCRA. RIN-EFF no se actualiza. El método y su validación están en `docs/metodo_reservas_IIEP.md`.

## Excel de coyuntura

- `coyuntura/coyuntura iiep reemplazar.xlsx` es el libro del economista; `python -m coyuntura.actualizador` lo actualiza desde `BD.xlsx` después de los scrapers (nunca vuelve a descargar fuentes). Requiere Excel (COM) y el libro cerrado.
- El mapeo vive en `coyuntura/actualizador/mapeo.py` por (`Código fuente`, `ID`); al cambiar una fuente, correr `python -m coyuntura.herramientas.verificar_mapeo`.
- Respetar el formato del economista: filas nuevas entre el último dato y la barra azul, fórmulas copiadas hacia abajo, imputaciones manuales conservadas, COMEX con fechas hasta diciembre y barra fija. Criterios completos en `coyuntura/README.md`.
- `bkp/` guarda una copia por actualización (sufijo AAMMDD); los meses cerrados se comprimen en zip y se conservan dos. `bkp/`, `logs/` y `.trabajo/` no se versionan.
- El Excel publicado se copia a `SeriesMacro/coyuntura/` para la descarga con «abrete sesamo».

## Documentación de trabajo

- `docs/guia_incorporar_fuentes.md` reúne el recorrido y las decisiones habituales para sumar fuentes. Ampliarla cuando una fuente nueva enseñe algo reutilizable.
