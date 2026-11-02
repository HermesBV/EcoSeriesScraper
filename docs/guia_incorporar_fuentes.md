# Guía para incorporar fuentes

Notas de trabajo acumuladas al sumar fuentes. Complementan `AGENTS.md`, que fija las reglas; acá queda el cómo.

## Recorrido

1. **Leer la fuente antes de escribir código.** Identificar qué publica la institución tal cual y qué es elaboración propia de otro (por ejemplo, el IIEP compila partidas del BCRA). Cada cosa va con su institución.
2. **Buscar la fuente estructurada.** Antes de leer un PDF, verificar si el mismo dato existe en una API o dataset (FMI SDMX, API del BCRA, CSV de FRED). El dataset IRFCL del FMI trae la planilla del BCRA ya revisada y con toda la historia; el PDF del BCRA queda sólo para lo que el dataset no tiene (las notas del país).
3. **Prototipar en el scratchpad y validar contra una referencia.** Comparar mes a mes contra una serie conocida (la base del IIEP sirvió de control para el lector de la planilla). Reportar cuántos períodos coinciden y explicar cada desvío antes de seguir.
4. **Distinguir error de lectura de diferencia de método.** Un desvío igual a una partida concreta (por ejemplo, los cheques cancelatorios que el IIEP no suma antes de 2013-07) es de método; un número absurdo es de lectura.
5. **Escribir el módulo** `scrapers/scraper_<INSTITUCION>_<fuente>.py` con `ejecutar()`, descargas en `fuentes_BD/<INSTITUCION>/<Fuente>/`, guardado con `guardar_datos_preservando_formato` y alta en el índice con `generar()`.
6. **Pruebas sin red** en `tests/`, con textos y valores reales de la fuente (incluidas sus erratas).
7. **Documentar** en `AGENTS.md` y `README.md`; registrar el estado en `Pendientes.md`.

## Decisiones que se repiten

- **Código fuente propio por módulo.** `generar(inventario)` reemplaza todas las filas de los códigos presentes en el inventario que recibe. Dos módulos que comparten código se borran mutuamente las series, salvo que cada uno entregue el inventario completo de ese código.
- **Institución = productor del dato.** FRED publica CPIAUCSL, pero la institución es el BLS. El FMI difunde la IRFCL, pero la declara el BCRA.
- **Área y subáreas reflejan cómo publica la institución; el Tema es nuestro.** El Tema se unifica por concepto entre instituciones (reglas en `tools/temas.py`); el área no se toca.
- **Títulos repetidos están permitidos.** «Reservas internacionales brutas» existe para BCRA e IIEP; la institución y el detalle las distinguen.
- **Actualización incremental.** Primera descarga completa; luego solapamiento corto (siete días en series diarias, tres meses en planillas mensuales que se revisan). Los caché de lectura (`notas_ii1.csv`) evitan reprocesar archivos viejos.
- **Orden de ejecución.** `main.py` corre los módulos en orden alfabético. Un módulo que deriva series de otras hojas de `BD.xlsx` debe llamarse de modo que corra después (BCRA antes que IIEP).
- **Un libro viejo no es un error.** Las búsquedas «del período actual hacia atrás» encuentran siempre algo, aunque la institución haya cambiado el nombre del archivo; el INDEC además responde HTTP 200 con una página HTML para lo que no existe. El control es `calendario/estado.json` (`tools/calendario/`): si la base va atrás del último período publicado, revisar en la página de la fuente qué enlace sirve hoy. Una fuente nueva con calendario suma su título al mapeo del lector; una sin calendario hereda la tolerancia de su frecuencia.
- **Ante un 403, primero el canal automático.** Buscar API o dataset oficial vigente; si no existe y el dato es público, repetir el pedido con cabeceras de navegador (BCRA ITC) o con Chrome oculto (Ámbito), siempre después de un primer pedido identificado. Nunca dejar una fuente para que la actualice una persona.
- **Nunca inventar valores.** Si falta un insumo o un control falla, el mes queda vacío y se imprime un aviso.

## Conexiones y formatos

- El certificado del BCRA falla con la cadena de `certifi`: usar `verify=False` sólo para dominios del BCRA.
- `curl` de Git no valida el certificado de `api.imf.org`; `requests` sí.
- API del FMI: `https://api.imf.org/external/sdmx/2.1/data/IMF.STA,<DATASET>/<clave>?detail=dataonly`, con cabecera `Accept: application/vnd.sdmx.data+csv;version=1.0.0`. `detail=dataonly` evita varios MB de metadatos repetidos. Los valores vienen en unidades (no en millones). Las etiquetas de cada código salen de `dataflow/IMF.STA/<DATASET>/latest?references=descendants`.
- API cambiaria del BCRA: hasta 1000 registros por consulta y no acepta fechas futuras; consultar por año.
- Los PDF del BCRA mezclan formatos numéricos (1.234,56 y 1,234.56), parten palabras con espacios y tienen erratas (2.470.12). Leer sobre el texto sin espacios y probar con los casos reales.
