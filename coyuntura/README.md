# Excel de coyuntura

`coyuntura iiep reemplazar.xlsx` es el libro con el que un economista del IIEP arma el informe de coyuntura. Este directorio lo actualiza con las series ya descargadas en `BD.xlsx`, sin volver a consultar las fuentes.

```text
coyuntura iiep reemplazar.xlsx   Excel vigente (el que descarga SeriesMacro con «abrete sesamo»)
bkp/                             Copias fechadas AAMMDD; meses cerrados comprimidos en «… AAMM.zip»
logs/                            Un log por actualización
actualizador/                    Código: mapeo.py (fuente→destino), plan.py, excel_com.py, respaldo.py
herramientas/verificar_mapeo.py  Control del mapeo contra los valores ya cargados
herramientas/reparar_rangos_comex.py  Una sola vez (31/10/2026): AC del cuadro anual de COMEX pasó de rangos fijos hasta la fila 902 a columnas enteras
```

## Uso

```powershell
python main.py                                   # scrapers: BD.xlsx e IndiceSeries.xlsx al día
python -m coyuntura.actualizador --simular       # muestra el plan sin tocar el Excel
python -m coyuntura.actualizador                 # aplica, verifica, publica y respalda
python -m coyuntura.herramientas.verificar_mapeo # cuánto coincide cada columna con su fuente
```

Requiere Excel instalado (la edición se hace por COM con `pywin32`), por eso corre en la misma máquina que los scrapers. El Excel vigente no debe estar abierto en Excel al publicar; el trabajo se hace sobre una copia en `.trabajo/` y sólo se reemplaza el vigente si la verificación pasa.

## Criterios que respeta

- **Fuentes.** La hoja «Fuentes y Esquema» del libro documenta de dónde sale cada columna; `actualizador/mapeo.py` la traduce a (Código fuente, ID) del inventario. Las celdas con fórmula en esa hoja (`B+C`, `(D+F+H+J)/1000`) son sumas de columnas de la fuente; el Blue de Ámbito es el promedio de compra y venta.
- **Barra azul.** Debajo del último dato de cada hoja hay una fila azul (tema accent1 o accent4) que delimita los rangos de los gráficos. Las filas nuevas se insertan justo encima, así la barra baja y las referencias se extienden como cuando se inserta a mano.
- **Columnas de fórmulas.** Se copian hacia abajo desde la fórmula vigente más cercana al último dato (el economista a veces pega valores en las filas recientes). Las referencias relativas entre hojas (`IPC!F1006`, `'Tipo de cambio mensual'!J300`) se mantienen porque todas las hojas mensuales crecen una fila por mes.
- **Reemplazar vs. agregar.** Las series que la fuente revisa se reescriben enteras (modo `reemplazar`); las cotizaciones de Ámbito sólo se completan hacia adelante (`agregar`). Una fecha que el Excel tiene y la fuente no publica se conserva y se lista en el log como «conservada». Una celda con fórmula del economista (`=+C2-D2` en Fiscal) nunca se pisa, salvo en columnas marcadas con `pisar_formulas` donde la fórmula es una estimación provisoria (CPI proyectado en TCR Mensual). Las diferencias de redondeo (menores a una millonésima) no cuentan como revisión. Un 0 de la fuente es «sin dato»: no completa celdas vacías ni pisa imputaciones (valores sin fórmula que el economista puso para tapar huecos de una serie).
- **COMEX.** La columna de fechas llega hasta diciembre del año del último dato y la barra azul queda fija debajo de diciembre; con el dato de enero se agregan los doce meses del año nuevo. El cuadro anual de abajo suma un año cuando existe el dato de diciembre; `X` (cuenta corriente INDEC) y `AB` (PBI nominal) se completan desde cuentas internacionales y PIB cuando el año tiene cuatro trimestres (promedio de los trimestres para el PBI, suma para la cuenta corriente).
- **Fiscal.** Los dos cuadros anuales (sumas y ratios sobre el PBI de COMEX) crecen con el mismo criterio.
- **Respaldos.** Cada actualización deja `bkp/coyuntura iiep reemplazar AAMMDD.xlsx`. Al cambiar de mes, los Excel del mes cerrado se comprimen en `coyuntura iiep reemplazar AAMM.zip` y sólo quedan los dos zip más nuevos: nunca más de tres meses respaldados.
- **Web.** Si `SeriesMacro` está junto a este repositorio, el Excel publicado se copia a `SeriesMacro/coyuntura/`; en la web se descarga escribiendo «abrete sesamo» en el buscador de Series Macro y pulsando Enter.

## Cuando cambia una fuente

Correr `verificar_mapeo` antes y después de tocar `mapeo.py`. Las columnas desestacionalizadas (EMAE, IPI, ISAC, ventas) coinciden al 85-95 % porque INDEC las revisa cada mes; cualquier otra columna por debajo del 80 % indica un ID mal mapeado.
