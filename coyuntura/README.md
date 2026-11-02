# Excel de coyuntura

`coyuntura iiep reemplazar.xlsx` es el libro con el que un economista del IIEP arma el informe de coyuntura. Este directorio lo actualiza con las series ya descargadas en `BD.xlsx`, sin volver a consultar las fuentes.

```text
coyuntura iiep reemplazar.xlsx   Excel vigente (el que descarga SeriesMacro con «abrete sesamo»)
coyuntura iiep fuentes y esquema.xlsx  Hoja «Fuentes y Esquema» separada del libro; privada, no se versiona
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

- **Fuentes.** La hoja «Fuentes y Esquema» (desde el 01/11/2026 en `coyuntura iiep fuentes y esquema.xlsx`, fuera de git porque tenía datos internos del equipo; el libro publicado ya no la incluye) documenta de dónde sale cada columna; `actualizador/mapeo.py` la traduce a (Código fuente, ID) del inventario. Las celdas con fórmula en esa hoja (`B+C`, `(D+F+H+J)/1000`) son sumas de columnas de la fuente; el Blue de Ámbito es el promedio de compra y venta.
- **Barra azul.** Debajo del último dato de cada hoja hay una fila azul (tema accent1 o accent4) que delimita los rangos de los gráficos. Las filas nuevas se insertan justo encima, así la barra baja y las referencias se extienden como cuando se inserta a mano.
- **Calendario.** Una fila nueva la abre la fuente principal de la hoja: en IPC sólo el IPC (el IPIM sale antes y abría un mes sin IPC, con -1 y `#DIV/0!` en Fiscal y TCR Mensual). Las hojas diarias sólo abren filas en días hábiles: los días en que el BCRA publicó el A3500 (fuera de su cobertura, lunes a viernes salvo 1/1, 1/5 y 25/12); así no entran los saldos del 1° de mes que el BCRA publica en feriados. Un 0 de la fuente no es dato.
- **Series continuas.** Ninguna serie queda con un agujero en el medio, porque los gráficos se cortan. Una fecha que el Excel tiene y la fuente no publica, o una celda vacía entre dos datos, toma el promedio del dato anterior y el posterior; el log las lista como «imputadas». Si falta un día hábil en el medio de una hoja diaria (el A3500 del 20/08/2025, el riesgo país del 31/03/2026), se inserta la fila: copia las fórmulas de la de arriba, las columnas del mapeo toman el dato de la fuente o el promedio, y las demás columnas de valores, el promedio de sus vecinas. Una fecha repetida (riesgo país 23/09/2025) se borra. Lo hace `actualizador/filas.py` antes de calcular el plan. Un número pegado como texto («1.000,00», las bandas cambiarias de 2025) se convierte a número.
- **Fórmulas en espera.** Una fórmula al final de una hoja que usa un dato que todavía no llegó (TCR Mensual T922 y V922 con el IPC de septiembre) se borra en vez de quedar con `#DIV/0!`; la columna sigue viva y se vuelve a estirar sola cuando el dato llegue.
- **Columnas de fórmulas.** Se copian hacia abajo desde la fórmula vigente más cercana al último dato (el economista a veces pega valores en las filas recientes), en toda fila posterior —nueva o existente— cuyas entradas tengan dato, incluidas las de otras hojas (`IPC!F1006`): así COMEX completa sus fórmulas cuando llega el mes y Salarios cuando llega el SIPA atrasado, y un mes sin datos queda vacío en vez de -1, 0 o `#DIV/0!`. Una columna que el economista abandonó (dejó de llenarla aunque sus entradas seguían llegando: EMAE AA:AP, Salarios J, Z y AI) no se revive. Lo decide `actualizador/formulas.py`. Las referencias relativas entre hojas se mantienen porque todas las hojas mensuales crecen una fila por mes; si el economista adelantó un mes apuntando a la barra azul de otra hoja (`TCR Mensual!T922` → `IPC!B1007`), la referencia se restaura después de insertar para que apunte al mes nuevo y no a la barra.
- **Reemplazar vs. agregar.** Todas las series se reescriben enteras desde la fuente (modo `reemplazar`), también Blue, MEP y riesgo país de Ámbito: el 01/11/2026 se corrigió así un corrimiento de un día hábil en partes del historial cargado a mano. El modo `agregar` (sólo completar hacia adelante) queda disponible en `mapeo.py`. Un valor del economista anterior al comienzo de la fuente se respeta; sin dato anterior o posterior para promediar se conserva y se lista como «conservada». Una celda con fórmula del economista (`=+C2-D2` en Fiscal) nunca se pisa, salvo en columnas marcadas con `pisar_formulas` donde la fórmula es una estimación provisoria (CPI proyectado en TCR Mensual). Las diferencias de redondeo (menores a una millonésima) no cuentan como revisión. Un 0 de la fuente es «sin dato» y se imputa como cualquier otro hueco.
- **COMEX.** La columna de fechas llega hasta diciembre del año del último dato y la barra azul queda fija debajo de diciembre; con el dato de enero se agregan los doce meses del año nuevo. El cuadro anual de abajo suma un año cuando existe el dato de diciembre; `X` (cuenta corriente INDEC) y `AB` (PBI nominal) se completan desde cuentas internacionales y PIB cuando el año tiene cuatro trimestres (promedio de los trimestres para el PBI, suma para la cuenta corriente).
- **Fiscal.** Los dos cuadros anuales (sumas y ratios sobre el PBI de COMEX) crecen con el mismo criterio. El año nuevo se inserta debajo del último; los rangos de fórmulas y de series de gráficos que terminaban en el último año (`VLOOKUP(…,$A$449:$AB$481,…)` de COMEX Z, el gráfico del cuadro anual) se amplían hasta el año nuevo.
- **Verificación.** Además de barra, fechas, gráficos y `#REF!`, el log avisa las fórmulas copiadas que quedan con error (`#DIV/0!`, `#N/A`). Errores que ya venían del libro y no se tocan: IPC H11:H75 (`=+F11/#REF!-1`, referencias rotas de los años noventa) y COMEX Z:AA de 2026, que buscan el año en el cuadro anual y se resuelven cuando se agregue 2026.
- **Respaldos.** Cada actualización deja `bkp/coyuntura iiep reemplazar AAMMDD.xlsx`. Al cambiar de mes, los Excel del mes cerrado se comprimen en `coyuntura iiep reemplazar AAMM.zip` y sólo quedan los dos zip más nuevos: nunca más de tres meses respaldados.
- **Web.** Si `SeriesMacro` está junto a este repositorio, el Excel publicado se copia a `SeriesMacro/coyuntura/`; en la web se descarga escribiendo «abrete sesamo» en el buscador de Series Macro y pulsando Enter.

## Cuando cambia una fuente

Correr `verificar_mapeo` antes y después de tocar `mapeo.py`. Las columnas desestacionalizadas (EMAE, IPI, ISAC, ventas) coinciden al 85-95 % porque INDEC las revisa cada mes; cualquier otra columna por debajo del 80 % indica un ID mal mapeado.
