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
- **Calendario.** Una fila nueva la abre la fuente principal de la hoja: en IPC sólo el IPC (el IPIM sale antes y abría un mes sin IPC, con -1 y `#DIV/0!` en Fiscal y TCR Mensual). Las hojas diarias no abren filas de sábado, domingo, 1/1, 1/5 ni 25/12 (el BCRA publica a veces el saldo del 1° de mes en feriado). Un 0 de la fuente tampoco es dato en una fila nueva.
- **Columnas de fórmulas.** Se copian hacia abajo desde la fórmula vigente más cercana al último dato (el economista a veces pega valores en las filas recientes), en toda fila posterior —nueva o existente— cuyas entradas tengan dato, incluidas las de otras hojas (`IPC!F1006`): así COMEX completa sus fórmulas cuando llega el mes y Salarios cuando llega el SIPA atrasado, y un mes sin datos queda vacío en vez de -1, 0 o `#DIV/0!`. Una columna que el economista abandonó (dejó de llenarla aunque sus entradas seguían llegando: EMAE AA:AP, Salarios J, Z y AI) no se revive. Lo decide `actualizador/formulas.py`. Las referencias relativas entre hojas se mantienen porque todas las hojas mensuales crecen una fila por mes; si el economista adelantó un mes apuntando a la barra azul de otra hoja (`TCR Mensual!T922` → `IPC!B1007`), la referencia se restaura después de insertar para que apunte al mes nuevo y no a la barra.
- **Huecos del Blue.** Ámbito no publica algunos días hábiles; esas filas quedan con F, G, H y L vacías para que el economista las impute. Por esos huecos Excel guarda la hoja sin fórmulas compartidas y el libro pesa ~1,4 MB más; no afecta los cálculos.
- **Reemplazar vs. agregar.** Las series que la fuente revisa se reescriben enteras (modo `reemplazar`); las cotizaciones de Ámbito sólo se completan hacia adelante (`agregar`). Una fecha que el Excel tiene y la fuente no publica se conserva y se lista en el log como «conservada». Una celda con fórmula del economista (`=+C2-D2` en Fiscal) nunca se pisa, salvo en columnas marcadas con `pisar_formulas` donde la fórmula es una estimación provisoria (CPI proyectado en TCR Mensual). Las diferencias de redondeo (menores a una millonésima) no cuentan como revisión. Un 0 de la fuente es «sin dato»: no completa celdas vacías ni pisa imputaciones (valores sin fórmula que el economista puso para tapar huecos de una serie).
- **COMEX.** La columna de fechas llega hasta diciembre del año del último dato y la barra azul queda fija debajo de diciembre; con el dato de enero se agregan los doce meses del año nuevo. El cuadro anual de abajo suma un año cuando existe el dato de diciembre; `X` (cuenta corriente INDEC) y `AB` (PBI nominal) se completan desde cuentas internacionales y PIB cuando el año tiene cuatro trimestres (promedio de los trimestres para el PBI, suma para la cuenta corriente).
- **Fiscal.** Los dos cuadros anuales (sumas y ratios sobre el PBI de COMEX) crecen con el mismo criterio. El año nuevo se inserta debajo del último; los rangos de fórmulas y de series de gráficos que terminaban en el último año (`VLOOKUP(…,$A$449:$AB$481,…)` de COMEX Z, el gráfico del cuadro anual) se amplían hasta el año nuevo.
- **Verificación.** Además de barra, fechas, gráficos y `#REF!`, el log avisa las fórmulas copiadas que quedan con error (`#DIV/0!`, `#N/A`).
- **Respaldos.** Cada actualización deja `bkp/coyuntura iiep reemplazar AAMMDD.xlsx`. Al cambiar de mes, los Excel del mes cerrado se comprimen en `coyuntura iiep reemplazar AAMM.zip` y sólo quedan los dos zip más nuevos: nunca más de tres meses respaldados.
- **Web.** Si `SeriesMacro` está junto a este repositorio, el Excel publicado se copia a `SeriesMacro/coyuntura/`; en la web se descarga escribiendo «abrete sesamo» en el buscador de Series Macro y pulsando Enter.

## Cuando cambia una fuente

Correr `verificar_mapeo` antes y después de tocar `mapeo.py`. Las columnas desestacionalizadas (EMAE, IPI, ISAC, ventas) coinciden al 85-95 % porque INDEC las revisa cada mes; cualquier otra columna por debajo del 80 % indica un ID mal mapeado.
