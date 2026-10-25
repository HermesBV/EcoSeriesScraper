# Reservas Internacionales de la Argentina (IIEP)

Base del Documento de Trabajo IIEP N° 126: Impávido, Monteforte y Romero (2026), «Estructura patrimonial y liquidez del Banco Central: descomponiendo las reservas internacionales de la Argentina (2000-2026)». Documento: https://ojs.economicas.uba.ar/DT-IIEP/article/view/3723. La nota metodológica de la base se publica como anexo en SeriesMacro (`anexos/IIEP/Metodologia_reservas_internacionales.pdf`).

## Qué es de quién

| Serie IIEP | Origen | ¿Se actualiza sola? |
|---|---|---|
| Brutas | BCRA, IRFCL I.A (idéntica al dataset IRFCL del FMI en los 312 meses) | Sí |
| Custodia | Compilación IIEP: encajes + cheques/CDI + depósitos del Tesoro en ME (notas del país; balance BCRA antes de 2002-03) | Sí, desde las notas |
| Propias | Brutas − custodia | Sí |
| Prestadas | −(II.1 + II.2 + II.3) − custodia | Sí |
| Netas | Propias − prestadas (con reclasificaciones 2002-2004 y 2015, ya cerradas) | Sí |
| Netas según planilla | Brutas + II.1 + II.2 + II.3 | Sí |
| Líquidas | Brutas − oro − DEG − porción no activada del swap chino (Anexo A.9 del DT) | Sí |
| Propias líquidas | Líquidas − custodia | Sí |
| RIN-EFF | BCRA, sólo por pedido de información pública (Trámite 10.532) | **No**: queda fija hasta 2026-07 |

## Actualización

- `scraper_BCRA_cotizaciones.py` y `scraper_BCRA_irfcl.py` corren antes y dejan en `BD.xlsx` las hojas `BCRA Cotizaciones D` y `BCRA IRFCL M`.
- `scraper_IIEP_reservas_internacionales.py` toma la base de los autores hasta `CORTE_HISTORICO` (2026-08) desde `fuentes_BD/IIEP/ReservasInternacionales/` y calcula los meses siguientes. Si falta ese CSV, conserva lo ya publicado en `BD.xlsx` hasta el corte.
- Para ampliar la base con una nueva entrega de los autores: reemplazar el CSV y mover `CORTE_HISTORICO`.

## Swap con China (líquidas)

- Acuerdo marco: 130.000 millones de CNY desde dic-2018, renovado en ago-2026. Porción activada: 35.000 millones de CNY desde ene-2023. Sólo se descuenta la porción no activada: `|II.2| − otros swaps − 35.000 × tipo de pase CNY`.
- II.2 replica el nocional chino con un desvío máximo de 1,27%. Si se aparta más de 3%, hay otro swap: en oct-nov 2025, el del Tesoro de EE.UU. (2.511,5 millones), declarado en la banda «hasta 1 mes» de II.2. El scraper descuenta esa banda y avisa; si aun así no aísla el swap chino, deja las líquidas vacías.
- El tipo de pase es el último del mes publicado por la API cambiaria del BCRA (USD por CNY).
- Si cambian el nocional o el tramo activado, actualizar `NOCIONAL_SWAP_CHINA_CNY` y `TRAMO_ACTIVADO_CNY`.

## Validación realizada (25/10/2026)

- Brutas del FMI contra la base IIEP: 312 de 312 meses exactos (2000-09 a 2026-08), incluido nov-2025, que la planilla PDF publicada no recoge revisado.
- Líquidas calculadas contra la base, ene-2023 a ago-2026: desvío máximo 0,25 (redondeo de los autores a un decimal en oct-nov 2025).
- Netas según planilla y prestadas + custodia: exactas desde dic-2022.
- Custodia de las notas del país contra la base: 188 de 215 meses exactos desde 2007-10. Los demás son diferencias de método (el IIEP no suma cheques cancelatorios antes de 2013-07) o ajustes de los autores (2014-11: 3,0; 2016-08: −100,0). Las planillas anteriores a 2007-10 tienen otro formato de notas y no se leen.
