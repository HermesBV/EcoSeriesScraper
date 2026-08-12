# Instrucciones de organización

- Cada fuente debe tener un único módulo en `scrapers/`, nombrado `scraper_<FUENTE>.py` (por ejemplo, `scraper_IED.py`).
- Cada fuente guarda todos sus archivos descargados en `fuentes_BD/<FUENTE>/` (por ejemplo, `fuentes_BD/IED/`). El módulo debe crear esa carpeta si no existe.
- `main.py` es el único archivo Python en la raíz: importa y ejecuta los scrapers. No colocar lógica específica de una fuente allí.
- No crear scripts de debug permanentes. Las validaciones reutilizables deben vivir junto al scraper correspondiente o en tests.
- Al agregar, renombrar o eliminar un scraper, actualizar siempre `README.md` con la fuente, el módulo y la carpeta de descargas.
- La codificación se define en `Codificacion`; los catálogos globales están en `Referencia_Codigos` y la jerarquía padre-hijo en `Mapa_Tematico`, todas dentro de `BD.xlsx`.
- El ID estable usa `IITTTTTTTTNNNVBBUEF`: institución, ruta temática, correlativo, valoración, año base, tipo/unidad, multiplicador y frecuencia. No agregar fechas ni estado al ID.
- El correlativo `NNN` es local al par institución + ruta temática. Puede repetirse en otro contexto, pero nunca dentro del mismo par.
- No agregar rutas temáticas ni correlativos a `Referencia_Codigos`: los temas se resuelven por su padre en `Mapa_Tematico` y el correlativo por la fila de `Codificacion`.
- En valoración usar `R` para precios constantes (también llamados reales), `C` para precios corrientes y `X` cuando no aplica.
- Monedas y tipos de valor usan un solo carácter según la hoja `Referencia_Codigos` de `BD.xlsx`; no escribir `ARS`, `USD`, `%` ni símbolos dentro del ID.
- Los únicos metadatos temporales/de relación previstos actualmente son: Fecha inicio, Fecha fin, Estado, Reemplaza por y Reemplaza a. Una serie vigente tiene Fecha fin vacía; al cerrarse se completa sin cambiar el ID.
- Al crear una serie, registrar todos los segmentos. Si un dato no está verificado, usar el código explícito de no asignado/no aplica y mantener el ID como provisorio.
- Toda modificación del esquema o del diccionario debe actualizar las hojas correspondientes de `BD.xlsx`, este archivo y `README.md`.
- Al agregar o modificar series, ejecutar `python tools/generar_codificacion.py` y revisar los IDs provisorios; el generador no reemplaza la verificación humana de institución, unidad, clasificación o vigencia.
- Las fechas de `BD.xlsx` deben seguir siendo fechas reales comparables, nunca strings: `yyyy` para anuales; `yyyy-mm` para semestrales, trimestrales y mensuales; `yyyy-mm-dd` para diarias.
- `Codigos.xlsx` es una entrada operativa temporal del scraper IED y debe permanecer separado de `BD.xlsx` hasta que la extracción deje de depender de una lista manual de IDs. No confundirlo con la hoja `Referencia_Codigos`, que documenta la nueva codificación.
