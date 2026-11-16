"""IPC: principales aperturas de la canasta por región.

El INDEC actualiza en el lugar ``sh_ipc_aperturas.xls``, con tres hojas (índices,
variación mensual y variación interanual) y, en cada una, un bloque por región con
45 filas. El nivel general y las doce divisiones ya llegan por ``sh_ipc_divisiones.xls``
(``scraper_INDEC_ipc``), así que acá sólo se toman las aperturas más finas
(alimentos, alquiler, combustibles, prepagas…).

Es la fuente correcta de las aperturas de GBA desde 2017: el cuadro 4.2.4 del
Ministerio las empalma por posición con otra clasificación y no se publica.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from scrapers.indec_workbooks import slug, text
from scrapers.periodic_workbooks import ROOT, Source, download
from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import cargar_indice, generar

SOURCE = Source("indec-ipc-aperturas", "INDEC", "IPC aperturas", "INDEC / Economía / Precios / IPC",
                "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls", "M", "INDEC IPC Ap")

# Hoja del libro -> (pestaña de BD.xlsx, unidad).
HOJAS = {
    "Índices aperturas": ("INDEC IPC Ap Índices", "Índice dic-2016=100"),
    "Variación mensual aperturas": ("INDEC IPC Ap Var. mensual", "%"),
    "Var. interanual aperturas": ("INDEC IPC Ap Var. interanual", "%"),
}
# Filas que ya publica scraper_INDEC_ipc desde sh_ipc_divisiones.xls.
YA_PUBLICADAS = {
    "Nivel general", "Alimentos y bebidas no alcohólicas", "Bebidas alcohólicas y tabaco",
    "Prendas de vestir y calzado", "Vivienda, agua, electricidad, gas y otros combustibles",
    "Equipamiento y mantenimiento del hogar", "Salud", "Transporte", "Comunicación",
    "Recreación y cultura", "Educación", "Restaurantes y hoteles", "Bienes y servicios varios",
}


def _es_fecha(valor: object) -> bool:
    return isinstance(valor, pd.Timestamp) or hasattr(valor, "year")


def extraer(path: Path, url: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Una pestaña por hoja del libro, con una columna «Región X - Apertura» por serie."""
    hojas: dict[str, pd.DataFrame] = {}
    inventario: list[dict] = []
    with pd.ExcelFile(path) as libro:
        for hoja, (pestana, unidad) in HOJAS.items():
            if hoja not in libro.sheet_names:
                raise ValueError(f"{path.name} no trae la hoja «{hoja}»")
            crudo = pd.read_excel(libro, sheet_name=hoja, header=None)
            titulo = text(crudo.iat[0, 0])
            detalle = next((text(v) for v in crudo.iloc[1:6, 0] if text(v) and "aperturas" in text(v)), "")
            series: dict[str, pd.Series] = {}
            region, fechas = "", None
            for fila in range(len(crudo)):
                rotulo = text(crudo.iat[fila, 0])
                if _es_fecha(crudo.iat[fila, 1]):
                    # Encabezado de bloque: la región y los meses van en la misma fila.
                    region = rotulo
                    columnas = [c for c in range(1, crudo.shape[1]) if _es_fecha(crudo.iat[fila, c])]
                    fechas = pd.DatetimeIndex([pd.Timestamp(crudo.iat[fila, c]).normalize() for c in columnas])
                    continue
                if fechas is None or not rotulo or rotulo in YA_PUBLICADAS:
                    continue
                if rotulo.startswith(("Nota", "Fuente", "Los índices", "Dentro de")):
                    region, fechas = "", None  # notas al pie: terminó el último bloque
                    continue
                valores = pd.to_numeric(crudo.iloc[fila, columnas], errors="coerce")
                if valores.notna().sum() == 0:
                    continue
                nombre = f"{region} - {rotulo}"
                if nombre in series:
                    raise ValueError(f"{hoja}: la serie «{nombre}» aparece dos veces")
                series[nombre] = pd.Series(valores.to_numpy(), index=fechas)
                presentes = fechas[valores.notna().to_numpy()]
                inventario.append({
                    "ID": f"{slug(hoja)}|{slug(nombre)}", "Código fuente": SOURCE.code, "Nombre serie": nombre,
                    "Variable": slug(nombre), "Unidad": unidad, "Valoración": "No aplica",
                    "Descripción": f"{titulo}. {detalle}".rstrip(". ") + ".", "Frecuencia": "M",
                    "Pestaña BD": pestana, "Columna BD": nombre, "Archivo origen": path.name, "Hoja origen": hoja,
                    "Origen": SOURCE.origin, "Fuente": url, "Catálogo ID": SOURCE.code, "Dataset ID": SOURCE.code,
                    "Título dataset": titulo, "Tema dataset": SOURCE.route, "Responsable dataset": SOURCE.origin,
                    "Fuente de valores": "Excel oficial", "Fecha inicio": presentes.min(), "Fecha fin": presentes.max(),
                    "Estado": "VIGENTE", "Institución": "INDEC", "Tema": "Precios",
                })
            if not series:
                raise ValueError(f"{path.name}: no se encontraron aperturas en «{hoja}»")
            tabla = pd.DataFrame(series).sort_index()
            hojas[pestana] = tabla.rename_axis("fecha").reset_index()
    return hojas, pd.DataFrame(inventario)


def ejecutar() -> None:
    path, url = download(SOURCE)
    hojas, indice = extraer(path, url)
    actual = cargar_indice()
    obsoletas = set(actual.loc[actual["Código fuente"].eq(SOURCE.code), "Pestaña BD"].dropna()) if not actual.empty else set()
    guardar_datos_preservando_formato(ROOT / "BD.xlsx", hojas, {nombre: "M" for nombre in hojas}, obsoletas)
    generar(indice)
    print(f"{SOURCE.code}: {len(indice)} series en {len(hojas)} hojas", flush=True)


if __name__ == "__main__":
    ejecutar()
