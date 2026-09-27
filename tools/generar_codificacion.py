"""Actualiza el inventario maestro en un libro separado de la base de datos."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

if __package__:
    from .metadata_series import mejorar_metadatos
else:
    from metadata_series import mejorar_metadatos


ROOT = Path(__file__).resolve().parents[1]
DB_FILE = ROOT / "BD.xlsx"
INDEX_FILE = ROOT / "IndiceSeries.xlsx"
TEMP_FILE = ROOT / ".IndiceSeries.tmp.xlsx"
CODE_SHEETS = {
    "Referencia_Codigos", "Parentesco_Codigos", "Introduccion_Codigos",
    "Mapa_Tematico", "Referencias",
}
INVENTORY_COLUMNS = [
    "ID", "Código fuente", "Nombre serie", "Variable", "Unidad", "Valoración", "Descripción",
    "Frecuencia", "Pestaña BD", "Columna BD", "Archivo origen", "Hoja origen",
    "Origen", "Fuente", "Catálogo ID", "Dataset ID", "Distribución ID",
    "Título dataset", "Tema dataset", "Responsable dataset", "Fuente de valores",
    "Fecha inicio", "Fecha fin", "Estado", "Institución", "Área",
    "Subárea 1", "Subárea 2", "Subárea 3", "Tema", "Desde", "Hasta",
]
CLASSIFICATION_COLUMNS = ["Institución", "Área", "Subárea 1", "Subárea 2", "Subárea 3", "Tema"]


def _normalizar_valoracion(
    valor: object, unidad: object, codigo_fuente: object = None,
    dataset_id: object = None, distribucion_id: object = None,
) -> str:
    """Separa ausencia de dato de magnitudes sin valoración nominal/real."""
    actual = str(valor).strip() if pd.notna(valor) else ""
    if actual and actual not in {"No aplica / no informado", "No informado"}:
        return actual
    texto = str(unidad).casefold().strip() if pd.notna(unidad) else ""
    if str(codigo_fuente).strip() == "datos.gob.ar":
        # Canastas: datos.gob.ar/dataset/sspm_444 y datasets 445/446.
        # Rubros: datos.gob.ar/dataset/sspm_458/archivo/sspm_458.1.
        if str(dataset_id).strip() in {"444", "445", "446"} and texto == "pesos":
            return "Precios corrientes"
        if str(distribucion_id).strip() == "458.1" and texto == "pesos":
            return "Precios corrientes"
    monetaria = ("peso", "dólar", "dolar", "usd", "ars", "moneda", "$", "u$s")
    if any(marca in texto for marca in monetaria):
        return "No informado"
    if re.fullmatch(r"(?:19|20)\d{2}\s*=\s*100", texto):
        return "No aplica"
    sin_valoracion = (
        "%", "porcentaje", "variación porcentual", "índice", "indice",
        "tonelada", "personas", "hogares", "unidades", "prestaciones",
        "metros cúbicos", "m3", "gwh", "días", "dias", "años", "tasas",
    )
    return "No aplica" if any(marca in texto for marca in sin_valoracion) else "No informado"


def _load_current_inventory() -> pd.DataFrame:
    source = INDEX_FILE if INDEX_FILE.is_file() else DB_FILE
    with pd.ExcelFile(source) as book:
        if "Codificacion" not in book.sheet_names:
            return pd.DataFrame(columns=INVENTORY_COLUMNS)
        data = pd.read_excel(book, sheet_name="Codificacion", dtype=object)
    return data if "ID" in data.columns else pd.DataFrame(columns=INVENTORY_COLUMNS)


def cargar_indice() -> pd.DataFrame:
    return _load_current_inventory()


def _normalize_inventory(inventory: pd.DataFrame, current: pd.DataFrame | None = None) -> pd.DataFrame:
    result = inventory.copy()
    if "ID origen" in result:
        result["ID"] = result["ID origen"]
        result = result.drop(columns=["ID origen"])
    if "ID" not in result:
        raise ValueError("El inventario no contiene 'ID'")
    if "Unidad" not in result and "Unidades" in result:
        result = result.rename(columns={"Unidades": "Unidad"})
    if "Código fuente" not in result:
        result["Código fuente"] = "datos.gob.ar"
    result["ID"] = result["ID"].astype(str).str.strip()
    result["Código fuente"] = result["Código fuente"].astype(str).str.strip()
    result = result[result["ID"].ne("") & result["ID"].ne("nan")]
    result = result.drop_duplicates(["Código fuente", "ID"], keep="last")
    for column in INVENTORY_COLUMNS:
        if column not in result:
            result[column] = None
    result["Valoración"] = [
        _normalizar_valoracion(valor, unidad, codigo, dataset, distribucion)
        for valor, unidad, codigo, dataset, distribucion in zip(
            result["Valoración"], result["Unidad"], result["Código fuente"],
            result["Dataset ID"], result["Distribución ID"],
        )
    ]

    if current is not None and not current.empty:
        previous = current.copy()
        if "Unidad" not in previous and "Unidades" in previous:
            previous = previous.rename(columns={"Unidades": "Unidad"})
        for column in INVENTORY_COLUMNS:
            if column not in previous:
                previous[column] = None
        keys = ["Código fuente", "ID"]
        for frame in (result, previous):
            frame["Código fuente"] = frame["Código fuente"].fillna("").astype(str).str.strip()
            frame["ID"] = frame["ID"].fillna("").astype(str).str.strip()
        exact = previous[keys + CLASSIFICATION_COLUMNS].drop_duplicates(keys, keep="last")
        result = result.merge(exact, on=keys, how="left", suffixes=("", "_previo"), sort=False)
        for column in CLASSIFICATION_COLUMNS:
            prior = result[f"{column}_previo"]
            empty = result[column].isna() | result[column].astype(str).str.strip().eq("")
            result.loc[empty, column] = prior.loc[empty]
            result = result.drop(columns=[f"{column}_previo"])

        dataset_keys = ["Código fuente", "Archivo origen", "Tema dataset", "Título dataset"]
        mapped = previous.groupby(dataset_keys, dropna=False)[CLASSIFICATION_COLUMNS].agg(
            lambda values: next((value for value in values if pd.notna(value) and str(value).strip()), None)
        ).reset_index()
        result = result.merge(mapped, on=dataset_keys, how="left", suffixes=("", "_dataset"), sort=False)
        for column in CLASSIFICATION_COLUMNS:
            inherited = result[f"{column}_dataset"]
            empty = result[column].isna() | result[column].astype(str).str.strip().eq("")
            result.loc[empty, column] = inherited.loc[empty]
            result = result.drop(columns=[f"{column}_dataset"])

    def format_period(value, frequency):
        date = pd.to_datetime(value, errors="coerce")
        if pd.isna(date):
            return None
        frequency = str(frequency).strip()
        if frequency == "A":
            return date.strftime("%Y")
        if frequency == "S":
            return f"{date.year:04d}-{1 if date.month <= 6 else 7:02d}"
        if frequency == "T":
            return f"{date.year:04d}-{((date.month - 1) // 3) * 3 + 1:02d}"
        if frequency == "M":
            return date.strftime("%Y-%m")
        return date.strftime("%Y-%m-%d")

    result["Desde"] = [format_period(value, freq) for value, freq in zip(result["Fecha inicio"], result["Frecuencia"])]
    result["Hasta"] = [format_period(value, freq) for value, freq in zip(result["Fecha fin"], result["Frecuencia"])]
    result = mejorar_metadatos(result)
    return result[INVENTORY_COLUMNS].sort_values(
        ["Archivo origen", "Hoja origen", "ID"], na_position="last"
    )


def _conservar_rangos_historicos(data: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    """Mantiene en el inventario las fechas ya registradas para la misma serie."""
    claves = ["Código fuente", "ID", "Pestaña BD", "Columna BD"]
    fechas = ["Fecha inicio", "Fecha fin"]
    if not all(col in data and col in current for col in claves + fechas):
        return data
    previo = current[claves + fechas].drop_duplicates(claves, keep="last")
    resultado = data.merge(previo, on=claves, how="left", suffixes=("", "_anterior"), sort=False)
    for columna, funcion in (("Fecha inicio", "min"), ("Fecha fin", "max")):
        ambas = pd.concat(
            [pd.to_datetime(resultado[columna], errors="coerce"),
             pd.to_datetime(resultado[f"{columna}_anterior"], errors="coerce")],
            axis=1,
        )
        resultado[columna] = getattr(ambas, funcion)(axis=1)
    resultado = resultado.drop(columns=[f"{columna}_anterior" for columna in fechas])
    return resultado


def _write_inventory(sheet, inventory: pd.DataFrame) -> None:
    header_fill = PatternFill("solid", fgColor="1F4E78")
    for column, name in enumerate(INVENTORY_COLUMNS, 1):
        cell = sheet.cell(1, column, name)
        cell.fill = header_fill
        cell.font = Font(color="FFFFFF", bold=True)
        cell.alignment = Alignment(horizontal="center")
    for row_number, row in enumerate(inventory.itertuples(index=False, name=None), 2):
        for column, value in enumerate(row, 1):
            sheet.cell(row_number, column, None if pd.isna(value) else value)
        for column in (22, 23):
            if sheet.cell(row_number, column).value is not None:
                sheet.cell(row_number, column).number_format = "yyyy-mm-dd"
    if len(inventory):
        table = Table(
            displayName="TablaInventarioSeries",
            ref=f"A1:{get_column_letter(len(INVENTORY_COLUMNS))}{len(inventory) + 1}",
        )
        table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
        sheet.add_table(table)
    sheet.freeze_panes = "A2"
    for column in range(1, len(INVENTORY_COLUMNS) + 1):
        sheet.column_dimensions[get_column_letter(column)].width = 22

def generar(inventory: pd.DataFrame | None = None) -> None:
    current = _load_current_inventory()
    if inventory is None:
        data = current
    else:
        data = inventory.copy()
        # Un scraper que entrega su inventario completo administra sólo los códigos
        # presentes en él; las fuentes incorporadas por otros módulos se preservan.
        managed_codes = set(data.get("Código fuente", pd.Series(dtype=str)).dropna().astype(str))
        if managed_codes and "Código fuente" in current:
            foreign = current[~current["Código fuente"].astype(str).isin(managed_codes)]
            data = pd.concat([foreign, data], ignore_index=True)
    if "Pestaña BD" in data:
        data = data[data["Pestaña BD"].ne("Comunicaciones BCRA")]
    data = _conservar_rangos_historicos(data, current)
    data = _normalize_inventory(data, current)

    if INDEX_FILE.is_file():
        book = load_workbook(INDEX_FILE)
    else:
        from openpyxl import Workbook
        book = Workbook()

    for name in CODE_SHEETS:
        if name in book.sheetnames:
            del book[name]
    if "Codificacion" in book.sheetnames:
        del book["Codificacion"]
    sheet = book.create_sheet("Codificacion", 0)
    _write_inventory(sheet, data)
    for name in list(book.sheetnames):
        if name != "Codificacion":
            del book[name]
    # La serie operativa de Heymann queda junto al inventario, no perdida al
    # final de una base con cientos de pestañas.

    try:
        book.save(TEMP_FILE)
        book.close()
        check = pd.read_excel(TEMP_FILE, sheet_name="Codificacion")
        if len(check) != len(data) or check.duplicated(["Código fuente", "ID"]).any():
            raise ValueError("La validación del inventario guardado falló")
        TEMP_FILE.replace(INDEX_FILE)
    finally:
        TEMP_FILE.unlink(missing_ok=True)


def separar_indice_de_bd() -> None:
    if not INDEX_FILE.is_file() or not DB_FILE.is_file():
        return
    book = load_workbook(DB_FILE)
    if "Codificacion" not in book.sheetnames:
        book.close()
        return
    del book["Codificacion"]
    temporal = DB_FILE.with_name(".BD_sin_indice.tmp.xlsx")
    try:
        book.save(temporal)
        book.close()
        temporal.replace(DB_FILE)
    finally:
        book.close()
        temporal.unlink(missing_ok=True)


if __name__ == "__main__":
    generar()
    separar_indice_de_bd()
