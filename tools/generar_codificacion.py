"""Regenera las hojas de codificación incluidas en BD.xlsx."""

from __future__ import annotations

import re
import sys
from copy import copy
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo


RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from scrapers.scraper_IED import (
    EXCEL_URLS,
    MAPEO_ORIGEN_ARCHIVO,
    frecuencia_de_pestana,
    nombre_pestana,
)
from scrapers.scraper_BCRA_comunicaciones import FECHA_DESDE as FECHA_DESDE_BCRA


# Solo códigos globales: su significado no depende de un padre.
REFERENCIAS = [
    ["Institución", "ME", "Ministerio de Economía de la Nación", "Sigla: MECON"],
    ["Institución", "BC", "Banco Central de la República Argentina", "Sigla: BCRA"],
    ["Valoración", "R", "Precios constantes", "Usar también para series llamadas reales"],
    ["Valoración", "C", "Precios corrientes", "Sin año base: BB=00"],
    ["Valoración", "X", "No aplica", "Tasas, cantidades u otras series sin valoración constante/corriente"],
    ["Tipo/unidad", "0", "No aplica o por asignar", ""],
    ["Tipo/unidad", "1", "ARS", "Peso argentino"],
    ["Tipo/unidad", "2", "USD", "Dólar estadounidense"],
    ["Tipo/unidad", "3", "EUR", "Euro"],
    ["Tipo/unidad", "4", "XDR", "Derecho especial de giro"],
    ["Tipo/unidad", "P", "Porcentaje", "Valor expresado de 0 a 100"],
    ["Tipo/unidad", "I", "Índice", "Registrar año base si corresponde"],
    ["Tipo/unidad", "Q", "Cantidad", "Personas, unidades físicas u otros conteos"],
    ["Tipo/unidad", "T", "Tasa", "Cuando no corresponda porcentaje"],
    ["Tipo/unidad", "R", "Razón", "Cociente o relación"],
    ["Tipo/unidad", "O", "Otro", "Documentar antes de confirmar el ID"],
    ["Multiplicador", "0", "Unidades", "10^0"],
    ["Multiplicador", "3", "Miles", "10^3"],
    ["Multiplicador", "6", "Millones", "10^6"],
    ["Multiplicador", "9", "Mil millones", "10^9"],
    ["Frecuencia", "D", "Diaria", ""],
    ["Frecuencia", "M", "Mensual", ""],
    ["Frecuencia", "T", "Trimestral", ""],
    ["Frecuencia", "S", "Semestral", ""],
    ["Frecuencia", "A", "Anual", ""],
    ["Frecuencia", "I", "Irregular o eventual", ""],
    ["Estado", "VIGENTE", "Serie abierta", "Fecha fin debe quedar vacía"],
    ["Estado", "CERRADA", "Serie finalizada", "Fecha fin debe estar completa"],
    ["Estado", "SUSTITUIDA", "Reemplazada por otra serie", "Completar Reemplazada por"],
]

# Los códigos locales pueden repetirse; la ruta completa surge del padre.
PARENTESCO_CODIGOS = [
    ["Tema", "ME", 1, "00", "I:ME", "00000000", "Sin clasificar", "Solo para borradores"],
    ["Tema", "ME", 1, "10", "I:ME", "10000000", "Actividad económica", ""],
    ["Tema", "ME", 2, "10", "T:10000000", "10100000", "Cuentas nacionales", ""],
    ["Tema", "ME", 3, "01", "T:10100000", "10100100", "Producto e ingreso", ""],
    ["Tema", "ME", 1, "20", "I:ME", "20000000", "Trabajo e ingresos", ""],
    ["Tema", "ME", 1, "30", "I:ME", "30000000", "Precios", ""],
    ["Tema", "ME", 1, "40", "I:ME", "40000000", "Sector externo", ""],
    ["Tema", "ME", 1, "50", "I:ME", "50000000", "Finanzas públicas", ""],
    ["Tema", "ME", 1, "60", "I:ME", "60000000", "Dinero y bancos", ""],
    ["Tema", "ME", 1, "70", "I:ME", "70000000", "Finanzas y mercados", ""],
    ["Tema", "ME", 1, "80", "I:ME", "80000000", "Contexto internacional", ""],
    ["Tema", "ME", 1, "90", "I:ME", "90000000", "Otros", ""],
    ["Tema", "BC", 1, "10", "I:BC", "10000000", "Normativa", ""],
    ["Tema", "BC", 2, "10", "T:10000000", "10100000", "Comunicaciones", ""],
]

TEMA_POR_ORIGEN = {
    "Actividad: Actividad_IED": "10000000",
    "Empleo e Ingresos: Apendice3A": "20000000",
    "Precios: Apendice4": "30000000",
    "Sector Externo: Apendice5": "40000000",
    "Finanzas Públicas: Apendice6": "50000000",
    "Dinero y Bancos: Apendice8": "60000000",
    "Finanzas: Apendice-Financiero": "70000000",
    "Contexto Internacional": "80000000",
}


def tipo_unidad(variable: str) -> str:
    texto = variable.lower()
    if "usd" in texto or "dólar" in texto or "dolar" in texto:
        return "2"
    if any(fragmento in texto for fragmento in ["$", "peso", "salario", "remuneración", "remuneracion", "haber", "recaudación", "recaudacion", "monetaria"]):
        return "1"
    if "%" in variable or "porcentaje" in texto:
        return "P"
    if any(fragmento in texto for fragmento in ["índice", "indice", "ipc ", "cer", "uva"]):
        return "I"
    if any(fragmento in texto for fragmento in ["población", "poblacion", "ocupados", "desocupados", "personas", "hogares"]):
        return "Q"
    if texto.startswith("tasa "):
        return "T"
    return "O"


def multiplicador(variable: str) -> str:
    texto = variable.lower()
    if "millon" in texto or "mill." in texto:
        return "6"
    return "3" if re.search(r"\bmiles?\b", texto) else "0"


def valoracion(pestana: str) -> str:
    texto = pestana.lower()
    if " real" in texto:
        return "R"
    return "C" if "corr." in texto or "corriente" in texto else "X"


def anio_base(variable: str, pestana: str, valor: str) -> str:
    if valor != "R" and "=100" not in variable.replace(" ", ""):
        return "00"
    texto = f"{variable} {pestana}"
    coincidencia = re.search(r"\b((?:19|20)\d{2})\s*(?:=\s*100|BASE)", texto, re.I)
    if coincidencia is None:
        coincidencia = re.search(r"BASE\s*((?:19|20)\d{2})", texto, re.I)
    return coincidencia.group(1)[-2:] if coincidencia else "00"


def construir_series() -> list[list[object]]:
    codigos = pd.read_excel(RAIZ / "Codigos.xlsx", dtype=str)
    bd = pd.read_excel(RAIZ / "BD.xlsx", sheet_name=None)
    contadores: dict[tuple[str, str], int] = {}
    existentes: dict[str, pd.Series] = {}
    if "Codificacion" in bd:
        cruda = pd.read_excel(RAIZ / "BD.xlsx", sheet_name="Codificacion", header=None)
        encabezado = cruda.index[cruda.iloc[:, 0].eq("ID provisorio")]
        if len(encabezado):
            fila_encabezado = int(encabezado[0])
            registradas = cruda.iloc[fila_encabezado + 1:].copy()
            registradas.columns = cruda.iloc[fila_encabezado]
            registradas = registradas.dropna(how="all")
        else:
            registradas = pd.DataFrame()
        for _, registrada in registradas.iterrows():
            id_fuente = registrada.get("ID fuente", registrada.get("ID anterior", ""))
            id_fuente = str(id_fuente).strip()
            if id_fuente and id_fuente.lower() != "nan":
                existentes[id_fuente] = registrada
                institucion_registrada = str(registrada["Institución"]).strip()
                contexto = (institucion_registrada, str(registrada["Ruta temática"]).zfill(8))
                contadores[contexto] = max(contadores.get(contexto, 0), int(registrada["Correlativo"]))
    limite_vigencia = pd.Timestamp(datetime.now().year - 3, 1, 1)
    resultado = []

    def conservar(valor: object) -> object:
        return None if pd.isna(valor) else valor

    for _, fila in codigos.iterrows():
        origen = str(fila.iloc[0]).strip()
        id_fuente = str(fila.iloc[1]).strip()
        variable = str(fila.iloc[2]).strip()
        pestana = nombre_pestana(fila.iloc[3])
        fuente = EXCEL_URLS.get(MAPEO_ORIGEN_ARCHIVO.get(origen), "URL no encontrada")
        if id_fuente in existentes:
            anterior = existentes[id_fuente]
            id_previo = str(anterior["ID provisorio"])
            id_actualizado = f"ME{id_previo[2:]}"
            resultado.append([
                id_actualizado, "ME",
                str(anterior["Ruta temática"]).zfill(8), str(anterior["Correlativo"]).zfill(3),
                anterior["Valoración"], str(anterior["Año base"]).zfill(2), anterior["Tipo/unidad"],
                str(anterior["Multiplicador"]), anterior["Frecuencia"],
                variable, pestana, origen, conservar(anterior["Fecha inicio"]), conservar(anterior["Fecha fin"]),
                anterior["Estado"], conservar(anterior["Reemplaza por"]), conservar(anterior["Reemplaza a"]),
                id_fuente, fuente,
            ])
            continue
        institucion = "ME"
        tema = TEMA_POR_ORIGEN.get(origen, "00000000")
        contexto = (institucion, tema)
        contadores[contexto] = contadores.get(contexto, 0) + 1
        correlativo = f"{contadores[contexto]:03d}"
        frecuencia = frecuencia_de_pestana(pestana) or "I"
        valor = valoracion(pestana)
        base = anio_base(variable, pestana, valor)
        unidad = tipo_unidad(variable)
        escala = multiplicador(variable)
        codigo = f"{institucion}{tema}{correlativo}{valor}{base}{unidad}{escala}{frecuencia}"

        fechas = pd.to_datetime(bd[pestana]["fecha"], errors="coerce").dropna()
        fecha_inicio = fechas.min()
        ultima_observacion = fechas.max()
        vigente = ultima_observacion >= limite_vigencia
        fecha_fin = None if vigente else ultima_observacion
        estado = "VIGENTE" if vigente else "CERRADA"
        resultado.append([
            codigo, institucion, tema, correlativo, valor, base, unidad, escala,
            frecuencia, variable, pestana, origen,
            fecha_inicio, fecha_fin, estado, None, None,
            id_fuente, fuente,
        ])
    resultado.append([
        "BC10100000001X00O0I", "BC", "10100000", "001", "X", "00", "O", "0", "I",
        "Comunicaciones BCRA tipos A, B, C y P",
        "Comunicaciones BCRA", "BCRA: Buscador de comunicaciones",
        pd.Timestamp(FECHA_DESDE_BCRA), None, "VIGENTE", None, None,
        "comunicaciones-A-B-C-P-desde-2026",
        "https://www.bcra.gob.ar/buscador-de-comunicaciones/",
    ])
    return resultado


def agregar_tabla(hoja, nombre: str, fila_encabezado: int = 1) -> None:
    relleno = PatternFill("solid", fgColor="1F4E78")
    for celda in hoja[fila_encabezado]:
        celda.fill = relleno
        celda.font = Font(color="FFFFFF", bold=True)
        celda.alignment = Alignment(horizontal="center")
    hoja.freeze_panes = f"A{fila_encabezado + 1}"
    referencia = f"A{fila_encabezado}:{get_column_letter(hoja.max_column)}{hoja.max_row}"
    tabla = Table(displayName=nombre, ref=referencia)
    tabla.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
    hoja.add_table(tabla)
    for columna in range(1, hoja.max_column + 1):
        largo = max(len(str(hoja.cell(fila, columna).value or "")) for fila in range(1, min(hoja.max_row, 200) + 1))
        hoja.column_dimensions[get_column_letter(columna)].width = min(max(largo + 2, 10), 48)


def actualizar_tabla(
    hoja,
    encabezados: list[str],
    filas: list[list[object]],
    nombre_tabla: str | None,
    fila_encabezado: int = 1,
) -> None:
    """Actualiza valores sin recrear la hoja ni alterar su formato manual."""
    fila_datos = fila_encabezado + 1
    filas_anteriores = max(0, hoja.max_row - fila_encabezado)
    columnas_anteriores = hoja.max_column
    plantilla = fila_datos if filas_anteriores else None

    for columna, encabezado in enumerate(encabezados, 1):
        hoja.cell(fila_encabezado, columna).value = encabezado
    for fila_indice, valores in enumerate(filas, fila_datos):
        if plantilla and fila_indice > hoja.max_row:
            for columna in range(1, len(encabezados) + 1):
                origen = hoja.cell(plantilla, min(columna, columnas_anteriores))
                destino = hoja.cell(fila_indice, columna)
                if origen.has_style:
                    destino._style = copy(origen._style)
                destino.number_format = origen.number_format
        for columna, valor in enumerate(valores, 1):
            hoja.cell(fila_indice, columna).value = valor

    ultima_fila = fila_encabezado + len(filas)
    for fila in range(ultima_fila + 1, fila_encabezado + filas_anteriores + 1):
        for columna in range(1, max(columnas_anteriores, len(encabezados)) + 1):
            hoja.cell(fila, columna).value = None
    for columna in range(len(encabezados) + 1, columnas_anteriores + 1):
        for fila in range(fila_encabezado, max(hoja.max_row, ultima_fila) + 1):
            hoja.cell(fila, columna).value = None

    referencia = f"A{fila_encabezado}:{get_column_letter(len(encabezados))}{ultima_fila}"
    if nombre_tabla:
        if nombre_tabla in hoja.tables:
            del hoja.tables[nombre_tabla]
        tabla = Table(displayName=nombre_tabla, ref=referencia)
        tabla.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
        hoja.add_table(tabla)


def generar() -> None:
    ruta_bd = RAIZ / "BD.xlsx"
    ruta_temporal = RAIZ / ".BD_codificacion.tmp.xlsx"
    libro = load_workbook(ruta_bd)
    for nombre in ["Introduccion_Codigos", "Referencias", "Mapa_Tematico"]:
        if nombre in libro.sheetnames:
            del libro[nombre]
    referencias = libro["Referencia_Codigos"] if "Referencia_Codigos" in libro.sheetnames else libro.create_sheet("Referencia_Codigos")
    actualizar_tabla(
        referencias,
        ["Segmento", "Código", "Significado", "Regla/contexto"],
        REFERENCIAS,
        "TablaReferenciaCodigos",
    )

    parentesco = libro["Parentesco_Codigos"] if "Parentesco_Codigos" in libro.sheetnames else libro.create_sheet("Parentesco_Codigos")
    actualizar_tabla(
        parentesco,
        ["Tipo nodo", "Institución", "Nivel temático", "Código local", "Parent", "Ruta completa", "Nombre", "Notas"],
        PARENTESCO_CODIGOS,
        "TablaParentescoCodigos",
    )

    series = libro["Codificacion"] if "Codificacion" in libro.sheetnames else libro.create_sheet("Codificacion")
    encabezados_series = [
        "ID provisorio", "Institución", "Ruta temática", "Correlativo", "Valoración",
        "Año base", "Tipo/unidad", "Multiplicador", "Frecuencia", "Variable",
        "Pestaña BD", "Origen", "Fecha inicio", "Fecha fin", "Estado",
        "Reemplaza por", "Reemplaza a", "ID fuente", "Fuente",
    ]
    actualizar_tabla(
        series,
        encabezados_series,
        construir_series(),
        None,
        fila_encabezado=5,
    )
    if "TablaSeriesCodificadas" in series.tables:
        del series.tables["TablaSeriesCodificadas"]
    for fila in range(6, series.max_row + 1):
        series.cell(fila, 13).number_format = "yyyy-mm-dd"
        series.cell(fila, 14).number_format = "yyyy-mm-dd"

    # Todas las hojas administrativas primero; después, todas las series.
    administrativas = [nombre for nombre in ["Referencia_Codigos", "Parentesco_Codigos", "Codificacion"] if nombre in libro.sheetnames]
    restantes = [nombre for nombre in libro.sheetnames if nombre not in administrativas]
    libro._sheets = [libro[nombre] for nombre in administrativas + restantes]

    try:
        libro.save(ruta_temporal)
        libro.close()
        ruta_temporal.replace(ruta_bd)
    finally:
        ruta_temporal.unlink(missing_ok=True)


if __name__ == "__main__":
    generar()
