"""Regenera las hojas de codificación incluidas en BD.xlsx."""

from __future__ import annotations

import re
import sys
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

from scrapers.scraper_IED import frecuencia_de_pestana, nombre_pestana


# Solo códigos globales: su significado no depende de un padre.
REFERENCIAS = [
    ["Institución", "00", "Por asignar", "Solo en borradores; identificar al productor de la serie"],
    ["Institución", "01", "INDEC", "Instituto Nacional de Estadística y Censos"],
    ["Institución", "02", "BCRA", "Banco Central de la República Argentina"],
    ["Institución", "03", "Área económica nacional", "Ministerio o secretaría correspondiente al período"],
    ["Institución", "04", "Área laboral nacional", "Ministerio o secretaría correspondiente al período"],
    ["Institución", "05", "ANSES", "Administración Nacional de la Seguridad Social"],
    ["Institución", "06", "Organismo tributario nacional", "AFIP o ARCA según el período"],
    ["Institución", "07", "CNV", "Comisión Nacional de Valores"],
    ["Institución", "09", "Organismo internacional", "Documentar el organismo"],
    ["Institución", "99", "Otro", "Documentar expresamente"],
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
MAPA_TEMATICO = [
    [1, "00", "", "00000000", "Sin clasificar", "Solo para borradores"],
    [1, "10", "", "10000000", "Actividad económica", ""],
    [2, "10", "10000000", "10100000", "Cuentas nacionales", ""],
    [3, "01", "10100000", "10100100", "Producto e ingreso", ""],
    [1, "20", "", "20000000", "Trabajo e ingresos", ""],
    [1, "30", "", "30000000", "Precios", ""],
    [1, "40", "", "40000000", "Sector externo", ""],
    [1, "50", "", "50000000", "Finanzas públicas", ""],
    [1, "60", "", "60000000", "Dinero y bancos", ""],
    [1, "70", "", "70000000", "Finanzas y mercados", ""],
    [1, "80", "", "80000000", "Contexto internacional", ""],
    [1, "90", "", "90000000", "Otros", ""],
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
            id_anterior = str(registrada.get("ID anterior", "")).strip()
            if id_anterior and id_anterior.lower() != "nan":
                existentes[id_anterior] = registrada
                contexto = (str(registrada["Institución"]).zfill(2), str(registrada["Ruta temática"]).zfill(8))
                contadores[contexto] = max(contadores.get(contexto, 0), int(registrada["Correlativo"]))
    limite_vigencia = pd.Timestamp(datetime.now().year - 3, 1, 1)
    resultado = []

    def conservar(valor: object) -> object:
        return None if pd.isna(valor) else valor

    for _, fila in codigos.iterrows():
        origen = str(fila.iloc[0]).strip()
        id_anterior = str(fila.iloc[1]).strip()
        variable = str(fila.iloc[2]).strip()
        pestana = nombre_pestana(fila.iloc[3])
        if id_anterior in existentes:
            anterior = existentes[id_anterior]
            resultado.append([
                anterior["ID provisorio"], str(anterior["Institución"]).zfill(2),
                str(anterior["Ruta temática"]).zfill(8), str(anterior["Correlativo"]).zfill(3),
                anterior["Valoración"], str(anterior["Año base"]).zfill(2), anterior["Tipo/unidad"],
                str(anterior["Multiplicador"]), anterior["Frecuencia"], id_anterior,
                variable, pestana, origen, conservar(anterior["Fecha inicio"]), conservar(anterior["Fecha fin"]),
                anterior["Estado"], conservar(anterior["Reemplaza por"]), conservar(anterior["Reemplaza a"]),
            ])
            continue
        institucion = "00"
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
            frecuencia, id_anterior, variable, pestana, origen,
            fecha_inicio, fecha_fin, estado, None, None,
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


def generar() -> None:
    ruta_bd = RAIZ / "BD.xlsx"
    ruta_temporal = RAIZ / ".BD_codificacion.tmp.xlsx"
    libro = load_workbook(ruta_bd)
    for nombre in ["Introduccion_Codigos", "Referencia_Codigos", "Mapa_Tematico", "Codificacion"]:
        if nombre in libro.sheetnames:
            del libro[nombre]
    referencias = libro.create_sheet("Referencia_Codigos")
    referencias.append(["Segmento", "Código", "Significado", "Regla/contexto"])
    for fila in REFERENCIAS:
        referencias.append(fila)
    agregar_tabla(referencias, "TablaReferenciaCodigos")

    mapa = libro.create_sheet("Mapa_Tematico")
    mapa.append(["Nivel", "Código local", "Ruta padre", "Ruta completa", "Nombre", "Notas"])
    for fila in MAPA_TEMATICO:
        mapa.append(fila)
    agregar_tabla(mapa, "TablaMapaTematico")

    series = libro.create_sheet("Codificacion")
    series.append(["Codificación compacta de series económicas"])
    series.append(["Formato", "IITTTTTTTTNNNVBBUEF"])
    series.append([])
    series.append(["Lectura", "II institución | TTTTTTTT ruta temática | NNN correlativo | V valoración | BB base | U tipo/unidad | E multiplicador | F frecuencia"])
    series.append([
        "ID provisorio", "Institución", "Ruta temática", "Correlativo", "Valoración",
        "Año base", "Tipo/unidad", "Multiplicador", "Frecuencia", "ID anterior",
        "Variable", "Pestaña BD", "Origen IED", "Fecha inicio", "Fecha fin", "Estado",
        "Reemplaza por", "Reemplaza a",
    ])
    for fila in construir_series():
        series.append(fila)
    agregar_tabla(series, "TablaSeriesCodificadas", fila_encabezado=5)
    series["A1"].font = Font(bold=True, size=14, color="1F4E78")
    series.column_dimensions["B"].width = max(series.column_dimensions["B"].width or 0, 110)
    for fila in range(6, series.max_row + 1):
        series.cell(fila, 14).number_format = "yyyy-mm-dd"
        series.cell(fila, 15).number_format = "yyyy-mm-dd"

    # Todas las hojas administrativas primero; después, todas las series.
    administrativas = [nombre for nombre in ["Referencias", "Codificacion", "Referencia_Codigos", "Mapa_Tematico"] if nombre in libro.sheetnames]
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
