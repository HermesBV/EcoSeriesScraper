"""Informe Mensual de Ingresos y Gastos (IMIG) de la Secretaría de Hacienda.

Hacienda publica un libro por mes con dos hojas útiles: la del mes (columna
«Dato mensual») y «Mensualizacion», con todos los meses del año en curso ya
revisados. Este módulo toma sólo los libros IMIG (sueltos o dentro de los zip
mensuales) de la misma página que usa ``scraper_MECON_hacienda`` y publica los
agregados del informe (ingresos, gastos, resultados e intereses) con sus
aperturas. La descarga se reutiliza del módulo de Hacienda, que ya cachea
cada archivo en ``fuentes_BD/MECON/Hacienda``.
"""

from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
from pathlib import Path
import re
import unicodedata
import zipfile

import pandas as pd

from scrapers.scraper_IED import guardar_datos_preservando_formato
from scrapers.scraper_MECON_hacienda import CARPETA, PAGINA, descargar
from tools.generar_codificacion import cargar_indice, generar

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CODIGO_FUENTE = "mecon-imig"
HOJA_BD = "MECON IMIG M"
ETIQUETA_DATO_MENSUAL = "dato mensual"


def _texto(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _slug(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def _sin_llamadas(texto: str) -> str:
    """Quita llamadas a notas como «(*)» o «(2)» al final de un concepto."""
    return re.sub(r"\s*\((?:\*+|\d+)\)\s*$", "", texto).strip()


def _es_fecha(value: object) -> bool:
    return isinstance(value, (datetime, date, pd.Timestamp)) and not isinstance(value, bool)


def _mes(value: object) -> pd.Timestamp:
    return pd.Timestamp(value.year, value.month, 1)


def _columnas_de_meses(raw: pd.DataFrame) -> tuple[int, dict[int, pd.Timestamp]] | None:
    """Fila de encabezado y columnas fechadas de una hoja IMIG.

    La hoja del mes tiene «Dato mensual» sobre una única fecha; «Mensualizacion»
    trae una fecha por columna. En ambos casos las fechas están en una fila de
    las diez primeras y los conceptos empiezan debajo.
    """
    for row in range(min(10, len(raw))):
        fechas = {col: _mes(raw.iat[row, col]) for col in range(raw.shape[1]) if _es_fecha(raw.iat[row, col])}
        if not fechas:
            continue
        arriba = [_texto(raw.iat[r, c]).casefold() for r in range(max(0, row - 2), row) for c in fechas]
        if len(fechas) == 1 and ETIQUETA_DATO_MENSUAL not in " ".join(arriba):
            continue
        # La hoja del mes trae el mes y el mismo mes del año anterior; la de
        # mensualización, todos los meses del año. Cada columna fechada vale
        # por sí misma: el libro más reciente decide después.
        return row, fechas
    return None


def extraer_hoja(raw: pd.DataFrame) -> dict[str, dict[pd.Timestamp, float]]:
    """Series ``{concepto: {mes: valor}}`` de una hoja del IMIG."""
    encontrado = _columnas_de_meses(raw)
    if encontrado is None:
        return {}
    header, fechas = encontrado
    primera_col = min(fechas)
    niveles: list[str] = []
    series: dict[str, dict[pd.Timestamp, float]] = {}
    for row in range(header + 1, len(raw)):
        etiquetas = [(col, _texto(raw.iat[row, col])) for col in range(min(primera_col, 6))]
        etiquetas = [(col, texto) for col, texto in etiquetas if texto]
        if not etiquetas:
            continue
        col, texto = etiquetas[-1]
        if texto.startswith("(") or texto.casefold().startswith(("nota", "fuente")):
            break
        texto = _sin_llamadas(texto)
        # Los resultados (primario, financiero) cierran el informe: las líneas
        # que siguen (intereses, recursos extraordinarios) no son sus aperturas.
        if next((nivel for nivel in niveles if nivel), "").casefold().startswith("resultado"):
            niveles = []
        niveles = niveles[:col] + [""] * max(0, col - len(niveles)) + [texto]
        concepto = " - ".join(parte for parte in niveles if parte)
        if concepto in series:
            continue
        for columna, fecha in fechas.items():
            valor = pd.to_numeric(pd.Series([raw.iat[row, columna]]), errors="coerce").iloc[0]
            if pd.notna(valor):
                series.setdefault(concepto, {})[fecha] = float(valor)
    return series


def _libros(archivos: list[tuple[str, Path]]):
    """Libros de Hacienda (sueltos o dentro de zip) como ``(nombre, bytes, url)``.

    Desde 2020 el IMIG viaja como hoja dentro del informe «Sector Público Base
    Caja»; antes era un libro propio. Se leen todos y cada hoja se reconoce por
    su estructura.
    """
    for url, path in archivos:
        if path.suffix.casefold() == ".zip":
            try:
                with zipfile.ZipFile(path) as archive:
                    for member in archive.infolist():
                        nombre = Path(member.filename).name
                        if nombre.casefold().endswith((".xls", ".xlsx")):
                            yield nombre, archive.read(member), url
            except zipfile.BadZipFile:
                print(f"IMIG: zip ilegible {path.name}", flush=True)
        elif path.suffix.casefold() in {".xls", ".xlsx"}:
            yield path.name, path.read_bytes(), url


def _hoja_descartada(nombre: str) -> bool:
    nombre = nombre.casefold()
    return any(termino in nombre for termino in ("varmensual", "salida prensa", "aif", "indice", "índice"))


def construir_salida(archivos: list[tuple[str, Path]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Consolida todos los libros IMIG; el dato más reciente de cada mes manda."""
    # Las series se identifican por el slug del concepto: entre informes cambian
    # mayúsculas, espacios y acentos («Otros Gastos Corrientes» / «Otros gastos
    # corrientes»). El nombre visible es el del informe más reciente.
    observaciones: dict[str, dict[pd.Timestamp, tuple[pd.Timestamp, float]]] = {}
    nombres: dict[str, tuple[pd.Timestamp, str]] = {}
    usados: set[str] = set()
    for nombre, contenido, url in sorted(_libros(archivos), key=lambda item: item[0]):
        try:
            with pd.ExcelFile(BytesIO(contenido)) as libro:
                hojas = {hoja: pd.read_excel(libro, sheet_name=hoja, header=None)
                         for hoja in libro.sheet_names if not _hoja_descartada(hoja)}
        except Exception as error:
            print(f"IMIG: no se pudo leer {nombre}: {error}", flush=True)
            continue
        series_libro: dict[str, dict[pd.Timestamp, float]] = {}
        for raw in hojas.values():
            for concepto, valores in extraer_hoja(raw).items():
                series_libro.setdefault(concepto, {}).update(valores)
        if not series_libro:
            continue
        usados.add(nombre)
        # El libro se fecha con el último mes que publica: un libro posterior
        # reemplaza los meses revisados de uno anterior.
        publicado = max(fecha for valores in series_libro.values() for fecha in valores)
        for concepto, valores in series_libro.items():
            identificador = _slug(concepto)
            if identificador not in nombres or nombres[identificador][0] <= publicado:
                nombres[identificador] = (publicado, concepto)
            destino = observaciones.setdefault(identificador, {})
            for fecha, valor in valores.items():
                if fecha not in destino or destino[fecha][0] <= publicado:
                    destino[fecha] = (publicado, valor)
    if not observaciones:
        raise ValueError("No se encontraron libros IMIG con datos")
    columnas = {nombres[identificador][1]: pd.Series({fecha: valor for fecha, (_, valor) in valores.items()})
                for identificador, valores in observaciones.items()}
    datos = pd.DataFrame(columnas).sort_index()
    datos.index.name = "fecha"
    datos = datos.reset_index()
    inventario = pd.DataFrame([{
        "ID": _slug(concepto), "Código fuente": CODIGO_FUENTE, "Nombre serie": concepto,
        "Variable": _slug(concepto), "Unidad": "millones de pesos", "Valoración": "Precios corrientes",
        "Descripción": f"{concepto}. Informe Mensual de Ingresos y Gastos, base caja, en millones de pesos.",
        "Frecuencia": "M", "Pestaña BD": HOJA_BD, "Columna BD": concepto,
        "Archivo origen": f"{len(usados)} informes mensuales de Hacienda", "Hoja origen": "IMIG",
        "Origen": "Secretaría de Hacienda, Ministerio de Economía", "Fuente": PAGINA,
        "Catálogo ID": "hacienda-informacion-estadistica", "Dataset ID": "imig",
        "Título dataset": "Informe Mensual de Ingresos y Gastos (IMIG)",
        "Tema dataset": "MECON / Hacienda / Informe Mensual de Ingresos y Gastos",
        "Responsable dataset": "Secretaría de Hacienda, Ministerio de Economía",
        "Fuente de valores": "Excel Hacienda", "Fecha inicio": datos.loc[datos[concepto].notna(), "fecha"].min(),
        "Fecha fin": datos.loc[datos[concepto].notna(), "fecha"].max(), "Estado": "VIGENTE",
        "Institución": "MECON", "Área": "Hacienda",
        "Subárea 1": "Informe Mensual de Ingresos y Gastos", "Tema": "Finanzas públicas",
    } for concepto in datos.columns if concepto != "fecha"])
    if inventario["ID"].duplicated().any():
        duplicados = inventario.loc[inventario["ID"].duplicated(), "ID"].tolist()
        raise ValueError(f"IMIG: identificadores duplicados {duplicados}")
    return datos, inventario


def procesar(descargar_archivos: bool = True) -> dict[str, int]:
    archivos = descargar() if descargar_archivos else [
        ("", path) for path in sorted(CARPETA.iterdir())
        if path.is_file() and path.suffix.casefold() in {".xls", ".xlsx", ".zip"}
    ]
    datos, inventario = construir_salida(archivos)
    actual = cargar_indice()
    obsoletas = set(actual.loc[actual["Código fuente"].eq(CODIGO_FUENTE), "Pestaña BD"].dropna()) if not actual.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, {HOJA_BD: datos}, {HOJA_BD: "M"}, obsoletas)
    generar(inventario)
    resumen = {"series": len(inventario), "meses": len(datos)}
    print(f"MECON IMIG terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar(True)
