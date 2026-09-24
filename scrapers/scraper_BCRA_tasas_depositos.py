"""Procesa las series diarias de tasas y saldos de depositos del BCRA."""

from __future__ import annotations

import hashlib
from pathlib import Path
import re

import pandas as pd
import requests
import xlrd


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
ARCHIVO_BD = RAIZ_PROYECTO / "BD.xlsx"
CARPETA_FUENTE = RAIZ_PROYECTO / "fuentes_BD" / "BCRA" / "TasasDepositos"
ARCHIVO_FUENTE = CARPETA_FUENTE / "pas2026.xls"
EXCEL_URL = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/pas2026.xls"
PAGINA_FUENTE = "https://www.bcra.gob.ar/publicaciones-y-estadisticas/series-diarias-de-tasas-de-interes-y-montos-colocados-saldos-de-depositos/"
CODIGO_FUENTE = "bcra-pas"
HOJAS_EXCLUIDAS = {"Indice", "Observaciones"}
FILA_CODIGOS = 25
FILA_DATOS = 26


def _sesion() -> requests.Session:
    sesion = requests.Session()
    sesion.headers["User-Agent"] = "EcoSeriesScraper/1.0 (datos publicos BCRA)"
    return sesion


def _excel_valido(ruta: Path) -> bool:
    if not ruta.is_file() or ruta.stat().st_size < 100_000:
        return False
    try:
        libro = xlrd.open_workbook(ruta, on_demand=True)
        valido = "Totales_diarios" in libro.sheet_names() and len(libro.sheet_names()) >= 20
        libro.release_resources()
        return valido
    except Exception:
        return False


def descargar_excel() -> Path:
    CARPETA_FUENTE.mkdir(parents=True, exist_ok=True)
    temporal = ARCHIVO_FUENTE.with_name("pas2026.descarga.xls")
    try:
        with _sesion().get(EXCEL_URL, stream=True, timeout=(20, 240)) as respuesta:
            respuesta.raise_for_status()
            with temporal.open("wb") as archivo:
                for bloque in respuesta.iter_content(64 * 1024):
                    if bloque:
                        archivo.write(bloque)
        if not _excel_valido(temporal):
            raise ValueError("La descarga no es el XLS de tasas y depositos del BCRA")
        temporal.replace(ARCHIVO_FUENTE)
    except (OSError, requests.RequestException, ValueError):
        if not _excel_valido(ARCHIVO_FUENTE):
            raise
        print("BCRA PAS: se reutiliza la copia local de pas2026.xls", flush=True)
    finally:
        temporal.unlink(missing_ok=True)
    return ARCHIVO_FUENTE


def _texto(valor: object) -> str:
    return re.sub(r"\s+", " ", str(valor or "")).strip()


def _nombre_hoja_bd(nombre: str) -> str:
    limpio = re.sub(r"[\\/*?:\[\]]", "-", nombre).strip()
    digest = hashlib.sha1(nombre.encode()).hexdigest()[:4]
    return f"PAS-{limpio[:22]}-{digest}"[:31]


def _valor_encabezado(hoja, fila: int, columna: int) -> object:
    valor = hoja.cell_value(fila, columna)
    if valor not in (None, ""):
        return valor
    for fila_ini, fila_fin, col_ini, col_fin in hoja.merged_cells:
        if fila_ini <= fila < fila_fin and col_ini <= columna < col_fin:
            return hoja.cell_value(fila_ini, col_ini)
    return ""


def _nombre_serie(hoja, columna: int, codigo: str) -> str:
    partes: list[str] = []
    for fila in range(15, FILA_CODIGOS):
        parte = _texto(_valor_encabezado(hoja, fila, columna))
        if parte and parte.casefold() not in {p.casefold() for p in partes}:
            partes.append(parte)
    detalle = " - ".join(partes)
    return f"{hoja.name}: {detalle}" if detalle else f"{hoja.name}: {codigo}"


def _unidad(nombre: str) -> str:
    texto = nombre.casefold()
    if "tasa" in texto or "interes" in texto or "interés" in texto:
        return "Porcentaje nominal anual"
    if "plazo" in texto:
        return "Dias"
    return "Miles de la moneda de origen"


def construir_salida(ruta: Path) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    libro = xlrd.open_workbook(ruta, formatting_info=True, on_demand=True)
    hojas_salida: dict[str, pd.DataFrame] = {}
    inventario: list[dict[str, object]] = []
    try:
        for nombre_hoja in libro.sheet_names():
            if nombre_hoja in HOJAS_EXCLUIDAS:
                continue
            hoja = libro.sheet_by_name(nombre_hoja)
            codigos = {
                columna: _texto(hoja.cell_value(FILA_CODIGOS, columna))
                for columna in range(3, hoja.ncols)
            }
            codigos = {columna: codigo for columna, codigo in codigos.items() if codigo}
            if not codigos:
                continue
            fechas = pd.to_numeric(
                pd.Series([hoja.cell_value(fila, 2) for fila in range(FILA_DATOS, hoja.nrows)]),
                errors="coerce",
            )
            fechas = pd.to_datetime(
                fechas.astype("Int64").astype(str), format="%Y%m%d", errors="coerce"
            )
            valores: dict[str, object] = {
                "fecha": fechas
            }
            metadatos: list[tuple[str, str, str]] = []
            for columna, codigo in codigos.items():
                identificador = f"{nombre_hoja}|{codigo}"
                nombre = _nombre_serie(hoja, columna, codigo)
                valores[codigo] = pd.to_numeric(
                    [hoja.cell_value(fila, columna) for fila in range(FILA_DATOS, hoja.nrows)],
                    errors="coerce",
                )
                metadatos.append((identificador, codigo, nombre))
            datos = pd.DataFrame(valores)
            datos = datos.dropna(subset=["fecha"]).dropna(subset=list(codigos.values()), how="all")
            datos = datos.sort_values("fecha").drop_duplicates("fecha", keep="last").reset_index(drop=True)
            frecuencia = "D" if datos["fecha"].dt.day.nunique() > 1 else "M"
            hoja_bd = _nombre_hoja_bd(nombre_hoja)
            hojas_salida[hoja_bd] = datos
            for identificador, codigo, nombre in metadatos:
                fechas_serie = datos.loc[datos[codigo].notna(), "fecha"]
                inventario.append({
                    "ID": identificador, "Código fuente": CODIGO_FUENTE,
                    "Nombre serie": nombre, "Variable": codigo,
                    "Unidades": _unidad(nombre), "Valoración": "No aplica / no informado",
                    "Descripción": nombre, "Frecuencia": frecuencia, "Pestaña BD": hoja_bd,
                    "Columna BD": codigo, "Archivo origen": ARCHIVO_FUENTE.name,
                    "Hoja origen": nombre_hoja,
                    "Origen": "Banco Central de la República Argentina (BCRA)",
                    "Fuente": PAGINA_FUENTE, "Catálogo ID": "bcra-pas",
                    "Dataset ID": "tasas-depositos-diarios",
                    "Título dataset": "Series diarias de tasas de interés y montos colocados / saldos de depósitos",
                    "Tema dataset": "BCRA / Estadísticas monetarias y financieras / Tasas de interés y depósitos",
                    "Responsable dataset": "Banco Central de la República Argentina (BCRA)",
                    "Fuente de valores": "Excel BCRA",
                    "Fecha inicio": fechas_serie.min() if len(fechas_serie) else None,
                    "Fecha fin": fechas_serie.max() if len(fechas_serie) else None,
                    "Estado": "VIGENTE",
                })
    finally:
        libro.release_resources()
    resultado = pd.DataFrame(inventario)
    if resultado[["Código fuente", "ID"]].duplicated().any():
        raise ValueError("Se generaron identidades duplicadas")
    return hojas_salida, resultado


def procesar(descargar: bool = True) -> dict[str, int]:
    ruta = descargar_excel() if descargar else ARCHIVO_FUENTE
    if not _excel_valido(ruta):
        raise FileNotFoundError(f"No existe un XLS BCRA valido en {ruta}")
    hojas, inventario = construir_salida(ruta)
    from scrapers.scraper_IED import guardar_datos_preservando_formato
    guardar_datos_preservando_formato(
        ARCHIVO_BD, hojas, frecuencias={nombre: "D" for nombre in hojas},
        hojas_obsoletas=set(),
    )
    from tools.generar_codificacion import generar
    generar(inventario)
    resumen = {"series": len(inventario), "hojas": len(hojas)}
    print(f"BCRA tasas y depositos terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar(descargar=True)


if __name__ == "__main__":
    ejecutar()
