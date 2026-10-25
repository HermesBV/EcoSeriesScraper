"""Cotizaciones diarias de monedas extranjeras publicadas por la API cambiaria del BCRA."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
import urllib3

from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import generar

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "fuentes_BD" / "BCRA" / "Cotizaciones"
URL = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{moneda}"
PAGE = "https://www.bcra.gob.ar/PublicacionesEstadisticas/Tipos_de_cambios.asp"
SHEET = "BCRA Cotizaciones D"
CODE = "bcra-cotizaciones"
# La API devuelve hasta 1000 registros por consulta y rechaza fechas futuras;
# por eso se consulta por año calendario.
PRIMER_ANIO = 2002
SOLAPAMIENTO_DIAS = 7
# Código ISO y nombre. El tipo de pase es USD por unidad de
# la moneda; el tipo de cotización, pesos por unidad.
MONEDAS = {"CNY": "Yuan renminbi"}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def columnas(moneda: str) -> tuple[str, str]:
    return f"{moneda} tipo de pase", f"{moneda} tipo de cotización"


def parse_respuesta(contenido: dict, moneda: str) -> pd.DataFrame:
    """Convierte la respuesta JSON de la API en filas fecha/tipo de pase/cotización."""
    filas = []
    for registro in contenido.get("results", []):
        for detalle in registro.get("detalle", []):
            if detalle.get("codigoMoneda") == moneda:
                filas.append({
                    "fecha": pd.Timestamp(registro["fecha"]),
                    "tipoPase": detalle.get("tipoPase"),
                    "tipoCotizacion": detalle.get("tipoCotizacion"),
                })
    return pd.DataFrame(filas, columns=["fecha", "tipoPase", "tipoCotizacion"])


def descargar_rango(moneda: str, desde: date, hasta: date) -> pd.DataFrame:
    partes = []
    for anio in range(desde.year, hasta.year + 1):
        inicio = max(desde, date(anio, 1, 1))
        fin = min(hasta, date(anio, 12, 31))
        respuesta = requests.get(
            URL.format(moneda=moneda),
            params={"fechadesde": inicio.isoformat(), "fechahasta": fin.isoformat()},
            timeout=(15, 90), verify=False,
        )
        respuesta.raise_for_status()
        contenido = respuesta.json()
        total = contenido.get("metadata", {}).get("resultset", {}).get("count", 0)
        if total > 1000:
            raise ValueError(f"La API devolvió {total} registros para {moneda} {anio}; superan el límite por consulta")
        partes.append(parse_respuesta(contenido, moneda))
    if not partes:
        return pd.DataFrame(columns=["fecha", "tipoPase", "tipoCotizacion"])
    return pd.concat(partes, ignore_index=True)


def actualizar_archivo(moneda: str, hoy: date | None = None) -> Path:
    """Descarga todo el historial la primera vez y luego reanuda con solapamiento corto."""
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    archivo = DIRECTORY / f"cotizaciones_{moneda}.csv"
    hoy = hoy or date.today()
    previo = pd.read_csv(archivo, parse_dates=["fecha"]) if archivo.is_file() else pd.DataFrame()
    desde = date(PRIMER_ANIO, 1, 1)
    if not previo.empty:
        desde = (previo["fecha"].max() - timedelta(days=SOLAPAMIENTO_DIAS)).date()
    nuevo = descargar_rango(moneda, desde, hoy)
    datos = pd.concat([previo, nuevo], ignore_index=True)
    datos = datos.dropna(subset=["tipoPase"]).sort_values("fecha").drop_duplicates("fecha", keep="last")
    if datos.empty:
        raise ValueError(f"La API no devolvió cotizaciones para {moneda}")
    temporal = archivo.with_suffix(".tmp.csv")
    try:
        datos.to_csv(temporal, index=False, date_format="%Y-%m-%d")
        temporal.replace(archivo)
    finally:
        temporal.unlink(missing_ok=True)
    return archivo


def procesar(fetch: bool = True) -> dict[str, int]:
    hoja = None
    filas_indice = []
    for moneda, nombre in MONEDAS.items():
        archivo = actualizar_archivo(moneda) if fetch else DIRECTORY / f"cotizaciones_{moneda}.csv"
        datos = pd.read_csv(archivo, parse_dates=["fecha"])
        pase, cotizacion = columnas(moneda)
        datos = datos.rename(columns={"tipoPase": pase, "tipoCotizacion": cotizacion})[["fecha", pase, cotizacion]]
        hoja = datos if hoja is None else hoja.merge(datos, on="fecha", how="outer")
        comunes = {
            "Código fuente": CODE, "Frecuencia": "D", "Pestaña BD": SHEET,
            "Archivo origen": archivo.name, "Hoja origen": moneda,
            "Origen": "Banco Central de la República Argentina (BCRA)", "Fuente": PAGE,
            "Catálogo ID": "api-bcra-estadisticas-cambiarias", "Dataset ID": "cotizaciones",
            "Título dataset": "Cotizaciones de monedas extranjeras",
            "Tema dataset": "Estadísticas cambiarias",
            "Responsable dataset": "Banco Central de la República Argentina (BCRA)",
            "Fuente de valores": "API Estadísticas Cambiarias v1.0 del BCRA",
            "Fecha inicio": datos["fecha"].min(), "Fecha fin": datos["fecha"].max(), "Estado": "VIGENTE",
            "Institución": "BCRA", "Área": "Estadísticas cambiarias", "Subárea 1": "Cotizaciones",
            "Subárea 2": nombre, "Tema": "Sector externo", "Valoración": "No aplica",
        }
        filas_indice += [
            {**comunes, "ID": f"{moneda}-tipo-pase-d", "Columna BD": pase,
             "Nombre serie": f"{nombre} ({moneda}): tipo de pase",
             "Variable": f"tipo_pase_{moneda.lower()}", "Unidad": f"Dólares estadounidenses por {moneda}",
             "Descripción": f"Tipo de pase diario del {nombre.lower()} publicado por el BCRA: dólares estadounidenses "
                            f"por unidad de {moneda}. Es la paridad que el BCRA usa para valuar activos y pasivos en esa moneda."},
            {**comunes, "ID": f"{moneda}-tipo-cotizacion-d", "Columna BD": cotizacion,
             "Nombre serie": f"{nombre} ({moneda}): tipo de cotización",
             "Variable": f"tipo_cotizacion_{moneda.lower()}", "Unidad": f"Pesos por {moneda}",
             "Descripción": f"Tipo de cotización diario del {nombre.lower()} publicado por el BCRA: pesos argentinos por unidad de {moneda}."},
        ]
    hoja = hoja.sort_values("fecha").reset_index(drop=True)
    guardar_datos_preservando_formato(ROOT / "BD.xlsx", {SHEET: hoja}, {SHEET: "D"}, {SHEET})
    nuevas = pd.DataFrame(filas_indice)
    generar(nuevas)
    resumen = {"series": len(nuevas), "fechas": len(hoja)}
    print(f"BCRA cotizaciones terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar()


if __name__ == "__main__":
    ejecutar()
