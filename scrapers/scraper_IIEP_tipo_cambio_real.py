"""Construye la serie histórica mensual de TCR bilateral con Estados Unidos."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
ARCHIVO_BD = RAIZ_PROYECTO / "BD.xlsx"
ARCHIVO_IIEP = RAIZ_PROYECTO / "fuentes_BD" / "IIEP" / "TipoCambioReal" / "3 Tipo de Cambio Real.xlsx"
HOJA_IIEP = "Mes"
COLUMNA_IIEP = "Importación (implícito)"
HOJA_BCRA = "BCRA ITCRM M"
COLUMNA_BCRA = "ITCRB Estados Unidos"
HOJA_SALIDA = "IIEP ITCRB EEUU M"
COLUMNA_SALIDA = "ITCRB Estados Unidos empalmado"
CODIGO_FUENTE = "iiep"
ID_ORIGEN = "itcrb-eeuu-empalmado-importacion-m"


def leer_iiep(ruta: Path = ARCHIVO_IIEP) -> pd.DataFrame:
    datos = pd.read_excel(ruta, sheet_name=HOJA_IIEP, header=0)
    if COLUMNA_IIEP not in datos.columns:
        raise ValueError(f"No se encontró la columna '{COLUMNA_IIEP}' en {ruta.name}/{HOJA_IIEP}")
    salida = datos.iloc[:, [0]].copy()
    salida.columns = ["fecha"]
    salida["valor"] = pd.to_numeric(datos[COLUMNA_IIEP], errors="coerce")
    salida["fecha"] = pd.to_datetime(salida["fecha"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    return salida.dropna().sort_values("fecha").drop_duplicates("fecha", keep="last").reset_index(drop=True)


def leer_bcra(ruta: Path = ARCHIVO_BD) -> pd.DataFrame:
    datos = pd.read_excel(ruta, sheet_name=HOJA_BCRA)
    if COLUMNA_BCRA not in datos.columns:
        raise ValueError(f"No se encontró la columna '{COLUMNA_BCRA}' en {HOJA_BCRA}")
    salida = datos.iloc[:, [0]].copy()
    salida.columns = ["fecha"]
    salida["valor"] = pd.to_numeric(datos[COLUMNA_BCRA], errors="coerce")
    salida["fecha"] = pd.to_datetime(salida["fecha"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    return salida.dropna().sort_values("fecha").drop_duplicates("fecha", keep="last").reset_index(drop=True)


def empalmar(iiep: pd.DataFrame, bcra: pd.DataFrame) -> tuple[pd.DataFrame, float, pd.Timestamp]:
    """Reescala el tramo histórico en el primer mes BCRA y conserva BCRA desde allí."""
    inicio_bcra = bcra["fecha"].min()
    coincidencia = iiep.loc[iiep["fecha"].eq(inicio_bcra), "valor"]
    valor_bcra = bcra.loc[bcra["fecha"].eq(inicio_bcra), "valor"]
    if coincidencia.empty or valor_bcra.empty:
        raise ValueError("Las series IIEP y BCRA no coinciden en el primer mes BCRA")
    factor = float(valor_bcra.iloc[0] / coincidencia.iloc[0])
    historico = iiep.loc[iiep["fecha"] < inicio_bcra].copy()
    historico["valor"] *= factor
    resultado = pd.concat([historico, bcra], ignore_index=True)
    resultado = resultado.rename(columns={"valor": COLUMNA_SALIDA})
    if resultado["fecha"].duplicated().any() or not resultado["fecha"].is_monotonic_increasing:
        raise ValueError("El empalme produjo fechas duplicadas o desordenadas")
    return resultado, factor, inicio_bcra


def procesar() -> dict[str, object]:
    if not ARCHIVO_IIEP.is_file():
        raise FileNotFoundError(f"Falta el archivo fuente IIEP: {ARCHIVO_IIEP}")
    iiep = leer_iiep()
    bcra = leer_bcra()
    serie, factor, inicio_bcra = empalmar(iiep, bcra)

    from scrapers.scraper_IED import guardar_datos_preservando_formato
    guardar_datos_preservando_formato(
        ARCHIVO_BD, {HOJA_SALIDA: serie}, frecuencias={HOJA_SALIDA: "M"},
        hojas_obsoletas={HOJA_SALIDA},
    )

    from tools.generar_codificacion import cargar_indice
    actual = cargar_indice()
    actual = actual[
        ~(
            actual["Código fuente"].astype(str).eq(CODIGO_FUENTE)
            & actual["ID"].astype(str).isin([ID_ORIGEN, f"{CODIGO_FUENTE}::{ID_ORIGEN}"])
        )
    ]
    inicio_iiep = serie["fecha"].min()
    fin = serie["fecha"].max()
    fila = {
        "Código fuente": CODIGO_FUENTE,
        "ID": ID_ORIGEN,
        "Nombre serie": "ITCRB Estados Unidos + IIEP",
        "Variable": "itcrb_estados_unidos_empalmado",
        "Unidades": "Índice, base BCRA 17-dic-2015=100",
        "Valoración": "No aplica / no informado",
        "Descripción": (
            f"Serie mensual empalmada por el IIEP. Desde {inicio_iiep:%Y-%m} hasta 1996-12 usa "
            f"'Importación (implícito)' del archivo IIEP, reescalada en {inicio_bcra:%Y-%m}; "
            f"desde {inicio_bcra:%Y-%m} usa el promedio mensual oficial ITCRB Estados Unidos del BCRA."
        ),
        "Frecuencia": "M",
        "Pestaña BD": HOJA_SALIDA,
        "Columna BD": COLUMNA_SALIDA,
        "Archivo origen": ARCHIVO_IIEP.name,
        "Hoja origen": HOJA_IIEP,
        "Origen": "Instituto Interdisciplinario de Economía Política (IIEP-UBA-CONICET)",
        "Fuente": "IIEP y Banco Central de la República Argentina (BCRA)",
        "Catálogo ID": "iiep-series-macro",
        "Dataset ID": "tipo-cambio-real-historico",
        "Título dataset": "Tipo de cambio real bilateral con Estados Unidos",
        "Tema dataset": "Tipo de cambio",
        "Responsable dataset": "Instituto Interdisciplinario de Economía Política (IIEP-UBA-CONICET)",
        "Fuente de valores": "Excel IIEP empalmado con Excel BCRA",
        "Fecha inicio": inicio_iiep,
        "Fecha fin": fin,
        "Estado": "VIGENTE",
    }
    from tools.generar_codificacion import generar
    generar(pd.concat([actual, pd.DataFrame([fila])], ignore_index=True))
    resumen = {
        "series": 1, "observaciones": len(serie), "factor_empalme": factor,
        "fecha_inicio": inicio_iiep, "inicio_bcra": inicio_bcra, "fecha_fin": fin,
    }
    print(f"IIEP TCR bilateral terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar()


if __name__ == "__main__":
    ejecutar()
