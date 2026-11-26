"""Construye la serie histórica mensual de TCR bilateral con Estados Unidos.

Hasta 1996-12 la serie es la del IIEP; desde 1997-01, el ITCRB Estados Unidos que el
BCRA publica en internet (hoja ``BCRA ITCRM M``, que actualiza un scraper del BCRA que
corre antes que éste). Los meses nuevos salen siempre del BCRA.

``fuentes_BD/IIEP/TipoCambioReal/3 Tipo de Cambio Real.xlsx`` es una semilla histórica
de una sola vez: le dio fuente al tramo previo a 1997, que no está en internet, y ese
tramo ya quedó empalmado en ``BD.xlsx``. No es la fuente que se actualiza y no se
versiona. Sin el archivo, el tramo histórico se toma de lo ya publicado en ``BD.xlsx``.
"""

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
    from tools import lote
    datos = lote.leer_hoja(ruta, HOJA_BCRA)
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


def continuar_publicado(publicado: pd.DataFrame, bcra: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp]:
    """Sin la semilla: el tramo ya empalmado de ``BD.xlsx`` antes del BCRA, y el BCRA desde allí."""
    inicio_bcra = bcra["fecha"].min()
    historico = publicado.loc[publicado["fecha"] < inicio_bcra].copy()
    if historico.empty:
        raise ValueError(f"{HOJA_SALIDA} no tiene meses anteriores a {inicio_bcra:%Y-%m} y falta {ARCHIVO_IIEP.name}")
    resultado = pd.concat([historico, bcra], ignore_index=True).rename(columns={"valor": COLUMNA_SALIDA})
    if resultado["fecha"].duplicated().any() or not resultado["fecha"].is_monotonic_increasing:
        raise ValueError("La continuación produjo fechas duplicadas o desordenadas")
    return resultado, inicio_bcra


def leer_publicado(ruta: Path = ARCHIVO_BD) -> pd.DataFrame:
    from tools import lote
    datos = lote.leer_hoja(ruta, HOJA_SALIDA)
    salida = datos.iloc[:, [0]].copy()
    salida.columns = ["fecha"]
    salida["valor"] = pd.to_numeric(datos[COLUMNA_SALIDA], errors="coerce")
    salida["fecha"] = pd.to_datetime(salida["fecha"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    return salida.dropna().sort_values("fecha").drop_duplicates("fecha", keep="last").reset_index(drop=True)


def procesar() -> dict[str, object]:
    bcra = leer_bcra()
    if ARCHIVO_IIEP.is_file():
        serie, factor, inicio_bcra = empalmar(leer_iiep(), bcra)
    else:
        # Lo normal: la semilla ya está en BD.xlsx (ver el docstring del módulo).
        serie, inicio_bcra = continuar_publicado(leer_publicado(), bcra)
        factor = None
        print(f"IIEP TCR bilateral: sin {ARCHIVO_IIEP.name}, tramo histórico tomado de {HOJA_SALIDA}", flush=True)

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
        "Nombre serie": "ITCRB Estados Unidos (mensual)",
        "Variable": "itcrb_estados_unidos_empalmado",
        "Unidad": "Índice 17-dic-2015=100",
        "Valoración": "No aplica",
        "Descripción": "ITCRB Estados Unidos, empalme IIEP (hasta 1996-12) y BCRA (desde 1997-01) índice Laspeyres geométrico encadenado.",
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
