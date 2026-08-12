"""Descarga y procesa las series del Informe Económico al Día (IED)."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from openpyxl import load_workbook
from tqdm import tqdm


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
CARPETA_FUENTE = RAIZ_PROYECTO / "fuentes_BD" / "IED"
CARPETA_LOGS = RAIZ_PROYECTO / "logs"
ARCHIVO_BD = RAIZ_PROYECTO / "BD.xlsx"
ARCHIVO_CODIGOS = RAIZ_PROYECTO / "Codigos.xlsx"
HOJAS_ADMINISTRATIVAS = {
    "Referencias", "Introduccion_Codigos", "Referencia_Codigos",
    "Mapa_Tematico", "Codificacion",
}

EXCEL_URLS = {
    "empleo_ingresos.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice3a.xlsx",
    "sector_externo.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice5.xlsx",
    "internacional.xlsx": "https://www.economia.gob.ar/download/infoeco/internacional_ied.xlsx",
    "dinero_bancos.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice8.xlsx",
    "precios.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice4.xlsx",
    "finanzas.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice-financiero.xlsx",
    "finanzas_publicas.xlsx": "https://www.economia.gob.ar/download/infoeco/apendice6.xlsx",
    "actividad.xlsx": "https://www.economia.gob.ar/download/infoeco/actividad_ied.xlsx",
}

MAPEO_ORIGEN_ARCHIVO = {
    "Empleo e Ingresos: Apendice3A": "empleo_ingresos.xlsx",
    "Actividad: Actividad_IED": "actividad.xlsx",
    "Contexto Internacional": "internacional.xlsx",
    "Precios: Apendice4": "precios.xlsx",
    "Finanzas: Apendice-Financiero": "finanzas.xlsx",
    "Finanzas Públicas: Apendice6": "finanzas_publicas.xlsx",
    "Sector Externo: Apendice5": "sector_externo.xlsx",
    "Dinero y Bancos: Apendice8": "dinero_bancos.xlsx",
}

# Correcciones de nombres históricos cuya frecuencia no coincidía con los datos.
RENOMBRES_PESTANAS = {
    "EPH Puntual - Pel M": "EPH Puntual - Pel S",
    "IPC Mensual M": "IPC Trimestral T",
    "TCN D": "TCN A",
    "ITCRM D": "ITCRM A",
    "EXPO IMPO P Y Q T": "EXPO IMPO P Y Q A",
    "TDI T": "TDI A",
    "IPI T": "IPI A",
    "8.8. Tipos de cambio historicos": "8.8 Tipos de cambio D",
    "8.8. Tipos de cambio historicos D": "8.8 Tipos de cambio D",
    "8.3 Prest. de Entidades finac.": "8.3 Préstamos entidades M",
    "8.3 Prest. de Entidades finac. M": "8.3 Préstamos entidades M",
}

MESES = {
    "ene": "Jan", "enero": "Jan", "feb": "Feb", "febrero": "Feb",
    "mar": "Mar", "marzo": "Mar", "abr": "Apr", "abril": "Apr",
    "may": "May", "mayo": "May", "jun": "Jun", "junio": "Jun",
    "jul": "Jul", "julio": "Jul", "ago": "Aug", "agosto": "Aug",
    "sep": "Sep", "sept": "Sep", "septiembre": "Sep",
    "oct": "Oct", "octubre": "Oct", "nov": "Nov", "noviembre": "Nov",
    "dic": "Dec", "diciembre": "Dec",
}


def escribir_log(id_serie: str, estado: str, mensaje: str = "") -> None:
    CARPETA_LOGS.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now()
    log_file = CARPETA_LOGS / f"log_{ahora:%Y%m%d}.txt"
    mensaje = mensaje.replace("\n", " || ")
    partes = [f"{ahora:%Y%m%d_%H%M%S}", id_serie, estado]
    if mensaje:
        partes.append(mensaje)
    with log_file.open("a", encoding="utf-8") as archivo:
        archivo.write("|".join(partes) + "\n")


def limpiar_nombres_definidos(ruta_archivo: Path) -> None:
    """Elimina rangos con nombre defectuosos de algunos libros del IED."""
    try:
        libro = load_workbook(ruta_archivo)
        for nombre in list(libro.defined_names):
            del libro.defined_names[nombre]
        libro.save(ruta_archivo)
    except Exception as exc:
        escribir_log("SISTEMA", "ADVERTENCIA_LIMPIEZA", f"{ruta_archivo.name}: {exc}")


def descargar_excels() -> dict[str, Path]:
    """Descarga cada libro de forma atómica para no destruir una copia válida."""
    CARPETA_FUENTE.mkdir(parents=True, exist_ok=True)
    descargados: dict[str, Path] = {}
    with requests.Session() as sesion:
        for nombre, url in tqdm(EXCEL_URLS.items(), desc="Descargando IED"):
            destino = CARPETA_FUENTE / nombre
            temporal = destino.with_name(f"{destino.stem}.descarga.xlsx")
            try:
                with sesion.get(url, stream=True, timeout=(15, 120)) as respuesta:
                    respuesta.raise_for_status()
                    with temporal.open("wb") as archivo:
                        for bloque in respuesta.iter_content(chunk_size=64 * 1024):
                            if bloque:
                                archivo.write(bloque)
                load_workbook(temporal, read_only=True).close()
                temporal.replace(destino)
                limpiar_nombres_definidos(destino)
                descargados[nombre] = destino
            except Exception as exc:
                temporal.unlink(missing_ok=True)
                escribir_log("SISTEMA", "ERROR_DESCARGA", f"{nombre}: {exc}")
                print(f"\nNo se pudo descargar {nombre}: {exc}")
    return descargados


def parse_fecha_manual(valor: object) -> pd.Timestamp | pd.NaT:
    if pd.isna(valor):
        return pd.NaT
    if isinstance(valor, (pd.Timestamp, datetime, np.datetime64)):
        fecha = pd.Timestamp(valor)
        return fecha if 1800 <= fecha.year <= datetime.now().year + 2 else pd.NaT

    texto = str(valor).strip()
    if not texto:
        return pd.NaT
    if re.fullmatch(r"\d{4}(?:\.0)?", texto):
        anio = int(float(texto))
        return pd.Timestamp(anio, 12, 31) if 1800 <= anio <= datetime.now().year + 2 else pd.NaT

    trimestre = re.fullmatch(r"(I|II|III|IV)[\s.-]+(\d{2}|\d{4})", texto, re.I)
    if trimestre:
        numero = {"I": 1, "II": 2, "III": 3, "IV": 4}[trimestre.group(1).upper()]
        texto_anio = trimestre.group(2)
        anio = int(texto_anio)
        if len(texto_anio) == 2:
            anio += 2000 if anio < 50 else 1900
        if not 1800 <= anio <= datetime.now().year + 2:
            return pd.NaT
        return pd.Period(year=anio, quarter=numero, freq="Q").end_time.normalize()

    mes_convertido = False
    for espanol in sorted(MESES, key=len, reverse=True):
        ingles = MESES[espanol]
        if texto.lower().startswith(espanol):
            texto = re.sub(rf"^{re.escape(espanol)}", ingles, texto, flags=re.I)
            mes_convertido = True
            break
    if mes_convertido:
        for formato in ("%b-%y", "%b %y", "%b.%y", "%b-%Y", "%b %Y", "%b.%Y"):
            try:
                return pd.to_datetime(texto, format=formato)
            except ValueError:
                continue
    try:
        fecha = pd.to_datetime(texto, format="mixed", dayfirst=True, errors="raise")
        return fecha if 1800 <= fecha.year <= datetime.now().year + 2 else pd.NaT
    except (TypeError, ValueError):
        return pd.NaT


def parse_fechas(fechas: list[object] | pd.Series) -> pd.DatetimeIndex:
    serie = pd.Series(fechas, dtype="object")
    resultado = pd.to_datetime(serie, format="mixed", dayfirst=True, errors="coerce")
    faltantes = resultado.isna() & serie.notna()
    if faltantes.any():
        resultado.loc[faltantes] = serie.loc[faltantes].map(parse_fecha_manual)
    return pd.DatetimeIndex(resultado)


def frecuencia_de_pestana(nombre: str) -> str:
    coincidencia = re.search(r"(?:^|\s)([ASTMD])$", nombre.strip(), re.I)
    return coincidencia.group(1).upper() if coincidencia else ""


def nombre_pestana(nombre: object) -> str:
    limpio = str(nombre).strip() or "Otros"
    return RENOMBRES_PESTANAS.get(limpio, limpio)[:31].strip()


def normalizar_fechas(fechas: pd.Series, frecuencia: str) -> pd.Series:
    fechas = pd.to_datetime(fechas, errors="coerce")
    if frecuencia == "A":
        return fechas.map(lambda x: pd.Timestamp(x.year, 12, 1) if pd.notna(x) else pd.NaT)
    if frecuencia == "T":
        return fechas.dt.to_period("Q").dt.end_time.dt.to_period("M").dt.start_time
    if frecuencia == "M":
        return fechas.dt.to_period("M").dt.start_time
    return fechas.dt.normalize()


def convertir_valor(valor: object) -> object:
    if pd.isna(valor):
        return np.nan
    if isinstance(valor, (int, float, np.number)) and not isinstance(valor, bool):
        return float(valor)
    texto = str(valor).strip()
    if not texto:
        return np.nan
    porcentaje = texto.endswith("%")
    if porcentaje:
        texto = texto[:-1].strip()
    if re.fullmatch(r"-?[\d.,]+", texto):
        if "," in texto:
            texto = texto.replace(".", "").replace(",", ".")
        try:
            return float(texto)
        except ValueError:
            pass
    return str(valor).strip()


def cargar_excel_completo(ruta: Path) -> dict[str, pd.DataFrame]:
    try:
        with pd.ExcelFile(ruta) as libro:
            return {
                hoja: pd.read_excel(libro, sheet_name=hoja, header=None)
                for hoja in libro.sheet_names
            }
    except Exception as exc:
        raise ValueError(f"No se pudo leer {ruta.name}: {exc}") from exc


def crear_indice_excel(excel_data: dict[str, pd.DataFrame]) -> dict[str, tuple[str, int, int]]:
    indice: dict[str, tuple[str, int, int]] = {}
    for hoja, dataframe in excel_data.items():
        for fila, valores in enumerate(dataframe.itertuples(index=False, name=None)):
            for columna, celda in enumerate(valores):
                if pd.isna(celda):
                    continue
                texto = str(celda).strip()
                if texto and texto not in indice:
                    indice[texto] = (hoja, fila, columna)
    return indice


def _columna_fecha(dataframe: pd.DataFrame, fila_id: int, columna_id: int) -> int:
    candidatas = range(min(3, dataframe.shape[1], max(columna_id, 1)))
    mejor_columna, mejor_puntaje = 0, -1
    muestra = dataframe.iloc[fila_id + 1 : fila_id + 31]
    for columna in candidatas:
        puntaje = sum(pd.notna(parse_fecha_manual(valor)) for valor in muestra.iloc[:, columna])
        if puntaje > mejor_puntaje:
            mejor_columna, mejor_puntaje = columna, puntaje
    return mejor_columna


def _tipo_fecha_cruda(valor: object) -> str:
    """Distingue bloques anuales/trimestrales pegados dentro de una hoja."""
    if isinstance(valor, (pd.Timestamp, datetime, np.datetime64)):
        return "fecha"
    texto = str(valor).strip()
    if re.fullmatch(r"\d{4}(?:\.0)?", texto):
        return "anio"
    if re.fullmatch(r"(I|II|III|IV)[\s.-]+(\d{2}|\d{4})", texto, re.I):
        return "trimestre"
    if any(texto.lower().startswith(mes) for mes in MESES):
        return "mes"
    return "fecha"


def extraer_serie_desde_indice(
    excel_data: dict[str, pd.DataFrame], ubicacion: tuple[str, int, int]
) -> pd.DataFrame:
    """Extrae una tabla y se detiene al terminar el bloque que contiene el ID."""
    hoja, fila_id, columna_id = ubicacion
    dataframe = excel_data[hoja]
    columna_fecha = _columna_fecha(dataframe, fila_id, columna_id)

    fila_inicio = None
    for fila in range(fila_id + 1, min(fila_id + 1001, len(dataframe))):
        if pd.notna(parse_fecha_manual(dataframe.iat[fila, columna_fecha])):
            fila_inicio = fila
            break
    if fila_inicio is None:
        raise ValueError(f"No hay fechas debajo del ID en la hoja {hoja}")

    tipo_bloque = _tipo_fecha_cruda(dataframe.iat[fila_inicio, columna_fecha])
    fechas: list[object] = []
    valores: list[object] = []
    for fila in range(fila_inicio, len(dataframe)):
        fecha_cruda = dataframe.iat[fila, columna_fecha]
        if tipo_bloque in {"anio", "trimestre", "mes"} and _tipo_fecha_cruda(fecha_cruda) != tipo_bloque:
            break
        fecha = parse_fecha_manual(fecha_cruda)
        if pd.isna(fecha):
            break
        fechas.append(fecha)
        valores.append(convertir_valor(dataframe.iat[fila, columna_id]))

    serie = pd.DataFrame({"fecha": pd.DatetimeIndex(fechas), "valor": valores})
    serie = serie[serie["fecha"] <= pd.Timestamp.today().normalize()]
    con_valor = serie["valor"].notna()
    if not con_valor.any():
        raise ValueError(f"El ID encontrado en {hoja} no contiene valores")
    primero, ultimo = con_valor[con_valor].index[[0, -1]]
    serie = serie.loc[primero:ultimo].copy()
    serie = compactar_fechas(serie)
    if serie.empty:
        raise ValueError("No se extrajeron datos válidos")
    return serie


def _ultimo_no_nulo(serie: pd.Series) -> object:
    valores = serie.dropna()
    return valores.iloc[-1] if not valores.empty else np.nan


def compactar_fechas(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Une fechas repetidas conservando el valor no vacío más reciente."""
    if dataframe.empty or "fecha" not in dataframe:
        return dataframe
    dataframe = dataframe.dropna(subset=["fecha"]).copy()
    columnas = [columna for columna in dataframe.columns if columna != "fecha"]
    if dataframe["fecha"].duplicated().any():
        dataframe = dataframe.groupby("fecha", as_index=False, sort=True)[columnas].agg(_ultimo_no_nulo)
    return dataframe.sort_values("fecha").reset_index(drop=True)


def preparar_hoja_bd(dataframe: pd.DataFrame, pestana: str) -> pd.DataFrame:
    dataframe = dataframe.copy()
    if "fecha" not in dataframe:
        dataframe.insert(0, "fecha", pd.NaT)
    dataframe["fecha"] = normalizar_fechas(dataframe["fecha"], frecuencia_de_pestana(pestana))
    hoy = pd.Timestamp.today().normalize()
    dataframe = dataframe[
        dataframe["fecha"].notna()
        & (dataframe["fecha"].dt.year >= 1800)
        & (dataframe["fecha"] <= hoy)
    ]
    dataframe = compactar_fechas(dataframe)
    columnas_datos = [columna for columna in dataframe if columna != "fecha"]
    if columnas_datos:
        dataframe = dataframe.dropna(how="all", subset=columnas_datos)
    return dataframe


def actualizar_serie(
    existente: pd.DataFrame, nueva: pd.DataFrame, variable: str, pestana: str
) -> pd.DataFrame:
    existente = preparar_hoja_bd(existente, pestana)
    nueva = nueva.rename(columns={"valor": variable})
    nueva["fecha"] = normalizar_fechas(nueva["fecha"], frecuencia_de_pestana(pestana))
    nueva = compactar_fechas(nueva)

    if variable not in existente:
        resultado = pd.merge(existente, nueva, on="fecha", how="outer")
    else:
        fecha_corte = nueva["fecha"].min()
        anterior = existente[existente["fecha"] < fecha_corte]
        actual = existente[existente["fecha"] >= fecha_corte].drop(columns=[variable])
        resultado = pd.concat(
            [anterior, pd.merge(actual, nueva, on="fecha", how="outer")], ignore_index=True
        )
    return preparar_hoja_bd(resultado, pestana)


def crear_referencias(codigos: pd.DataFrame) -> pd.DataFrame:
    frecuencias = {"A": "Anual", "S": "Semestral", "T": "Trimestral", "M": "Mensual", "D": "Diaria"}
    filas = []
    for _, fila in codigos.iterrows():
        pestana = nombre_pestana(fila["Pestaña Renombrada"])
        origen = str(fila["Excel Origen"]).strip()
        archivo = MAPEO_ORIGEN_ARCHIVO.get(origen)
        filas.append({
            "Tema": origen.split(":")[0].strip(),
            "Variable": fila["Variable"],
            "Frecuencia": frecuencias.get(frecuencia_de_pestana(pestana), "Otra"),
            "ID": fila["ID"],
            "Pestaña": pestana,
            "Fuente": EXCEL_URLS.get(archivo, "URL no encontrada"),
        })
    return pd.DataFrame(filas).sort_values(["Tema", "Fuente", "Pestaña", "Variable", "Frecuencia"])


def aplicar_formatos_fecha(ruta: Path) -> None:
    """Muestra fechas sin hora conservando celdas de fecha comparables."""
    libro = load_workbook(ruta)
    formatos = {"A": "yyyy", "S": "yyyy-mm", "T": "yyyy-mm", "M": "yyyy-mm", "D": "yyyy-mm-dd"}
    for hoja in libro.worksheets:
        if hoja.title == "Referencias":
            continue
        formato = formatos.get(frecuencia_de_pestana(hoja.title), "yyyy-mm-dd")
        for celda in hoja["A"][1:]:
            if celda.value is not None:
                celda.number_format = formato
        hoja.column_dimensions["A"].width = max(10, len("fecha") + 2)
    libro.save(ruta)


def procesar_datos(descargar: bool = True) -> dict[str, int]:
    """Ejecuta el flujo completo del IED y actualiza BD.xlsx."""
    escribir_log("SISTEMA", "INICIO", "Inicio de la extracción IED")
    if descargar:
        archivos = descargar_excels()
    else:
        archivos = {nombre: CARPETA_FUENTE / nombre for nombre in EXCEL_URLS if (CARPETA_FUENTE / nombre).exists()}
    faltantes = set(EXCEL_URLS) - set(archivos)
    if faltantes:
        raise RuntimeError(f"Faltan libros del IED: {', '.join(sorted(faltantes))}")

    libros: dict[str, dict[str, pd.DataFrame]] = {}
    indices: dict[str, dict[str, tuple[str, int, int]]] = {}
    for nombre, ruta in tqdm(archivos.items(), desc="Cargando libros IED"):
        libros[nombre] = cargar_excel_completo(ruta)
        indices[nombre] = crear_indice_excel(libros[nombre])

    columnas = ["Excel Origen", "ID", "Variable", "Pestaña Renombrada"]
    codigos = pd.read_excel(ARCHIVO_CODIGOS, usecols=columnas, dtype=str)
    codigos["Pestaña Renombrada"] = codigos["Pestaña Renombrada"].fillna("Otros")
    hojas_existentes = pd.read_excel(ARCHIVO_BD, sheet_name=None) if ARCHIVO_BD.exists() else {}
    codificacion_existente = None
    if ARCHIVO_BD.exists() and "Codificacion" in hojas_existentes:
        codificacion_existente = pd.read_excel(ARCHIVO_BD, sheet_name="Codificacion", header=None)
    bd: dict[str, pd.DataFrame] = {}
    for nombre, datos in hojas_existentes.items():
        if nombre in HOJAS_ADMINISTRATIVAS:
            continue
        nombre_canonico = nombre_pestana(nombre)
        datos = preparar_hoja_bd(datos, nombre_canonico)
        if nombre_canonico in bd:
            datos = pd.concat([bd[nombre_canonico], datos], ignore_index=True)
            datos = preparar_hoja_bd(datos, nombre_canonico)
        bd[nombre_canonico] = datos

    exitosas, errores = 0, []
    for indice_fila, fila in tqdm(codigos.iterrows(), total=len(codigos), desc="Procesando IDs IED"):
        id_serie = str(fila["ID"]).strip()
        origen = str(fila["Excel Origen"]).strip()
        variable = str(fila["Variable"]).strip()
        pestana = nombre_pestana(fila["Pestaña Renombrada"])
        try:
            if not id_serie or id_serie.lower() == "nan":
                raise ValueError("ID vacío")
            if not variable or variable.lower() == "nan":
                raise ValueError("Variable vacía")
            nombre_archivo = MAPEO_ORIGEN_ARCHIVO.get(origen)
            if nombre_archivo is None:
                raise KeyError(f"Origen sin mapear: {origen}")
            ubicacion = indices[nombre_archivo].get(id_serie)
            if ubicacion is None:
                raise ValueError(f"ID no encontrado en {nombre_archivo}: {id_serie}")
            serie = extraer_serie_desde_indice(libros[nombre_archivo], ubicacion)
            base = bd.get(pestana, pd.DataFrame(columns=["fecha"]))
            bd[pestana] = actualizar_serie(base, serie, variable, pestana)
            exitosas += 1
            escribir_log(id_serie, "OK", f"{nombre_archivo}; registros: {len(serie)}")
        except Exception as exc:
            errores.append((indice_fila, str(exc)))
            escribir_log(id_serie, "ERROR", str(exc))

    with pd.ExcelWriter(ARCHIVO_BD, engine="openpyxl", datetime_format="YYYY-MM-DD") as writer:
        crear_referencias(codigos).to_excel(writer, sheet_name="Referencias", index=False)
        # El generador posterior usa esta copia para conservar IDs y metadatos manuales.
        if codificacion_existente is not None:
            codificacion_existente.to_excel(writer, sheet_name="Codificacion", header=False, index=False)
        for nombre, datos in bd.items():
            datos = preparar_hoja_bd(datos, nombre)
            if len(datos.columns) > 1 and not datos.empty:
                datos.to_excel(writer, sheet_name=str(nombre).strip()[:31], index=False)
    aplicar_formatos_fecha(ARCHIVO_BD)
    from tools.generar_codificacion import generar as generar_codificacion
    generar_codificacion()

    resumen = {"total": len(codigos), "exitosas": exitosas, "fallidas": len(errores)}
    escribir_log("SISTEMA", "FIN", str(resumen))
    print(f"\nIED terminado: {exitosas}/{len(codigos)} series; errores: {len(errores)}")
    return resumen


def ejecutar() -> None:
    procesar_datos(descargar=True)
