"""Planilla de Reservas Internacionales y Liquidez en Moneda Extranjera (IRFCL) del BCRA.

Las partidas numéricas se toman del dataset IRFCL del FMI, donde el BCRA declara la
misma planilla con su historia revisada. Las partidas que sólo figuran en las notas
del país (componentes de la custodia) se leen de los PDF mensuales del BCRA.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from io import BytesIO, StringIO
from pathlib import Path
import re

import pandas as pd
import requests
import urllib3
from pypdf import PdfReader

from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import generar

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "fuentes_BD" / "BCRA" / "IRFCL"
PDF_DIRECTORY = DIRECTORY / "pdf"
FMI_FILE = DIRECTORY / "irfcl_fmi_argentina.csv"
NOTAS_FILE = DIRECTORY / "notas_ii1.csv"
FMI_URL = "https://api.imf.org/external/sdmx/2.1/data/IMF.STA,IRFCL/ARG.{indicadores}.S1X.M"
FMI_PAGE = "https://data.imf.org/en/datasets/IMF.STA:IRFCL"
PDF_URL = "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/temp{mes:02d}{anio:02d}.pdf"
PAGE = "https://www.bcra.gob.ar/PublicacionesEstadisticas/Reservas_internacionales.asp"
SHEET = "BCRA IRFCL M"
CODE = "bcra-irfcl"
PRIMER_MES_NOTAS = pd.Period("2002-03", "M")
MESES_REVISADOS = 3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SECCION_I = "I. Activos de reserva oficiales y otros activos en moneda extranjera"
SECCION_II = "II. Drenajes netos predeterminados de corto plazo"
NOTAS = "Notas del país"


@dataclass(frozen=True)
class Serie:
    id: str
    columna: str
    nombre: str
    subarea: str
    descripcion: str
    indicador_fmi: str | None = None
    unidad: str = "Millones de dólares estadounidenses"


SERIES = (
    Serie("irfcl-ia-activos-reserva-m", "I.A Activos de reserva oficiales", "Reservas internacionales brutas (activos de reserva oficiales, IRFCL I.A)", SECCION_I,
          "Total de activos de reserva oficiales del BCRA (Sección I.A de la planilla IRFCL), a fin de mes y a valor aproximado de mercado.", "IRFCLDT1_IRFCL65_USD"),
    Serie("irfcl-ia1-divisas-m", "I.A.1 Reservas en divisas", "Reservas en divisas convertibles (IRFCL I.A.1)", SECCION_I,
          "Valores más monedas y depósitos en divisas convertibles incluidos en los activos de reserva oficiales.", "IRFCLDT1_IRFCLCDCFC_USD"),
    Serie("irfcl-ia1a-valores-m", "I.A.1.a Valores", "Valores en divisas (IRFCL I.A.1.a)", SECCION_I,
          "Títulos valores en divisas incluidos en los activos de reserva oficiales.", "IRFCLDT1_IRFCL32_USD"),
    Serie("irfcl-ia1b-bancos-centrales-bpi-fmi-m", "I.A.1.b.i Depósitos en bancos centrales, BPI y FMI", "Monedas y depósitos en otros bancos centrales, BPI y FMI (IRFCL I.A.1.b.i)", SECCION_I,
          "Monedas y depósitos de reserva colocados en otros bancos centrales nacionales, el Banco de Pagos Internacionales y el FMI.", "IRFCLDT1_IRFCL31_ONCBBISIMF_USD"),
    Serie("irfcl-ia1b-bancos-residentes-m", "I.A.1.b.ii Depósitos en bancos del país", "Monedas y depósitos en bancos con casa matriz en el país (IRFCL I.A.1.b.ii)", SECCION_I,
          "Monedas y depósitos de reserva colocados en bancos y otras instituciones financieras con casa matriz en la Argentina.", "IRFCLDT1_IRFCL31_BOFIRC_USD"),
    Serie("irfcl-ia1b-bancos-exterior-m", "I.A.1.b.iii Depósitos en bancos del exterior", "Monedas y depósitos en bancos con casa matriz en el exterior (IRFCL I.A.1.b.iii)", SECCION_I,
          "Monedas y depósitos de reserva colocados en bancos y otras instituciones financieras con casa matriz fuera de la Argentina.", "IRFCLDT1_IRFCL31_BOFIORC_USD"),
    Serie("irfcl-ia2-posicion-fmi-m", "I.A.2 Posición de reserva en el FMI", "Posición de reserva en el FMI (IRFCL I.A.2)", SECCION_I,
          "Posición de reserva de la Argentina en el Fondo Monetario Internacional.", "IRFCLDT1_IRFCL57_USD"),
    Serie("irfcl-ia3-deg-m", "I.A.3 DEG", "Derechos especiales de giro (IRFCL I.A.3)", SECCION_I,
          "Tenencias de derechos especiales de giro (DEG) incluidas en los activos de reserva. El FMI difunde esta línea como activos de reserva denominados en DEG.", "IRFCLDT1_IRFCL65_DIC_XDR_USD"),
    Serie("irfcl-ia4-oro-m", "I.A.4 Oro", "Oro monetario (IRFCL I.A.4)", SECCION_I,
          "Oro incluido en los activos de reserva, con depósitos de oro, a valor de mercado.", "IRFCLDT1_IRFCL56_USD"),
    Serie("irfcl-ia4-oro-onzas-m", "I.A.4 Oro (onzas troy)", "Oro monetario, volumen (IRFCL I.A.4)", SECCION_I,
          "Volumen de oro incluido en los activos de reserva.", "IRFCLDT1_IRFCL56V_FTO", "Millones de onzas troy finas"),
    Serie("irfcl-ia5-otros-m", "I.A.5 Otros activos de reserva", "Otros activos de reserva (IRFCL I.A.5)", SECCION_I,
          "Otros activos de reserva: derivados financieros, préstamos a no residentes y otros, incluidos convenios multilaterales de crédito.", "IRFCLDT1_IRFCL58_USD"),
    Serie("irfcl-ib-otros-activos-me-m", "I.B Otros activos en moneda extranjera", "Otros activos en moneda extranjera no incluidos en reservas (IRFCL I.B)", SECCION_I,
          "Activos en moneda extranjera del BCRA que no integran los activos de reserva oficiales.", "IRFCLDT1_IRFCL34_USD"),
    Serie("irfcl-ii-total-m", "II Drenajes netos predeterminados", "Drenajes netos predeterminados de corto plazo, total (IRFCL II)", SECCION_II,
          "Total de la Sección II: egresos e ingresos predeterminados de activos en moneda extranjera a menos de un año. Se publica con signo negativo.", "IRFCLDT2_USD"),
    Serie("irfcl-ii1-prestamos-valores-depositos-m", "II.1 Préstamos, valores y depósitos", "Préstamos, valores y depósitos en moneda extranjera (IRFCL II.1)", SECCION_II,
          "Drenajes predeterminados por préstamos, valores y depósitos en moneda extranjera, capital e intereses. Incluye los pasivos declarados en las notas del país. Se publica con signo negativo.", "IRFCLDT2_IRFCL78_USD"),
    Serie("irfcl-ii2-forwards-swaps-m", "II.2 Forwards, futuros y swaps", "Posiciones cortas en forwards, futuros y swaps de monedas (IRFCL II.2)", SECCION_II,
          "Posiciones cortas agregadas en forwards y futuros en moneda extranjera frente al peso, incluido el tramo a término de los swaps de monedas. Desde diciembre de 2022 incluye el swap con el Banco Popular de China. Se publica con signo negativo.", "IRFCLDT2_IRFCL1T_SHP_USD"),
    Serie("irfcl-ii2-hasta-1-mes-m", "II.2 Hasta 1 mes", "Posiciones cortas en forwards y swaps, hasta 1 mes (IRFCL II.2)", SECCION_II,
          "Banda de vencimiento de hasta un mes de la Sección II.2. Se publica con signo negativo.", "IRFCLDT2_IRFCL1_SUTM_SHP_USD"),
    Serie("irfcl-ii2-1-a-3-meses-m", "II.2 Más de 1 y hasta 3 meses", "Posiciones cortas en forwards y swaps, de 1 a 3 meses (IRFCL II.2)", SECCION_II,
          "Banda de vencimiento de más de un mes y hasta tres meses de la Sección II.2. Se publica con signo negativo.", "IRFCLDT2_IRFCL1_SM1MUT3M_SHP_USD"),
    Serie("irfcl-ii2-3-a-12-meses-m", "II.2 Más de 3 meses y hasta 1 año", "Posiciones cortas en forwards y swaps, de 3 meses a 1 año (IRFCL II.2)", SECCION_II,
          "Banda de vencimiento de más de tres meses y hasta un año de la Sección II.2. Se publica con signo negativo.", "IRFCLDT2_IRFCL1_SM3MUTY_SHP_USD"),
    Serie("irfcl-ii3-otros-m", "II.3 Otros", "Otros drenajes predeterminados (IRFCL II.3)", SECCION_II,
          "Otros drenajes netos predeterminados de corto plazo, principalmente pases pasivos y repos en moneda extranjera. Se publica con signo negativo.", "IRFCLDT2_IRFCL85_USD"),
    Serie("irfcl-nota-cheques-cdi-m", "Nota II.1 Cheques cancelatorios y CDI", "Cheques cancelatorios en otras monedas y certificados de depósito para la inversión (nota IRFCL II.1)", NOTAS,
          "Partida declarada en la nota del país sobre la Sección II.1: cheques cancelatorios en otras monedas y certificados de depósito para la inversión (CDI)."),
    Serie("irfcl-nota-depositos-otras-monedas-m", "Nota II.1 Depósitos en otras monedas", "Depósitos en otras monedas en el BCRA (nota IRFCL II.1)", NOTAS,
          "Partida declarada en la nota del país sobre la Sección II.1: depósitos en moneda extranjera de las entidades financieras en el BCRA (encajes)."),
    Serie("irfcl-nota-depositos-gobierno-m", "Nota II.1 Depósitos del Gobierno Nacional", "Depósitos del Gobierno Nacional en otras monedas en el BCRA (nota IRFCL II.1)", NOTAS,
          "Partida declarada en la nota del país sobre la Sección II.1: depósitos del Tesoro Nacional en moneda extranjera en el BCRA."),
)
SERIES_FMI = tuple(serie for serie in SERIES if serie.indicador_fmi)
COLUMNAS_NOTAS = {
    "cheques": "Nota II.1 Cheques cancelatorios y CDI",
    "otras_monedas": "Nota II.1 Depósitos en otras monedas",
    "gobierno": "Nota II.1 Depósitos del Gobierno Nacional",
}


# --- Dataset IRFCL del FMI -------------------------------------------------

def parse_fmi(contenido: str) -> pd.DataFrame:
    """Lee el CSV SDMX del FMI y devuelve una fila por mes en millones."""
    datos = pd.read_csv(StringIO(contenido), engine="python", usecols=["INDICATOR", "TIME_PERIOD", "OBS_VALUE"])
    faltantes = {serie.indicador_fmi for serie in SERIES_FMI} - set(datos["INDICATOR"])
    if {"IRFCLDT1_IRFCL65_USD", "IRFCLDT2_IRFCL78_USD", "IRFCLDT2_IRFCL1T_SHP_USD", "IRFCLDT2_IRFCL85_USD"} & faltantes:
        raise ValueError(f"El dataset IRFCL del FMI no trae partidas esenciales: {sorted(faltantes)}")
    datos["fecha"] = pd.PeriodIndex(datos["TIME_PERIOD"].str.replace("-M", "-"), freq="M").to_timestamp()
    # El FMI expresa los valores en unidades; la planilla del BCRA, en millones.
    datos["valor"] = pd.to_numeric(datos["OBS_VALUE"], errors="coerce") / 1e6
    ancho = datos.pivot_table(index="fecha", columns="INDICATOR", values="valor", aggfunc="last")
    salida = pd.DataFrame(index=ancho.index)
    for serie in SERIES_FMI:
        salida[serie.columna] = ancho[serie.indicador_fmi] if serie.indicador_fmi in ancho else pd.NA
    return salida.reset_index().sort_values("fecha").reset_index(drop=True)


def descargar_fmi() -> Path:
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    indicadores = "+".join(serie.indicador_fmi for serie in SERIES_FMI)
    try:
        respuesta = requests.get(
            FMI_URL.format(indicadores=indicadores), params={"detail": "dataonly"}, timeout=(15, 180),
            headers={"Accept": "application/vnd.sdmx.data+csv;version=1.0.0", "User-Agent": "Mozilla/5.0"},
        )
        respuesta.raise_for_status()
        parse_fmi(respuesta.text)
        temporal = FMI_FILE.with_suffix(".tmp.csv")
        try:
            temporal.write_text(respuesta.text, encoding="utf-8")
            temporal.replace(FMI_FILE)
        finally:
            temporal.unlink(missing_ok=True)
    except (requests.RequestException, ValueError):
        if not FMI_FILE.is_file():
            raise
    return FMI_FILE


# --- Notas del país de la planilla PDF del BCRA ----------------------------

_NUMERO = r"\d+(?:[.,]\d{3})*(?:[.,]\d+)?"
# Algunas planillas parten palabras con espacios ("Aut oridad"); la nota se lee
# sobre el texto sin espacios.
_NOTA_II1 = re.compile(
    r"Enlasecci.nII\.1delaplanillacorrespondientealaAutoridadMonetariaseincluyen;?(.*?)"
    r"(?=\([a-z]\)[A-ZÁÉÍÓÚ]|NOTAS|$)", re.IGNORECASE | re.S,
)
_PARTIDA = re.compile(r"([^,;]*?)USD(" + _NUMERO + ")", re.IGNORECASE)


def numero(texto: str) -> float:
    """Convierte números con formato español (1.234,56) o inglés (1,234.56)."""
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d{1,2}", texto):
        return float(texto.replace(",", ""))
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+\.\d{1,2}", texto):
        # Errata del BCRA: punto como separador decimal tras miles (2.470.12).
        entero, decimales = texto.rsplit(".", 1)
        return float(entero.replace(".", "") + "." + decimales)
    return float(texto.replace(".", "").replace(",", "."))


def parse_notas(texto: str) -> dict[str, float]:
    """Extrae de la nota del país sobre II.1 las partidas que forman la custodia."""
    nota = _NOTA_II1.search(re.sub(r"\s+", "", texto))
    if not nota:
        return {}
    partidas: dict[str, float] = {}
    for etiqueta, valor in _PARTIDA.findall(nota.group(1)):
        etiqueta = etiqueta.casefold()
        if "chequescancelatorios" in etiqueta:
            clave = "cheques"
        elif "gobiernonacional" in etiqueta:
            clave = "gobierno"
        elif "otrasmonedas" in etiqueta and "depósitos" in etiqueta:
            clave = "otras_monedas"
        else:
            continue
        partidas.setdefault(clave, numero(valor))
    return partidas


def ruta_pdf(periodo: pd.Period) -> Path:
    return PDF_DIRECTORY / f"temp{periodo.month:02d}{periodo.year % 100:02d}.pdf"


def descargar_pdf(periodo: pd.Period, forzar: bool = False) -> Path | None:
    ruta = ruta_pdf(periodo)
    if ruta.is_file() and not forzar:
        return ruta
    PDF_DIRECTORY.mkdir(parents=True, exist_ok=True)
    try:
        respuesta = requests.get(
            PDF_URL.format(mes=periodo.month, anio=periodo.year % 100), timeout=(15, 90),
            verify=False, headers={"User-Agent": "Mozilla/5.0"},
        )
    except requests.RequestException:
        return ruta if ruta.is_file() else None
    if respuesta.status_code != 200 or not respuesta.content.startswith(b"%PDF"):
        return ruta if ruta.is_file() else None
    temporal = ruta.with_suffix(".tmp")
    try:
        temporal.write_bytes(respuesta.content)
        temporal.replace(ruta)
    finally:
        temporal.unlink(missing_ok=True)
    return ruta


def leer_notas(hasta: pd.Period, fetch: bool = True) -> tuple[pd.DataFrame, list[str]]:
    """Lee la nota II.1 de cada planilla; reutiliza el caché salvo en los últimos meses."""
    columnas = list(COLUMNAS_NOTAS.values())
    cache = pd.DataFrame(columns=["periodo", *columnas])
    if NOTAS_FILE.is_file():
        cache = pd.read_csv(NOTAS_FILE, dtype={"periodo": str})
    leidas = {fila["periodo"]: fila for fila in cache.to_dict("records")}
    filas, sin_nota = [], []
    for periodo in pd.period_range(PRIMER_MES_NOTAS, hasta, freq="M"):
        reciente = (hasta - periodo).n < MESES_REVISADOS
        clave = str(periodo)
        if clave in leidas and not reciente:
            fila = leidas[clave]
        else:
            ruta = descargar_pdf(periodo, forzar=reciente) if fetch else ruta_pdf(periodo)
            if ruta is None or not ruta.is_file():
                continue
            try:
                texto = " ".join(pagina.extract_text() or "" for pagina in PdfReader(BytesIO(ruta.read_bytes())).pages)
                partidas = parse_notas(texto)
            except Exception:
                partidas = {}
            fila = {"periodo": clave, **{columna: None for columna in columnas},
                    **{COLUMNAS_NOTAS[k]: v for k, v in partidas.items()}}
            leidas[clave] = fila
        if all(pd.isna(fila.get(columna)) for columna in columnas):
            sin_nota.append(clave)
            continue
        filas.append({"fecha": periodo.to_timestamp(), **{columna: fila.get(columna) for columna in columnas}})
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(sorted(leidas.values(), key=lambda fila: fila["periodo"]), columns=["periodo", *columnas]).to_csv(
        NOTAS_FILE, index=False
    )
    return pd.DataFrame(filas, columns=["fecha", *columnas]), sin_nota


# --- Publicación --------------------------------------------------------------

def construir_hoja(fmi: pd.DataFrame, notas: pd.DataFrame) -> pd.DataFrame:
    hoja = fmi.merge(notas, on="fecha", how="outer") if not notas.empty else fmi.copy()
    for serie in SERIES:
        if serie.columna not in hoja:
            hoja[serie.columna] = pd.NA
    return hoja[["fecha", *(serie.columna for serie in SERIES)]].sort_values("fecha").reset_index(drop=True)


def filas_indice(hoja: pd.DataFrame) -> pd.DataFrame:
    filas = []
    for serie in SERIES:
        valores = hoja[["fecha", serie.columna]].dropna()
        if valores.empty:
            continue
        de_fmi = serie.indicador_fmi is not None
        filas.append({
            "ID": serie.id, "Código fuente": CODE, "Nombre serie": serie.nombre,
            "Variable": serie.id.removesuffix("-m").replace("-", "_"), "Unidad": serie.unidad,
            "Valoración": "No aplica" if "onzas" in serie.unidad else "Precios corrientes",
            "Descripción": serie.descripcion + " Saldo a fin de mes.", "Frecuencia": "M",
            "Pestaña BD": SHEET, "Columna BD": serie.columna,
            "Archivo origen": FMI_FILE.name if de_fmi else "tempMMAA.pdf",
            "Hoja origen": serie.indicador_fmi if de_fmi else "Notas del país",
            "Origen": "Banco Central de la República Argentina (BCRA)",
            "Fuente": FMI_PAGE if de_fmi else PAGE,
            "Catálogo ID": "irfcl", "Dataset ID": "irfcl-argentina",
            "Título dataset": "Reservas Internacionales y Liquidez en Moneda Extranjera (IRFCL)",
            "Tema dataset": "Reservas internacionales",
            "Responsable dataset": "Banco Central de la República Argentina (BCRA)",
            "Fuente de valores": (
                "Dataset IRFCL del FMI (API SDMX), declarado por el BCRA, sector autoridad monetaria" if de_fmi
                else "Planilla IRFCL mensual del BCRA (PDF), notas del país"
            ),
            "Fecha inicio": valores["fecha"].min(), "Fecha fin": valores["fecha"].max(), "Estado": "VIGENTE",
            "Institución": "BCRA", "Área": "Reservas Internacionales y Liquidez en Moneda Extranjera",
            "Subárea 1": serie.subarea, "Tema": "Sector externo",
        })
    return pd.DataFrame(filas)


def procesar(fetch: bool = True) -> dict[str, object]:
    fmi = parse_fmi((descargar_fmi() if fetch else FMI_FILE).read_text(encoding="utf-8"))
    ultimo = max(fmi["fecha"].max().to_period("M"), pd.Period(date.today(), "M") - 1)
    notas, sin_nota = leer_notas(ultimo, fetch)
    hoja = construir_hoja(fmi, notas)
    guardar_datos_preservando_formato(ROOT / "BD.xlsx", {SHEET: hoja}, {SHEET: "M"}, {SHEET})
    indice = filas_indice(hoja)
    generar(indice)
    resumen = {
        "series": len(indice), "meses": len(hoja), "desde": hoja["fecha"].min(), "hasta": hoja["fecha"].max(),
        "meses_con_notas": len(notas), "meses_sin_nota_ii1": len(sin_nota),
    }
    if sin_nota:
        print(f"Planillas IRFCL sin nota II.1 legible: {', '.join(sin_nota)}", flush=True)
    print(f"BCRA IRFCL terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar()


if __name__ == "__main__":
    ejecutar()
