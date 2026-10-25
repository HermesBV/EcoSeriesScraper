"""Reservas internacionales de la Argentina según el DT-IIEP N° 126.

Impávido, Monteforte y Romero (2026) descomponen las reservas del BCRA en brutas,
custodia, propias, prestadas, netas y líquidas. Los meses hasta ``CORTE_HISTORICO``
son la base publicada por los autores; los posteriores se calculan con su método a
partir de las hojas ``BCRA IRFCL M`` y ``BCRA Cotizaciones D`` que actualizan los
scrapers del BCRA, que corren antes que éste.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from scrapers.scraper_BCRA_cotizaciones import SHEET as HOJA_COTIZACIONES, columnas as columnas_cotizacion
from scrapers.scraper_BCRA_irfcl import COLUMNAS_NOTAS, SHEET as HOJA_IRFCL
from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import generar

ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
DIRECTORY = ROOT / "fuentes_BD" / "IIEP" / "ReservasInternacionales"
ARCHIVO_HISTORICO = DIRECTORY / "Reservas_internacionales_Argentina_agosto_2026.csv"
CORTE_HISTORICO = pd.Timestamp("2026-08-01")
HOJA = "IIEP Reservas M"
CODIGO = "iiep-rin"
DOCUMENTO = "https://ojs.economicas.uba.ar/DT-IIEP/article/view/3723"
AUTORES = "Franco Impávido, Ezequiel Monteforte y Miguel Romero"

# Swap con el Banco Popular de China (DT, Anexo A.9). El acuerdo marco es de 130.000
# millones de yuanes desde diciembre de 2018 (renovado en agosto de 2026) y la porción
# activada, 35.000 millones desde enero de 2023. Sólo se descuenta la porción no activada.
NOCIONAL_SWAP_CHINA_CNY = 130_000
TRAMO_ACTIVADO_CNY = 35_000
# II.2 replica el nocional chino con un desvío máximo de 1,27%. Si se aparta más,
# la sección contiene otro swap (p. ej. el del Tesoro de EE.UU. en oct-nov 2025).
TOLERANCIA_SWAP = 0.03

COL_BRUTAS = "I.A Activos de reserva oficiales"
COL_DEG = "I.A.3 DEG"
COL_ORO = "I.A.4 Oro"
COL_II1 = "II.1 Préstamos, valores y depósitos"
COL_II2 = "II.2 Forwards, futuros y swaps"
COL_II2_1M = "II.2 Hasta 1 mes"
COL_II3 = "II.3 Otros"


@dataclass(frozen=True)
class Serie:
    clave: str
    columna: str
    nombre: str
    descripcion: str


SERIES = (
    Serie("brutas", "Reservas brutas", "Reservas internacionales brutas",
          "Activos de reserva oficiales del BCRA (Sección I.A de la planilla IRFCL)."),
    Serie("custodia", "Custodia", "Custodia de reservas",
          "Divisas que el BCRA mantiene en custodia y que integran las brutas: encajes en moneda extranjera, cheques "
          "cancelatorios y CDI, y depósitos del Tesoro en moneda extranjera. Desde 2002-03 surgen de las notas del país "
          "de la planilla IRFCL; entre 2000-09 y 2002-01, del balance del BCRA."),
    Serie("propias", "Reservas propias", "Reservas internacionales propias",
          "Brutas menos custodia."),
    Serie("prestadas", "Reservas prestadas", "Reservas prestadas",
          "Pasivos de reserva de la Sección II de la planilla IRFCL netos de la custodia: swaps (II.2), pases y repos (II.3) "
          "y otros compromisos (II.1). El swap con China pasa de II.3 a II.2 en 2022-12 y se trata de forma homogénea."),
    Serie("netas", "Reservas netas", "Reservas internacionales netas",
          "Propias menos prestadas. Reclasifica el encaje declarado (2002-03 a 2004-08) y excluye la Letra Intransferible "
          "(2015-02 a 2015-11); desde 2004-09 coincide con las netas según planilla salvo ese tramo. Julio de 2026: "
          "+5.819,8 según la planilla; con el pase a 2028 repuesto, −180,2."),
    Serie("netas_planilla", "Reservas netas según planilla", "Reservas internacionales netas según planilla IRFCL",
          "Brutas más las Secciones II.1, II.2 y II.3 de la planilla IRFCL, tal como se publican y sin ajustes."),
    Serie("rin_eff", "RIN-EFF", "Reservas internacionales netas RIN-EFF",
          "Reservas netas con la definición del Acuerdo de Facilidades Extendidas con el FMI, a precios de programa. "
          "Serie oficial del BCRA que no se publica: toda la serie (2020-12 a 2026-07) proviene del pedido de acceso a "
          "la información pública Trámite 10.532, con la valuación de la segunda revisión del acuerdo. Reemplaza a la "
          "serie de la primera revisión que usa el cuerpo del DT (hasta 2025-07), cuyos valores difieren. Sólo se "
          "extiende con un nuevo pedido al BCRA: no se actualiza automáticamente."),
    Serie("liquidas", "Reservas líquidas", "Reservas internacionales líquidas",
          "Brutas menos oro, DEG y porción no activada del swap con China, con el criterio del FMI (DT, Sección V y "
          "Anexo A.9). Oro y DEG de 2000-09 a 2025-07 provienen del Trámite 9455 del BCRA."),
    Serie("propias_liquidas", "Reservas propias líquidas", "Reservas propias líquidas",
          "Líquidas menos custodia."),
)
COLUMNAS = {serie.clave: serie.columna for serie in SERIES}


def leer_historico(ruta: Path = ARCHIVO_HISTORICO) -> pd.DataFrame:
    """Lee la base del IIEP; sin el archivo, conserva lo ya publicado en BD.xlsx."""
    if ruta.is_file():
        datos = pd.read_csv(ruta, parse_dates=["fecha"])
        faltantes = set(COLUMNAS) - set(datos.columns)
        if faltantes:
            raise ValueError(f"La base IIEP no trae las columnas {sorted(faltantes)}")
        datos = datos.rename(columns=COLUMNAS)[["fecha", *COLUMNAS.values()]]
    else:
        datos = pd.read_excel(ARCHIVO_BD, sheet_name=HOJA)
        datos["fecha"] = pd.to_datetime(datos["fecha"])
    datos["fecha"] = datos["fecha"].dt.to_period("M").dt.to_timestamp()
    return datos[datos["fecha"].le(CORTE_HISTORICO)].sort_values("fecha").reset_index(drop=True)


def cny_fin_de_mes(cotizaciones: pd.DataFrame) -> pd.Series:
    """Último tipo de pase (USD por CNY) publicado en cada mes."""
    pase = columnas_cotizacion("CNY")[0]
    datos = cotizaciones[["fecha", pase]].dropna().sort_values("fecha")
    mensual = datos.groupby(datos["fecha"].dt.to_period("M"))[pase].last()
    mensual.index = mensual.index.to_timestamp()
    return mensual


def porcion_no_activada(ii2: float, ii2_hasta_1_mes: float, cny: float) -> tuple[float | None, str | None]:
    """Componente en yuanes no activado del swap chino, en millones de USD."""
    nocional = NOCIONAL_SWAP_CHINA_CNY * cny
    swap_china = abs(ii2)
    aviso = None
    if abs(swap_china / nocional - 1) > TOLERANCIA_SWAP:
        # La planilla declara el otro swap en la banda de hasta un mes (DT, A.4 y A.9).
        swap_china = abs(ii2) - abs(ii2_hasta_1_mes)
        aviso = f"II.2 difiere del nocional chino; se descuentan {abs(ii2_hasta_1_mes):,.2f} de la banda hasta 1 mes"
        if abs(swap_china / nocional - 1) > TOLERANCIA_SWAP:
            return None, "II.2 no permite aislar el swap con China; revisar la planilla y los parámetros del swap"
    return swap_china - TRAMO_ACTIVADO_CNY * cny, aviso


def calcular_meses(irfcl: pd.DataFrame, cny: pd.Series, desde: pd.Timestamp) -> tuple[pd.DataFrame, list[str]]:
    """Aplica el método del DT a los meses posteriores a ``desde``."""
    filas, avisos = [], []
    notas = list(COLUMNAS_NOTAS.values())
    for _, fila in irfcl[irfcl["fecha"].gt(desde)].sort_values("fecha").iterrows():
        mes = fila["fecha"].strftime("%Y-%m")
        requeridas = [COL_BRUTAS, COL_II1, COL_II2, COL_II3, *notas]
        faltantes = [columna for columna in requeridas if pd.isna(fila.get(columna))]
        if faltantes:
            avisos.append(f"{mes}: faltan {', '.join(faltantes)}; mes sin calcular")
            continue
        brutas = float(fila[COL_BRUTAS])
        pasivos = float(fila[COL_II1]) + float(fila[COL_II2]) + float(fila[COL_II3])
        custodia = sum(float(fila[columna]) for columna in notas)
        if custodia > abs(float(fila[COL_II1])) + 0.01:
            avisos.append(f"{mes}: la custodia ({custodia:,.2f}) supera II.1; mes sin calcular")
            continue
        valores = {
            "brutas": brutas, "custodia": custodia, "propias": brutas - custodia,
            "prestadas": -pasivos - custodia, "netas_planilla": brutas + pasivos, "rin_eff": None,
        }
        valores["netas"] = valores["propias"] - valores["prestadas"]
        tipo = cny.get(fila["fecha"])
        if pd.isna(tipo) or pd.isna(fila.get(COL_ORO)) or pd.isna(fila.get(COL_DEG)):
            avisos.append(f"{mes}: falta oro, DEG o yuan de fin de mes; líquidas sin calcular")
            valores["liquidas"] = valores["propias_liquidas"] = None
        else:
            no_activada, aviso = porcion_no_activada(float(fila[COL_II2]), float(fila.get(COL_II2_1M) or 0), float(tipo))
            if aviso:
                avisos.append(f"{mes}: {aviso}")
            if no_activada is None:
                valores["liquidas"] = valores["propias_liquidas"] = None
            else:
                valores["liquidas"] = brutas - float(fila[COL_ORO]) - float(fila[COL_DEG]) - no_activada
                valores["propias_liquidas"] = valores["liquidas"] - custodia
        filas.append({"fecha": fila["fecha"], **{COLUMNAS[clave]: valor for clave, valor in valores.items()}})
    return pd.DataFrame(filas, columns=["fecha", *COLUMNAS.values()]), avisos


def leer_hojas_bcra() -> tuple[pd.DataFrame, pd.Series]:
    with pd.ExcelFile(ARCHIVO_BD) as libro:
        irfcl = pd.read_excel(libro, sheet_name=HOJA_IRFCL)
        cotizaciones = pd.read_excel(libro, sheet_name=HOJA_COTIZACIONES)
    for datos in (irfcl, cotizaciones):
        datos["fecha"] = pd.to_datetime(datos["fecha"])
    irfcl["fecha"] = irfcl["fecha"].dt.to_period("M").dt.to_timestamp()
    return irfcl, cny_fin_de_mes(cotizaciones)


def filas_indice(serie: pd.DataFrame) -> pd.DataFrame:
    filas = []
    for definicion in SERIES:
        valores = serie[["fecha", definicion.columna]].dropna()
        automatica = definicion.clave != "rin_eff"
        actualizacion = (
            f" Hasta {CORTE_HISTORICO:%Y-%m}, base publicada por los autores; desde entonces se calcula con el mismo "
            "método a partir de la planilla IRFCL del BCRA (dataset IRFCL del FMI y notas del país) y del tipo de pase "
            "del yuan del BCRA." if automatica else ""
        )
        filas.append({
            "ID": f"rin-{definicion.clave.replace('_', '-')}-m", "Código fuente": CODIGO,
            "Nombre serie": definicion.nombre, "Variable": f"rin_{definicion.clave}",
            "Unidad": "Millones de dólares estadounidenses", "Valoración": "Precios corrientes",
            "Descripción": f"{definicion.descripcion} Saldo a fin de mes.{actualizacion}",
            "Frecuencia": "M", "Pestaña BD": HOJA, "Columna BD": definicion.columna,
            "Archivo origen": ARCHIVO_HISTORICO.name, "Hoja origen": "Series",
            "Origen": "Instituto Interdisciplinario de Economía Política (IIEP-UBA-CONICET)",
            "Fuente": DOCUMENTO, "Catálogo ID": "iiep-series-macro", "Dataset ID": "reservas-internacionales-argentina",
            "Título dataset": "Reservas Internacionales de la Argentina",
            "Tema dataset": "Reservas internacionales",
            "Responsable dataset": f"IIEP-UBA-CONICET ({AUTORES})",
            "Fuente de valores": "Base IIEP (DT-IIEP N° 126) y cálculo propio con el método del DT" if automatica
            else "Base IIEP (DT-IIEP N° 126)",
            "Fecha inicio": valores["fecha"].min(), "Fecha fin": valores["fecha"].max(), "Estado": "VIGENTE",
            "Institución": "IIEP", "Área": "Reservas Internacionales", "Tema": "Sector externo",
        })
    return pd.DataFrame(filas)


def procesar() -> dict[str, object]:
    historico = leer_historico()
    irfcl, cny = leer_hojas_bcra()
    nuevos, avisos = calcular_meses(irfcl, cny, CORTE_HISTORICO)
    serie = pd.concat([historico, nuevos], ignore_index=True).sort_values("fecha").reset_index(drop=True)
    if serie["fecha"].duplicated().any():
        raise ValueError("La serie IIEP de reservas tiene meses duplicados")
    guardar_datos_preservando_formato(ARCHIVO_BD, {HOJA: serie}, {HOJA: "M"}, {HOJA})
    generar(filas_indice(serie))
    for aviso in avisos:
        print(f"IIEP reservas: {aviso}", flush=True)
    resumen = {
        "series": len(SERIES), "meses": len(serie), "historicos": len(historico), "calculados": len(nuevos),
        "hasta": serie["fecha"].max(),
    }
    print(f"IIEP reservas internacionales terminado: {resumen}", flush=True)
    return resumen


def ejecutar() -> None:
    procesar()


if __name__ == "__main__":
    ejecutar()
