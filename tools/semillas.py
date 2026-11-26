"""Semillas históricas: datos que llegan una sola vez y quedan en ``BD.xlsx``.

Una semilla es un archivo que alguien pasa a mano (un Excel, un CSV) con un tramo
que no está en internet. Se carga una vez; desde ahí la fuente de ese tramo es
``BD.xlsx`` y el archivo ya no hace falta. Protocolo completo: ``docs/semillas.md``.

- Los archivos van a ``semillas/<código fuente>/`` (ignorada por git, separada de
  ``fuentes_BD/``, que es lo que bajan los scrapers).
- ``semillas/registro.json`` (versionado) dice qué se cargó: archivo, sha256,
  fecha, quién la pasó, hoja, columnas y meses que llenó.
- Caso a, tramo histórico de una serie que se sigue actualizando: el scraper llama a
  ``tramo_historico``, que lee la semilla si está y si no, lo publicado.
- Caso b, serie sólo histórica: un módulo ``scrapers/semilla_<FUENTE>.py`` (que
  ``main.py`` no corre) y ``python -m tools.semillas cargar semilla_<FUENTE> --paso X``.
  La corrida diaria conserva su hoja y sus filas del índice como las de cualquier
  fuente ajena.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
CARPETA = RAIZ / "semillas"
REGISTRO = CARPETA / "registro.json"
ARCHIVO_BD = RAIZ / "BD.xlsx"


def ruta(codigo: str, archivo: str) -> Path:
    """Dónde va el archivo de una semilla: ``semillas/<código fuente>/<archivo>``."""
    return CARPETA / codigo / archivo


def sha256(archivo: Path) -> str:
    digesto = hashlib.sha256()
    with archivo.open("rb") as entrada:
        for bloque in iter(lambda: entrada.read(1 << 20), b""):
            digesto.update(bloque)
    return digesto.hexdigest()


def leer_registro(registro: Path | None = None) -> list[dict]:
    registro = registro or REGISTRO
    if not registro.is_file():
        return []
    return json.loads(registro.read_text(encoding="utf-8"))


def registrar(entrada: dict, registro: Path | None = None) -> None:
    """Agrega o reemplaza (por código fuente y archivo) la entrada de una semilla."""
    registro = registro or REGISTRO
    entradas = [e for e in leer_registro(registro)
                if (e.get("codigo"), e.get("archivo")) != (entrada["codigo"], entrada["archivo"])]
    entradas.append(entrada)
    entradas.sort(key=lambda e: (e["codigo"], e["archivo"]))
    registro.parent.mkdir(parents=True, exist_ok=True)
    temporal = registro.with_suffix(".tmp")
    temporal.write_text(json.dumps(entradas, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporal.replace(registro)


def leer_publicado(hoja: str, ruta_bd: Path | None = None) -> pd.DataFrame:
    """Hoja de ``BD.xlsx`` (o lo pendiente del lote) con ``fecha`` como fecha."""
    from tools import lote
    datos = lote.leer_hoja(ruta_bd or ARCHIVO_BD, hoja)
    datos["fecha"] = pd.to_datetime(datos["fecha"], errors="coerce")
    return datos.dropna(subset=["fecha"]).sort_values("fecha").reset_index(drop=True)


def tramo_historico(archivo: Path, leer: Callable[[Path], pd.DataFrame], hoja: str,
                    ruta_bd: Path | None = None) -> tuple[pd.DataFrame, str]:
    """El tramo histórico de una serie que se sigue actualizando (caso a).

    Con la semilla en disco la lee con ``leer`` (la primera carga, o para recargarla
    corregida); sin ella, toma lo ya publicado en ``hoja``. Devuelve los datos y su
    origen, ``"semilla"`` o ``"BD.xlsx"``: quien llama recorta el tramo y, si la
    semilla necesita un ajuste (un empalme), lo aplica sólo cuando viene de ella.
    """
    if archivo.is_file():
        return leer(archivo), "semilla"
    try:
        return leer_publicado(hoja, ruta_bd), "BD.xlsx"
    except ValueError as error:
        raise FileNotFoundError(
            f"Falta la semilla {archivo.name} y {hoja} todavía no está en la base: hay que cargarla una vez "
            f"(docs/semillas.md)") from error


def meses(datos: pd.DataFrame, columnas: list[str]) -> dict[str, object]:
    """Rango que llenó una semilla: primera y última fecha con dato y cuántas fechas."""
    con_dato = datos.loc[datos[columnas].notna().any(axis=1), "fecha"]
    return {"desde": f"{con_dato.min():%Y-%m-%d}", "hasta": f"{con_dato.max():%Y-%m-%d}", "fechas": int(len(con_dato))}


def entrada_registro(codigo: str, archivo: Path, hoja: str, columnas: list[str], datos: pd.DataFrame,
                     paso: str, caso: str, nota: str = "") -> dict:
    return {
        "codigo": codigo, "archivo": archivo.name, "sha256": sha256(archivo), "cargada": date.today().isoformat(),
        "paso": paso, "caso": caso, "hoja": hoja, "columnas": columnas, **meses(datos, columnas),
        **({"nota": nota} if nota else {}),
    }


def cargar(modulo: str, paso: str, archivo: Path | None = None, ruta_bd: Path | None = None) -> dict:
    """Caso b: carga una serie sólo histórica desde ``scrapers/<modulo>.py`` y la registra.

    El módulo define ``CODIGO``, ``HOJA``, ``FRECUENCIA``, ``ARCHIVO`` (el nombre del
    archivo en ``semillas/<CODIGO>/``), ``leer(ruta) -> DataFrame`` (``fecha`` y una
    columna por serie) y ``filas_indice(datos) -> DataFrame``.
    """
    from scrapers.scraper_IED import guardar_datos_preservando_formato
    from tools.generar_codificacion import generar
    semilla = importlib.import_module(f"scrapers.{modulo}")
    archivo = archivo or ruta(semilla.CODIGO, semilla.ARCHIVO)
    if not archivo.is_file():
        raise FileNotFoundError(f"No está la semilla: {archivo}")
    datos = semilla.leer(archivo)
    columnas = [c for c in datos.columns if c != "fecha"]
    guardar_datos_preservando_formato(ruta_bd or ARCHIVO_BD, {semilla.HOJA: datos}, {semilla.HOJA: semilla.FRECUENCIA},
                                      {semilla.HOJA})
    generar(semilla.filas_indice(datos))
    entrada = entrada_registro(semilla.CODIGO, archivo, semilla.HOJA, columnas, datos, paso, "b")
    registrar(entrada)
    return entrada


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Semillas históricas (docs/semillas.md)")
    ordenes = parser.add_subparsers(dest="orden", required=True)
    orden = ordenes.add_parser("cargar", help="caso b: carga una serie sólo histórica y la registra")
    orden.add_argument("modulo", help="módulo de scrapers/, p. ej. semilla_IIEP_algo")
    orden.add_argument("--paso", required=True, help="quién pasó el archivo")
    orden.add_argument("--archivo", type=Path, help="otra ruta que semillas/<código>/<ARCHIVO>")
    orden = ordenes.add_parser("registrar", help="caso a: registra una semilla que ya cargó su scraper")
    for nombre in ("codigo", "hoja", "paso"):
        orden.add_argument(f"--{nombre}", required=True)
    orden.add_argument("--archivo", type=Path, required=True)
    orden.add_argument("--columnas", nargs="+", required=True, help="columnas de la hoja que llenó la semilla")
    orden.add_argument("--hasta", help="última fecha del tramo de la semilla (AAAA-MM-DD)")
    orden.add_argument("--cargada", help="fecha en que se cargó, si no fue hoy (AAAA-MM-DD)")
    orden.add_argument("--nota", default="", help="algo que la próxima persona tenga que saber")
    ordenes.add_parser("ver", help="lista el registro")
    args = parser.parse_args(argv)
    if args.orden == "cargar":
        print(json.dumps(cargar(args.modulo, args.paso, args.archivo), ensure_ascii=False, indent=2))
    elif args.orden == "registrar":
        datos = leer_publicado(args.hoja)
        if args.hasta:
            datos = datos[datos["fecha"].le(pd.Timestamp(args.hasta))]
        entrada = entrada_registro(args.codigo, args.archivo, args.hoja, args.columnas, datos, args.paso, "a",
                                   args.nota)
        if args.cargada:
            entrada["cargada"] = args.cargada
        registrar(entrada)
        print(json.dumps(entrada, ensure_ascii=False, indent=2))
    else:
        for entrada in leer_registro():
            print(f"{entrada['codigo']:<12} {entrada['hoja']:<20} {entrada['desde']} a {entrada['hasta']} "
                  f"({entrada['fechas']}) · {entrada['archivo']} · {entrada['paso']}, {entrada['cargada']}")


if __name__ == "__main__":
    main()
