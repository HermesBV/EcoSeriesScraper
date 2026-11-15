"""Revisión de la taxonomía sobre el inventario publicado, sin escribir nada.

    python -m tools.taxonomia.revisar --tema Precios
    python -m tools.taxonomia.revisar --tema Precios --hoja "4.1.2 IPC Capitulos" --muestras 8
    python -m tools.taxonomia.revisar --tema Precios --solo-problemas

Muestra la cobertura de las reglas, el árbol resultante con ejemplos (nombre nuevo ⟵
lo que decía la fuente) y los problemas que se pueden detectar automáticamente.
Termina con código 1 si hay problemas.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from . import NIVELES, aplicar
from .texto import plano

RAIZ = Path(__file__).resolve().parents[2]
INDICE = RAIZ / "IndiceSeries.xlsx"
TOPE_HOJA = 150   # series sueltas en un mismo grupo final: más que esto pide otra apertura
TOPE_NOMBRE = 120

# Lo que no debe quedar en un nombre: ya tiene su columna o es ruido de la fuente.
HIGIENE = (
    (r"\b(?:valores?|datos?|serie)\s+(?:mensual(?:es)?|trimestral(?:es)?|anual(?:es)?|diari[oa]s?)\b", "frecuencia en el nombre"),
    (r"\b(?:mensual|trimestral|anual|diari[oa])\.?$", "frecuencia al final del nombre"),
    (r"\bserie discontinuada\b|\bdiscontinuad[oa]\b", "estado en el nombre"),
    (r"\bcuadro\s+\d", "número de cuadro"),
    (r"\bbase\s+(?:\w+\s+)?(?:de\s+)?(?:19|20)\d{2}\b|(?:19|20)\d{2}\s*=\s*100", "base en el nombre"),
    (r"\b(?:19|20)\d{2}\s*[-–]\s*(?:19|20)\d{2}\b", "rango de años en el nombre"),
    (r"_", "guion bajo"),
    (r"\s-\s", "guion suelto como separador (usar « · »)"),
    (r"\|", "barra vertical"),
    (r"\s{2,}", "espacios repetidos"),
    (r"\(\s*\d+\s*\)", "llamada de nota al pie"),
    (r"\b\d+/(?=\s|$)", "llamada de nota al pie"),
    (r"^[^A-ZÁÉÍÓÚÑ0-9¿«\"]", "no empieza con mayúscula"),
    (r"[.,;:]$", "puntuación al final"),
)


def cargar() -> pd.DataFrame:
    """Inventario publicado; se guarda una copia rápida de lectura junto al índice."""
    cache = RAIZ / ".lote" / "indice_revision.json"
    if cache.is_file() and cache.stat().st_mtime >= INDICE.stat().st_mtime:
        return pd.DataFrame(json.loads(cache.read_text(encoding="utf-8")), dtype=object)
    datos = pd.read_excel(INDICE, sheet_name="Codificacion", dtype=object)
    cache.parent.mkdir(exist_ok=True)
    datos.astype(str).where(datos.notna(), None).to_json(cache, orient="records", force_ascii=False)
    return datos


def _t(valor: object) -> str:
    return " ".join(str(valor).split()) if pd.notna(valor) else ""


def main() -> int:
    opciones = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    opciones.add_argument("--tema", help="series que están o quedan en ese tema")
    opciones.add_argument("--archivo", help="limita a un Archivo origen")
    opciones.add_argument("--fuente", help="limita a un Código fuente")
    opciones.add_argument("--hoja", help="limita a una Hoja origen")
    opciones.add_argument("--muestras", type=int, default=3, help="ejemplos por grupo final")
    opciones.add_argument("--solo-problemas", action="store_true")
    opciones.add_argument("--sin-arbol", action="store_true")
    args = opciones.parse_args()

    antes = cargar()
    # El nombre de la fuente es el vigente la primera vez; después queda en su columna.
    if "Nombre fuente" not in antes:
        antes = antes.assign(**{"Nombre fuente": antes["Nombre serie"]})
    despues = aplicar(antes)
    cambio = despues["Nombre serie"].astype(str).ne(antes["Nombre serie"].astype(str))
    for columna in ("Tema", *NIVELES):
        cambio |= despues[columna].map(_t).ne(antes[columna].map(_t))
    clasificada = cambio | despues["Tema"].map(_t).eq(args.tema) & despues["Área"].map(_t).ne(antes["Área"].map(_t))

    if not (args.tema or args.fuente or args.archivo or args.hoja):
        opciones.error("indicar --tema, --fuente, --archivo o --hoja")
    alcance = pd.Series(True, index=antes.index)
    if args.tema:
        alcance = antes["Tema"].map(_t).eq(args.tema) | despues["Tema"].map(_t).eq(args.tema)
    if args.archivo:
        alcance &= antes["Archivo origen"].map(_t).eq(args.archivo)
    if args.fuente:
        alcance &= antes["Código fuente"].map(_t).eq(args.fuente)
    if args.hoja:
        alcance &= antes["Hoja origen"].map(_t).eq(args.hoja)
    filas = despues[alcance]
    problemas: list[str] = []

    # --- Cobertura ---
    sin_regla = filas[~clasificada[alcance]]
    print(f"== Cobertura: {len(filas) - len(sin_regla):,} de {len(filas):,} series del alcance tienen regla")
    if len(sin_regla):
        grupos = Counter(zip(sin_regla["Código fuente"].map(_t), sin_regla["Archivo origen"].map(_t),
                             sin_regla["Hoja origen"].map(_t), sin_regla["Frecuencia"].map(_t)))
        for clave, n in sorted(grupos.items()):
            problemas.append(f"sin regla: {' | '.join(clave)} ({n})")
    con_regla = filas[clasificada[alcance]]
    if args.tema:
        salen = con_regla[con_regla["Tema"].map(_t).ne(args.tema)]
        if len(salen):
            print(f"   {len(salen):,} series salen del tema hacia: {dict(Counter(salen['Tema'].map(_t)))}")
        con_regla = con_regla[con_regla["Tema"].map(_t).eq(args.tema)]

    # --- Árbol ---
    arbol: dict[tuple, list[int]] = defaultdict(list)
    for indice, fila in con_regla.iterrows():
        camino = (_t(fila["Tema"]), *(v for v in (_t(fila[c]) for c in NIVELES) if v))
        arbol[camino].append(indice)
    if not args.solo_problemas and not args.sin_arbol:
        print(f"\n== Árbol ({len(con_regla):,} series en {len(arbol)} grupos finales)")
        totales: Counter[tuple] = Counter()
        for camino, indices in arbol.items():
            for corte in range(1, len(camino) + 1):
                totales[camino[:corte]] += len(indices)
        vistos: set[tuple] = set()
        for camino in sorted(arbol):
            for corte in range(1, len(camino) + 1):
                if camino[:corte] not in vistos:
                    vistos.add(camino[:corte])
                    print(f"{'  ' * corte}{camino[corte - 1]}  [{totales[camino[:corte]]}]")
            indices = arbol[camino]
            paso = max(1, len(indices) // max(args.muestras, 1))
            for indice in indices[::paso][:args.muestras]:
                fila = despues.loc[indice]
                origen = _t(antes.at[indice, "Nombre fuente"])[:70]
                detalle = _t(fila["Descripción"])[:70]
                print(f"{'  ' * (len(camino) + 1)}· {_t(fila['Nombre serie'])}   [{_t(fila['Frecuencia'])}, {_t(fila['Unidad']) or 's/u'}, {_t(fila['Código fuente'])}]"
                      f"\n{'  ' * (len(camino) + 1)}    ⟵ {origen} | {detalle}")

    # --- Problemas ---
    for camino, indices in sorted(arbol.items()):
        if len(indices) > TOPE_HOJA:
            problemas.append(f"grupo final con {len(indices)} series (tope {TOPE_HOJA}): {' > '.join(camino)}")
    por_nivel: dict[tuple, set[str]] = defaultdict(set)
    for camino in arbol:
        for corte in range(len(camino)):
            por_nivel[camino[:corte]].add(camino[corte])
    for padre, hijos in por_nivel.items():
        iguales = defaultdict(set)
        for hijo in hijos:
            iguales[plano(hijo)].add(hijo)
        for variantes in iguales.values():
            if len(variantes) > 1:
                problemas.append(f"mismo grupo escrito distinto bajo «{' > '.join(padre)}»: {sorted(variantes)}")
    for indice, fila in con_regla.iterrows():
        nombre = _t(fila["Nombre serie"])
        for patron, motivo in HIGIENE:
            if re.search(patron, nombre, re.IGNORECASE if "mayúscula" not in motivo else 0):
                problemas.append(f"nombre ({motivo}): {nombre!r}  [{_t(fila['Código fuente'])}::{_t(fila['ID'])[:50]}]")
        if len(nombre) > TOPE_NOMBRE:
            problemas.append(f"nombre de {len(nombre)} caracteres: {nombre!r}")
    # Dos series de la misma hoja y frecuencia con igual nombre y unidad: se perdió algo que las distinguía.
    # Se compara dentro del mismo grupo del árbol: la misma partida en la versión vigente y
    # en «Series históricas» comparte nombre a propósito.
    repetidas = Counter(zip(con_regla["Código fuente"].map(_t), con_regla["Hoja origen"].map(_t),
                            con_regla["Frecuencia"].map(_t), con_regla["Unidad"].map(_t),
                            con_regla["Nombre serie"].map(_t),
                            *(con_regla[nivel].map(_t) for nivel in NIVELES)))
    for (fuente, hoja, frecuencia, unidad, nombre, *_niveles), n in sorted(repetidas.items()):
        if n > 1:
            problemas.append(f"{n} series distintas con el mismo nombre en {fuente} | {hoja} | {frecuencia}: {nombre!r}")

    print(f"\n== Problemas: {len(problemas)}")
    resumen = Counter(re.sub(r":.*", "", p) for p in problemas)
    for motivo, n in resumen.most_common():
        print(f"   {n:5d}  {motivo}")
    for problema in problemas[:400]:
        print("  -", problema)
    if len(problemas) > 400:
        print(f"  … y {len(problemas) - 400} más")
    return 1 if problemas else 0


if __name__ == "__main__":
    sys.exit(main())
