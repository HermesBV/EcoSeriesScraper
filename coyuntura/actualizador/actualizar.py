"""Orquestación: analiza el libro, calcula el plan, lo aplica con Excel y publica.

Pasos:

1. Copia de trabajo del Excel vigente en ``coyuntura/.trabajo``.
2. Lectura de las series de ``BD.xlsx`` que pide ``mapeo.py``.
3. Plan por hoja (filas nuevas, celdas a escribir, fórmulas a copiar) y por
   bloque anual; con ``--simular`` termina acá e imprime el resumen.
4. Aplicación con Excel (COM), recálculo completo y guardado.
5. Verificación de la copia guardada con openpyxl (barra azul desplazada,
   fechas, fórmulas sin ``#REF!``, misma cantidad de gráficos).
6. Publicación: reemplazo atómico del Excel vigente, copia fechada en ``bkp``
   con rotación, copia para SeriesMacro y log en ``coyuntura/logs``.
"""

from __future__ import annotations

import argparse
from datetime import date
import os
from pathlib import Path
import shutil
import sys
import time

from openpyxl import load_workbook

from . import config, mapeo, respaldo
from .estructura import Libro
from .excel_com import Excel
from .fuentes import Fuentes
from .plan import PlanBloque, PlanHoja, planificar_bloque, planificar_hoja

MAX_EJEMPLOS = 12


class Registro:
    """Acumula el log en memoria y lo imprime a medida que avanza."""

    def __init__(self) -> None:
        self.lineas: list[str] = []

    def __call__(self, texto: str = "") -> None:
        self.lineas.append(texto)
        print(texto, flush=True)

    def guardar(self, ruta: Path) -> None:
        ruta.parent.mkdir(parents=True, exist_ok=True)
        ruta.write_text("\n".join(self.lineas) + "\n", encoding="utf-8")


def planificar(libro: Libro, fuentes: Fuentes, log: Registro) -> tuple[list[PlanHoja], list[PlanBloque]]:
    faltantes = fuentes.cargar(mapeo.fuentes_necesarias())
    for clave in sorted(faltantes):
        log(f"AVISO: la serie {clave} no está en el inventario; sus columnas no se actualizan")
    planes: dict[str, PlanHoja] = {}
    estructuras = {}
    for hoja in mapeo.HOJAS:
        if hoja.nombre not in libro.hojas():
            log(f"AVISO: la hoja {hoja.nombre!r} no existe en el libro")
            continue
        columnas = {letra: columna for letra, columna in hoja.columnas.items()
                    if not any(clave in faltantes for clave in mapeo.claves_de(columna.fuente))}
        hoja_util = mapeo.Hoja(hoja.nombre, hoja.frecuencia, hoja.col_fecha, hoja.fila_datos,
                               columnas, hoja.calendario, hoja.especial)
        col_anio = next((b.col_anio for b in mapeo.BLOQUES_ANUALES if b.hoja == hoja.nombre), None)
        estructura = libro.analizar(hoja_util, col_anio)
        estructuras[hoja.nombre] = estructura
        planes[hoja.nombre] = planificar_hoja(hoja_util, estructura, fuentes, planes)
    bloques = []
    for bloque in mapeo.BLOQUES_ANUALES:
        if bloque.hoja not in planes:
            continue
        columnas = {letra: columna for letra, columna in bloque.columnas.items()
                    if not any(clave in faltantes for clave in mapeo.claves_de(columna.fuente))}
        bloque_util = mapeo.BloqueAnual(bloque.hoja, bloque.col_anio, bloque.orden, bloque.hoja_mensual,
                                        bloque.col_control, columnas)
        plan = planificar_bloque(bloque_util, libro, estructuras[bloque.hoja], planes[bloque.hoja_mensual], fuentes)
        if plan is None:
            log(f"AVISO: {bloque.hoja}: no se encontró el bloque anual {bloque.orden}")
        else:
            bloques.append(plan)
    return list(planes.values()), bloques


def resumir(planes: list[PlanHoja], bloques: list[PlanBloque], log: Registro) -> None:
    for plan in planes:
        e = plan.estructura
        nuevos = [x for x in plan.escrituras if x.motivo == "nuevo"]
        reemplazos = [x for x in plan.escrituras if x.motivo == "reemplazo"]
        completas = [x for x in plan.escrituras if x.motivo == "completa"]
        log(f"[{plan.hoja.nombre}] último dato {e.ultima_fecha.date()} (fila {e.ultima_fila}), barra azul fila {e.fila_azul}")
        if plan.filas_nuevas:
            log(f"    filas nuevas: {len(plan.filas_nuevas)} ({plan.filas_nuevas[0][1].date()} a {plan.filas_nuevas[-1][1].date()}); "
                f"fórmulas copiadas en {', '.join(sorted(plan.formulas, key=lambda c: (len(c), c))) or '-'}")
        log(f"    celdas: {len(nuevos)} nuevas, {len(completas)} completadas, {len(reemplazos)} reemplazadas, "
            f"{len(plan.conservadas)} conservadas sin fuente, {plan.formulas_respetadas} fórmulas respetadas")
        for x in reemplazos[:MAX_EJEMPLOS]:
            log(f"      reemplazo {x.col}{x.fila}: {x.anterior!r} -> {x.valor!r}")
        if len(reemplazos) > MAX_EJEMPLOS:
            log(f"      ... y {len(reemplazos) - MAX_EJEMPLOS} reemplazos más")
        for fila, col, valor in plan.conservadas[:MAX_EJEMPLOS]:
            log(f"      conservada {col}{fila}: {valor!r}")
        if len(plan.conservadas) > MAX_EJEMPLOS:
            log(f"      ... y {len(plan.conservadas) - MAX_EJEMPLOS} conservadas más")
        for aviso in plan.avisos:
            log(f"    AVISO: {aviso}")
    for plan in bloques:
        b = plan.bloque
        log(f"[{b.hoja} bloque anual {b.orden}] filas {plan.fila_inicio}-{plan.fila_fin}, años {plan.anios[0]}-{plan.anios[-1]}; "
            f"nuevos: {plan.nuevos or '-'}; celdas: {len(plan.escrituras)}")
        for x in plan.escrituras[:MAX_EJEMPLOS]:
            log(f"      {x.motivo} {x.col}{x.fila}: {x.anterior!r} -> {x.valor!r}")


def aplicar(ruta: Path, planes: list[PlanHoja], bloques: list[PlanBloque], log: Registro) -> None:
    inicio = time.time()
    with Excel() as excel:
        excel.abrir(ruta)
        # Primero todas las inserciones y recién después fórmulas y valores: las
        # fórmulas copiadas en una hoja apuntan a filas de otras hojas (Fiscal a
        # IPC, IPC a Tipo de cambio mensual) y Excel las corre si la otra hoja
        # recibe filas más tarde. Es el orden en que lo hace el economista.
        insertadas: dict[str, int] = {}
        for plan in planes:
            if plan.insertadas:
                excel.insertar_filas(excel.hoja(plan.hoja.nombre), plan.estructura.fila_azul, plan.insertadas)
            insertadas[plan.hoja.nombre] = plan.insertadas
        # Los bloques anuales están debajo de la barra azul: sus filas ya bajaron
        # tantas posiciones como filas mensuales se insertaron en esa hoja. Dos
        # bloques de la misma hoja se corren además por lo que creció el anterior.
        corrimiento: dict[str, int] = {}
        filas_bloque: dict[int, int] = {}
        for indice, plan in enumerate(bloques):
            b = plan.bloque
            desplaza = insertadas.get(b.hoja, 0) + corrimiento.get(b.hoja, 0)
            filas_bloque[indice] = desplaza
            if plan.nuevos:
                excel.insertar_filas(excel.hoja(b.hoja), plan.fila_fin + desplaza + 1, len(plan.nuevos))
            corrimiento[b.hoja] = corrimiento.get(b.hoja, 0) + len(plan.nuevos)
        for plan in planes:
            if not plan.cambia:
                continue
            ws = excel.hoja(plan.hoja.nombre)
            if plan.filas_nuevas:
                for fila, fecha in plan.filas_nuevas:
                    excel.escribir_fecha(ws, fila, plan.hoja.col_fecha, fecha)
                desde, hasta = plan.filas_nuevas[0][0], plan.filas_nuevas[-1][0]
                for col, fila_formula in plan.formulas.items():
                    excel.copiar_formula(ws, col, fila_formula, desde, hasta)
            for x in plan.escrituras:
                excel.escribir(ws, x.fila, x.col, x.valor)
            log(f"    Excel: {plan.hoja.nombre}: {plan.insertadas} filas insertadas, {len(plan.escrituras)} celdas escritas")
        for indice, plan in enumerate(bloques):
            b = plan.bloque
            ws = excel.hoja(b.hoja)
            desplaza = filas_bloque[indice]
            fin = plan.fila_fin + desplaza
            if plan.nuevos:
                for i, anio in enumerate(plan.nuevos):
                    for col in plan.columnas_anio:
                        excel.escribir(ws, fin + 1 + i, col, anio)
                for col in plan.formulas:
                    excel.copiar_formula(ws, col, fin, fin + 1, fin + len(plan.nuevos))
            for x in plan.escrituras:
                excel.escribir(ws, x.fila + desplaza, x.col, x.valor)
            if plan.nuevos or plan.escrituras:
                log(f"    Excel: {b.hoja} bloque {b.orden}: años nuevos {plan.nuevos or '-'}, {len(plan.escrituras)} celdas")
        log("    Excel: recalculando...")
        excel.recalcular()
        excel.guardar()
    log(f"    Excel: guardado en {time.time() - inicio:.0f} s")


def verificar(ruta: Path, planes: list[PlanHoja], log: Registro) -> list[str]:
    """Controles sobre la copia guardada; devuelve los problemas encontrados."""
    problemas: list[str] = []
    formulas = load_workbook(ruta)
    valores = load_workbook(ruta, data_only=True)
    for plan in planes:
        hoja = plan.hoja
        ws, wv = formulas[hoja.nombre], valores[hoja.nombre]
        if len(ws._charts) != plan.estructura.graficos:
            problemas.append(f"{hoja.nombre}: había {plan.estructura.graficos} gráficos y quedaron {len(ws._charts)}")
        if not plan.cambia:
            continue
        esperada = plan.estructura.fila_azul + plan.insertadas
        libro = Libro.__new__(Libro)
        libro.formulas, libro.valores = formulas, valores
        try:
            nueva = libro.analizar(hoja, None)
        except ValueError as error:
            problemas.append(str(error))
            continue
        if nueva.fila_azul != esperada:
            problemas.append(f"{hoja.nombre}: la barra azul debía quedar en {esperada} y está en {nueva.fila_azul}")
        if plan.filas_nuevas and nueva.ultima_fecha != plan.filas_nuevas[-1][1]:
            problemas.append(f"{hoja.nombre}: la última fecha es {nueva.ultima_fecha} y debía ser {plan.filas_nuevas[-1][1]}")
        for fila, _ in plan.filas_nuevas:
            for col in plan.formulas:
                celda = ws[f"{col}{fila}"].value
                if not (isinstance(celda, str) and celda.startswith("=")):
                    problemas.append(f"{hoja.nombre}!{col}{fila}: debía tener fórmula y tiene {celda!r}")
                    break
        for fila in ws.iter_rows(min_row=max(1, nueva.ultima_fila - 2), max_row=nueva.fila_azul + 60):
            for celda in fila:
                if isinstance(celda.value, str) and "#REF!" in celda.value:
                    problemas.append(f"{hoja.nombre}!{celda.coordinate}: fórmula con #REF!")
    for problema in problemas:
        log(f"PROBLEMA: {problema}")
    return problemas


def publicar(trabajo: Path, hoy: date, log: Registro) -> None:
    config.ARCHIVO.parent.mkdir(parents=True, exist_ok=True)
    os.replace(trabajo, config.ARCHIVO)
    copia = respaldo.guardar_copia(config.ARCHIVO, hoy)
    log(f"Publicado {config.ARCHIVO.name}; respaldo {copia.name}")
    rotacion = respaldo.rotar(hoy)
    for nombre in rotacion["comprimidos"]:
        log(f"Respaldo comprimido: {nombre}")
    for nombre in rotacion["borrados"]:
        log(f"Respaldo borrado por antigüedad: {nombre}")
    if config.DESTINO_WEB.parent.parent.is_dir():
        config.DESTINO_WEB.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(config.ARCHIVO, config.DESTINO_WEB)
        log(f"Copia para SeriesMacro: {config.DESTINO_WEB}")
    else:
        log("SeriesMacro no está junto a este repositorio; no se copió el Excel para la web")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Actualiza el Excel de coyuntura desde BD.xlsx")
    parser.add_argument("--simular", action="store_true", help="calcula e imprime el plan sin tocar Excel")
    parser.add_argument("--origen", type=Path, default=config.ARCHIVO, help="Excel de coyuntura a actualizar")
    parser.add_argument("--fecha", type=date.fromisoformat, default=date.today(), help="fecha del respaldo (AAAA-MM-DD)")
    args = parser.parse_args(argv)
    log = Registro()
    log(f"Actualización del Excel de coyuntura — {args.fecha:%Y-%m-%d}")
    origen: Path = args.origen
    if not origen.is_file():
        log(f"ERROR: no existe {origen}")
        return 1
    inicio = time.time()
    libro = Libro(origen)
    fuentes = Fuentes()
    planes, bloques = planificar(libro, fuentes, log)
    resumir(planes, bloques, log)
    log(f"Plan calculado en {time.time() - inicio:.0f} s")
    if args.simular or not any(p.cambia for p in planes) and not any(b.nuevos or b.escrituras for b in bloques):
        log("Sin cambios para aplicar" if not args.simular else "Simulación: no se modificó el Excel")
        return 0
    config.CARPETA_TRABAJO.mkdir(parents=True, exist_ok=True)
    trabajo = config.CARPETA_TRABAJO / f"{config.NOMBRE_BASE}.{args.fecha:%y%m%d}.xlsx"
    shutil.copy2(origen, trabajo)
    try:
        aplicar(trabajo, planes, bloques, log)
        problemas = verificar(trabajo, planes, log)
        if problemas:
            log(f"ERROR: la verificación encontró {len(problemas)} problemas; el Excel vigente no se modificó. "
                f"Copia con problemas: {trabajo}")
            return 2
        publicar(trabajo, args.fecha, log)
    finally:
        log.guardar(config.CARPETA_LOGS / f"actualizacion_{args.fecha:%y%m%d}.txt")
    log(f"Listo en {time.time() - inicio:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
