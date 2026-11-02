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

from . import config, filas, mapeo, respaldo
from .estructura import Libro
from .excel_com import Excel
from .formulas import planificar_formulas, referencias_a_barra, tramos
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
    # Las fórmulas se deciden con todas las hojas planificadas: sus entradas
    # pueden estar en otra hoja (Fiscal y Salarios leen IPC).
    planificar_formulas(libro, list(planes.values()))
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
            log(f"    filas nuevas: {len(plan.filas_nuevas)} ({plan.filas_nuevas[0][1].date()} a {plan.filas_nuevas[-1][1].date()})")
        if plan.formulas:
            detalle = ", ".join(
                f"{col} " + ",".join(str(a) if a == b else f"{a}-{b}" for a, b in tramos(filas))
                for col, (_, filas) in sorted(plan.formulas.items(), key=lambda x: (len(x[0]), x[0])))
            log(f"    fórmulas copiadas: {detalle}")
        log(f"    celdas: {len(nuevos)} nuevas, {len(completas)} completadas, {len(reemplazos)} reemplazadas, "
            f"{len(plan.imputadas)} imputadas, {len(plan.conservadas)} conservadas sin fuente, "
            f"{plan.formulas_respetadas} fórmulas respetadas")
        for x in reemplazos[:MAX_EJEMPLOS]:
            log(f"      reemplazo {x.col}{x.fila}: {x.anterior!r} -> {x.valor!r}")
        if len(reemplazos) > MAX_EJEMPLOS:
            log(f"      ... y {len(reemplazos) - MAX_EJEMPLOS} reemplazos más")
        for fila, col, anterior, valor in plan.imputadas[:MAX_EJEMPLOS]:
            log(f"      imputada {col}{fila}: {anterior!r} -> {valor!r} (promedio del dato anterior y el posterior)")
        if len(plan.imputadas) > MAX_EJEMPLOS:
            log(f"      ... y {len(plan.imputadas) - MAX_EJEMPLOS} imputadas más")
        if plan.limpiar:
            log(f"    fórmulas borradas hasta que llegue su dato: {', '.join(plan.limpiar)}")
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


def aplicar(ruta: Path, planes: list[PlanHoja], bloques: list[PlanBloque], log: Registro,
            protegidas: list[tuple[str, str, str]] = ()) -> None:
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
        # Referencias que el economista adelantó a la barra de otra hoja: la
        # inserción las corrió a la nueva barra; vuelven a su fila (el mes nuevo).
        for nombre, celda, formula in protegidas:
            excel.hoja(nombre).Range(celda).Formula = formula
            log(f"    Excel: {nombre}!{celda} conserva {formula}")
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
                fin = plan.fila_fin + desplaza
                excel.insertar_filas(excel.hoja(b.hoja), fin + 1, len(plan.nuevos))
                # Insertar debajo del último año no amplía los rangos que terminan
                # en él (VLOOKUP de COMEX Z, gráficos del cuadro anual).
                ampliadas = excel.ampliar_rangos(b.hoja, plan.fila_inicio + desplaza, fin, len(plan.nuevos))
                log(f"    Excel: {b.hoja} bloque {b.orden}: {ampliadas} fórmulas o series ampliadas hasta el año nuevo")
            corrimiento[b.hoja] = corrimiento.get(b.hoja, 0) + len(plan.nuevos)
        for plan in planes:
            if not plan.cambia:
                continue
            ws = excel.hoja(plan.hoja.nombre)
            for fila, fecha in plan.filas_nuevas:
                excel.escribir_fecha(ws, fila, plan.hoja.col_fecha, fecha)
            for col, (fila_formula, filas) in plan.formulas.items():
                for desde, hasta in tramos(filas):
                    excel.copiar_formula(ws, col, fila_formula, desde, hasta)
            for x in plan.escrituras:
                excel.escribir(ws, x.fila, x.col, x.valor)
            for celda in plan.limpiar:
                ws.Range(celda).ClearContents()
            log(f"    Excel: {plan.hoja.nombre}: {plan.insertadas} filas insertadas, {len(plan.escrituras)} celdas escritas, "
                f"{sum(len(filas) for _, filas in plan.formulas.values())} fórmulas copiadas")
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


def verificar(ruta: Path, planes: list[PlanHoja], log: Registro,
              protegidas: list[tuple[str, str, str]] = ()) -> list[str]:
    """Controles sobre la copia guardada; devuelve los problemas encontrados.

    Los errores calculados (#DIV/0!, #N/A) en las fórmulas copiadas sólo se
    avisan: algunos vienen del libro (COMEX Z busca el año en el cuadro anual).
    """
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
        errores = []
        for col, (_, filas) in plan.formulas.items():
            for fila in filas:
                celda = ws[f"{col}{fila}"].value
                if not (isinstance(celda, str) and celda.startswith("=")):
                    problemas.append(f"{hoja.nombre}!{col}{fila}: debía tener fórmula y tiene {celda!r}")
                valor = wv[f"{col}{fila}"].value
                if isinstance(valor, str) and valor.startswith("#"):
                    errores.append(f"{col}{fila}={valor}")
        if errores:
            log(f"AVISO: {hoja.nombre}: fórmulas copiadas con error: {', '.join(errores[:MAX_EJEMPLOS])}"
                + (f" y {len(errores) - MAX_EJEMPLOS} más" if len(errores) > MAX_EJEMPLOS else ""))
        for fila in ws.iter_rows(min_row=max(1, nueva.ultima_fila - 2), max_row=nueva.fila_azul + 60):
            for celda in fila:
                if isinstance(celda.value, str) and "#REF!" in celda.value:
                    problemas.append(f"{hoja.nombre}!{celda.coordinate}: fórmula con #REF!")
    for nombre, celda, formula in protegidas:
        if formulas[nombre][celda].value != formula:
            problemas.append(f"{nombre}!{celda}: debía conservar {formula} y tiene {formulas[nombre][celda].value}")
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
    config.CARPETA_TRABAJO.mkdir(parents=True, exist_ok=True)
    trabajo = config.CARPETA_TRABAJO / f"{config.NOMBRE_BASE}.{args.fecha:%y%m%d}.xlsx"
    shutil.copy2(origen, trabajo)
    try:
        fuentes = Fuentes()
        faltantes = set(fuentes.cargar(mapeo.fuentes_necesarias()))
        # Primero las filas que faltan o sobran en el medio de las hojas
        # diarias; el plan se calcula después, sobre el libro ya continuo.
        arreglos = filas.detectar(Libro(trabajo), fuentes, faltantes)
        for arreglo in arreglos:
            for fila, fecha in arreglo.insertar:
                log(f"[{arreglo.hoja}] falta el {fecha.date()} en el medio de la serie: se inserta debajo de la fila {fila}")
            for fila, fecha in arreglo.borrar:
                log(f"[{arreglo.hoja}] el {fecha.date()} está repetido (fila {fila}): se borra")
        if arreglos and not args.simular:
            with Excel() as excel:
                excel.abrir(trabajo)
                filas.aplicar(excel, arreglos, {h.nombre: h.col_fecha for h in mapeo.HOJAS}, log)
                excel.guardar()
        libro = Libro(trabajo)
        planes, bloques = planificar(libro, fuentes, log)
        resumir(planes, bloques, log)
        log(f"Plan calculado en {time.time() - inicio:.0f} s")
        protegidas = referencias_a_barra(libro, planes)
        for nombre, celda, formula in protegidas:
            log(f"[{nombre}] {celda} apunta a la barra de otra hoja ({formula}); conserva su fila")
        if args.simular or not arreglos and not any(p.cambia for p in planes)                 and not any(b.nuevos or b.escrituras for b in bloques):
            log("Sin cambios para aplicar" if not args.simular else "Simulación: no se modificó el Excel")
            trabajo.unlink(missing_ok=True)
            return 0
        aplicar(trabajo, planes, bloques, log, protegidas)
        problemas = verificar(trabajo, planes, log, protegidas)
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
