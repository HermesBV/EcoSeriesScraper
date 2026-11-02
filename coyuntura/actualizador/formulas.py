"""Qué fórmulas del economista se extienden y en qué filas.

Una columna de fórmulas está «viva» si el economista la dejó de llenar justo
donde dejaron de existir sus datos: después de su última celda no hay ninguna
fila con todas sus entradas cargadas. Si las hay y la columna quedó vacía, la
abandonó (EMAE AA:AP desde feb-2025, Salarios J) y no se revive.

Una fórmula viva se copia en cada fila posterior, nueva o existente, cuyas
entradas tengan dato en el estado final del libro, incluidas las de otras hojas
(Fiscal usa IPC, Salarios usa IPC, TCR Mensual usa IPC y Tipo de cambio
mensual). Así una fila que la fuente completa después (COMEX hasta diciembre,
SIPA con un mes de atraso) recibe sus fórmulas, y una fila sin datos no se
llena de -1, 0 o #DIV/0!.

Las entradas son las referencias con fila relativa (``C442``, ``IPC!$B1002``,
los extremos de ``Z430:Z441``), evaluadas en la fila de destino.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from openpyxl.utils import column_index_from_string, get_column_letter

from .estructura import Libro
from .plan import PlanHoja

# Referencia A1 con hoja opcional. El lookbehind evita tomar el final de un
# nombre de función (LOG10) o de otra referencia.
PATRON_REF = re.compile(
    r"(?<![\w$.'!])(?:'((?:[^']|'')+)'!|([A-Za-z_][\w.]*)!)?(\$?)([A-Z]{1,3})(\$?)(\d+)(?![\w(])")
PATRON_RANGO = re.compile(
    r"(?<![\w$.'!])((?:'(?:[^']|'')+'|[A-Za-z_][\w.]*)!)?(\$?[A-Z]{1,3}\$?)(\d+):(\$?[A-Z]{1,3}\$?)(\d+)(?![\w(])")


@dataclass(frozen=True)
class Ref:
    hoja: str | None
    col: str
    fila: int
    fila_relativa: bool


def referencias(formula: str) -> list[Ref]:
    refs = []
    for match in PATRON_REF.finditer(formula):
        citada, simple, _, col, fija, fila = match.groups()
        hoja = citada.replace("''", "'") if citada is not None else simple
        refs.append(Ref(hoja, col, int(fila), not fija))
    return refs


def ampliar_rangos(formula: str, hoja_formula: str, hoja: str, inicio: int, fin: int, nuevas: int) -> str:
    """Extiende hasta ``fin + nuevas`` los rangos de ``hoja`` que empiezan en
    ``[inicio, fin)`` y terminan en ``fin`` (el último año de un cuadro anual)."""

    def reemplazar(match: re.Match) -> str:
        prefijo, col1, fila1, col2, fila2 = match.groups()
        destino = hoja_formula
        if prefijo:
            destino = prefijo[:-1]
            if destino.startswith("'"):
                destino = destino[1:-1].replace("''", "'")
        if destino != hoja or int(fila2) != fin or not inicio <= int(fila1) < fin:
            return match.group(0)
        return f"{prefijo or ''}{col1}{fila1}:{col2}{fin + nuevas}"

    return PATRON_RANGO.sub(reemplazar, formula)


def _vacia(valor: object) -> bool:
    return valor is None or valor == ""


class Estado:
    """Contenido final de cada hoja planificada (antes de aplicar en Excel)."""

    def __init__(self, libro: Libro, planes: list[PlanHoja]) -> None:
        self.libro = libro
        self.planes = {plan.hoja.nombre: plan for plan in planes}
        self.escritas = {plan.hoja.nombre: {(x.fila, x.col) for x in plan.escrituras} for plan in planes}
        self.vivas: dict[str, dict[str, tuple[int, str, list[Ref], int]]] = {}
        self._memo: dict[tuple[str, int, str], bool] = {}
        self.limpias: set[tuple[str, int, str]] = set()  # fórmulas que se borran
        self._en_curso: set[tuple[str, int, str]] = set()

    def ultima_final(self, nombre: str) -> int:
        plan = self.planes[nombre]
        return plan.estructura.ultima_fila + plan.insertadas

    def _existente(self, nombre: str, fila: int, col: str) -> bool:
        if nombre not in self.libro.formulas.sheetnames:
            return True  # hoja externa o de otro libro: no se puede juzgar
        return not _vacia(self.libro.formulas[nombre].cell(fila, column_index_from_string(col)).value)

    def disponible(self, nombre: str, fila: int, col: str, final: bool = True) -> bool:
        """¿La celda tiene dato? ``fila`` está en la numeración final (o la
        original si ``final`` es False)."""
        plan = self.planes.get(nombre)
        if plan is None:
            return self._existente(nombre, fila, col)
        e = plan.estructura
        if not final:
            return self._existente(nombre, fila, col)
        if fila < e.fila_azul:
            if (nombre, fila, col) in self.limpias:
                return False
            if self._existente(nombre, fila, col) or (fila, col) in self.escritas[nombre]:
                return True
        elif fila < e.fila_azul + plan.insertadas:
            if col == plan.hoja.col_fecha or (fila, col) in self.escritas[nombre]:
                return True
        else:
            return self._existente(nombre, fila - plan.insertadas, col)
        viva = self.vivas.get(nombre, {}).get(col)
        if viva is None or fila <= viva[3] or fila > self.ultima_final(nombre):
            return False
        return self.entradas_listas(nombre, fila, col)

    def entradas_listas(self, nombre: str, fila: int, col: str, final: bool = True) -> bool:
        clave = (nombre, fila, col, final)
        if clave in self._memo:
            return self._memo[clave]
        if clave in self._en_curso:
            return False
        self._en_curso.add(clave)
        try:
            plantilla, _, refs, _ = self.vivas[nombre][col] if final else self._candidata[(nombre, col)]
            corrimiento = fila - plantilla
            listo = all(self.disponible(ref.hoja or nombre, ref.fila + corrimiento, ref.col, final)
                        for ref in refs if ref.fila_relativa)
        finally:
            self._en_curso.discard(clave)
        self._memo[clave] = listo
        return listo

    def detectar_vivas(self) -> None:
        self._candidata: dict[tuple[str, str], tuple[int, str, list[Ref], int]] = {}
        for nombre, plan in self.planes.items():
            e = plan.estructura
            ws = self.libro.formulas[nombre]
            self.vivas[nombre] = {}
            for col, (plantilla, formula) in e.formulas.items():
                if col in plan.hoja.columnas or col == plan.hoja.col_fecha:
                    continue
                indice = column_index_from_string(col)
                ultima_llena = next(fila for fila in range(e.ultima_fila, plantilla - 1, -1)
                                    if not _vacia(ws.cell(fila, indice).value))
                refs = referencias(formula)
                datos = (plantilla, formula, refs, ultima_llena)
                self._candidata[(nombre, col)] = datos
                if not any(ref.fila_relativa for ref in refs):
                    viva = ultima_llena == e.ultima_fila
                else:
                    viva = not any(self.entradas_listas(nombre, fila, col, final=False)
                                   for fila in range(ultima_llena + 1, e.ultima_fila + 1))
                if viva:
                    self.vivas[nombre][col] = datos
        self._memo.clear()


def _formulas_en_espera(estado: Estado, plan: PlanHoja) -> list[str]:
    """Fórmulas al final de la hoja cuyas entradas todavía no tienen dato.

    El economista a veces adelanta un mes (``TCR Mensual!T922`` usa el IPC de un
    mes que el INDEC no publicó) y la celda queda en #DIV/0! hasta que llega el
    dato. Se borran; cuando el dato llegue la columna, que sigue viva, se
    vuelve a estirar hasta esa fila.
    """
    nombre = plan.hoja.nombre
    e = plan.estructura
    ws = estado.libro.formulas[nombre]
    barras = {p.hoja.nombre: (p.estructura.fila_azul, p.insertadas) for p in estado.planes.values()}
    celdas = []
    for indice in range(1, e.max_col + 1):
        col = get_column_letter(indice)
        if col in plan.hoja.columnas or col == plan.hoja.col_fecha:
            continue
        for fila in range(e.ultima_fila, plan.hoja.fila_datos - 1, -1):
            formula = ws.cell(fila, indice).value
            if not (isinstance(formula, str) and formula.startswith("=")):
                break
            listo = True
            for ref in referencias(formula):
                if not ref.fila_relativa:
                    continue
                destino = ref.hoja or nombre
                barra, nuevas = barras.get(destino, (None, 0))
                if barra is not None and ref.fila >= barra + nuevas and destino != nombre:
                    listo = False   # apunta a la barra o más abajo: no es un dato
                elif not estado.disponible(destino, ref.fila, ref.col):
                    listo = False
                if not listo:
                    break
            if listo:
                break
            celdas.append(f"{col}{fila}")
    return celdas


def planificar_formulas(libro: Libro, planes: list[PlanHoja]) -> None:
    """Completa ``plan.formulas`` de cada hoja: columna -> (fila plantilla, filas),
    y ``plan.limpiar``: fórmulas finales que esperan un dato que no llegó."""
    estado = Estado(libro, planes)
    estado.detectar_vivas()
    while True:   # borrar una fórmula puede dejar sin entrada a otra de la misma fila
        antes = len(estado.limpias)
        for plan in planes:
            plan.limpiar = _formulas_en_espera(estado, plan)
            for celda in plan.limpiar:
                col = celda.rstrip("0123456789")
                estado.limpias.add((plan.hoja.nombre, int(celda[len(col):]), col))
        if len(estado.limpias) == antes:
            break
        estado._memo.clear()
    for plan in planes:
        nombre = plan.hoja.nombre
        plan.formulas = {}
        for col, (plantilla, _, refs, ultima_llena) in estado.vivas[nombre].items():
            filas = []
            for fila in range(ultima_llena + 1, estado.ultima_final(nombre) + 1):
                if not any(ref.fila_relativa for ref in refs):
                    if fila >= plan.estructura.fila_azul:  # sin entradas: sólo filas nuevas
                        filas.append(fila)
                elif estado.entradas_listas(nombre, fila, col):
                    filas.append(fila)
            if filas:
                plan.formulas[col] = (plantilla, filas)


def tramos(filas: list[int]) -> list[tuple[int, int]]:
    """Agrupa filas consecutivas en tramos (desde, hasta)."""
    resultado: list[tuple[int, int]] = []
    for fila in sorted(filas):
        if resultado and fila == resultado[-1][1] + 1:
            resultado[-1] = (resultado[-1][0], fila)
        else:
            resultado.append((fila, fila))
    return resultado


def referencias_a_barra(libro: Libro, planes: list[PlanHoja]) -> list[tuple[str, str, str]]:
    """Fórmulas que apuntan a la barra azul de otra hoja que va a recibir filas.

    El economista a veces adelanta un mes en una hoja (TCR Mensual!T922 usa
    ``IPC!B1007`` antes de que IPC tenga ese mes). Al insertar filas en la barra
    Excel corre esa referencia a la nueva barra; hay que devolverle su fila, que
    pasa a ser el mes nuevo. Devuelve (hoja, celda, fórmula original).
    """
    barras = {plan.hoja.nombre: (plan.estructura.fila_azul, plan.insertadas) for plan in planes if plan.insertadas}
    if not barras:
        return []
    protegidas = []
    for plan in planes:
        nombre = plan.hoja.nombre
        ws = libro.formulas[nombre]
        e = plan.estructura
        for fila in range(max(plan.hoja.fila_datos, e.ultima_fila - 40), e.fila_azul):
            for indice in range(1, e.max_col + 1):
                formula = ws.cell(fila, indice).value
                if not (isinstance(formula, str) and formula.startswith("=") and "!" in formula):
                    continue
                apunta, conflicto = False, False
                for ref in referencias(formula):
                    destino = ref.hoja or nombre
                    if destino not in barras:
                        continue
                    barra, nuevas = barras[destino]
                    if ref.fila < barra:
                        continue
                    if destino != nombre and barra <= ref.fila < barra + nuevas:
                        apunta = True
                    else:
                        conflicto = True
                if apunta and not conflicto:
                    protegidas.append((nombre, f"{get_column_letter(indice)}{fila}", formula))
    return protegidas
