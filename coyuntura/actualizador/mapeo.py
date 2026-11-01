"""Mapeo de cada hoja del Excel de coyuntura a las series de ``BD.xlsx``.

Las series se identifican por (``Código fuente``, ``ID``) del inventario
``Codificacion``, nunca por nombre. ``coyuntura/herramientas/verificar_mapeo.py``
compara los valores ya cargados en el Excel con cada fuente y es la forma de
revisar este archivo cuando una fuente cambia de formato.

Modos de escritura por columna:

* ``reemplazar``: la fuente manda en todas las fechas que publica (series que
  se revisan: INDEC desestacionalizadas, BCRA, balance cambiario, CPI).
* ``agregar``: sólo se completan celdas vacías (cotizaciones de Ámbito, que no
  se revisan; lo que el economista haya corregido a mano queda como está).

En ambos modos una fecha que el Excel tiene y la fuente no (imputaciones del
economista) se conserva y se informa en el log.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Serie:
    codigo: str
    id: str
    factor: float = 1.0


@dataclass(frozen=True)
class Suma:
    partes: tuple[Serie, ...]
    factor: float = 1.0


@dataclass(frozen=True)
class Promedio:
    partes: tuple[Serie, ...]


@dataclass(frozen=True)
class Anual:
    """Agrega una serie trimestral a años completos (exactamente cuatro trimestres)."""
    serie: Serie
    metodo: str  # «suma» o «promedio»


Fuente = Serie | Suma | Promedio | Anual


@dataclass(frozen=True)
class Columna:
    fuente: Fuente
    modo: str = "reemplazar"
    # Una celda con fórmula es del economista (p. ej. «=+C2-D2») y no se pisa,
    # salvo en columnas donde la fórmula es una estimación provisoria que el
    # dato publicado debe reemplazar (CPI proyectado en «TCR Mensual»).
    pisar_formulas: bool = False


@dataclass(frozen=True)
class Hoja:
    nombre: str
    frecuencia: str  # «D» diaria, «M» mensual
    col_fecha: str
    fila_datos: int
    columnas: dict[str, Columna] = field(default_factory=dict)
    # «union»: todas las fechas que publican las fuentes de la hoja;
    # lista de letras: sólo esas columnas fijan el calendario;
    # («hoja», nombre, letra): los meses presentes en otra hoja del libro.
    calendario: object = "union"
    # «comex»: la columna de fechas se extiende hasta diciembre del año del
    # último dato y la barra azul queda fija debajo de diciembre.
    especial: str | None = None


@dataclass(frozen=True)
class BloqueAnual:
    """Cuadro de años debajo de la barra azul (uno por año calendario)."""
    hoja: str
    col_anio: str
    orden: int  # primer, segundo... bloque contiguo de años debajo de la barra
    hoja_mensual: str
    col_control: str  # columna mensual que debe tener diciembre para sumar el año
    columnas: dict[str, Columna] = field(default_factory=dict)


def _dmd(*ids: str) -> tuple[Serie, ...]:
    return tuple(Serie("bcra-dmd", identificador) for identificador in ids)


def _itcrm_m(nombre: str) -> Columna:
    return Columna(Serie("bcra-itc", f"ITCRMSerie.xlsx|ITCRM y bilaterales prom. mens.|{nombre}"))


def _ipi(indice: int) -> Columna:
    return Columna(Serie("indec-ipi-manufacturero", f"ipi_manufacturero.xls|Cuadro 2|{indice}"))


def _emae_letra(columna: int) -> Columna:
    # El ID del scraper EMAE es posicional: número de columna (1 = A) del cuadro INDEC.
    return Columna(Serie("indec-emae", f"emae_actividad.xls|Tabla Letras|{columna}"))


def _imig(concepto: str) -> Columna:
    return Columna(Serie("mecon-imig", concepto))


SIPA_A4 = ("a-4|a-4-remuneracion-de-las-personas-con-empleo-asalariado-registrado-en-el-sector-privado"
           "-por-todo-concepto-en-pesos-a-valores-corrientes-total-pais-remuneracion-mediana-")

HOJAS: list[Hoja] = [
    Hoja("Reservas diario", "D", "B", 2, {
        "C": Columna(Serie("bcra-dmd", "74")),
    }),
    Hoja("Reservas mensual", "M", "A", 2, {
        "J": Columna(Serie("iiep-rin", "rin-brutas-m")),
        "R": Columna(Serie("iiep-rin", "rin-netas-m")),
        "Z": Columna(Serie("iiep-rin", "rin-liquidas-m")),
    }),
    Hoja("Agregados monetarios diario", "D", "B", 2, {
        "C": Columna(Serie("bcra-dmd", "67")),               # Billetes y monedas en poder del público
        "E": Columna(Serie("bcra-dmd", "73")),               # Base monetaria más cuasimonedas
        "I": Columna(Suma(_dmd("85", "86"))),                # Vista total = cta. cte. + caja de ahorro
        "J": Columna(Suma(_dmd("94", "95"))),                # Vista privada
        "M": Columna(Suma(tuple(Serie("bcra-pas", f"Estra_dia_pri|pasdespri{n}") for n in (2, 4, 6, 8)), 0.001)),
        "P": Columna(Serie("bcra-dmd", "197")),              # M2 transaccional privado
        "Q": Columna(Suma(_dmd("87", "88", "89"))),          # Plazo total
        "R": Columna(Suma(_dmd("96", "97", "98"))),          # Plazo privado
    }, calendario=["C", "E"]),
    Hoja("Agregados monetarios mensual", "M", "A", 3, {}, calendario=("hoja", "Agregados monetarios diario", "B")),
    Hoja("Fiscal", "M", "B", 2, {
        "C": _imig("ingresos-totales"),
        "D": _imig("gastos-primarios"),
        "E": _imig("resultado-primario"),
        "F": _imig("intereses-netos"),
        "G": _imig("resultado-financiero"),
        "I": Columna(Serie("datos.gob.ar", "142.3_TOTAL_2001_M_37")),  # Recaudación (RECA, total)
    }),
    Hoja("IPC", "M", "A", 3, {
        "F": Columna(Serie("indec-ipc", "indices-ipc-cobertura-nacional|total-nacional-nivel-general")),
        "J": Columna(Serie("indec-ipc", "indices-ipc-cobertura-nacional|total-nacional-bienes")),
        "N": Columna(Serie("indec-ipc", "indices-ipc-cobertura-nacional|total-nacional-servicios")),
        "R": Columna(Serie("indec-ipc", "indices-ipc-cobertura-nacional|total-nacional-nucleo")),
        "V": Columna(Serie("indec-ipc", "indices-ipc-cobertura-nacional|total-nacional-estacional")),
        "Z": Columna(Serie("indec-ipc", "indices-ipc-cobertura-nacional|total-nacional-regulados")),
        "AD": Columna(Serie("indec-sipm", "IPIM|NG")),
    }, calendario=["F"]),  # el IPIM sale antes que el IPC: un mes sin IPC no abre fila
    Hoja("EMAE", "M", "A", 2, {
        "B": Columna(Serie("indec-emae", "emae_mensual.xls|Tabla|3")),
        "F": Columna(Serie("indec-emae", "emae_mensual.xls|Tabla|5")),
        "J": _emae_letra(3),
        "K": _emae_letra(4),
        "L": _emae_letra(5),
        "M": _emae_letra(6),
        "N": _emae_letra(7),
        "O": _emae_letra(8),
        "P": _emae_letra(9),
        "Q": _emae_letra(10),
        "R": _emae_letra(11),
        "S": _emae_letra(12),
        "T": _emae_letra(13),
        "U": _emae_letra(14),
        "V": _emae_letra(15),
        "W": _emae_letra(16),
        "X": _emae_letra(17),
        "Y": _emae_letra(18),
    }),
    Hoja("Ventas", "M", "A", 2, {
        "B": Columna(Serie("indec-supermercados", "supermercados.xlsx|Cuadro 1|2")),
        "F": Columna(Serie("indec-supermercados", "supermercados.xlsx|Cuadro 1|6")),
    }),
    Hoja("ISAC", "M", "A", 2, {
        "B": Columna(Serie("indec-isac", "isac.xls|Cuadro 1|3")),
        "F": Columna(Serie("indec-isac", "isac.xls|Cuadro 1|7")),
    }),
    Hoja("IPI", "M", "A", 2, {
        "B": Columna(Serie("indec-ipi-manufacturero", "ipi_manufacturero.xls|Cuadro 1|4")),
        "F": Columna(Serie("indec-ipi-manufacturero", "ipi_manufacturero.xls|Cuadro 1|8")),
        "I": _ipi(5), "J": _ipi(19), "K": _ipi(22), "L": _ipi(27), "M": _ipi(31), "N": _ipi(35),
        "O": _ipi(41), "P": _ipi(50), "Q": _ipi(54), "R": _ipi(61), "S": _ipi(65), "T": _ipi(69),
        "U": _ipi(74), "V": _ipi(78), "W": _ipi(82), "X": _ipi(85),
    }),
    Hoja("COMEX", "M", "B", 3, {
        "C": Columna(Serie("indec-ica", "balanmensual.xls|FOB-CIF|3")),      # Exportaciones FOB
        "E": Columna(Serie("indec-ica", "balanmensual.xls|FOB-CIF|8")),      # Importaciones CIF
        "H": Columna(Serie("bcra-mc-bc", "Balance Cambiario|bal003")),       # Cobros de exportaciones de bienes
        "J": Columna(Serie("bcra-mc-bc", "Balance Cambiario|bal004")),       # Pagos de importaciones de bienes
        "M": Columna(Serie("bcra-mc-bc", "Balance Cambiario|bal006")),       # Servicios - ingresos
        "O": Columna(Serie("bcra-mc-bc", "Balance Cambiario|bal007")),       # Servicios - egresos
        "Y": Columna(Serie("bcra-mc-bc", "Balance Cambiario|bal001")),       # Cuenta corriente cambiaria
    }, especial="comex"),
    Hoja("Riesgo país", "D", "B", 2, {
        "C": Columna(Serie("ambito-riesgo-pais", "ambito-riesgo-pais-puntos"), "agregar"),
    }),
    Hoja("Salarios", "M", "A", 3, {
        "B": Columna(Serie("mch-ripte", "ripte-mensual")),
        "C": Columna(Serie("mch-sipa", SIPA_A4 + "con-estacionalidad")),
        "D": Columna(Serie("mch-sipa", SIPA_A4 + "sin-estacionalidad")),
        "E": Columna(Serie("indec-salarios", "cuadro-1|sector-registrado-sector-privado-registrado-numero-indice")),
        "F": Columna(Serie("indec-salarios", "cuadro-1|sector-registrado-sector-publico-numero-indice")),
        "G": Columna(Serie("indec-salarios", "cuadro-1|sector-registrado-total-registrado-1-numero-indice")),
        "H": Columna(Serie("indec-salarios", "cuadro-1|sector-privado-no-registrado-2-total-registrado-1-numero-indice")),
        "I": Columna(Serie("indec-salarios", "cuadro-1|total-indice-de-salarios-total-registrado-1-numero-indice")),
    }),
    Hoja("Tipo de cambio diario", "D", "B", 2, {
        "C": Columna(Serie("bcra-com3500", "TCR diario y TCNPM|Tipo de Cambio de Referencia - en Pesos - por Dólar")),
        "F": Columna(Promedio((Serie("ambito-dolar-blue", "ambito-dolar-blue-compra"),
                               Serie("ambito-dolar-blue", "ambito-dolar-blue-venta"))), "agregar"),
        "I": Columna(Serie("ambito-dolar-mep", "ambito-dolar-mep-referencia"), "agregar"),
        "N": Columna(Serie("bcra-bandas", "Bandas de Flotacion Cambiaria|Bandas Inferior a partir del 01/01/2026")),
        "O": Columna(Serie("bcra-bandas", "Bandas de Flotacion Cambiaria|Bandas Superior a partir del 01/01/2026")),
    }, calendario=["C"]),
    Hoja("Tipo de cambio mensual", "M", "A", 3, {}, calendario=("hoja", "Tipo de cambio diario", "B")),
    Hoja("TCR diario", "D", "B", 2, {
        "C": Columna(Serie("bcra-itc", "ITCRMSerie.xlsx|ITCRM y bilaterales|ITCRM")),
    }),
    Hoja("TCR Mensual", "M", "B", 2, {
        "C": _itcrm_m("ITCRM"), "D": _itcrm_m("ITCRB Brasil"), "E": _itcrm_m("ITCRB Canadá"),
        "F": _itcrm_m("ITCRB Chile"), "G": _itcrm_m("ITCRB Estados Unidos"), "H": _itcrm_m("ITCRB México"),
        "I": _itcrm_m("ITCRB Uruguay"), "J": _itcrm_m("ITCRB China"), "K": _itcrm_m("ITCRB India"),
        "L": _itcrm_m("ITCRB Japón"), "M": _itcrm_m("ITCRB Reino Unido"), "N": _itcrm_m("ITCRB Suiza"),
        "O": _itcrm_m("ITCRB Zona Euro"), "P": _itcrm_m("ITCRB Vietnam"), "Q": _itcrm_m("ITCRB Sudamérica*"),
        "U": Columna(Serie("fred-cpiaucsl", "CPIAUCSL"), pisar_formulas=True),
    }),
]

BLOQUES_ANUALES: list[BloqueAnual] = [
    BloqueAnual("COMEX", "B", 1, "COMEX", "C", {
        "X": Columna(Anual(Serie("indec-cin", "cuadro-1|1-cuenta-corriente"), "suma")),
        "AB": Columna(Anual(Serie("indec-pib", "cuadro-8|B1b"), "promedio")),
    }),
    BloqueAnual("Fiscal", "B", 1, "Fiscal", "C"),   # sumas anuales
    BloqueAnual("Fiscal", "B", 2, "Fiscal", "C"),   # ratios sobre el PBI nominal de COMEX
]


def hoja(nombre: str) -> Hoja:
    return next(item for item in HOJAS if item.nombre == nombre)


def fuentes_necesarias() -> set[tuple[str, str]]:
    claves: set[tuple[str, str]] = set()
    for item in HOJAS:
        for columna in item.columnas.values():
            claves.update(claves_de(columna.fuente))
    for bloque in BLOQUES_ANUALES:
        for columna in bloque.columnas.values():
            claves.update(claves_de(columna.fuente))
    return claves


def claves_de(fuente: Fuente) -> list[tuple[str, str]]:
    if isinstance(fuente, Serie):
        return [(fuente.codigo, fuente.id)]
    if isinstance(fuente, Anual):
        return claves_de(fuente.serie)
    return [clave for parte in fuente.partes for clave in claves_de(parte)]
