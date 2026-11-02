"""Calendario de publicaciones de las instituciones de la base.

Lee los calendarios oficiales que existen (INDEC, BCRA, BLS), los acumula en
``calendario/publicaciones.json`` (la historia queda: cada lectura sólo
reemplaza su propia ventana de fechas) y lo exporta a
``calendario/publicaciones.ics`` para suscribirse desde Google Calendar u otro
cliente. ``calendario/estado.json`` dice, por código fuente y frecuencia, si la
base tiene lo último publicado:

- con calendario, compara el último período difundido con la ``Fecha fin`` más
  reciente de las series vigentes del código (``falta`` es un atraso seguro);
- sin calendario (Ámbito, BCRA diario, MECON, MCH, IIEP), avisa
  ``posible atraso`` si el último dato es más viejo que la tolerancia de su
  frecuencia.

Sólo informa; nunca modifica la base. Corre al final de ``main.py`` (el servidor
que ejecuta los scrapers lo actualiza en cada corrida); suelto, ``python -m tools.calendario``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, datetime, time, timedelta
from pathlib import Path
import hashlib
import json
import os

import pandas as pd

from tools.calendario import bcra, bls, indec
from tools.calendario.modelo import Lectura, Publicacion

RAIZ = Path(__file__).resolve().parents[2]
CARPETA = RAIZ / "calendario"
LECTORES: dict[str, Callable[[date], Lectura]] = {
    "INDEC": indec.descargar, "BCRA": bcra.descargar, "BLS": bls.descargar,
}
# Códigos sin un productor único (el catálogo de datos.gob.ar reúne decenas).
EXCLUIDOS = {"datos.gob.ar"}
# Días desde el comienzo del último período en base hasta considerarlo atrasado.
TOLERANCIA = {"D": 30, "S": 30, "M": 75, "T": 200, "A": 500}
TOLERANCIA_CODIGO = {("mch-sipa", "M"): 150, ("mch-ripte", "M"): 110}
DIFUSION_SIN_HORA = time(23, 59)  # Una difusión sin hora cuenta como salida al final del día.
UTC_ARGENTINA = timedelta(hours=3)  # Argentina no tiene horario de verano: UTC-3 todo el año.


def _momento(publicacion: Publicacion) -> datetime:
    return datetime.combine(publicacion.fecha, publicacion.hora or DIFUSION_SIN_HORA)


def fusionar(anteriores: Iterable[Publicacion], lecturas: Iterable[Lectura]) -> list[Publicacion]:
    """Cada lectura reemplaza lo guardado de su institución dentro de su ventana."""
    resultado = list(anteriores)
    for lectura in lecturas:
        resultado = [p for p in resultado
                     if not (p.institucion == lectura.institucion and lectura.desde <= p.fecha < lectura.hasta)]
        resultado.extend(lectura.publicaciones)
    unicas = {p.clave: p for p in resultado}
    return sorted(unicas.values(), key=lambda p: (_momento(p), p.institucion, p.titulo))


def _resumen(publicacion: Publicacion | None) -> dict | None:
    if publicacion is None:
        return None
    return {"titulo": publicacion.titulo, "fecha": publicacion.fecha.isoformat(),
            "hora": publicacion.hora.strftime("%H:%M") if publicacion.hora else None,
            "periodo": publicacion.periodo.strftime("%Y-%m-%d") if publicacion.periodo is not None else None}


def estado(publicaciones: list[Publicacion], indice: pd.DataFrame, ahora: datetime) -> list[dict]:
    vigentes = indice[(indice["Estado"].astype(str).str.upper() == "VIGENTE")
                      & ~indice["Código fuente"].isin(EXCLUIDOS)].copy()
    vigentes["Fecha fin"] = pd.to_datetime(vigentes["Fecha fin"], errors="coerce")
    vigentes = vigentes.dropna(subset=["Fecha fin"])
    grupos = vigentes.groupby(["Código fuente", "Frecuencia"], dropna=False)
    resultado = []
    for (codigo, frecuencia), filas in grupos:
        en_base = filas["Fecha fin"].max()
        propias = [p for p in publicaciones if codigo in p.codigos and p.periodo is not None]
        salidas = [p for p in propias if _momento(p) <= ahora]
        futuras = [p for p in propias if _momento(p) > ahora]
        fila = {"codigo": codigo, "frecuencia": frecuencia,
                "institucion": str(filas["Institución"].mode().iat[0]) if "Institución" in filas else None,
                "en_base": en_base.strftime("%Y-%m-%d"), "series": int(len(filas))}
        if salidas:
            ultima = max(salidas, key=lambda p: (p.periodo, _momento(p)))
            fila.update(metodo="calendario", publicado=_resumen(ultima),
                        proxima=_resumen(min(futuras, key=_momento) if futuras else None),
                        situacion="falta" if en_base < ultima.periodo else "al día")
        else:
            tolerancia = TOLERANCIA_CODIGO.get((codigo, frecuencia), TOLERANCIA.get(frecuencia, 75))
            atraso = (ahora - en_base.to_pydatetime()).days
            fila.update(metodo="regla", tolerancia_dias=tolerancia, dias_desde_ultimo=atraso,
                        publicado=None, proxima=_resumen(min(futuras, key=_momento) if futuras else None),
                        situacion="posible atraso" if atraso > tolerancia else "al día")
        resultado.append(fila)
    orden = {"falta": 0, "posible atraso": 1, "al día": 2}
    return sorted(resultado, key=lambda f: (orden[f["situacion"]], f["codigo"], str(f["frecuencia"])))


def _escapar(texto: str) -> str:
    return texto.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")


def _plegar(linea: str) -> str:
    """Corta en 75 octetos como pide RFC 5545, sin partir caracteres."""
    partes, actual = [], ""
    for caracter in linea:
        if len((actual + caracter).encode("utf-8")) > (75 if not partes else 74):
            partes.append(actual)
            actual = ""
        actual += caracter
    partes.append(actual)
    return "\r\n ".join(partes)


def a_ics(publicaciones: Iterable[Publicacion], generado: datetime) -> str:
    sello = f"{generado + UTC_ARGENTINA:%Y%m%dT%H%M%S}Z"  # ``generado`` es hora argentina.
    lineas = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//SeriesScraper//Calendario de publicaciones//ES",
              "CALSCALE:GREGORIAN", "METHOD:PUBLISH", "X-WR-CALNAME:Publicaciones económicas",
              "X-WR-TIMEZONE:America/Argentina/Buenos_Aires"]
    for publicacion in publicaciones:
        uid = hashlib.sha1("|".join(publicacion.clave).encode("utf-8")).hexdigest()
        lineas += ["BEGIN:VEVENT", f"UID:{uid}@seriesscraper", f"DTSTAMP:{sello}"]
        if publicacion.hora:
            inicio = datetime.combine(publicacion.fecha, publicacion.hora) + UTC_ARGENTINA
            lineas += [f"DTSTART:{inicio:%Y%m%dT%H%M%S}Z", f"DTEND:{inicio + timedelta(minutes=30):%Y%m%dT%H%M%S}Z"]
        else:
            lineas += [f"DTSTART;VALUE=DATE:{publicacion.fecha:%Y%m%d}",
                       f"DTEND;VALUE=DATE:{publicacion.fecha + timedelta(days=1):%Y%m%d}"]
        detalle = [f"Fuente: {publicacion.calendario}"]
        if publicacion.codigos:
            detalle.insert(0, "Series de la base: " + ", ".join(publicacion.codigos))
        lineas += [f"SUMMARY:{_escapar(f'{publicacion.institucion}: {publicacion.titulo}')}",
                   f"DESCRIPTION:{_escapar(chr(10).join(detalle))}",
                   f"CATEGORIES:{_escapar(publicacion.institucion)}", "TRANSP:TRANSPARENT", "END:VEVENT"]
    lineas.append("END:VCALENDAR")
    return "\r\n".join(_plegar(linea) for linea in lineas) + "\r\n"


def _escribir(ruta: Path, contenido: str, nueva_linea: str | None = None) -> None:
    temporal = ruta.with_name(f".{ruta.name}.tmp")
    with temporal.open("w", encoding="utf-8", newline=nueva_linea) as archivo:
        archivo.write(contenido)
    os.replace(temporal, ruta)


def cargar(carpeta: Path = CARPETA) -> list[Publicacion]:
    ruta = carpeta / "publicaciones.json"
    if not ruta.is_file():
        return []
    return [Publicacion.de_dict(d) for d in json.loads(ruta.read_text(encoding="utf-8"))["publicaciones"]]


def actualizar(ahora: datetime | None = None, carpeta: Path = CARPETA,
               lectores: dict[str, Callable[[date], Lectura]] | None = None,
               indice: pd.DataFrame | None = None) -> list[dict]:
    """Lee los calendarios, guarda los tres archivos y devuelve el estado por fuente."""
    ahora = ahora or datetime.now()
    lecturas, errores = [], {}
    for nombre, lector in (lectores or LECTORES).items():
        try:
            lecturas.append(lector(ahora.date()))
        except Exception as exc:  # Un calendario caído no impide leer los otros.
            errores[nombre] = f"{type(exc).__name__}: {exc}"
    publicaciones = fusionar(cargar(carpeta), lecturas)
    if indice is None:
        from tools.generar_codificacion import cargar_indice
        indice = cargar_indice()
    estados = estado(publicaciones, indice, ahora)
    carpeta.mkdir(parents=True, exist_ok=True)
    generado = ahora.isoformat(timespec="seconds")
    lecturas_info = {l.institucion: {"calendario": l.calendario, "desde": l.desde.isoformat(),
                                     "hasta": l.hasta.isoformat(), "publicaciones": len(l.publicaciones)}
                     for l in lecturas}
    _escribir(carpeta / "publicaciones.json", json.dumps(
        {"generado": generado, "lecturas": lecturas_info, "errores": errores,
         "publicaciones": [p.a_dict() for p in publicaciones]}, ensure_ascii=False, indent=1) + "\n")
    _escribir(carpeta / "estado.json", json.dumps(
        {"generado": generado, "errores": errores, "fuentes": estados}, ensure_ascii=False, indent=1) + "\n")
    _escribir(carpeta / "publicaciones.ics", a_ics(publicaciones, ahora), nueva_linea="")
    informar(estados, errores)
    return estados


def informar(estados: list[dict], errores: dict[str, str]) -> None:
    print("\nCalendario de publicaciones (situación / código / en base / último publicado / próxima):", flush=True)
    for fila in estados:
        publicado = fila["publicado"]
        proxima = fila["proxima"]
        ultimo = f"{publicado['periodo'][:7]} el {publicado['fecha'][8:10]}/{publicado['fecha'][5:7]}" if publicado \
            else f"sin calendario, {fila['dias_desde_ultimo']} días (tolera {fila['tolerancia_dias']})"
        siguiente = f"{proxima['periodo'][:7]} el {proxima['fecha'][8:10]}/{proxima['fecha'][5:7]}" if proxima else "-"
        print(f"  {fila['situacion']:15} {fila['codigo']:24} {fila['frecuencia']} base {fila['en_base'][:7]}"
              f" | {ultimo} | próxima {siguiente}", flush=True)
    for fila in estados:
        if fila["situacion"] != "al día":
            print(f"AVISO {fila['codigo']} ({fila['frecuencia']}): {fila['situacion']}; la base llega a {fila['en_base'][:10]}",
                  flush=True)
    for nombre, error in errores.items():
        print(f"AVISO calendario {nombre}: no se pudo leer ({error})", flush=True)
