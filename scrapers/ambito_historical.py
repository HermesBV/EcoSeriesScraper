"""Lectura compartida del histórico de Ámbito, con promedios por día."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import cargar_indice, generar

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Source:
    code: str
    folder: str
    title: str
    page: str
    api_paths: tuple[str, ...]
    columns: tuple[str, ...]
    origin: str
    unit: str
    topic: str
    first_date: date = date(1950, 1, 1)

    @property
    def directory(self) -> Path:
        return ROOT / "fuentes_BD" / "Ambito" / self.folder

    @property
    def file(self) -> Path:
        return self.directory / "history.csv"

    @property
    def sheet(self) -> str:
        return "Ambito " + self.folder + " diario"


class _Table(HTMLParser):
    def __init__(self):
        super().__init__()
        self.inside = False
        self.cell = None
        self.row = None
        self.rows = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "tbody" and "general-historical__tbody" in attrs.get("class", "").split():
            self.inside = True
        elif self.inside and tag == "tr":
            self.row = []
        elif self.inside and tag in {"td", "th"} and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if self.inside and tag in {"td", "th"} and self.cell is not None:
            self.row.append(" ".join(self.cell).strip())
            self.cell = None
        elif self.inside and tag == "tr" and self.row is not None:
            if self.row:
                self.rows.append(self.row)
            self.row = None
        elif tag == "tbody" and self.inside:
            self.inside = False


def _number(raw: object) -> float | None:
    value = re.sub(r"[^\d,.-]", "", str(raw)).strip()
    if not value or value in {"-", "--"}:
        return None
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
    elif value.count(".") == 1 and len(value.rsplit(".", 1)[1]) == 3:
        value = value.replace(".", "")
    try:
        return float(value)
    except ValueError:
        return None


def parse_history(body: str, source: Source) -> pd.DataFrame:
    stripped = body.lstrip()
    if stripped.startswith("[") or stripped.startswith("{"):
        import json
        payload = json.loads(body)
        if isinstance(payload, dict):
            payload = next((payload[key] for key in ("data", "results", "items") if key in payload), payload)
        if not isinstance(payload, list):
            raise ValueError("Formato JSON histórico desconocido")
        if payload and isinstance(payload[0], list):
            header = [str(x).casefold() for x in payload[0]]
            rows = payload[1:] if any("fecha" in x for x in header) else payload
        elif payload and isinstance(payload[0], dict):
            header = [str(x).casefold() for x in payload[0]]
            rows = [[item.get(key) for key in payload[0]] for item in payload]
        else:
            raise ValueError("Histórico JSON vacío")
    else:
        parser = _Table()
        parser.feed(body)
        header, rows = [], parser.rows
    if not rows:
        raise ValueError("La tabla histórica está vacía")
    if header and any(not any(column.casefold() in value for value in header) for column in source.columns):
        raise ValueError("El histórico no contiene las columnas esperadas")
    date_col = next((i for i, value in enumerate(header) if "fecha" in value), 0)
    data = []
    for row in rows:
        if len(row) <= date_col:
            continue
        raw_date = str(row[date_col]).strip()
        timestamp = pd.to_datetime(raw_date, dayfirst=bool(re.match(r"^\d{2}[-/]\d{2}[-/]\d{4}$", raw_date)), errors="coerce")
        if pd.isna(timestamp):
            continue
        values = {}
        for ordinal, column in enumerate(source.columns):
            position = next((i for i, value in enumerate(header) if column.casefold() in value), ordinal + 1)
            values[column] = _number(row[position]) if position < len(row) else None
        if any(value is not None for value in values.values()):
            data.append({"fecha": timestamp.normalize(), **values})
    if not data:
        raise ValueError("No se reconocieron observaciones históricas")
    frame = pd.DataFrame(data)
    # Algunas fechas tienen dos cotizaciones: el promedio conserva una observación diaria.
    return frame.groupby("fecha", as_index=False)[list(source.columns)].mean().sort_values("fecha")


def _today() -> date:
    from datetime import datetime
    return datetime.now(ZoneInfo("America/Argentina/Buenos_Aires")).date()


def _chrome() -> str:
    found = shutil.which("chrome") or shutil.which("chrome.exe")
    if found:
        return found
    candidates = [Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
                  Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe")]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    raise RuntimeError("Chrome no está disponible para consultar Ámbito")


def _browser_history(url: str) -> str:
    with tempfile.TemporaryDirectory(prefix="ambito-chrome-", ignore_cleanup_errors=True) as profile:
        result = subprocess.run(
            [_chrome(), "--headless=new", "--disable-gpu", "--no-first-run",
             f"--user-data-dir={profile}", "--virtual-time-budget=12000", "--dump-dom", url],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120,
            check=False,
        )
    match = re.search(r"<pre[^>]*>(.*?)</pre>", result.stdout, re.S)
    if result.returncode != 0 or not match:
        raise RuntimeError(f"Chrome no devolvió el histórico ({result.returncode})")
    return unescape(match.group(1))


def _fetch_history(source: Source, start: date, end: date) -> tuple[pd.DataFrame, str]:
    errors = []
    for path in source.api_paths:
        url = f"https://mercados.ambito.com/{path}/historico-general/{start:%Y-%m-%d}/{end:%Y-%m-%d}"
        try:
            try:
                response = requests.get(url, timeout=(15, 120), headers={
                    "Referer": source.page, "User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
                body = response.text
            except requests.RequestException:
                body = _browser_history(url)
            return parse_history(body, source), url
        except (requests.RequestException, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("; ".join(errors))


def _fetch_splitting(source: Source, start: date, end: date, skipped: list[date]) -> list[tuple[pd.DataFrame, str]]:
    try:
        return [_fetch_history(source, start, end)]
    except RuntimeError:
        if start == end:
            skipped.append(start)
            return []
        midpoint = start + timedelta(days=(end - start).days // 2)
        return (_fetch_splitting(source, start, midpoint, skipped)
                + _fetch_splitting(source, midpoint + timedelta(days=1), end, skipped))


def download(source: Source, today: date | None = None) -> Path:
    today = today or _today()
    source.directory.mkdir(parents=True, exist_ok=True)
    if source.file.exists():
        previous = pd.read_csv(source.file, parse_dates=["fecha"])
        start = max(source.first_date, previous["fecha"].max().date() - timedelta(days=7))
    else:
        previous = pd.DataFrame()
        start = source.first_date
    if start > today:
        return source.file
    if previous.empty and source.code == "ambito-dolar-mep":
        periods = [(date(year, 1, 1), min(date(year, 12, 31), today))
                   for year in range(start.year, today.year + 1)]
    else:
        periods = [(start, today)]
    chunks = []
    skipped: list[date] = []
    for period_start, period_end in periods:
        if source.code == "ambito-dolar-mep" and previous.empty:
            chunks.extend(_fetch_splitting(source, period_start, period_end, skipped))
        else:
            try:
                chunks.append(_fetch_history(source, period_start, period_end))
            except RuntimeError:
                if source.file.exists():
                    return source.file
                raise
    if not chunks:
        raise RuntimeError("Ámbito no devolvió observaciones históricas")
    frame = pd.concat([chunk for chunk, _ in chunks], ignore_index=True)
    if not previous.empty:
        frame = pd.concat([previous[~previous["fecha"].isin(frame["fecha"])], frame], ignore_index=True)
    frame = frame.groupby("fecha", as_index=False)[list(source.columns)].mean().sort_values("fecha")
    if len(frame) < 2:
        raise ValueError("Histórico demasiado corto")
    temporary = source.directory / ".history.download.csv"
    try:
        frame.to_csv(temporary, index=False)
        temporary.replace(source.file)
        (source.directory / "source_url.txt").write_text("\n".join(url for _, url in chunks) + "\n", encoding="utf-8")
        if skipped:
            (source.directory / "fechas_no_disponibles.txt").write_text(
                "\n".join(day.isoformat() for day in sorted(set(skipped))) + "\n", encoding="utf-8")
    finally:
        temporary.unlink(missing_ok=True)
    return source.file


def process(source: Source, fetch: bool = True) -> dict[str, int]:
    path = download(source) if fetch else source.file
    frame = pd.read_csv(path, parse_dates=["fecha"]).sort_values("fecha")
    if frame.empty or frame["fecha"].duplicated().any():
        raise ValueError("Histórico de Ámbito vacío o con fechas duplicadas")
    rows = []
    for column in source.columns:
        present = frame.loc[frame[column].notna(), "fecha"]
        if present.empty:
            continue
        rows.append({
            "ID": source.code + "-" + column.casefold(), "Código fuente": source.code,
            "Nombre serie": source.title + (" - " + column if len(source.columns) > 1 else ""),
            "Variable": source.code + "-" + column.casefold(), "Unidad": source.unit,
            "Valoración": "No aplica" if source.code == "ambito-riesgo-pais" else "Precios corrientes",
            "Descripción": source.title + ". Promedio de observaciones cuando hay más de una cotización en el día.",
            "Frecuencia": "D", "Pestaña BD": source.sheet, "Columna BD": column,
            "Archivo origen": path.name, "Hoja origen": "Histórico",
            "Origen": source.origin, "Fuente": source.page,
            "Catálogo ID": "Ámbito", "Dataset ID": source.code, "Título dataset": source.title,
            "Tema dataset": "Ámbito / Economía / Mercados / " + source.topic,
            "Responsable dataset": source.origin, "Fuente de valores": "Histórico HTML/API Ámbito",
            "Fecha inicio": present.min(), "Fecha fin": present.max(), "Estado": "VIGENTE",
            "Institución": source.origin, "Área": "Economía", "Subárea 1": "Mercados",
            "Subárea 2": source.topic,
            "Tema": {"Riesgo país": "Mercados financieros", "Tipo de cambio": "Sector externo"}[source.topic],
        })
    if not rows:
        raise ValueError("No hay series numéricas en el histórico")
    index = pd.DataFrame(rows)
    current = cargar_indice()
    obsolete = set(current.loc[current["Código fuente"].eq(source.code), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ROOT / "BD.xlsx", {source.sheet: frame}, {source.sheet: "D"}, obsolete)
    generar(index)
    return {"series": len(index), "dias": len(frame)}
