"""Serie histórica RIPTE desde el PDF enlazado en la página oficial."""

from __future__ import annotations

from datetime import date
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
import re
from urllib.parse import urljoin

import pandas as pd
from pypdf import PdfReader
import requests

from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import cargar_indice, generar

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "fuentes_BD" / "MCH" / "RIPTE"
PAGE_URL = "https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte"
PDF = DIRECTORY / "latest.pdf"
SHEET = "MCH RIPTE mensual"
CODE = "mch-ripte"
MONTHS = {name: number for number, name in enumerate(
    "Enero Febrero Marzo Abril Mayo Junio Julio Agosto Septiembre Octubre Noviembre Diciembre".split(), 1)}


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.hrefs.append(href)


def find_pdf_url(html: str) -> str:
    parser = _Links()
    parser.feed(html)
    for href in parser.hrefs:
        match = re.search(r"(/sites/default/files/[^\"'?#]*ripte[^\"'?#]*\.pdf)", href, re.I)
        if match:
            return urljoin(PAGE_URL, match.group(1))
    raise ValueError("La página RIPTE no contiene un enlace al PDF de la serie")


def download() -> tuple[Path, str]:
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    try:
        page = requests.get(PAGE_URL, timeout=(15, 60))
        page.raise_for_status()
        url = find_pdf_url(page.text)
        response = requests.get(url, timeout=(15, 120))
        response.raise_for_status()
        if not response.content.startswith(b"%PDF-"):
            raise ValueError("El enlace RIPTE no devuelve un PDF")
        reader = PdfReader(BytesIO(response.content))
        if len(reader.pages) < 2:
            raise ValueError("PDF RIPTE incompleto")
        temporary = DIRECTORY / ".latest.download.pdf"
        try:
            temporary.write_bytes(response.content)
            parse_pdf(temporary)
            temporary.replace(PDF)
            (DIRECTORY / "source_url.txt").write_text(url + "\n", encoding="utf-8")
        finally:
            temporary.unlink(missing_ok=True)
        return PDF, url
    except (requests.RequestException, ValueError):
        if not PDF.is_file():
            raise
        url_file = DIRECTORY / "source_url.txt"
        return PDF, url_file.read_text(encoding="utf-8").strip() if url_file.exists() else PAGE_URL


def parse_pdf(path: Path) -> pd.DataFrame:
    rows = []
    year = None
    pattern = re.compile(r"^(" + "|".join(MONTHS) + r")\s+\$\s*([\d.\s]+,\s*\d{2})", re.I)
    for page in PdfReader(path).pages[1:]:
        for line in (page.extract_text() or "").splitlines():
            line = line.strip()
            if re.fullmatch(r"(?:19|20)\d{2}", line):
                year = int(line)
                continue
            match = pattern.match(line)
            if match and year:
                month = MONTHS[match[1].capitalize()]
                amount = float(match[2].replace(".", "").replace(" ", "").replace(",", "."))
                rows.append((date(year, month, 1), amount))
    frame = pd.DataFrame(rows, columns=["fecha", "RIPTE (pesos)"])
    frame = frame.drop_duplicates("fecha", keep="last").sort_values("fecha").reset_index(drop=True)
    if len(frame) < 300 or frame.iloc[0, 0] != date(1994, 7, 1):
        raise ValueError("La serie RIPTE extraída no cubre la historia esperada")
    return frame


def procesar(fetch: bool = True) -> dict[str, int]:
    path, url = download() if fetch else (PDF, (DIRECTORY / "source_url.txt").read_text(encoding="utf-8").strip())
    frame = parse_pdf(path)
    inventory = pd.DataFrame([{
        "ID": "ripte-mensual", "Código fuente": CODE, "Nombre serie": "RIPTE",
        "Variable": "remuneracion-imponible-promedio-trabajadores-estables",
        "Unidad": "pesos", "Valoración": "Precios corrientes",
        "Descripción": "Remuneración Imponible Promedio de los Trabajadores Estables",
        "Frecuencia": "M", "Pestaña BD": SHEET, "Columna BD": "RIPTE (pesos)",
        "Archivo origen": path.name, "Hoja origen": "Serie histórica",
        "Origen": "Ministerio de Capital Humano", "Fuente": url,
        "Catálogo ID": CODE, "Dataset ID": "ripte", "Título dataset": "RIPTE",
        "Tema dataset": "MCH / Trabajo / Seguridad social / RIPTE",
        "Responsable dataset": "Subsecretaría de Seguridad Social",
        "Fuente de valores": "PDF oficial", "Fecha inicio": frame["fecha"].min(),
        "Fecha fin": frame["fecha"].max(), "Estado": "VIGENTE",
        "Institución": "Ministerio de Capital Humano", "Área": "Trabajo",
        "Subárea 1": "Seguridad social", "Subárea 2": "RIPTE", "Tema": "Trabajo e ingresos",
    }])
    current = cargar_indice()
    obsolete = set(current.loc[current["Código fuente"].eq(CODE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ROOT / "BD.xlsx", {SHEET: frame}, {SHEET: "M"}, obsolete)
    generar(inventory)
    return {"series": 1, "meses": len(frame)}


def ejecutar() -> None:
    procesar()
