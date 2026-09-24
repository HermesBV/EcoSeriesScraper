"""Construye series mensuales a partir de los informes de caja de Hacienda."""

from __future__ import annotations

from html import unescape
from io import BytesIO
from pathlib import Path
from datetime import date, datetime
import hashlib
import re
import shutil
import subprocess
import tempfile
import unicodedata
import zipfile
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from scrapers.scraper_IED import guardar_datos_preservando_formato


ROOT = Path(__file__).resolve().parents[1]
ARCHIVO_BD = ROOT / "BD.xlsx"
CARPETA = ROOT / "fuentes_BD" / "MECON" / "Hacienda"
PAGINA = "https://www.argentina.gob.ar/economia/sechacienda/infoestadistica"
CODIGO_FUENTE = "mecon-hacienda-caja"
HOJA_BD = "MECON Hacienda Caja M"
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
         "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10,
         "noviembre": 11, "diciembre": 12}
MESES_CORTOS = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
                "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12}


def _texto(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def descubrir_urls(session: requests.Session | None = None) -> list[str]:
    own = session is None
    session = session or requests.Session()
    try:
        response = session.get(PAGINA, timeout=(20, 120)); response.raise_for_status()
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', response.text, flags=re.IGNORECASE)
        urls = {urljoin(PAGINA, unescape(href)) for href in hrefs
                if re.search(r"\.(?:xlsx?|zip|rar)(?:\?|$)", href, flags=re.IGNORECASE)}
        return sorted(urls)
    finally:
        if own: session.close()


def descargar() -> list[tuple[str, Path]]:
    CARPETA.mkdir(parents=True, exist_ok=True)
    results = []
    with requests.Session() as session:
        session.headers["User-Agent"] = "EcoSeriesScraper/1.0 (datos publicos Hacienda)"
        for url in descubrir_urls(session):
            basename = Path(urlparse(url).path).name
            name = f"{hashlib.sha1(url.encode()).hexdigest()[:8]}-{basename}"
            path = CARPETA / name
            if not path.is_file():
                temp = path.with_name(f"{path.name}.descarga")
                try:
                    with session.get(url, stream=True, timeout=(20, 240)) as response:
                        response.raise_for_status()
                        with temp.open("wb") as handle:
                            for block in response.iter_content(64 * 1024):
                                if block: handle.write(block)
                    if not urlparse(url).path.casefold().endswith((".zip", ".rar")):
                        with pd.ExcelFile(temp):
                            pass
                    temp.replace(path)
                except Exception as error:
                    print(f"Hacienda: se omite {url}: {error}", flush=True)
                finally:
                    temp.unlink(missing_ok=True)
            if path.is_file(): results.append((url, path))
    return results


def _periodo(raw: pd.DataFrame, path: Path) -> pd.Timestamp | None:
    filename = unicodedata.normalize("NFKD", path.name).encode("ascii", "ignore").decode().casefold()
    for month, number in {**MESES, **MESES_CORTOS}.items():
        match = re.search(rf"{month}[^a-z0-9]{{0,12}}(20\d{{2}}|\d{{2}})", filename)
        if match:
            year = int(match.group(1))
            if year < 100:
                year += 2000 if year < 70 else 1900
            return pd.Timestamp(year, number, 1)
    for value in raw.iloc[:8].to_numpy().ravel():
        if isinstance(value, (pd.Timestamp, datetime, date)):
            return pd.Timestamp(value.year, value.month, 1)
    text = " ".join(_texto(x) for x in raw.iloc[:8].to_numpy().ravel()) + " " + path.name
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().casefold()
    for month, number in MESES.items():
        match = re.search(rf"{month}[^0-9]{{0,20}}(20\d{{2}})", normalized)
        if match: return pd.Timestamp(int(match.group(1)), number, 1)
    return None


def _header(raw: pd.DataFrame, column: int) -> str:
    block = raw.iloc[7:10].ffill(axis=1)
    parts = []
    for value in block.iloc[:, column]:
        part = _texto(value)
        if part and part.casefold() not in {x.casefold() for x in parts}: parts.append(part)
    return " - ".join(parts)


def construir_salida(files: list[tuple[str, Path]]) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    observations: dict[str, list[tuple[pd.Timestamp, float]]] = {}
    metadata: dict[str, dict[str, str]] = {}
    used_files = set()
    for url, path in files:
        try:
            sources: list[tuple[str, object]] = []
            if path.suffix.casefold() == ".zip":
                with zipfile.ZipFile(path) as archive:
                    for member in archive.infolist():
                        if member.filename.casefold().endswith((".xls", ".xlsx")):
                            sources.append((member.filename, BytesIO(archive.read(member))))
            elif path.suffix.casefold() == ".rar":
                extractor = shutil.which("UnRAR.exe") or shutil.which("unrar")
                if not extractor and Path(r"C:\Program Files\WinRAR\UnRAR.exe").is_file():
                    extractor = r"C:\Program Files\WinRAR\UnRAR.exe"
                if not extractor:
                    print(f"Hacienda: no se encontró UnRAR para procesar {path.name}", flush=True)
                    continue
                with tempfile.TemporaryDirectory(prefix="hacienda_rar_") as temp_dir:
                    result = subprocess.run(
                        [extractor, "x", "-inul", "-o+", str(path), str(Path(temp_dir) / "")],
                        capture_output=True, text=True, timeout=120,
                    )
                    if result.returncode:
                        raise RuntimeError(result.stderr.strip() or f"UnRAR terminó con código {result.returncode}")
                    for member in Path(temp_dir).rglob("*"):
                        if member.is_file() and member.suffix.casefold() in {".xls", ".xlsx"}:
                            sources.append((member.name, BytesIO(member.read_bytes())))
            else:
                sources.append((path.name, path))

            for source_name, source in sources:
                book = pd.ExcelFile(source)
                for sheet in book.sheet_names:
                    sheet_lower = sheet.casefold()
                    if any(term in sheet_lower for term in ("varmensual", "salida prensa")):
                        continue
                    raw = pd.read_excel(book, sheet_name=sheet, header=None)
                    if raw.empty or raw.shape[1] < 4:
                        continue
                    date = _periodo(raw, Path(source_name))
                    if date is None:
                        continue

                    # Informes anteriores a 2020 publican el dato mensual en una
                    # columna con fecha y la comparación interanual en otra. Los
                    # informes recientes publican categorías en columnas.
                    date_columns = {}
                    for row in range(min(8, len(raw))):
                        for column in range(raw.shape[1]):
                            value = raw.iat[row, column]
                            if isinstance(value, (int, float)):
                                continue
                            parsed = pd.to_datetime(value, errors="coerce")
                            if pd.notna(parsed) and 1900 <= parsed.year <= 2100:
                                date_columns[column] = parsed.to_period("M")
                    current_columns = [column for column, period in date_columns.items()
                                       if period == date.to_period("M")]
                    legacy = bool(current_columns)
                    used_files.add(f"{path.name}:{source_name}:{sheet}" if path.suffix.casefold() == ".zip"
                                   else f"{path.name}:{sheet}")

                    sheet_kind = (
                        "aif" if "aif" in sheet_lower else
                        "acumulado" if any(term in sheet_lower for term in ("acumul", "ene-", "ene "))
                        else "mensual"
                    )
                    if legacy:
                        value_column = current_columns[0]
                        label_columns = range(min(value_column, 7))
                        last_labels = [""] * len(label_columns)
                        for row in range(5, len(raw)):
                            for column in label_columns:
                                part = _texto(raw.iat[row, column])
                                if part:
                                    last_labels[column] = part
                                    for deeper in range(column + 1, len(last_labels)):
                                        last_labels[deeper] = ""
                            labels = [item for item in last_labels if item]
                            concept_label = " / ".join(labels)
                            value = pd.to_numeric(pd.Series([raw.iat[row, value_column]]), errors="coerce").iloc[0]
                            if not concept_label or pd.isna(value):
                                continue
                            key = f"{_slug(concept_label)}|{sheet_kind}"
                            observations.setdefault(key, []).append((date, float(value)))
                            metadata[key] = {"concept": concept_label, "dimension": sheet_kind, "url": url}
                    else:
                        for row in range(10, len(raw)):
                            concept = _texto(raw.iat[row, 1]) if raw.shape[1] > 1 else ""
                            code = _texto(raw.iat[row, 0]) if raw.shape[1] else ""
                            if not concept:
                                continue
                            concept_label = f"{code} {concept}".strip()
                            concept_key = _slug(concept_label)
                            for column in range(2, raw.shape[1]):
                                value = pd.to_numeric(pd.Series([raw.iat[row, column]]), errors="coerce").iloc[0]
                                if pd.isna(value):
                                    continue
                                dimension = _header(raw, column) or f"columna-{column + 1}"
                                key = f"{concept_key}|{_slug(dimension)}|{sheet_kind}"
                                observations.setdefault(key, []).append((date, float(value)))
                                metadata[key] = {"concept": concept_label, "dimension": f"{sheet_kind} - {dimension}", "url": url}
        except Exception as error:
            print(f"Hacienda: no se pudo interpretar {path.name}: {error}", flush=True)
    frames, inventory = [], []
    for key, values in sorted(observations.items()):
        column = f"{metadata[key]['concept']} - {metadata[key]['dimension']}"
        series = pd.DataFrame(values, columns=["fecha", column]).sort_values("fecha").drop_duplicates("fecha", keep="last")
        frames.append(series.set_index("fecha"))
        inventory.append({
            "ID": key, "Código fuente": CODIGO_FUENTE, "Nombre serie": column,
            "Variable": _slug(metadata[key]["concept"]), "Unidades": "Millones de pesos",
            "Valoración": "Precios corrientes", "Descripción": "Sector Público Base Caja e Informe Mensual de Ingresos y Gastos",
            "Frecuencia": "M", "Pestaña BD": HOJA_BD, "Columna BD": column,
            "Archivo origen": "; ".join(sorted(used_files)), "Hoja origen": "Informe mensual",
            "Origen": "Secretaría de Hacienda, Ministerio de Economía", "Fuente": PAGINA,
            "Catálogo ID": "hacienda-informacion-estadistica", "Dataset ID": "sector-publico-base-caja",
            "Título dataset": "Sector Público Base Caja e Informe Mensual de Ingresos y Gastos",
            "Tema dataset": "MECON / Hacienda / Sector Público Base Caja e Informe Mensual de Ingresos y Gastos",
            "Responsable dataset": "Secretaría de Hacienda, Ministerio de Economía",
            "Fuente de valores": "Excel Hacienda", "Fecha inicio": series["fecha"].min(),
            "Fecha fin": series["fecha"].max(), "Estado": "VIGENTE",
        })
    if not frames: raise ValueError("No se extrajeron series de los informes de Hacienda")
    data = pd.concat(frames, axis=1).sort_index().reset_index()
    inventory_df = pd.DataFrame(inventory)
    # La planilla admite 16.384 columnas. Hacienda publica miles de aperturas;
    # repartirlas en pestañas mantiene toda la cobertura sin superar el límite.
    series_columns = [column for column in data.columns if column != "fecha"]
    outputs = {}
    for number, start in enumerate(range(0, len(series_columns), 12_000), start=1):
        sheet = f"MECON Hacienda {number:02d}"
        columns = series_columns[start:start + 12_000]
        outputs[sheet] = data[["fecha", *columns]].copy()
        inventory_df.loc[inventory_df["Columna BD"].isin(columns), "Pestaña BD"] = sheet
    return outputs, inventory_df


def procesar(descargar_archivos: bool = True) -> dict[str, int]:
    files = descargar() if descargar_archivos else [
        ("", p) for p in CARPETA.iterdir()
        if p.is_file() and p.suffix.casefold() in {".xls", ".xlsx", ".zip", ".rar"}
    ]
    sheets, inventory = construir_salida(files)
    from tools.generar_codificacion import cargar_indice
    current = cargar_indice()
    obsolete = set(current.loc[current["Código fuente"].eq(CODIGO_FUENTE), "Pestaña BD"].dropna()) if not current.empty else set()
    guardar_datos_preservando_formato(ARCHIVO_BD, sheets, {name: "M" for name in sheets}, obsolete)
    from tools.generar_codificacion import generar
    generar(inventory)
    result = {"series": len(inventory), "hojas": 1, "archivos": len(files)}
    print(f"MECON Hacienda terminado: {result}", flush=True)
    return result


def ejecutar() -> None:
    procesar(True)
