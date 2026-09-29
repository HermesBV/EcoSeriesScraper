"""Descarga por período y extracción de tablas oficiales de la segunda iteración."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import hashlib
import re

import pandas as pd
import requests

from scrapers.indec_workbooks import extract_tables, sheet_name, slug, text
from scrapers.scraper_IED import guardar_datos_preservando_formato
from tools.generar_codificacion import cargar_indice, generar

ROOT = Path(__file__).resolve().parents[1]
ROMAN = ("I", "II", "III", "IV")


@dataclass(frozen=True)
class Source:
    code: str
    institution: str
    folder: str
    route: str
    template: str
    cadence: str
    prefix: str
    origin: str = "Instituto Nacional de Estadística y Censos (INDEC)"

    @property
    def directory(self) -> Path:
        return ROOT / "fuentes_BD" / self.institution / self.folder

    @property
    def file(self) -> Path:
        return self.directory / ("latest.xlsx" if self.template.endswith(".xlsx") else "latest.xls")


def candidate_urls(source: Source, today: date | None = None, periods: int = 60):
    today = today or date.today()
    start = today.year * 12 + today.month - 1
    seen = set()
    for offset in range(periods):
        month_index = start - offset * (3 if source.cadence == "T" else 1)
        year, month_zero = divmod(month_index, 12)
        month = month_zero + 1
        quarter = (month_zero // 3) + 1
        key = (year, quarter) if source.cadence == "T" else (year, month)
        if key in seen:
            continue
        seen.add(key)
        yield source.template.format(MM=f"{month:02d}", AA=f"{year % 100:02d}",
                                     AAAA=year, T=ROMAN[quarter - 1])


def download(source: Source, today: date | None = None) -> tuple[Path, str]:
    source.directory.mkdir(parents=True, exist_ok=True)
    temporary = source.file.with_name(".latest.download" + source.file.suffix)
    error = None
    try:
        for url in candidate_urls(source, today):
            try:
                response = requests.get(url, timeout=(15, 120))
                response.raise_for_status()
                body = response.content
                if not (body.startswith(bytes.fromhex("d0cf11e0")) or body.startswith(bytes.fromhex("504b0304"))):
                    continue  # El servidor también responde HTML con HTTP 200.
                temporary.write_bytes(body)
                with pd.ExcelFile(temporary) as book:
                    if not book.sheet_names:
                        raise ValueError("Libro sin hojas")
                temporary.replace(source.file)
                (source.directory / "source_url.txt").write_text(url + "\n", encoding="utf-8")
                return source.file, url
            except requests.HTTPError as exc:
                error = exc
                if exc.response is None or exc.response.status_code not in {404, 410}:
                    break
                continue
            except requests.RequestException as exc:
                error = exc
                break
            except (ValueError, OSError) as exc:
                error = exc
                continue
        if source.file.is_file():
            url_file = source.directory / "source_url.txt"
            return source.file, url_file.read_text(encoding="utf-8").strip() if url_file.exists() else source.template
        raise RuntimeError(f"No se encontró un libro Excel válido para {source.code}") from error
    finally:
        temporary.unlink(missing_ok=True)


def _period(value: object, previous_year: int | None = None) -> pd.Timestamp | None:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return pd.Timestamp(value).replace(day=1)
    value = text(value)
    match = re.fullmatch(r"(20\d{2}|19\d{2})\s*T\s*([1-4])", value, re.I)
    if match:
        return pd.Timestamp(int(match[1]), (int(match[2]) - 1) * 3 + 1, 1)
    match = re.search(r"([1-4])\s*[°ºª]?\s*trimestre", value, re.I)
    if match and previous_year:
        return pd.Timestamp(previous_year, (int(match[1]) - 1) * 3 + 1, 1)
    return None


def _cin_contextual_label(tab: str, label: str, state: dict[str, str]) -> str:
    """Conserva la rama publicada por INDEC en cuadros con rótulos repetidos."""
    if tab == "Cuadro 20":
        if re.match(r"^(?:B90\.|[AP]\.\s)", label):
            state.update(major=label, category="", instrument="")
            return label
        if re.match(r"^[IVX]+\.\s", label, re.I):
            state.update(category=label, instrument="")
            return label
        if re.match(r"^[a-z]\.\s", label):
            state["instrument"] = label
            return label
        return " - ".join([*(state.get(key, "") for key in ("major", "category", "instrument") if state.get(key)), label])

    if tab in {"Cuadro 16", "Cuadro 21"}:
        if re.match(r"^(?:B90\.|[AP]\.\s)", label):
            state.update(major=label, sector="")
            return label
        if re.match(r"^S[A-Z0-9]+\.\s", label):
            state["sector"] = label
            return " - ".join([part for part in (state.get("major"), label) if part])
        return " - ".join([part for part in (state.get("major"), state.get("sector"), label) if part])

    if tab == "Cuadro 17":
        if re.match(r"^[AP]\.\s", label):
            state.update(major=label, category="", instrument="")
            return label
        if re.match(r"^\d+\.\s", label):
            state.update(category=label, instrument="")
            return " - ".join([part for part in (state.get("major"), label) if part])
        if label.casefold() in {"participaciones de capital", "instrumentos de deuda"}:
            state["instrument"] = label
            return " - ".join([part for part in (state.get("major"), state.get("category"), label) if part])
        return " - ".join([part for part in (state.get("major"), state.get("category"), state.get("instrument"), label) if part])

    if tab in {"Cuadro 23", "Cuadro 24", "Cuadro 29"}:
        if re.match(r"^(?:Gobierno general|Banco central|Sociedades |Otras sociedades |Otros sectores)", label, re.I):
            state.update(institution=label, term="")
            return label
        if re.match(r"^A (?:corto|largo) plazo$", label, re.I):
            state["term"] = label
            return " - ".join([part for part in (state.get("institution"), label) if part])
        if label.casefold().startswith(("total deuda", "saldo deuda")):
            state.clear()
            return label
        return " - ".join([part for part in (state.get("institution"), state.get("term"), label) if part])

    return label


def extract_horizontal(path: Path, source: Source, url: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    outputs = {}
    inventory = []
    with pd.ExcelFile(path) as book:
        for tab in book.sheet_names:
            if slug(tab) in {"indice", "caratula", "nota-metodologica", "ponderaciones"}:
                continue
            raw = pd.read_excel(book, sheet_name=tab, header=None)
            if raw.empty or raw.shape[1] < 5:
                continue
            best = (0, None, [])
            for row in range(min(12, len(raw))):
                dates = []
                year = None
                for col in range(raw.shape[1]):
                    if row and col:
                        year_raw = raw.iat[row - 1, col]
                        marked_year = re.match(r"^((?:19|20)\d{2})(?:\s|$)", text(year_raw))
                        if marked_year:
                            year = int(marked_year[1])
                    dates.append(_period(raw.iat[row, col], year))
                score = sum(value is not None for value in dates)
                if score > best[0]:
                    best = (score, row, dates)
            count, header, dates = best
            if count < 6 or header is None:
                continue
            columns = [(col, dt) for col, dt in enumerate(dates) if dt is not None]
            if len(set(dt for _, dt in columns)) < 6:
                continue
            frequency = "T" if any("T" in text(raw.iat[header, col]) or "trimestre" in text(raw.iat[header, col]).lower() for col, _ in columns[:4]) else "M"
            name = sheet_name(source.prefix, source.code, tab)
            series = {}
            header_label = text(raw.iat[header, 0])
            group = header_label if source.code == "indec-ipc" and header_label.casefold() == "total nacional" else ""
            cin_context: dict[str, str] = {}
            context = ""
            section = ""
            category = ""
            sector_context = ""
            for row in range(header + 1, len(raw)):
                cells = [raw.iat[row, col] for col, _ in columns]
                values = pd.to_numeric(pd.Series([
                    None if isinstance(value, (datetime, date, pd.Timestamp)) else value
                    for value in cells
                ], dtype=object), errors="coerce")
                parts = [text(raw.iat[row, col]) for col in range(min(columns[0][0], 3))]
                # Algunos cuadros mezclan valores anuales antes de las columnas
                # trimestrales; esos importes no son nombres de serie.
                label = next((part for part in reversed(parts)
                              if part and not re.fullmatch(r"[-+]?\d+(?:[.,]\d+)?", part)), "")
                if not label:
                    label = next((part for part in reversed(parts) if part), "")
                if values.notna().sum() < 2:
                    if source.code == "indec-ipc" and (label.casefold().startswith("región ") or label.casefold() == "total nacional"):
                        group = label
                    if source.code == "indec-cin" and label:
                        _cin_contextual_label(tab, label, cin_context)
                    continue
                if not label:
                    continue
                if len(parts) > 2 and label == parts[2] and parts[1] and not re.fullmatch(r"[-+]?\d+(?:[.,]\d+)?", parts[1]):
                    label = f"{parts[1]} - {label}"
                label = f"{group} - {label}" if group else label
                native = parts[0] if len(parts) > 1 and parts[0] and parts[0] != label else None
                if source.code == "indec-cin":
                    label = _cin_contextual_label(tab, label, cin_context)
                if source.code == "indec-ipc" and label in series and pd.Series(series[label]).equals(values):
                    continue
                base_label = label
                coded_label = bool(re.match(r"^\d+(?:\.[A-Za-z0-9]+)*\.?\s", label))
                sector_label = bool(re.match(r"^S[A-Z0-9]+\.\s", label))
                if label in series:
                    prefix = [section]
                    if sector_label:
                        prefix.append(context)
                    elif not coded_label:
                        prefix.extend((context, category, sector_context))
                    prefix = [part for part in prefix if part and part != label]
                    if prefix:
                        label = " - ".join([*prefix, label])
                    if native and native != label:
                        if label in series:
                            label = f"{label} [{native}]"
                    if label in series:
                        digest = hashlib.sha1(f"{tab}|{row}|{base_label}".encode()).hexdigest()[:8]
                        label = f"{label} [{digest}]"
                    if label in series:
                        raise ValueError(f"Nombre de serie duplicado en {tab}: {label}")
                if coded_label:
                    context = base_label
                    sector_context = ""
                elif sector_label:
                    sector_context = base_label
                elif re.match(r"^(activos|pasivos|crédito|débito|adquisición neta de activos financieros|emisión neta de pasivos)$", base_label, re.I):
                    section = base_label
                    category = ""
                elif re.match(r"^(?:A\.\s*ACTIVOS|P\.\s*PASIVOS|B90\.)", base_label, re.I):
                    section = base_label
                    category = ""
                elif re.match(r"^(inversión directa|inversión de cartera|otra inversión)$", base_label, re.I):
                    category = base_label
                ident = f"{slug(tab)}|{native}" if native and native != label and source.code != "indec-ipc" else f"{slug(tab)}|{slug(label)}"
                if ident in {item["ID"] for item in inventory}:
                    continue
                series[label] = list(values)
                valid = [dt for (_, dt), val in zip(columns, values) if pd.notna(val)]
                title = text(raw.iat[0, 0]) or tab
                if re.fullmatch(r"cuadro\s+\d+", title, re.I) and len(raw) > 1:
                    title = text(raw.iat[1, 0]) or title
                unit_note = text(raw.iat[1, 0]) if len(raw) > 1 else ""
                description = f"{title}. {unit_note}" if any(word in unit_note.casefold() for word in ("dólar", "peso", "porcent", "índice", "indice")) else title
                inventory.append({
                    "ID": ident, "Código fuente": source.code, "Nombre serie": label,
                    "Variable": slug(label), "Unidad": "Ver descripción de la serie",
                    "Valoración": "No informado", "Descripción": description,
                    "Frecuencia": frequency, "Pestaña BD": name, "Columna BD": label,
                    "Archivo origen": path.name, "Hoja origen": tab, "Origen": source.origin,
                    "Fuente": url, "Catálogo ID": source.code, "Dataset ID": source.code,
                    "Título dataset": title, "Tema dataset": source.route,
                    "Responsable dataset": source.origin, "Fuente de valores": "Excel oficial",
                    "Fecha inicio": min(valid), "Fecha fin": max(valid), "Estado": "VIGENTE",
                })
            if series:
                frame = pd.DataFrame({"fecha": [dt for _, dt in columns], **series})
                outputs[name] = frame.sort_values("fecha").drop_duplicates("fecha", keep="last").reset_index(drop=True)
    return outputs, pd.DataFrame(inventory)


def process(source: Source, fetch: bool = True, write_data: bool = True) -> dict[str, int]:
    path, url = download(source) if fetch else (source.file, (source.directory / "source_url.txt").read_text(encoding="utf-8").strip())
    if not path.is_file():
        raise FileNotFoundError(path)
    sheets, index = extract_horizontal(path, source, url)
    vertical_sheets, vertical_index = extract_tables(path, source.route, source.code, source.prefix)
    # Cada hoja se interpreta una sola vez; la dirección horizontal tiene
    # prioridad si hay una fila de períodos claramente identificada.
    for name, frame in vertical_sheets.items():
        if name not in sheets:
            sheets[name] = frame
    if not vertical_index.empty:
        vertical_index = vertical_index[~vertical_index["Pestaña BD"].isin(index.get("Pestaña BD", pd.Series(dtype=str)))]
        vertical_index["Fuente"] = url
        vertical_index["Origen"] = source.origin
        vertical_index["Responsable dataset"] = source.origin
        index = pd.concat([index, vertical_index], ignore_index=True)
    if not sheets or index.empty:
        raise ValueError(f"No se extrajeron series de {source.code}")
    vertical_ids = index["ID"].astype(str).str.startswith("latest.")
    index.loc[vertical_ids, "ID"] = [
        f"{slug(sheet)}|{slug(label)}"
        for sheet, label in zip(index.loc[vertical_ids, "Hoja origen"], index.loc[vertical_ids, "Nombre serie"])
    ]
    route_parts = [part.strip() for part in source.route.split("/")]
    for column, part in zip(("Institución", "Área", "Subárea 1", "Subárea 2", "Subárea 3"), route_parts):
        index[column] = part
    index["Institución"] = source.institution if source.institution == "INDEC" else source.origin
    index["Tema"] = {
        "indec-ipc": "Precios", "indec-pib": "Actividad", "indec-cin": "Sector externo",
        "indec-salarios": "Salarios", "mch-sipa": "Trabajo",
    }[source.code]
    for row in index.index:
        description = str(index.at[row, "Descripción"]).casefold()
        label = str(index.at[row, "Nombre serie"]).casefold()
        tab = str(index.at[row, "Hoja origen"]).casefold()
        context = description + " " + label + " " + tab
        if source.code == "indec-ipc" and ("variación" in tab or "var." in tab):
            index.at[row, "Valoración"] = "No aplica"
            index.at[row, "Unidad"] = "%"
        elif "número índice" in label or "numero indice" in label:
            index.at[row, "Valoración"] = "No aplica"
            index.at[row, "Unidad"] = "índice"
        elif "variación" in context or "variaciones" in context or "porcentaje" in context:
            index.at[row, "Valoración"] = "No aplica"
            index.at[row, "Unidad"] = "%"
        elif "precios corrientes" in context or "precios de corrientes" in context:
            index.at[row, "Valoración"] = "Precios corrientes"
            index.at[row, "Unidad"] = "millones de pesos" if "millones de pesos" in context else "pesos"
        elif re.search(r"precios (?:de |del |constantes).*?(?:19|20)\d{2}", context) or "precios constantes" in context:
            index.at[row, "Valoración"] = "Precios constantes"
            index.at[row, "Unidad"] = "millones de pesos" if "millones de pesos" in context else "pesos"
        elif "millones de dólares" in context:
            index.at[row, "Valoración"] = "Precios corrientes"
            index.at[row, "Unidad"] = "millones de dólares"
        elif "índice" in context or "indice" in context:
            index.at[row, "Valoración"] = "No aplica"
            index.at[row, "Unidad"] = "índice"
        elif source.code == "mch-sipa" and "en miles" in context:
            index.at[row, "Valoración"] = "No aplica"
            index.at[row, "Unidad"] = "miles de personas"
        elif source.code == "mch-sipa" and "pesos" in context:
            index.at[row, "Valoración"] = "Precios corrientes"
            index.at[row, "Unidad"] = "pesos"
    if write_data:
        current = cargar_indice()
        obsolete = set(current.loc[current["Código fuente"].eq(source.code), "Pestaña BD"].dropna()) if not current.empty else set()
        guardar_datos_preservando_formato(ROOT / "BD.xlsx", sheets, {name: str(index.loc[index["Pestaña BD"].eq(name), "Frecuencia"].iloc[0]) for name in sheets}, obsolete)
    generar(index)
    return {"series": len(index), "hojas": len(sheets)}
