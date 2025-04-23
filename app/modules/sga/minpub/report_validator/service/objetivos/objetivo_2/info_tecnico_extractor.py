import re
from pathlib import Path
from typing import List, Dict

import pandas as pd
from docx import Document
from utils.logger_config import get_sga_logger

logger = get_sga_logger()

def log_exceptions(func):
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper


@log_exceptions
def extract_tecnico_reports(path_docx: str) -> pd.DataFrame:
    """
    From a single .docx that contains multiple 'REPORTE TÉCNICO Nº X' sections,
    extract each section into one row of a DataFrame.
    """
    
    doc = Document(path_docx)
    paragraphs_list = [p.text.strip() for p in doc.paragraphs]

    heading_pattern = re.compile(r"REPORTE T[EÉ]CNICO Nº\s*(\d+)", re.IGNORECASE)
    headings: List[Dict] = []
    for indice_paragraph, text in enumerate(paragraphs_list):
        match = heading_pattern.search(text)
        if match:
            headings.append({"index": indice_paragraph, "nro_incidencia": match.group(1)})

    if not headings:
        raise ValueError("No se encontró ningún 'REPORTE TÉCNICO Nº ...' en el documento.")

    headings.append({"index": len(paragraphs_list), "nro_incidencia": None})
    records: List[Dict] = []

    patterns = {
        "CUISMP":                   re.compile(r"CUISMP\s*:\s*(\d+)", re.IGNORECASE),
        "Fecha y hora inicio":      re.compile(r"Fecha y hora inicio\s*:\s*(.+)", re.IGNORECASE),
        "Fecha y hora fin":         re.compile(r"Fecha y hora fin\s*:\s*(.+)", re.IGNORECASE),
        "Tipo Caso":                re.compile(r"Tipo Caso\s*:\s*(.+)", re.IGNORECASE),
        "Observación":              re.compile(r"Observaci.o.n\s*:\s*(.+)", re.IGNORECASE),
        "Determinación de la causa":re.compile(r"Determinaci.o.n de la causa\s*:\s*(.+)", re.IGNORECASE),
        "Medidas tomadas":          re.compile(r"Medidas correctivas y/o preventivas tomadas\s*:\s*(.+)", re.IGNORECASE),
    }

    for head, next in zip(headings, headings[1:]):
        start, end = head["index"], next["index"]
        block = "\n".join(paragraphs_list[start:end])

        row = {"nro_incidencia": head["nro_incidencia"]}
        for pattern_key, pattern_value in patterns.items():
            match = pattern_value.search(block)
            row[pattern_key] = match.group(1).strip() if match else ""

        records.append(row)
    
    df = pd.DataFrame.from_records(records)

    return df



