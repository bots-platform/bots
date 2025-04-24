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
    Extrae cada sección 'REPORTE TÉCNICO Nº X' en una fila.
    Saca CUISMP, Tipo Caso, Observación, Determinación de la causa, 
    Medidas tomadas, y además PARSEA la sección de MEDIDAS para 
    capturar Fecha y hora inicio / Fecha y hora fin que aparecen justo
    debajo de ese título.
    """
    doc = Document(path_docx)
    paragraphs = [p.text.strip() for p in doc.paragraphs]

    
    heading_pat = re.compile(r"REPORTE T[EÉ]CNICO Nº\s*(\d+)", re.IGNORECASE)
    headings: List[Dict] = []
    for idx, text in enumerate(paragraphs):
        m = heading_pat.search(text)
        if m:
            headings.append({"index": idx, "nro_incidencia": m.group(1)})
    if not headings:
        raise ValueError("No se encontró ‘REPORTE TÉCNICO Nº ...’ en el documento")


    headings.append({"index": len(paragraphs), "nro_incidencia": None})

    records: List[Dict] = []

    common_patterns = {
        "CUISMP":                   re.compile(r"CUISMP\s*(?:\:|\s)\s*(\d+)", re.IGNORECASE),
        "Tipo Caso":                re.compile(r"Tipo Caso\s*:\s*(.+)",    re.IGNORECASE),
        "Observación":              re.compile(r"Observaci.o.n\s*:\s*(.+)", re.IGNORECASE),
        "DETERMINACIÓN DE LA CAUSA":re.compile(r"Determinaci.o.n de la causa\s*:\s*(.+)", re.IGNORECASE),  
    }

    inicio_pat = re.compile(r"^Fecha y hora inicio\s*:\s*(.+)$", re.IGNORECASE)
    fin_pat    = re.compile(r"^Fecha y hora fin\s*:\s*(.+)$",    re.IGNORECASE)
    medidas_title_pat = re.compile(r"^MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", re.IGNORECASE)


    for head, nxt in zip(headings, headings[1:]):
        start, end = head["index"], nxt["index"]
        block_lines = paragraphs[start:end]
        block_text  = "\n".join(block_lines)

 
        row: Dict[str, str] = {
            "nro_incidencia": head["nro_incidencia"],
            "CUISMP":         "",
            "Tipo Caso":      "",
            "Observación":    "",
            "DETERMINACION DE LA CAUSA": "",
            "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS": "" ,  # texto completo si quieres
            "Fecha y hora inicio": "",
            "Fecha y hora fin":    "",
        }

        for key, pat in common_patterns.items():
            m = pat.search(block_text)
            if m:
                row[key] = m.group(1).strip()

        med_idx = None
        for i, line in enumerate(block_lines):
            if medidas_title_pat.match(line):
                med_idx = i
                break
        if med_idx is not None:

            meds = []
            for sub in block_lines[med_idx+1:]:
                if not sub:
                   
                    break
                meds.append(sub)
              
                mi = inicio_pat.match(sub)
                if mi:
                    row["Fecha y hora inicio"] = mi.group(1).strip()
                mf = fin_pat.match(sub)
                if mf:
                    row["Fecha y hora fin"] = mf.group(1).strip()
            row["MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS"] = " ".join(meds).strip()

        records.append(row)


    df = pd.DataFrame.from_records(records)

    return df

