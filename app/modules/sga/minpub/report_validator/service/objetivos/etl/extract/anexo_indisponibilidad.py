
import pandas as pd
from docx import Document
from typing import List, Dict
from typing import Tuple, Optional
import re

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def extract_indisponibilidad_anexos(path_docx: str) ->  Optional[pd.DataFrame]:
    """
    Extrae de un .docx que contiene varios ANEXO X – TICKET Y:
      - la línea de introducción ("Se tuvo indisponibilidad...")
      - las líneas de periodos ("DD/MM/YYYY hh:mm hasta el día DD/MM/YYYY hh:mm")
      - la línea del total de horas (“(Total de horas sin acceso… HH:MM horas)”)
    Retorna un DataFrame con columnas:
      ['ticket', 'indisponibilidad_header', 'indisponibilidad_periodos', 'indisponibilidad_total']
    """

    doc = Document(path_docx)


    raw = [p.text for p in doc.paragraphs]
    paragraphs_list = []
    for para in raw:
        for line in para.splitlines():
            text = line.strip()
            if text:
                paragraphs_list.append(text)

    anexo_pattern_ticket = re.compile(r"ANEXO\s+\d+\s+–\s+TICKET\s+(\d+)", re.IGNORECASE)
    linea0_pat_pattern   = re.compile(r"^Se tuvo indisponibilidad por parte del cliente.*", re.IGNORECASE)
    periodo_pattern      = re.compile(
        r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*hasta el día\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*$",
        re.IGNORECASE
    )
    total_hour_pattern   = re.compile(r"^\(Total de horas sin acceso a la sede:\s*(\d{1,3}:\d{2})\s*horas\)", re.IGNORECASE)

    
    _day_pad_start = re.compile(r"^(\d)(?=/)")
    _day_pad_end   = re.compile(r"(?<=hasta el día\s)(\d)(?=/)", re.IGNORECASE)
    _hour_pad      = re.compile(r"\b(\d)(?=:\d{2}(?::\d{2})?)")

    def pad_periodo(line: str) -> str:
        
        line = _day_pad_start.sub(lambda m: m.group(1).zfill(2), line)
        line = _day_pad_end.sub(lambda m: m.group(1).zfill(2), line)
        return _hour_pad.sub(lambda m: m.group(1).zfill(2), line)
    
    def pad_total(tm: str) -> str:
        return _hour_pad.sub(lambda m: m.group(1).zfill(2), tm)

    records: List[Dict] = []

    for idx, text in enumerate(paragraphs_list):
        m_ticket = anexo_pattern_ticket.search(text)
        if not m_ticket:
            continue
        ticket = m_ticket.group(1)

        linea0  = ""
        periodos: List[str] = []
        footer   = ""
        total = ""


        for line in paragraphs_list[idx + 1:]:
            if anexo_pattern_ticket.search(line):
                break

            if linea0_pat_pattern.match(line):
                linea0 = line

            if periodo_pattern.match(line):
                periodos.append(pad_periodo(line))

            if total_hour_pattern.search(line):
                footer = line
                
            m_total =  total_hour_pattern.search(line)
            if m_total:
                total = m_total.group(1)
                break
                

        records.append({
            "ticket": ticket,
            "indisponibilidad_header": linea0,
            "indisponibilidad_periodos": "\n".join(periodos),
            "indisponibilidad_footer": footer,
            "indisponibilidad_total": total
        })
    
    if not records:
        return None

    return pd.DataFrame.from_records(records)


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

    medidas_title_pat = re.compile(r"^MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", re.IGNORECASE)

    inicio_pat = re.compile(r"^Fecha y hora inicio\s*:\s*(.+)$", re.IGNORECASE)
    fin_pat    = re.compile(r"^Fecha y hora fin\s*:\s*(.+)$",    re.IGNORECASE)


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


