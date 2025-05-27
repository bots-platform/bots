
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
    evidencia_pattern    = re.compile(r"EVIDENCIA RESPONSABILIDAD DE TERCEROS", re.IGNORECASE)

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

        if evidencia_pattern.search(text):
            break
        m_ticket = anexo_pattern_ticket.search(text)
        if not m_ticket:
            continue
        ticket = m_ticket.group(1)

        linea0  = ""
        periodos: List[str] = []
        footer   = ""
        total = ""
        skip_ticket = False


        for line in paragraphs_list[idx + 1:]:
            if anexo_pattern_ticket.search(line):
                break

            if evidencia_pattern.search(line):
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
            "indisponibilidad_total": pad_total(total)
        })
    
    if not records:
        return None

    return pd.DataFrame.from_records(records)

