import re
from typing import List, Dict
import pandas as pd
from docx import Document

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

@log_exceptions
def extract_indisponibilidad_anexos(path_docx: str) -> pd.DataFrame:
    """
    Extrae de un .docx que contiene varios ANEXO X – TICKET Y:
      - la línea de introducción ("Se tuvo indisponibilidad...")
      - la(s) línea(s) que contienen fechas y horas ("DD/MM/YYYY hh:mm hasta el día DD/MM/YYYY hh:mm")
      - la línea del total de horas (“(Total de horas sin acceso… HH:MM horas)”)
    Retorna un DataFrame con columnas:
      ['ticket', 'indisponibilidad_text', 'periodo', 'total']
    """

    doc = Document(path_docx)
    paragraphs_list = [p.text.strip() for p in doc.paragraphs if p.text.strip()]


    anexo_pattern     = re.compile(r"ANEXO\s+\d+\s+–\s+TICKET\s+(\d+)", re.IGNORECASE)
    linea0_pat_pattern    = re.compile(r"^Se tuvo indisponibilidad por parte del cliente.*", re.IGNORECASE)
    periodo_pattern   = re.compile(
        r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*hasta el día\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*$",
        re.IGNORECASE
    )
    total_hour_pattern     = re.compile(r"^\(Total de horas sin acceso a la sede:\s*\d{1,3}:\d{2}\s*horas\)", re.IGNORECASE)

    records: List[Dict] = []

    for idx, text in enumerate(paragraphs_list):
        match = anexo_pattern.search(text)
        if not match:
            continue

        ticket = match.group(1)
        linea0 = ""
        periodo = ""
        total = ""

        for line in paragraphs_list[idx+1:]:
            if linea0_pat_pattern.match(line):
                linea0 = line
            elif periodo_pattern.match(line):
                periodo = line
            elif total_hour_pattern.match(line):
                total = line
                break

        records.append({
            "ticket": ticket,
            "indisponibilidad_extract": linea0,
            "periodo": periodo,
            "total": total
        })

    return pd.DataFrame.from_records(records)


