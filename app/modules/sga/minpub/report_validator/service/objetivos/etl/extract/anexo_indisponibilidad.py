from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from docx import Document
import re

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

def create_schema() -> StructType:
    """
    Creates the schema for the indisponibilidad data.
    """
    return StructType([
        StructField("ticket", StringType(), True),
        StructField("indisponibilidad_header", StringType(), True),
        StructField("indisponibilidad_periodos", StringType(), True),
        StructField("indisponibilidad_footer", StringType(), True),
        StructField("indisponibilidad_total", StringType(), True)
    ])

def process_document_content(path_docx: str) -> List[str]:
    """
    Process the Word document and return a list of text lines.
    
    Args:
        path_docx (str): Path to the Word document
        
    Returns:
        List[str]: List of text lines from the document
    """
    doc = Document(path_docx)
    raw = [p.text for p in doc.paragraphs]
    paragraphs_list = []
    for para in raw:
        for line in para.splitlines():
            text = line.strip()
            if text:
                paragraphs_list.append(text)
    return paragraphs_list

@log_exceptions
def extract_indisponibilidad_anexos(path_docx: str) -> Optional[DataFrame]:
    """
    Extracts data from a .docx file containing multiple ANEXO X – TICKET Y:
      - Introduction line ("Se tuvo indisponibilidad...")
      - Period lines ("DD/MM/YYYY hh:mm hasta el día DD/MM/YYYY hh:mm")
      - Total hours line ("(Total de horas sin acceso… HH:MM horas)")
    
    Returns a PySpark DataFrame with columns:
      ['ticket', 'indisponibilidad_header', 'indisponibilidad_periodos', 'indisponibilidad_footer', 'indisponibilidad_total']
    
    Args:
        path_docx (str): Path to the Word document
        
    Returns:
        Optional[DataFrame]: PySpark DataFrame containing the processed data or None if no records found
    """
    # Compile regex patterns
    anexo_pattern_ticket = re.compile(r"ANEXO\s+\d+\s+–\s+TICKET\s+(\d+)", re.IGNORECASE)
    linea0_pat_pattern = re.compile(r"^Se tuvo indisponibilidad por parte del cliente.*", re.IGNORECASE)
    periodo_pattern = re.compile(
        r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*hasta el día\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*$",
        re.IGNORECASE
    )
    total_hour_pattern = re.compile(r"^\(Total de horas sin acceso a la sede:\s*(\d{1,3}:\d{2})\s*horas\)", re.IGNORECASE)
    evidencia_pattern = re.compile(r"EVIDENCIA RESPONSABILIDAD DE TERCEROS", re.IGNORECASE)

    # Padding patterns
    _day_pad_start = re.compile(r"^(\d)(?=/)")
    _day_pad_end = re.compile(r"(?<=hasta el día\s)(\d)(?=/)", re.IGNORECASE)
    _hour_pad = re.compile(r"\b(\d)(?=:\d{2}(?::\d{2})?)")

    def pad_periodo(line: str) -> str:
        line = _day_pad_start.sub(lambda m: m.group(1).zfill(2), line)
        line = _day_pad_end.sub(lambda m: m.group(1).zfill(2), line)
        return _hour_pad.sub(lambda m: m.group(1).zfill(2), line)
    
    def pad_total(tm: str) -> str:
        return _hour_pad.sub(lambda m: m.group(1).zfill(2), tm)

    # Process document content
    paragraphs_list = process_document_content(path_docx)
    records: List[Dict] = []

    # Process each paragraph
    for idx, text in enumerate(paragraphs_list):
        if evidencia_pattern.search(text):
            break
            
        m_ticket = anexo_pattern_ticket.search(text)
        if not m_ticket:
            continue
            
        ticket = m_ticket.group(1)
        linea0 = ""
        periodos: List[str] = []
        footer = ""
        total = ""

        # Process subsequent lines until next ticket or evidence
        for line in paragraphs_list[idx + 1:]:
            if anexo_pattern_ticket.search(line) or evidencia_pattern.search(line):
                break

            if linea0_pat_pattern.match(line):
                linea0 = line

            if periodo_pattern.match(line):
                periodos.append(pad_periodo(line))

            if total_hour_pattern.search(line):
                footer = line
                m_total = total_hour_pattern.search(line)
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

    # Create SparkSession and convert records to DataFrame
    with spark_manager.get_session():
        spark = spark_manager.get_spark()
        try:
            # Convert records to DataFrame using the defined schema
            df = spark.createDataFrame(records, schema=create_schema())
            # Cache the DataFrame for better performance if it will be used multiple times
            df.cache()
            return df
        except Exception as e:
            raise Exception(f"Error processing Word document: {str(e)}") 