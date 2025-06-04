from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from docx import Document
import re

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("InformeTecnicoProcessor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_schema() -> StructType:
    """
    Creates the schema for the informe técnico data.
    """
    return StructType([
        StructField("nro_incidencia", StringType(), True),
        StructField("CUISMP", StringType(), True),
        StructField("Tipo Caso", StringType(), True),
        StructField("Observación", StringType(), True),
        StructField("DETERMINACIÓN DE LA CAUSA", StringType(), True),
        StructField("MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", StringType(), True),
        StructField("Fecha y hora inicio", StringType(), True),
        StructField("Fecha y hora fin", StringType(), True)
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
    return [p.text.strip() for p in doc.paragraphs]

def extract_report_blocks(paragraphs: List[str]) -> List[Dict]:
    """
    Extract report blocks from the document paragraphs.
    
    Args:
        paragraphs (List[str]): List of document paragraphs
        
    Returns:
        List[Dict]: List of report blocks with their indices
    """
    heading_pat = re.compile(r"REPORTE T[EÉ]CNICO Nº\s*(\d+)", re.IGNORECASE)
    headings: List[Dict] = []
    
    for idx, text in enumerate(paragraphs):
        m = heading_pat.search(text)
        if m:
            headings.append({"index": idx, "nro_incidencia": m.group(1)})
            
    if not headings:
        raise ValueError("No se encontró 'REPORTE TÉCNICO Nº ...' en el documento")
        
    headings.append({"index": len(paragraphs), "nro_incidencia": None})
    return headings

@log_exceptions
def extract_tecnico_reports_without_hours_last_dates(path_docx: str) -> DataFrame:
    """
    Extrae cada sección 'REPORTE TÉCNICO Nº X' en una fila.
    Saca CUISMP, Tipo Caso, Observación, Determinación de la causa, 
    Medidas tomadas, y además PARSEA la sección de MEDIDAS para 
    capturar Fecha y hora inicio / Fecha y hora fin que aparecen justo
    debajo de ese título.
    
    Args:
        path_docx (str): Path to the Word document
        
    Returns:
        DataFrame: PySpark DataFrame containing the processed data
    """
    # Compile regex patterns
    common_patterns = {
        "CUISMP": re.compile(r"CUISMP\s*(?:\:|\s)\s*(\d+)", re.IGNORECASE),
        "Tipo Caso": re.compile(r"Tipo Caso\s*:\s*(.+)", re.IGNORECASE),
        "Observación": re.compile(r"Observaci[oó]n\s*:\s*(.+)", re.IGNORECASE),
    }
    
    medidas_title_pat = re.compile(r"^MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", re.IGNORECASE)
    
    pattern = (
        r'^(?:Fecha y Hora|Hora)(?:\s+de)?\s+'    # "Fecha y Hora" o "Hora", con " de" opcional
        r'(Inicio|Fin)(?:\s*:)?\s*' 
        r'(\d{1,2}/\d{1,2}/\d{4})'                # grupo(2) = fecha
        r'\s*(?:a las\s*)?'                      # "a las" opcional
        r'(\d{1,2}:\d{2})'                       # grupo(3) = hora
        r'(?:\s+horas\.?)?'                      # "horas" opcional
    )
    rx = re.compile(pattern, re.IGNORECASE)
    
    # Process document
    paragraphs = process_document_content(path_docx)
    headings = extract_report_blocks(paragraphs)
    records: List[Dict] = []
    
    # Create SparkSession
    spark = get_spark_session()
    
    try:
        for head, nxt in zip(headings, headings[1:]):
            start, end = head["index"], nxt["index"]
            block_lines = paragraphs[start:end]
            block_text = "\n".join(block_lines)
            
            row: Dict[str, str] = {
                "nro_incidencia": head["nro_incidencia"],
                "CUISMP": "",
                "Tipo Caso": "",
                "Observación": "",
                "DETERMINACIÓN DE LA CAUSA": "",
                "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS": "",
                "Fecha y hora inicio": "",
                "Fecha y hora fin": "",
            }
            
            # Process determination of cause
            cause_idx = next(
                (i for i, line in enumerate(block_lines)
                 if re.match(r"Determinaci[oó]n\s+de\s+la\s+causa\s*:?\s*$",
                            line, re.IGNORECASE)),
                None
            )
            if cause_idx is not None:
                for j in range(cause_idx + 1, len(block_lines)):
                    text = block_lines[j].strip()
                    if text:
                        row["DETERMINACIÓN DE LA CAUSA"] = text
                        break
            
            # Process common patterns
            for key, pat in common_patterns.items():
                m = pat.search(block_text)
                if m:
                    row[key] = m.group(1).strip()
            
            # Process measures section
            med_idx = None
            for i, line in enumerate(block_lines):
                if medidas_title_pat.match(line):
                    med_idx = i
                    break
                    
            if med_idx is not None:
                meds = []
                for sub in block_lines[med_idx+1:]:
                    if not sub:
                        continue
                    if sub.lower().startswith("detalle de solicitudes") or sub.lower().startswith("solicitudes"):
                        break
                    
                    mi = rx.match(sub)
                    if mi:
                        if mi.group(1).lower() == "inicio":
                            row["Fecha y hora inicio"] = f"{mi.group(2)} {mi.group(3)}"
                        elif mi.group(1).lower() == "fin":
                            row["Fecha y hora fin"] = f"{mi.group(2)} {mi.group(3)}"
                        continue
                    
                    meds.append(sub)
                
                row["MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS"] = " ".join(meds).strip()
            
            records.append(row)
        
        # Create DataFrame with the defined schema
        df = spark.createDataFrame(records, schema=create_schema())
        
        # Cache the DataFrame for better performance if it will be used multiple times
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error processing Word document: {str(e)}")
    finally:
        spark.stop()

