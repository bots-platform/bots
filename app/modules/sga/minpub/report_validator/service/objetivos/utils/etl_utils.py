import re
import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when, regexp_extract, length, split
from app.core.spark_manager import spark_manager
import pandas as pd

_FIN_DAYS_HOUR_PAT = re.compile(
    r'^(?:(?P<days>\d+)\s+day[s]?,\s*)?'  
    r'(?P<hours>\d{1,2}):' 
)

_FIN_INICIO_PAT = re.compile(
    r'^(?:(?P<days>\d+)\s+days?,\s*)?'   
    r'(?P<hours>\d+):(?P<minutes>\d{2})'  
    r'(?:[:]\d{2})?'                    
    r'\.?$' 
)

def to_hhmm(s: any) -> str:
    """
    Parse strings like "1 day, 03:45:23" or "03:12" into "HH:MM".
    Returns None on no‐match.
    """
    text = str(s).strip()
    m = _FIN_INICIO_PAT.match(text)
    if not m:
        return None
    days = int(m.group('days')) if m.group('days') else 0
    hrs  = int(m.group('hours'))
    mins = int(m.group('minutes'))
    total_h = days * 24 + hrs
    return f"{total_h:02d}:{mins:02d}"

def parse_hhmm_to_minutes(hhmm: str) -> float:
    """
    Given "HH:MM", returns total minutes as float. None or unparsable → None.
    """
    if hhmm is None:
        return None
    try:
        h, m = str(hhmm).split(':')
        return float(h) * 60 + float(m)
    except Exception:
        return None

def hhmm_to_minutes(hhmm: str) -> int:
    """
    Given "HH:MM", returns total minutes as int (assumes valid input).
    """
    h, m = hhmm.split(':')
    return int(h) * 60 + int(m)

def extract_total_hours(s: any) -> int:
    """
    From strings like "2 days, 05:23:12" or "07:45", returns total hours as int.
    None or unparsable → None.
    """
    text = str(s).strip()
    m = _FIN_DAYS_HOUR_PAT.match(text)
    if not m:
        return None
    days = int(m.group('days')) if m.group('days') else 0
    hrs  = int(m.group('hours'))
    return days * 24 + hrs

def normalize_text(text):
    """
    Normalize text by removing special characters, extra spaces, etc.
    """
    if text is None:
        return ''
    text = unicodedata.normalize("NFKD", str(text))   # Normaliza tildes y caracteres raros
    text = text.replace('\r', ' ').replace('\n', ' ').replace('\xa0', ' ')
    text = ' '.join(text.split())  # Elimina espacios dobles y deja uno solo
    return text.strip()

def get_dataframe_summary(df):
    """
    Returns a summary DataFrame for the given PySpark DataFrame.
    
    The summary includes:
      - Data Type
      - Non Null Count
      - Null Count
      - Null Percentage
      - Unique Values count

    Returns:
        pd.DataFrame: The summary as a Pandas DataFrame.
    Notes:
        All Spark operations are performed inside the context manager.
    """
    with spark_manager.get_session_context():
        schema = df.schema
        total_rows = df.count()
        null_counts = {field.name: df.filter(F.col(field.name).isNull()).count() 
                      for field in schema}
        unique_counts = {field.name: df.select(field.name).distinct().count() 
                        for field in schema}
        summary_data = []
        for field in schema:
            null_count = null_counts[field.name]
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            summary_data.append({
                'Column': field.name,
                'Data Type': str(field.dataType),
                'Non Null Count': total_rows - null_count,
                'Null Count': null_count,
                'Null Percentage': round(null_percentage, 2),
                'Unique Values': unique_counts[field.name]
            })
        # Convert to Pandas DataFrame
        return pd.DataFrame(summary_data)