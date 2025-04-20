import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
import language_tool_python
import re

_tool_es = language_tool_python

from utils.logger_config import get_sga_logger
 
logger = get_sga_logger()
                                                            
def log_exceptions(func):
    """
    Decorator to log exceptions in a function using the shared 'logger'.
    It will also re-raise the exception so that the caller can handle it
    appropriately (e.g., fail fast or continue).
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            logger.error(
                f"Error in function '{func.__name__}': {exc}",
                exc_info=True
            )
            # Optionally, decide whether to re-raise or swallow the exception.
            # Usually best practice is to re-raise so the pipeline can decide what to do:
            raise
    return wrapper


@log_exceptions
def validation_medidas_correctivas(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validation the column medidas correctivas y o medidas tomadas, se debe obtener
    the first and the last date from paragraph, excluding the two last lines if exists 
    dates called fecha hora inicio and fecha hora fin
    """

    df = merged_df.copy()

    df['mc_first_ok'] = True
    df['mc_last_ok'] = True
    df['it_first_ok'] = True
    df['it_last_ok'] = True
    df['ortografia_ok'] = True

    @log_exceptions
    def extract_date_range_last(text: str):
        """
        Returns (fecha_hora_inicio, fecha hora fin) strings
        extracted from the final 2 lines that begin with:
        'Fecha y hora inicio:' and 'Fecha y hora fin:'

        If parsing fails or lines are missing, returns (None, None)
        """
        if not isinstance(text, str):
            return (None, None)
        
        lines = [line.strip() for line in text.splitlines if line.strip()]

        if len(lines) < 2:
            return (None, None)
        
        last_two = lines[-2:]
        inicio = None
        fin = None

        start_pattern = "^Fecha y hora inicio:\s*(.*)$"
        end_pattern = "^Fecha y hora fin:\s*(.*)$"

        start_match = re.match(start_pattern, last_two[0], flags=re.IGNORECASE)
        if start_pattern:
            start = start_match.group(1).strip()
        
        end_match = re.match(end_pattern, last_two[1], flags=re.IGNORECASE)
        if end_match:
            end = end_match.group(1).strip()

        return  (start, end)
    
    @log_exceptions
    def extract_date_range_body(text: str):
        if not isinstance(text, str):
            return (None, None)
        
        lines = [ln.strip()  for ln in text.splitlines() if ln.strip()]

        def is_meta_line(ln):
            low = ln.lowe()
            return low.startswith("fecha y hora inicio") or low.startswith("fecha y hora fin")
        
        while len(lines) and is_meta_line(lines[-1]):
            lines.pop()

        body = "\n".join(lines)

        pattern = r"(\d{2}/\d{2}/d{4})\s*(?:a las\s*)?(\d{2}:\d{2})"
        matches = re.findall(pattern, body, flags=re.IGNORECASE)

        if not matches:
            return (None, None)
        
        first_date, first_time = matches[0]
        last_date, last_time = matches[-1]

        return (f"{first_date} {first_time}", f"{last_date} {last_time}")
    
    @log_exceptions
    def has_repetition(text: str) -> bool:
        """
        Returns True if text contains o forbiden repetition of:
        - "Inmediatamente" twice, or
        - "A través" twice.
        """
        if not isinstance(text, str):
            return False
        
        patterns = [
            r'(?i)\b(inmediatamente)\b.*\b\1\b',
            r'(?i)\b(a través)\b.*\b\1\b',
        ]
        return any(re.search(p, text) for p in patterns )
   
    @log_exceptions
    def is_langtool_clean(text:str) -> bool:
        """
        Return True if LanguageTool reports zero issues in the text.
        """
        if not isinstance(text, str) or not text.strip():
            return True
        
        matches = _tool_es.check(text)
        return len(matches) == 0
    
    @log_exceptions
    def check_orthografia(text: str) -> bool:
        """"
        Master orthography check: no forbidden repetition AND clean per LanguageTool.
        """
        if has_repetition(text):
            return False
        return is_langtool_clean(text)

    date_range_mc_body = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].apply(extract_date_range_body)
    df[['first_dt_mc', 'last_dt_mc']] = pd.DataFrame(date_range_mc_body.tolist(), index=df.index)

    date_range_it_body = df['it_medidas_tomadas'].apply(extract_date_range_body)
    df[['first_dt_it', 'last_dt_it']] = pd.DataFrame(date_range_it_body.tolist(), index=df.index)

    date_range_it_last = df['it_medidas_tomadas'].apply(extract_date_range_last)
    df[['start_dt_last_it', 'end_dt_last_it']] = pd.DataFrame(date_range_it_last.tolist(), index=df.index)

    
    df['FECHA_Y_HORA_INICIO_fmt'] = (
        df['FECHA Y HORA INICIO']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

    df['FECHA_Y_HORA_FIN_fmt'] = (
        df['FECHA Y HORA FIN']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )
    
    df['mc_first_ok'] = (
        df['first_dt_mc'] == df['FECHA_Y_HORA_FIN_fmt']
    )

    df['mc_last_ok'] = (
        df['last_dt_mc'] == df['FECHA_Y_HORA_FIN_fmt']
    )

    df['mc_last_ok'] = (
        df['first_dt_it'] == df['start_dt_last_it']
    )

    df['it_last_ok'] = (
        df['last_dt_it'] == df['end_dt_last_it']
    )

    df['ortografia_ok'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].apply(check_orthografia)

 
    df['Validation_OK'] = (
        df['mc_first_ok'] &
        df['mc_last_ok'] &
        df['mc_last_ok'] &
        df['it_last_ok'] &
        df['ortografia_ok']
    )

    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df

@log_exceptions
def build_failure_messages_medidas_correctivas(df:pd.DataFrame) -> pd.DataFrame:
    """
    Build detailed error messages for medidas correctivas validation failures.

    """

    if not isinstance(df, pd.DataFrame) or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])
    
    mensajes = np.where(
       df['Validation_OK'],
       "Validación exitosa: MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS",
       (
           np.where(~df['fechas_parrafos_match_columns_first_date'],
                    " La fecha/hora de inicio del parrafo en MEDIDAS CORRECTIVAS:  ( " + df['first_date'] +
                      " ) no coincide con la columna FECHA Y HORA INICIO DE EXCEL: " + df['FECHA Y HORA INICIO'] + ". ",
                    "") +

                     np.where(~df['fechas_parrafos_match_columns_last_date'],
                    " La fecha/hora de fin del parrafo en MEDIDAS CORRECTIVAS:( " + df['last_date'] +
                      " ) no coincide con la columna FECHA Y HORA FIN DE EXCEL: " +
                      df['FECHA Y HORA FIN']+ ". ", 
                    "") + 

                    np.where(~df['no_errores_ortograficos'],
                    " Habian errores ortográficos/gramaticales en el parrafo en MEDIDAS CORRECTIVAS:  ( " +
                      df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'] +
                      " ) ylos errores encontrados son : ( " +
                      df['spelling_errors']+ " ) .",
                    "") 
       )
    )
    
    df['mensaje'] = mensajes
    df['objetivo'] = 1.8
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]
    


