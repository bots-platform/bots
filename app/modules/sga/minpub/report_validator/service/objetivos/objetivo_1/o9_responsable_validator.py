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


def validate_responsable(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates that RESPONSABLE (EXCEL-CORTE) equals the first word of tipificacion_tipo (SGA 335)
    
    Adds:
        - responsable_expected: first word of tipificacion_tipo
        - responsable_ok: True/False comparison
        - Validation_OK: AND-combined with any existing flag
        - fail_count: 0/1
    """
    df = df_merged.copy()

    df['responsable_expected'] = (
        df['tipificacion_tipo']
        .astype()
        .str.split('-', n=1)
        .str[0]
        .str.strip()
    )
    df['responsable_OK'] = (
        df['RESPONSABILIDAD'].astype(str).str.strip()
        == df['responsable_expected']
    )

    df['Validation_OK'] = df['responsable_OK']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df

def build_failure_messages_responsable(df:pd.DataFrame) -> pd.DataFrame:
    """
    Builds specific failure messages for the RESPONSABILIDAD vs tipificacion_tipo check.
    Returns a Dataframe with columns ['ID', 'mensaje', 'objetivo'] for failing records only.
    """

    if df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])
    
    messages = np.where(
        df['Validation_OK'],
        "Validation exitosa : RESPONSABLE coincide  con la primera palabra de tipificacion_tipo",
        (
            "RESPONSABILIDAD: " 
            + df["RESPONSABILIDAD"].astype(str)
            + " ' no coincide con la primera palabra' "
            + df['responsable_expected'].astype(str)
            +"' de tipificacion_tipo'."
        )
    )

    df['mensaje'] = messages
    df['objetivo'] = 1.9
    df_failures = df[df['fail_count'] > 0]

    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]

