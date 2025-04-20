import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime



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
def build_message_merge_sga_335_corte_excel_unmatch( df: pd.DataFrame) -> pd.DataFrame:

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


    mensaje = ( "Nro Incidencia " + df['nro_incidencia'] + " con FECHA Y HORA INICIO DE CORTE EXCEL " +
    df['FECHA_Y_HORA_INICIO_fmt'] + " y FECHA HORA FIN DE CORTE EXCEL  " + 
    df['FECHA_Y_HORA_FIN_fmt']+  " no se encuentra en el reporte dinamico SGA - 335 "
    )

    df['mensaje'] = mensaje
    df['objetivo'] = 1.1

    return df[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]

