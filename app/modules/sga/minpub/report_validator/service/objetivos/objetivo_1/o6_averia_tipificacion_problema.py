
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
def validation_averia_tipificacion_problema(merged_df:pd.DataFrame) -> pd.DataFrame:
    """
    Valdate that:
    - Averia (Excel -CORTE) either match tipificacion del problema directly
    - OR The first 37 characteres  match, if direct comparasion fails.

    Returns a dataframe with boolean flag and a 'fail_count' colum.
    """
    df = merged_df.copy()

    df['averia_direct_match'] = (df['AVERÍA'] == df['tipificacion_problema']) 
    df['averia_partial_37'] = (df['AVERÍA'].str[:37] == df['tipificacion_problema'].str[:37])

    df['Validation_OK'] = df['averia_direct_match'] | df['averia_partial_37']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df

@log_exceptions
def build_failure_messages_averia(df:pd.DataFrame) -> pd.DataFrame:
    """"
    Build descriptive failure menssages for rows that don't pass validation. 
    Returns Dataframe with only failing rows.
    """

    failed_rows = df[~df['Validation_OK']].copy()

    if not failed_rows.empty:
        failed_rows['mensaje'] = ("No coincide AVERÍA en EXCEL-CORTE:  ("+df['AVERÍA'].astype(str)+ 
        ") con tipificacion problema en SGA 335: (" + df["tipificacion_problema"].astype(str) +
        "). ") 
        failed_rows['objetivo'] = 1.6

    return failed_rows[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


