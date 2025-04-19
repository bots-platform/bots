
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



import re
pattern = re.compile(r'(?:COMPONENTE|Componente|COMONENTE)\s*(?:IV|II|III|I|V)(?:\s*-|\s*)\s*', re.IGNORECASE)

@log_exceptions
def remove_componente_prefix(text: str) -> str:
    """
    Remove a prefix like  'COMPONENETE - II' (or variations) from the start of the string
    Also removes extra spaces o dashes.
    """ 
    if not isinstance(text, str):
        return ""
    cleaned = pattern.sub("", text).strip()
    return cleaned

@log_exceptions
def validation_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa(merged_df: pd.DataFrame) -> pd.DataFrame:
    df = merged_df.copy()

    df['causa_excel_clean'] = df['DETERMINACIÓN DE LA CAUSA'].apply(remove_componente_prefix) 
    df['causa_sga335_clean'] = df['it_determinacion_de_la_causa'].apply(remove_componente_prefix)

    df['causa_match'] =  df['causa_excel_clean'] == df['causa_sga335_clean']
    df['tipo_caso_match'] = df['tipo_caso'] == df['TIPO CASO']
    df['cid_match'] = df['cid'] == df['CID']
    df['cod_incidencia_padre_match'] = df['codincidencepadre'] == df['CODINCIDENCEPADRE'] 


    df['Validation_OK'] = (
        df['causa_match'] &
        df['tipo_caso_match'] &
        df['cid_match'] &
        df['cod_incidencia_padre_match']
    )
    
    df['fail_count'] =(
       (~df['causa_match']).astype(int) + 
       (~df['tipo_caso_match']).astype(int) + 
       (~df['cid_match']).astype(int) + 
       (~df['cod_incidencia_padre_match']).astype(int) 
    ) 

    return df

@log_exceptions
def build_failure_messages_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa(df:pd.DataFrame) -> pd.DataFrame:
    mensaje = np.where(
        df['Validation_OK'],
        "Determinacion de la causa coincide  despues de remover 'COMPONENTE ' prefix",
         (
            np.where(~df['causa_match'],
                     " No coincide Determinacion de la causa Excel-Corte: (" +
                       df['causa_excel_clean'].astype(str)+") con SGA: ("+ df['causa_sga335_clean']+ ")."  ,  "")+
            np.where(~df['tipo_caso_match'],
                     "No coincide Tipo de Caso de CORTE-EXCEL: (" + df['TIPO CASO'].astype(str) +") con SGA: (" + 
                     df['tipo_caso'].astype(str)+ "). ", "")+
            np.where(~df['cid_match'],
                     "No coincide cid  de CORTE-EXCEL : ("+ df['CID'].astype(str) +") con SGA:(" + df['cid'].astype(str) +
                      "). ", "")+
           np.where(~df['cod_incidencia_padre_match'],
                     "No coincide nro incidencia padre EXCEL-CORTE: (" + df['CODINCIDENCEPADRE'].astype(str) +
                       ") con SGA: (" +df['codincidencepadre'].astype(str)+ "). ", "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = 1.5
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]

