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
def validation_cuismp_distrito_fiscal_medidas(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs the CUISMP and DF validations on the merged DataFrame.
    
    Returns a DataFrame with the new boolean flags and a failure count.
    """

    df = merged_df.copy()

    df['CUISMP_match'] = df['CUISMP_sga_dinamico_335_excel_matched'] == df['CUISMP_sharepoint_cid_cuismp']

    df['DF_match'] = df['DF'] == df['Distrito Fiscal']

    medidas_col = 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'

    df['CUISMP_in_medias_tomadas'] = df.apply(
        lambda row: (
            pd.notnull(row[medidas_col]) 
            and pd.notnull(row['CUISMP_sga_dinamico_335_excel_matched']) 
            and row['CUISMP_sga_dinamico_335_excel_matched'] in row[medidas_col]
        ), 
        axis=1
    )

    df['Validation_OK'] = df['CUISMP_match'] & df['DF_match'] & df['CUISMP_in_medias_tomadas']

    df['fail_count'] = (~df['CUISMP_match']).astype(int) + \
                                (~df['DF_match']).astype(int) + \
                                (~df['CUISMP_in_medias_tomadas']).astype(int)

    return df  




@log_exceptions
def build_failure_messages_cuismp_distrito_fiscal_medidas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the 'mensaje' column using vectorized operations.
    Adds the 'objetivo' column (constant value of 1) and filters
    rows that fail at least one validation.
    
    Returns a DataFrame with:
      ['nro_incidencia', 'mensaje', 'objetivo']

    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation successful",
        (
            np.where(~df['CUISMP_match'], 
                     "CUISMP en Sharepoint CID-CUISMP: " + df['CUISMP_sharepoint_cid_cuismp'].astype(str) +
                     " es diferente a EXCEL -CORTE (CUISMP) " + df['CUISMP_sga_dinamico_335_excel_matched'].astype(str) + ". ", "") +
            np.where(~df['DF_match'], 
                     " Distrito Fiscal en Sharepoint CID-CUISMP : " + df['Distrito Fiscal'].astype(str) +
                     " es diferente a EXCEL -CORTE (DF) " + df['DF'].astype(str) + ". ", "") +
            np.where(~df['CUISMP_in_medias_tomadas'], 
                     " CUISMP "+ df['CUISMP_sga_dinamico_335_excel_matched'].astype(str) + " no encontrado in MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS.", "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = 1.1
    
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


