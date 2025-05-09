import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
from utils.logger_config import get_sga_logger
 
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_cuismp_distrito_fiscal_medidas(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
     Valida coincidencias de CUISMP y Distrito Fiscal, y presencia de CUISMP en medidas.

    Parámetros
    ----------
    merged_df : pandas.DataFrame
        Debe contener las columnas:
        - CUISMP_sga_dinamico_335_excel_matched
        - CUISMP_sharepoint_cid_cuismp
        - DF
        - Distrito Fiscal
        - MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS

    Devuelve
    -------
    pandas.DataFrame
        Copia del DataFrame con estas columnas añadidas:
        - CUISMP_match
        - DF_match
        - CUISMP_in_medias_tomadas
        - Validation_OK
        - fail_count
    """

    df = merged_df.copy()

    df['CUISMP_match'] = df['CUISMP_sga_dinamico_335_excel_matched'] == df['CUISMP_sharepoint_cid_cuismp']

    df['DF_match'] = df['DF'].astype(str).str.lower() == df['Distrito Fiscal'].astype(str).str.lower()

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
   Genera mensajes de error y filtra filas fallidas de CUISMP y Distrito Fiscal.

    Para cada fila con `fail_count > 0`, construye la columna `mensaje`
    con la descripción de las validaciones que fallaron y añade `objetivo="1.1"`.

    Parameters
    ----------
    df : pandas.DataFrame
        Debe contener:
        - Flags de validación: Validation_OK, CUISMP_match, DF_match,
          CUISMP_in_medias_tomadas, fail_count
        - Datos de CUISMP y DF: CUISMP_sharepoint_cid_cuismp,
          CUISMP_sga_dinamico_335_excel_matched, Distrito Fiscal, DF
        - Identificadores: nro_incidencia, TIPO REPORTE

    Returns
    -------
    pandas.DataFrame
        DataFrame filtrado con filas donde `fail_count > 0` y columnas:
        ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo'].

    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation successful",
        (
            np.where(~df['CUISMP_match'], 
                     "\n CUISMP en Sharepoint CID-CUISMP: \n" + df['CUISMP_sharepoint_cid_cuismp'].astype(str) +
                     "\n es diferente a EXCEL -CORTE (CUISMP): \n" + df['CUISMP_sga_dinamico_335_excel_matched'].astype(str), "") +
            np.where(~df['DF_match'], 
                     "\n Distrito Fiscal en Sharepoint CID-CUISMP: \n" + df['Distrito Fiscal'].astype(str) +
                     "\n es diferente a EXCEL -CORTE (DF):  \n" + df['DF'].astype(str), "") +
            np.where(~df['CUISMP_in_medias_tomadas'], 
                     "\n CUISMP: \n "+ df['CUISMP_sga_dinamico_335_excel_matched'].astype(str) +
                    "\n no encontrado in MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS.", "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.1"
    
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


