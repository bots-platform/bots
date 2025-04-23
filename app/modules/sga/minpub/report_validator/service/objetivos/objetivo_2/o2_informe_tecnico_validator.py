# objetivo_2_validator.py

import pandas as pd
import numpy as np
from utils.logger_config import get_sga_logger
from typing import Tuple


logger = get_sga_logger()

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper

@log_exceptions
def validate_informe_tecnico_word( merged_df: pd.DataFrame, componente_word: str) -> pd.DataFrame:
    """
    Validate values columns coming from word and excel files
    Retun a Dataframe  with new Boolean
    
    Columnas en EXCEL	                                        Columnas en WORD DATOS

    "FECHA Y HORA INICIO",                                      "Fecha y hora inicio":   
    "FECHA Y HORA FIN",                                         "Fecha y hora fin": 
    "CUISMP",                                                   "CUISMP": 
    "TIPO CASO",                                                "Tipo Caso":                    
    "OBSERVACION",                                              "Observación": 
    "DETERMINACIÓN DE LA CAUSA",                                "DETERMINACIÓN DE LA CAUSA":  
    "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS",              "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS": 
    """

    df = merged_df.copy()

    if componente_word == 'COMPONENTE II':
        df['CUISMP_word'] = df['CUISMP_word_datos_informe']
    elif componente_word == 'COMPONENTE IV':
        df['CUISMP_word'] = df['CUISMP_word_telefonia_informe']

    if componente_word == 'COMPONENTE II':
        df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word_datos_informe']
    elif componente_word == 'COMPONENTE IV':
        df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word_telefonia_informe']


    df['Fecha_hora_inicio_match'] = df['FECHA Y HORA INICIO'] == df['Fecha y Hora Inicio']
    df['fecha_hora_fin_match'] = df['FECHA Y HORA FIN'] == df['Fecha y Hora Fin']
    df['CUISMP_match'] = df['CUISMP_corte_excel'] == df['CUISMP_word']
    df['tipo_caso_match'] = df['TIPO CASO'] == df['Tipo Caso']
    df['observacion_match'] = df['OBSERVACIÓN'] == df['Observación']
    df['dt_causa_match'] = df['DETERMINACIÓN DE LA CAUSA'] == df['DETERMINACIÓN DE LA CAUSA'] 
    df['medidas_correctivas_match'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_corte_excel'] == df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word']

    df['Validation_OK'] = (
        df['Fecha_hora_inicio_match'] &
        df['fecha_hora_fin_match'] &
        df['CUISMP_match'] &
        df['tipo_caso_match'] &
        df['observacion_match'] &
        df['dt_causa_match'] &
        df['medidas_correctivas_match']
    )

    df['fail_count'] = (
        df['Fecha_hora_inicio_match'].stype(int)+ 
        df['fecha_hora_fin_match'].stype(int)+ 
        df['CUISMP_match'].stype(int)+ 
        df['tipo_caso_match'].stype(int)+ 
        df['observacion_match'].stype(int)+ 
        df['dt_causa_match'].stype(int)+ 
        df['medidas_correctivas_match'].stype(int)
    )
    return df


@log_exceptions
def build_failure_messages_validate_informe_tecnico_word(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the 'mensaje' column using vectorized operations.
    Adds the 'objetivo2' column (constant value of 2) and filters
    rows that fail at least one validation.
    
    Returns a DataFrame with:
      ['nro_incidencia', 'mensaje', 'objetivo']

    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation successful",
        (
            np.where(~df['TICKET_match'], 
                     "No coincide TICKET de WORD: " + df['Número de ticket'].astype(str) +
                     " con EXCEL-CORTE: " + df['TICKET'].astype(str) + ". ", "") +

            np.where(~df['Fecha_hora_inicio_match'],
                     " No coincide Fecha y Hora Inicio de WORD : " + df['Fecha y Hora Inicio'].astype(str) +
                     " es diferente a EXCEL-CORTE:  " + df['FECHA Y HORA INICIO'].astype(str) + ". ", "") +

            np.where(~df['fecha_hora_fin_match'],
                     " No coincide Fecha y Hora Inicio de WORD : " + df['Fecha y Hora Fin'].astype(str) +
                     " es diferente a EXCEL-CORTE:  " + df['FECHA Y HORA FIN'].astype(str) + ". ", "") +

            np.where(~df['CUISMP_match'],
                     " No coincide CUISMP_word_telefonia de WORD : " + df['CUISMP_word'].astype(str) +
                     " es diferente a CUISMP_corte_excel: " + df['CUISMP_corte_excel'].astype(str) + ". ", "") +

            np.where(~df['tipo_caso_match'],
                     " No coincide Avería reportada de WORD : " + df['Tipo Caso'].astype(str) +
                     " es diferente a TIPO CASO de Excel: " + df['TIPO CASO'].astype(str) + ". ", "") +
                    
            
            np.where(~df['observacion_match'],
                     " No coincide observacion de WORD : " + df['Observación'].astype(str) +
                     " es diferente a AVERÍA de Excel: " + df['OBSERVACIÓN'].astype(str) + ". ", "") +

        
            np.where(~df['dt_causa_match'],
                     " No coincide Determinación de la causa de WORD-Datos : " + df['DETERMINACIÓN DE LA CAUSA'].astype(str) +
                     " es diferente a DETERMINACION DE LA CAUSA de Excel: " + df['DETERMINACION DE LA CAUSA'].astype(str) + ". ", "") +


            np.where(~df['medidas_correctivas_match'],
                     " No coincide MEDIDAS CORRECTIVAS de WORD-Datos : " + df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'].astype(str) +
                     " es diferente a RESPONSABILIDAD de Excel: " + df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_corte_excel'].astype(str) + ". ", "") 

        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = 2.2
    
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]



