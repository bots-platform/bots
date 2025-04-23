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
def validate_informe_tecnico_word( merged_df: pd.DataFrame) -> pd.DataFrame:
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


    df['Fecha_hora_inicio_match'] = df['FECHA Y HORA INICIO'] == df['Fecha y Hora Inicio']
    df['fecha_hora_fin_match'] = df['FECHA Y HORA FIN'] == df['Fecha y Hora Fin']
    df['CUISMP_match'] = df['CUISMP_corte_excel'] == df['CUISMP_word_telefonia']
    df['tipo_caso_match'] = df['TIPO CASO'] == df['Tipo Caso']
    df['averia_observacion'] = df['OBSERVACION'] == df['Observación']
    df['dt_causa_match'] = df['DETERMINACION DE LA CAUSA'] == df['DETERMINACIÓN DE LA CAUSA'] 
    df['medidas_correctivas_match'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'] == df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS']



    df['Validation_OK'] = (
        df['Fecha_hora_inicio_match'] &
        df['fecha_hora_fin_match'] &
        df['CUISMP_match'] &
        df['tipo_caso_match'] &
        df['averia_observacion'] &
        df['dt_causa_match'] &
        df['medidas_correctivas_match']
    )

    df['fail_count'] = (
        df['Fecha_hora_inicio_match'].stype(int)+ 
        df['fecha_hora_fin_match'].stype(int)+ 
        df['CUISMP_match'].stype(int)+ 
        df['tipo_caso_match'].stype(int)+ 
        df['averia_observacion'].stype(int)+ 
        df['dt_causa_match'].stype(int)+ 
        df['medidas_correctivas_match'].stype(int)
    )
    return df