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
def validate_averias_word( merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate values columns coming from word and excel files
    Retun a Dataframe  with new Boolean
    
    Columnas en EXCEL	        Columnas en WORD DATOS

    TICKET	                    Número de ticket
    FECHA Y HORA INICIO	        Fecha y Hora Inicio
    FECHA Y HORA FIN	        Fecha y Hora Fin
    CUISMP	                    CUISMP
    TIPO CASO	                Avería reportada
    AVERÍA	                    Causa
    TIEMPO (HH:MM)	            Tiempo real de afectación (HH:MM)
    COMPONENTE	                Componente
    DF	                        Distrito Fiscal
    FIN-INICIO (HH:MM)	        Tiempo Total (HH:MM)
    DETERMINACION DE LA CAUSA	Determinación de la causa
    RESPONSABILIDAD	            Responsable

    """
    df = merged_df.copy()

    df['TICKET_match'] = df['TICKET'] == df['Número de ticket']
    df['Fecha_hora_inicio_match'] = df['FECHA Y HORA INICIO'] == df['Fecha y Hora Inicio']
    df['fecha_hora_fin_match'] = df['FECHA Y HORA FIN'] == df['Fecha y Hora Fin']
    df['CUISMP_match'] = df['CUISMP_word_telefonia'] == df['CUISMP_corte_excel']
    df['tipo_caso_match'] = df['TIPO CASO'] == df['Número de ticket']
    df['averia_match'] = df['AVERÍA'] == df['Causa']
    df['tiempo_hhmm_match'] = df['TIEMPO (HH:MM)'] == df['Tiempo real de afectación (HH:MM)']
    df['componente_match'] = df['COMPONENTE'] == df['Componente']
    df['df_match'] = df['DF'] == df['Distrito Fiscal']
    df['fin_inicio_hhmm_match'] = df['FIN-INICIO (HH:MM)'] == df['Tiempo Total (HH:MM)']
    df['dt_causa_match'] = df['DETERMINACION DE LA CAUSA']	== df['Determinación de la causa']
    df['responsabilidad_match'] = df['RESPONSABILIDAD'] == df['Responsable']

    df['Validation_OK'] = (
        df['TICKET_match'] &
        df['Fecha_hora_inicio_match'] &
        df['fecha_hora_fin_match'] &
        df['CUISMP_match'] &
        df['tipo_caso_match'] &
        df['averia_match'] &
        df['tiempo_hhmm_match'] &
        df['componente_match'] &
        df['df_match'] &
        df['fin_inicio_hhmm_match'] &
        df['dt_causa_match'] &
        df['responsabilidad_match']
    )

    df['fail_count'] = (
        df['TICKET_match'].stype(int)+
        df['Fecha_hora_inicio_match'].stype(int)+ 
        df['fecha_hora_fin_match'].stype(int)+ 
        df['CUISMP_match'].stype(int)+ 
        df['tipo_caso_match'].stype(int)+ 
        df['averia_match'].stype(int)+ 
        df['tiempo_hhmm_match'].stype(int)+ 
        df['componente_match'].stype(int)+ 
        df['df_match'].stype(int)+ 
        df['fin_inicio_hhmm_match'].stype(int)+ 
        df['dt_causa_match'].stype(int)+ 
        df['responsabilidad_match'].stype(int)
    )
    return df
 
