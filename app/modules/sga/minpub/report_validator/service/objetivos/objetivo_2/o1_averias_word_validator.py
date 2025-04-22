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
def validate_averias_word( merged_df: pd.DataFrame, componente_word) -> pd.DataFrame:
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

    df['componente_word'] = componente_word
    df['Componente'] =  df['componente_word']

    df['TICKET_match'] = df['TICKET'] == df['Número de ticket']
    df['Fecha_hora_inicio_match'] = df['FECHA Y HORA INICIO'] == df['Fecha y Hora Inicio']
    df['fecha_hora_fin_match'] = df['FECHA Y HORA FIN'] == df['Fecha y Hora Fin']
    df['CUISMP_match'] = df['CUISMP_corte_excel'] == df['CUISMP_word_telefonia']
    df['tipo_caso_match'] = df['TIPO CASO'] == df['Número de ticket']
    df['averia_match'] = df['AVERÍA'] == df['Causa']
    df['tiempo_hhmm_match'] = df['TIEMPO (HH:MM)'] == df['Tiempo real de afectación (HH:MM)']

    df['componente_match'] = df['COMPONENTE'] == df['Componente']

    df['df_match'] = df['DF'] == df['Distrito Fiscal']
    df['fin_inicio_hhmm_match'] = df['FIN-INICIO (HH:MM)'] == df['Tiempo Total (HH:MM)']
    #df['dt_causa_match'] = df['DETERMINACION DE LA CAUSA']	== df['Determinación de la causa']
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
 

@log_exceptions
def build_failure_messages_validate_averias_word(df: pd.DataFrame, componente_word) -> pd.DataFrame:
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
                     "No coincide TICKET de WORD-DATOS: " + df['Número de ticket'].astype(str) +
                     " con EXCEL-CORTE: " + df['TICKET'].astype(str) + ". ", "") +

            np.where(~df['Fecha_hora_inicio_match'],
                     " No coincide Fecha y Hora Inicio de WORD-Datos : " + df['Fecha y Hora Inicio'].astype(str) +
                     " es diferente a EXCEL-CORTE:  " + df['FECHA Y HORA INICIO'].astype(str) + ". ", "") +

            np.where(~df['fecha_hora_fin_match'],
                     " No coincide Fecha y Hora Inicio de WORD-Datos : " + df['Fecha y Hora Fin'].astype(str) +
                     " es diferente a EXCEL-CORTE:  " + df['FECHA Y HORA FIN'].astype(str) + ". ", "") +

            np.where(~df['CUISMP_match'],
                     " No coincide CUISMP_word_telefonia de WORD-Datos : " + df['CUISMP_word_telefonia'].astype(str) +
                     " es diferente a CUISMP_corte_excel: " + df['CUISMP_corte_excel'].astype(str) + ". ", "") +

            np.where(~df['tipo_caso_match'],
                     " No coincide Avería reportada de WORD-Datos : " + df['Avería reportada'].astype(str) +
                     " es diferente a TIPO CASO de Excel: " + df['TIPO CASO'].astype(str) + ". ", "") +
                    
            
            np.where(~df['averia_match'],
                     " No coincide Causa de WORD-Datos : " + df['Causa'].astype(str) +
                     " es diferente a AVERÍA de Excel: " + df['AVERÍA'].astype(str) + ". ", "") +

            np.where(~df['tiempo_hhmm_match'],
                     " No coincide TIEMPO (HH:MM) de WORD-Datos : " + df['TIEMPO (HH:MM)'].astype(str) +
                     " es diferente a Tiempo real de afectación (HH:MM) de Excel: " + df['Tiempo real de afectación (HH:MM)'].astype(str) + ". ", "") +


            np.where(~df['componente_match'],
                     " No coincide Componente de WORD-Datos : " + df['Componente'].astype(str) +
                     " es diferente a Tiempo real de afectación (HH:MM) de Excel: " + df['COMPONENTE'].astype(str) + ". ", "") +


            np.where(~df['df_match'],
                     " No coincide Distrito Fiscal de WORD-Datos : " + df['Distrito Fiscal'].astype(str) +
                     " es diferente a DF de Excel: " + df['DF'].astype(str) + ". ", "") +

             np.where(~df['fin_inicio_hhmm_match'],
                     " No coincide Tiempo Total (HH:MM) de WORD-Datos : " + df['Tiempo Total (HH:MM)'].astype(str) +
                     " es diferente a FIN-INICIO (HH:MM) de Excel: " + df['FIN-INICIO (HH:MM)'].astype(str) + ". ", "") +


            np.where(~df['dt_causa_match'],
                     " No coincide Determinación de la causa de WORD-Datos : " + df['Determinación de la causa'].astype(str) +
                     " es diferente a DETERMINACION DE LA CAUSA de Excel: " + df['DETERMINACION DE LA CAUSA'].astype(str) + ". ", "") +


            np.where(~df['responsabilidad_match'],
                     " No coincide Responsable de WORD-Datos : " + df['Responsable'].astype(str) +
                     " es diferente a RESPONSABILIDAD de Excel: " + df['RESPONSABILIDAD'].astype(str) + ". ", "") 

        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = 2.1
    
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


