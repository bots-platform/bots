import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
import re

from app.modules.sga.minpub.report_validator.service.objetivos.calculations import has_repetition
from app.modules.sga.minpub.report_validator.service.objetivos.calculations import extract_date_range_body
from app.modules.sga.minpub.report_validator.service.objetivos.calculations import extract_date_range_last

# import language_tool_python
# _tool_es = language_tool_python.LanguageTool('es')

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

def validation_medidas_correctivas(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validation the column medidas correctivas y o medidas tomadas, se debe obtener
    the first and the last date from paragraph, excluding the two last lines if exists 
    dates called fecha hora inicio and fecha hora fin
    """

    df = merged_df.copy()

    df['mc_first_ok'] = True
    df['mc_last_ok'] = True
    df['it_first_ok'] = True
    df['it_last_ok'] = True
    # df['ortografia_ok'] = True
    df['no_repeticion_ok'] = True



    date_range_mc_body = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].apply(extract_date_range_body)
    df[['first_dt_mc', 'last_dt_mc']] = pd.DataFrame(date_range_mc_body.tolist(), index=df.index)

    date_range_it_body = df['it_medidas_tomadas'].apply(extract_date_range_body)
    df[['first_dt_it', 'last_dt_it']] = pd.DataFrame(date_range_it_body.tolist(), index=df.index)

    date_range_it_last = df['it_medidas_tomadas'].apply(extract_date_range_last)
    df[['start_dt_last_it', 'end_dt_last_it']] = pd.DataFrame(date_range_it_last.tolist(), index=df.index)

    
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
    
    df['mc_first_ok'] = (
        df['first_dt_mc'] == df['FECHA_Y_HORA_INICIO_fmt']
    )

    df['mc_last_ok'] = (
        df['last_dt_mc'] == df['FECHA_Y_HORA_FIN_fmt']
    )

    df['it_first_ok'] = (
        df['first_dt_it'] == df['start_dt_last_it']
    )

    df['it_last_ok'] = (
        df['last_dt_it'] == df['end_dt_last_it']
    )

    # df['ortografia_ok'] = ~df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].apply(is_langtool_clean)
    df['no_repeticion_ok'] = ~df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].apply(has_repetition)

 
    df['Validation_OK'] = (
        df['mc_first_ok'] &
        df['mc_last_ok'] &
        df['it_first_ok'] &
        df['it_last_ok'] &
        # df['ortografia_ok'] &
        df['no_repeticion_ok'] 
    )

    df['fail_count'] = ( 
        (~df['mc_first_ok']).astype(int)+
        (~df['mc_last_ok']).astype(int)+
        (~df['it_first_ok']).astype(int)+
        (~df['it_last_ok']).astype(int)+
        # (~df['ortografia_ok']).astype(int)+ 
        (~df['no_repeticion_ok']).astype(int)           
    )

    return df


@log_exceptions
def build_failure_messages_medidas_correctivas(df:pd.DataFrame) -> pd.DataFrame:
    """
    Build detailed error messages for medidas correctivas validation failures.

    """

    if not isinstance(df, pd.DataFrame) or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])
    
    mensajes = np.where(
       df['Validation_OK'],
       "Validación exitosa: MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS",
       (
                     np.where(~df['mc_first_ok'],
                    " \n\n La primera fecha/hora  del parrafo (CUERPO) en columna MEDIDAS CORRECTIVAS - EXCEL: \n\n  ( " + df['first_dt_mc'].astype(str) +
                      " ) no coincide con la columna FECHA Y HORA INICIO DE EXCEL - CORTE : " + df['FECHA_Y_HORA_INICIO_fmt'].astype(str) + ". ",
                    "") +

                      np.where(~df['mc_last_ok'],
                    "\n\n  La última fecha/hora del parrafo (CUERPO) en columna MEDIDAS CORRECTIVAS - EXCEL: \n\n ( " + df['last_dt_mc'].astype(str) +
                      " ) no coincide con la columna FECHA Y HORA FIN DE EXCEL - CORTE : " +
                      df['FECHA_Y_HORA_FIN_fmt'].astype(str)+ ". ", 
                    "") + 

                      np.where(~df['it_first_ok'],
                    "\n\n  La primera fecha/hora del parrafo en it_medidas_tomadas SGA:  \n\n ( " + df['first_dt_it'].astype(str) +
                      " ) no coincide con la Fecha y hora inicio de la penultima fila it_medidas_tomadas SGA : " + df['start_dt_last_it'].astype(str) + ". ",
                    "") +

                     np.where(~df['it_last_ok'],
                    "\n\n  La última fecha/hora del parrafo (CUERPO) en it_medidas_tomadas SGA : \n\n ( " + df['last_dt_it'].astype(str) +
                      " ) no coincide con la Fecha y hora fin de la última fila SGA it_medidas_tomadas SGA: " +
                      df['end_dt_last_it'].astype(str) + ". ", 
                    "") + 

                    # np.where(~df['ortografia_ok'],
                    # "  Errores ortográficos/gramaticales en el parrafo en MEDIDAS CORRECTIVAS",
                    # "")  +

                    np.where(~df['no_repeticion_ok'],
                    "\n\n  Hay palabras repetidas inmediatamente/a través en el parrafo en MEDIDAS CORRECTIVAS:",
                    "") 
       )
    )
    
    df['mensaje'] = mensajes
    df['objetivo'] = "1.8"
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]