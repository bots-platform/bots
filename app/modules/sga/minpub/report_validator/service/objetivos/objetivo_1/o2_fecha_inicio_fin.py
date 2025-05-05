import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime


from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


@log_exceptions
def validation_fecha_inicio_fin(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida las columnas de fechas en CORTE EXCEL y las compara con las del REPORTE DINÁMICO 335.
    
    Reglas:
      1) En CORTE EXCEL, “FECHA Y HORA INICIO” y “FECHA Y HORA FIN” no deben ser vacías.
      2) A partir del “nro_incidencia” en REPORTE DINÁMICO 335:
            - Si la columna “Masivo” tiene el valor “Si”, se utiliza “FECHA_GENERACION” como fecha de inicio.
            - En caso contrario, se utiliza “INTERRUPCION_INICIO”.
      3) Se compara:
            - La fecha esperada de inicio (según lo anterior) con “FECHA Y HORA INICIO” de CORTE EXCEL.
            - “INTERRUPCION_FIN” de REPORTE DINÁMICO 335 con “FECHA Y HORA FIN” de CORTE EXCEL.
      4) Ambas comparaciones deben coincidir para considerarse válidas.
    
    Devuelve un DataFrame con las siguientes columnas adicionales:
      - 'NotEmpty': Flag que indica que las fechas de CORTE EXCEL no son vacías.
      - 'Fecha_Inicio_match': Flag que indica si la fecha de inicio es correcta.
      - 'Fecha_Fin_match': Flag que indica si la fecha de fin es correcta.
      - 'Validation_OK': True si todas las condiciones se cumplen.
      - 'fail_count': Número de validaciones fallidas.
    """
    df = merged_df.copy()

    df['NotEmpty'] = df['FECHA Y HORA INICIO'].notna() & df['FECHA Y HORA FIN'].notna()

    df['Expected_Inicio'] = np.where(df['masivo'] == "Si",
                                     df['fecha_generacion'],
                                     df['interrupcion_inicio'])
        

    df['Expected_Inicio_trimmed'] = df['Expected_Inicio'].apply(lambda x: x.replace(second=0))
    df['interrupcion_fin_trimmed'] = df['interrupcion_fin'].apply(lambda x: x.replace(second=0))


    df['Fecha_Inicio_match'] = df['Expected_Inicio_trimmed'] == df['FECHA Y HORA INICIO']
    df['Fecha_Fin_match'] = df['interrupcion_fin_trimmed'] == df['FECHA Y HORA FIN']

    df['Validation_OK'] = df['NotEmpty'] & df['Fecha_Inicio_match'] & df['Fecha_Fin_match']

    df['fail_count'] = (~df['NotEmpty']).astype(int) + (~df['Fecha_Inicio_match']).astype(int) + (~df['Fecha_Fin_match']).astype(int)

    return df

@log_exceptions
def build_failure_messages_fechas_fin_inicio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye el mensaje de error para las validaciones de fechas.
    
    Devuelve un DataFrame con las columnas:
      - 'nro_incidencia'
      - 'mensaje': mensaje descriptivo de las fallas.
      - 'objetivo': valor constante (ej. 2, si esta validación pertenece al objetivo 2).
    """

    df['FECHA Y HORA INICIO'] = pd.to_datetime(df['FECHA Y HORA INICIO'], format='%Y-%m-%d', errors='coerce')
    df['FECHA Y HORA FIN'] = pd.to_datetime(df['FECHA Y HORA FIN'], format='%Y-%m-%d', errors='coerce')

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

    df['Expected_Inicio_trimmed_fm'] = (
        df['Expected_Inicio_trimmed']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

    df['interrupcion_fin_trimmed_fm'] = (
        df['interrupcion_fin_trimmed']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )



    mensaje = np.where(
        df['Validation_OK'],
        "Validación de fechas exitosa",
        (
            np.where(~df['NotEmpty'],
                     "\n Las columnas ‘FECHA Y HORA INICIO’ y/o ‘FECHA Y HORA FIN’ están vacías. ",
                     "") +
            np.where(~df['Fecha_Inicio_match'],
                     "\n (interrupcion_inicio|fecha generacion) en SGA: \n" +
                     df['Expected_Inicio_trimmed_fm']+
                     "\n no coincide con FECHA Y HORA INICIO CORTE-EXCEL: \n" +
                    df['FECHA_Y_HORA_INICIO_fmt'],
                     "") +
            np.where(~df['Fecha_Fin_match'],
                     "\n (interrupcion_fin) en SGA: \n" +
                    df['interrupcion_fin_trimmed_fm']+
                     "\n no coincide con FECHA Y HORA FIN CORTE-EXCEL: \n" +
                    df['FECHA_Y_HORA_FIN_fmt'] +
                      "",
                     "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.2"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


