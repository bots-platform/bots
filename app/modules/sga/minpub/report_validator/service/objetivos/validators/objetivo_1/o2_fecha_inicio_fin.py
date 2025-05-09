import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime


from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_fecha_inicio_fin(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
     Valida fechas de inicio y fin entre Corte Excel y Reporte Dinámico 335.

    Comprueba que las columnas de Excel no estén vacías y que las fechas
    coincidan con las esperadas según la lógica de negocio. Añade indicadores
    de validación y cuenta el número de fallos.

    Parameters
    ----------
    merged_df : pandas.DataFrame
        DataFrame que contiene al menos las columnas:
        - FECHA Y HORA INICIO (datetime)
        - FECHA Y HORA FIN (datetime)
        - Expected_Inicio_truncated (datetime)
        - interrupcion_fin_truncated (datetime)

    Returns
    -------
    pandas.DataFrame
        Copia del DataFrame de entrada con estas columnas adicionales:
        - NotEmpty (bool): True si ambas fechas de Excel no son nulas.
        - Fecha_Inicio_match (bool): True si la fecha de inicio coincide.
        - Fecha_Fin_match (bool): True si la fecha de fin coincide.
        - Validation_OK (bool): True si todas las validaciones pasan.
        - fail_count (int): Número de validaciones que fallaron (0–3).
    """
    df = merged_df.copy()

    df['NotEmpty'] = df['FECHA Y HORA INICIO'].notna() & df['FECHA Y HORA FIN'].notna()

    df['Fecha_Inicio_match'] = df['Expected_Inicio_truncated'] == df['FECHA Y HORA INICIO']
    df['Fecha_Fin_match'] = df['interrupcion_fin_truncated'] == df['FECHA Y HORA FIN']

    df['Validation_OK'] = df['NotEmpty'] & df['Fecha_Inicio_match'] & df['Fecha_Fin_match']

    df['fail_count'] = (~df['NotEmpty']).astype(int) + (~df['Fecha_Inicio_match']).astype(int) + (~df['Fecha_Fin_match']).astype(int)

    return df

@log_exceptions
def build_failure_messages_fechas_fin_inicio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera mensajes de error de validación de fechas y filtra los fallos.

    Parámetros
    ----------
    df : pandas.DataFrame
        Debe contener:
        - Flags de validación: Validation_OK, NotEmpty,
          Fecha_Inicio_match, Fecha_Fin_match, fail_count
        - Fechas formateadas: Expected_Inicio_truncated_fm,
          FECHA_Y_HORA_INICIO_fmt, interrupcion_fin_truncated_fm,
          FECHA_Y_HORA_FIN_fmt
        - Identificadores: nro_incidencia, TIPO REPORTE

    Devuelve
    -------
    pandas.DataFrame
        Filtra filas con `fail_count > 0` y columnas:
        - nro_incidencia
        - mensaje (descripción de las fallas)
        - TIPO REPORTE
        - objetivo (constante "1.2")
    """

    mensaje = np.where(
        df['Validation_OK'],
        "Validación de fechas exitosa",
        (
            np.where(~df['NotEmpty'],
                     "\n Las columnas ‘FECHA Y HORA INICIO’ y/o ‘FECHA Y HORA FIN’ están vacías. ",
                     "") +
            np.where(~df['Fecha_Inicio_match'],
                     "\n (interrupcion_inicio|fecha generacion) en SGA: \n" +
                     df['Expected_Inicio_truncated_fm']+
                     "\n no coincide con FECHA Y HORA INICIO CORTE-EXCEL: \n" +
                    df['FECHA_Y_HORA_INICIO_fmt'],
                     "") +
            np.where(~df['Fecha_Fin_match'],
                     "\n (interrupcion_fin) en SGA: \n" +
                    df['interrupcion_fin_truncated_fm']+
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


