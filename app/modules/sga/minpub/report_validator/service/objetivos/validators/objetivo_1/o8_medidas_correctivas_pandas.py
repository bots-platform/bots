import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_medidas_correctivas(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS' column in CORTE-EXCEL by checking:
    - If the value is non-empty
    - If the value matches the expected format based on TIPO REPORTE
    
    Returns a DataFrame with boolean flags and 'fail_count'.
    """
    df = df_merged.copy()

    df['non_empty_medidas'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed'].notna()

    # Check if MEDIDAS CORRECTIVAS matches expected format based on TIPO REPORTE
    df['match_medidas'] = (
        (df['TIPO REPORTE_trimed'] == 'INCIDENTE') & 
        (df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed'].str.startswith('Se realizó la revisión del servicio')) |
        (df['TIPO REPORTE_trimed'] == 'INTERRUPCION') & 
        (df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed'].str.startswith('Se realizó la revisión del servicio'))
    )

    df['Validation_OK'] = (
        df['non_empty_medidas'] &
        df['match_medidas']
    )

    df['fail_count'] = (
        (~df['non_empty_medidas']).astype(int) +
        (~df['match_medidas']).astype(int)
    )

    return df

@log_exceptions
def build_failure_messages_medidas_correctivas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS exitosa",
        (
            np.where(~df['non_empty_medidas'],
                     "\n  MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['match_medidas'],
                     "\n  MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS en CORTE-EXCEL: \n" + 
                     df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed'].astype(str) +
                     "\n no coincide con el formato esperado para el TIPO REPORTE: \n" + 
                     df['TIPO REPORTE_trimed'].astype(str), "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.8"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 