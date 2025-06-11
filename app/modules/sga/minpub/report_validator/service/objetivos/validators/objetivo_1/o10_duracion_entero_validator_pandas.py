import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_duracion_entero(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the 'DURACIÓN ENTERO' column in CORTE-EXCEL by checking:
    - If the value is non-empty
    - If the value is a valid integer
    - If the value is within the expected range (0-999)
    
    Returns a DataFrame with boolean flags and 'fail_count'.
    """
    df = df_merged.copy()

    df['non_empty_duracion'] = df['DURACIÓN ENTERO_trimed'].notna()

    # Check if DURACIÓN ENTERO is a valid integer
    df['is_integer'] = df['DURACIÓN ENTERO_trimed'].str.isdigit()

    # Check if DURACIÓN ENTERO is within range (0-999)
    df['in_range'] = (
        df['DURACIÓN ENTERO_trimed'].str.isdigit() &
        (df['DURACIÓN ENTERO_trimed'].astype(float) >= 0) &
        (df['DURACIÓN ENTERO_trimed'].astype(float) <= 999)
    )

    df['Validation_OK'] = (
        df['non_empty_duracion'] &
        df['is_integer'] &
        df['in_range']
    )

    df['fail_count'] = (
        (~df['non_empty_duracion']).astype(int) +
        (~df['is_integer']).astype(int) +
        (~df['in_range']).astype(int)
    )

    return df

@log_exceptions
def build_failure_messages_duracion_entero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'DURACIÓN ENTERO' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de DURACIÓN ENTERO exitosa",
        (
            np.where(~df['non_empty_duracion'],
                     "\n  DURACIÓN ENTERO en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['is_integer'],
                     "\n  DURACIÓN ENTERO en CORTE-EXCEL: \n" + 
                     df['DURACIÓN ENTERO_trimed'].astype(str) +
                     "\n no es un número entero válido.", "") +
            np.where(~df['in_range'],
                     "\n  DURACIÓN ENTERO en CORTE-EXCEL: \n" + 
                     df['DURACIÓN ENTERO_trimed'].astype(str) +
                     "\n no está en el rango permitido (0-999).", "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.10"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 