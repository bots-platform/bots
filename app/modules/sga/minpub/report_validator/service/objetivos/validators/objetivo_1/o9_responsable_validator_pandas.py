import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_responsable(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the 'RESPONSABLE' column in CORTE-EXCEL by checking:
    - If the value is non-empty
    - If the value matches the expected format
    
    Returns a DataFrame with boolean flags and 'fail_count'.
    """
    df = df_merged.copy()

    df['non_empty_responsable'] = df['RESPONSABLE_trimed'].notna()

    # Check if RESPONSABLE matches expected format
    df['match_responsable'] = (
        (df['RESPONSABLE_trimed'] == 'SOPORTE TÉCNICO') |
        (df['RESPONSABLE_trimed'] == 'SOPORTE TECNICO') |
        (df['RESPONSABLE_trimed'] == 'SOPORTE TÉCNICO - CLARO') |
        (df['RESPONSABLE_trimed'] == 'SOPORTE TECNICO - CLARO')
    )

    df['Validation_OK'] = (
        df['non_empty_responsable'] &
        df['match_responsable']
    )

    df['fail_count'] = (
        (~df['non_empty_responsable']).astype(int) +
        (~df['match_responsable']).astype(int)
    )

    return df

@log_exceptions
def build_failure_messages_responsable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'RESPONSABLE' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de RESPONSABLE exitosa",
        (
            np.where(~df['non_empty_responsable'],
                     "\n  RESPONSABLE en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['match_responsable'],
                     "\n  RESPONSABLE en CORTE-EXCEL: \n" + 
                     df['RESPONSABLE_trimed'].astype(str) +
                     "\n no coincide con los valores permitidos: \n" + 
                     "SOPORTE TÉCNICO, SOPORTE TECNICO, SOPORTE TÉCNICO - CLARO, SOPORTE TECNICO - CLARO", "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.9"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 