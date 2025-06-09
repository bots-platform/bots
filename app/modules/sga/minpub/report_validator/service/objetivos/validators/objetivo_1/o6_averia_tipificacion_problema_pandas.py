import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_averia_tipificacion_problema(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the 'AVERIA' and 'TIPIFICACION DEL PROBLEMA' columns in CORTE-EXCEL by comparing:
    - The 'AVERIA' value in CORTE-EXCEL
    vs.
    - The expected 'AVERIA' value based on 'TIPIFICACION DEL PROBLEMA' column.
    Returns a DataFrame with boolean flags and 'fail_count'.
    """
    df = df_merged.copy()

    df['non_empty_averia'] = df['AVERIA_trimed'].notna()
    df['non_empty_tipificacion'] = df['TIPIFICACION DEL PROBLEMA_trimed'].notna()

    df['match_averia'] = (
        (df['AVERIA_trimed'] == 'SI') & (df['TIPIFICACION DEL PROBLEMA_trimed'] == 'AVERIA') |
        (df['AVERIA_trimed'] == 'NO') & (df['TIPIFICACION DEL PROBLEMA_trimed'] != 'AVERIA')
    )

    df['Validation_OK'] = (
        df['non_empty_averia'] &
        df['non_empty_tipificacion'] &
        df['match_averia']
    )

    df['fail_count'] = (
        (~df['non_empty_averia']).astype(int) +
        (~df['non_empty_tipificacion']).astype(int) +
        (~df['match_averia']).astype(int)
    )

    return df

@log_exceptions
def build_failure_messages_averia_tipificacion_problema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'AVERIA' and 'TIPIFICACION DEL PROBLEMA' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de AVERIA y TIPIFICACION DEL PROBLEMA exitosa",
        (
            np.where(~df['non_empty_averia'],
                     "\n  AVERIA en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['non_empty_tipificacion'],
                     "\n  TIPIFICACION DEL PROBLEMA en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['match_averia'],
                     "\n  AVERIA en CORTE-EXCEL: \n" + df['AVERIA_trimed'].astype(str) +
                     "\n no coincide con el valor esperado basado en TIPIFICACION DEL PROBLEMA: \n" + 
                     df['TIPIFICACION DEL PROBLEMA_trimed'].astype(str), "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.6"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 