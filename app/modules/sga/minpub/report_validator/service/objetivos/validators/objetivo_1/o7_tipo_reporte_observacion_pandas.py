import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_tipo_reporte_observacion(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the 'TIPO REPORTE' and 'OBSERVACION' columns in CORTE-EXCEL by comparing:
    - The 'TIPO REPORTE' value in CORTE-EXCEL
    vs.
    - The expected 'TIPO REPORTE' value based on 'OBSERVACION' column.
    Returns a DataFrame with boolean flags and 'fail_count'.
    """
    df = df_merged.copy()

    df['non_empty_tipo_reporte'] = df['TIPO REPORTE_trimed'].notna()
    df['non_empty_observacion'] = df['OBSERVACION_trimed'].notna()

    df['match_tipo_reporte'] = (
        (df['TIPO REPORTE_trimed'] == 'INCIDENTE') & (df['OBSERVACION_trimed'] == 'INCIDENTE') |
        (df['TIPO REPORTE_trimed'] == 'INTERRUPCION') & (df['OBSERVACION_trimed'] == 'INTERRUPCION')
    )

    df['Validation_OK'] = (
        df['non_empty_tipo_reporte'] &
        df['non_empty_observacion'] &
        df['match_tipo_reporte']
    )

    df['fail_count'] = (
        (~df['non_empty_tipo_reporte']).astype(int) +
        (~df['non_empty_observacion']).astype(int) +
        (~df['match_tipo_reporte']).astype(int)
    )

    return df

@log_exceptions
def build_failure_messages_tipo_reporte_observacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'TIPO REPORTE' and 'OBSERVACION' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de TIPO REPORTE y OBSERVACION exitosa",
        (
            np.where(~df['non_empty_tipo_reporte'],
                     "\n  TIPO REPORTE en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['non_empty_observacion'],
                     "\n  OBSERVACION en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['match_tipo_reporte'],
                     "\n  TIPO REPORTE en CORTE-EXCEL: \n" + df['TIPO REPORTE_trimed'].astype(str) +
                     "\n no coincide con el valor esperado basado en OBSERVACION: \n" + 
                     df['OBSERVACION_trimed'].astype(str), "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.7"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 