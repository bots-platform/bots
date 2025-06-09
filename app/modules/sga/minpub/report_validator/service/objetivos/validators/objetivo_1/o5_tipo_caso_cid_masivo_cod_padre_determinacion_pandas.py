import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_tipo_caso_cid_masivo_cod_padre_determinacion(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the 'TIPO CASO' column in CORTE-EXCEL by comparing:
    - The 'TIPO CASO' value in CORTE-EXCEL
    vs.
    - The expected 'TIPO CASO' value based on 'CID' and 'MASIVO' columns.
    Returns a DataFrame with boolean flags and 'fail_count'.
    """
    df = df_merged.copy()

    df['non_empty_tipo_caso'] = df['TIPO CASO_trimed'].notna()
    df['non_empty_cid'] = df['CID_trimed'].notna()
    df['non_empty_masivo'] = df['MASIVO_trimed'].notna()

    df['match_tipo_caso'] = (
        (df['TIPO CASO_trimed'] == 'MASIVO') & (df['MASIVO_trimed'] == 'SI') |
        (df['TIPO CASO_trimed'] == 'INDIVIDUAL') & (df['MASIVO_trimed'] == 'NO')
    )

    df['Validation_OK'] = (
        df['non_empty_tipo_caso'] &
        df['non_empty_cid'] &
        df['non_empty_masivo'] &
        df['match_tipo_caso']
    )

    df['fail_count'] = (
        (~df['non_empty_tipo_caso']).astype(int) +
        (~df['non_empty_cid']).astype(int) +
        (~df['non_empty_masivo']).astype(int) +
        (~df['match_tipo_caso']).astype(int)
    )

    return df

@log_exceptions
def build_failure_messages_tipo_caso_cid_masivo_cod_padre_determinacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'TIPO CASO' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de TIPO CASO exitosa",
        (
            np.where(~df['non_empty_tipo_caso'],
                     "\n  TIPO CASO en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['non_empty_cid'],
                     "\n  CID en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['non_empty_masivo'],
                     "\n  MASIVO en CORTE-EXCEL es vacio. \n", "") +
            np.where(~df['match_tipo_caso'],
                     "\n  TIPO CASO en CORTE-EXCEL: \n" + df['TIPO CASO_trimed'].astype(str) +
                     "\n no coincide con el valor esperado basado en MASIVO: \n" + df['MASIVO_trimed'].astype(str), "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.5"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 