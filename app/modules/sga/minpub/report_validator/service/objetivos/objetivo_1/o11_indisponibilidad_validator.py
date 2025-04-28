
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime, timedelta

import re

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

@log_exceptions
def validate_indisponibilidad(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Builds an 'expected_indisponibilidad' text from clock_stops_paragraph (merging overlaps)
    and compares it to the user‑entered INDISPONIBILIDAD column.
    Adds columns:
      - expected_indisponibilidad (str)
      - indisponibilidad_ok (bool)
      - Validation_OK (bool)
      - fail_count (0/1)
    """
    df = df_merged.copy()
    
    df['expected_indisponibilidad'] = df['clock_stops_paragraph']
    df['indisponibilidad_ok'] = (
        df['INDISPONIBILIDAD'].astype(str).str.strip()
        == df['expected_indisponibilidad']
    )

    df['Validation_OK'] = df['indisponibilidad_ok']
    df['fail_count']   = (~df['Validation_OK']).astype(int)
    return df


@log_exceptions
def build_failure_messages_indisponibilidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame of failures with columns:
    ['nro_incidencia','mensaje','objetivo']
    """
    if df is None or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia','mensaje','objetivo'])

    mensajes = np.where(
        df['Validation_OK'],
        "Validación exitosa: INDISPONIBILIDAD coincide con las paradas de reloj",
        
        "INDISPONIBILIDAD inválida:\n"
        + "✘ Ingresado:\n"
        + df['INDISPONIBILIDAD'].astype(str)
        + "\n\n✔ Esperado:\n"
        + df['expected_indisponibilidad']
    )

    df['mensaje']  = mensajes
    df['objetivo'] = "1.11"

    return df[df['fail_count'] > 0][['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


