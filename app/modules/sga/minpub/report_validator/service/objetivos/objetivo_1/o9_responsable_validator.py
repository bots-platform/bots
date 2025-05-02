import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

import re



from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


def validate_responsable(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validates that RESPONSABLE (EXCEL-CORTE) equals the first word of tipificacion_tipo (SGA 335)
    
    Adds:
        - responsable_expected: first word of tipificacion_tipo
        - responsable_ok: True/False comparison
        - Validation_OK: AND-combined with any existing flag
        - fail_count: 0/1
    """
    df = df_merged.copy()

    df['responsable_expected'] = (
        df['tipificacion_tipo']
        .astype(str)
        .str.split('-', n=1)
        .str[0]
        .str.strip()
    )
    df['responsable_OK'] = (
        df['RESPONSABILIDAD'].astype(str).str.strip()
        == df['responsable_expected']
    )

    df['Validation_OK'] = df['responsable_OK']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df

def build_failure_messages_responsable(df:pd.DataFrame) -> pd.DataFrame:
    """
    Builds specific failure messages for the RESPONSABILIDAD vs tipificacion_tipo check.
    Returns a Dataframe with columns ['ID', 'mensaje', 'objetivo'] for failing records only.
    """

    if df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])
    
    messages = np.where(
        df['Validation_OK'],
        "\n\n Validation exitosa : RESPONSABLE coincide  con la primera palabra de tipificacion_tipo",
        (
            "\n\n No coincide la columna RESPONSABILIDAD en CORTE-EXCEL: \n\n" 
            + df["RESPONSABILIDAD"].astype(str)
            + "\n\n no coincide con la primera palabra de tipificacion_tipo en SGA 335 \n\n"
            + df['responsable_expected'].astype(str)
            +"."
        )
    )

    df['mensaje'] = messages
    df['objetivo'] = "1.9"
    df_failures = df[df['fail_count'] > 0]

    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]

