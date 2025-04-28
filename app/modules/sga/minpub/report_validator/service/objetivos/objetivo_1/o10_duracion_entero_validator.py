
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
import language_tool_python
import re

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


def validate_duracion_entero(df_merged: pd.DataFrame)-> pd.DataFrame:
    """
    1) Extract  the hour to the left of ':' from 'TIEMPO (HH:MM)' -> extracted_hour
    2) Check the extracted_hour == DURACION ENTERO -> duracion_entero_ok
    3) Based on DURACION ENTERO , compute agrupacion_expected:

    0 -> 'Menor a 1h'
    1-3 -> 'Entre 1h a 4h'
    4-7 -> 'Entre 4h a 8h'
    8-23 -> 'Entre 8h a 24h'

    4) Check that Agrupacion entero == agrupacion_expected -> agrupacion_entero_ok
    5) Combine into Validation_OK & fail_count
    """
    df = df_merged.copy()

    df['extracted_hour'] = (
        df['TIEMPO (HH:MM)']
        .astype(str)
        .str.split(':', n=1)
        .str[0]
        .astype(int)
    )

    df['duracion_entero_ok'] = (
        df['extracted_hour'] == df['Duracion entero']
    )

    conditions = [
        df['Duracion entero'] == 0,
        df['Duracion entero'].isin([1, 2,3 ]),
        df['Duracion entero'].isin([4, 5, 6, 7]),
        df['Duracion entero'].between(8, 23),
    ]

    choises = [
        'Menor a 1h',
        'Entre 1h a 4h',
        'Entre 4h a 8h',
        'Entre 8h a 24h',
    ]

    df['agrupacion_expected'] = np.select(conditions, choises, default='Mayor a 24h') 

    df['agrupacion_entero_ok'] = (
        df['Agrupación entero'].astype(str).str.strip() == df['agrupacion_expected']
    )

    df['Validation_OK'] = df['duracion_entero_ok'] & df['agrupacion_entero_ok']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df


@log_exceptions
def build_failure_messages_duracion_entero(df: pd.DataFrame) -> pd.DataFrame:
    """
    For rows where fail_count > 0, build a descriptive 'mensaje' and 
    assign an 'objetivo' code.
    Returns columns ['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'].
    """
    if df is None or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])

    mensajes = np.where(
        df['Validation_OK'],
        "Validación exitosa: DURACION ENTERO y Agrupacion entero",
        np.where(
            ~df['duracion_entero_ok'],
            "Duración entero EXCEL-CORTE: " 
            + df['Duracion entero'].astype(str) 
            + " ≠ hora extraída de TIEMPO (HH:MM): " 
            + df['extracted_hour'].astype(str) 
            + ".",
            ""
        )
        + np.where(
            ~df['agrupacion_entero_ok'],
            " Agrupación entero '"
            + df['Agrupación entero'].astype(str)
            + "' incorrecta, debe ser '"
            + df['agrupacion_expected']
            + "'.",
            ""
        )
    )

    df['mensaje']  = mensajes
    df['objetivo'] =  1.10 
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]




