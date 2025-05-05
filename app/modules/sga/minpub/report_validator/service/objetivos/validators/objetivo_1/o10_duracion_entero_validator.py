
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
import language_tool_python
import re

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

def validate_duracion_entero(df_merged: pd.DataFrame) -> pd.DataFrame:
    df = df_merged.copy()


    pat = re.compile(
        r'^(?:(?P<days>\d+)\s+day[s]?,\s*)?'  # "1 day, " o nada
        r'(?P<hours>\d{1,2}):'               # horas antes del primer ':'
    )

    def extract_total_hours(x):
        s = str(x).strip()
        m = pat.match(s)
        if not m:
            return pd.NA  # o 0, según lo que quieras
        days = int(m.group('days')) if m.group('days') else 0
        hrs  = int(m.group('hours'))
        return days * 24 + hrs

    df['extracted_hour'] = df['TIEMPO (HH:MM)'].apply(extract_total_hours).astype(int)

    df['duracion_entero_ok'] = df['extracted_hour'] == df['Duracion entero']


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
        df['Agrupación entero'].str.strip()
        == df['agrupacion_expected'].astype(str)
    )

    df['Validation_OK'] = df['duracion_entero_ok'] & df['agrupacion_entero_ok']
    df['fail_count']   = (~df['Validation_OK']).astype(int)

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
            "\n Duración entero EXCEL-CORTE: \n" 
            + df['Duracion entero'].astype(str) 
            + "\n es diferente a hora extraída de TIEMPO (HH:MM) en EXCEL-CORTE: \n" 
            + df['extracted_hour'].astype(str),
            ""
        )
        + np.where(
            ~df['agrupacion_entero_ok'],
            "\n Es incorrecto Agrupación entero en CORTE-EXCEL: \n"
            + df['Agrupación entero'].astype(str)
            + "\n debe ser: \n"
            + df['agrupacion_expected'],
            ""
        )
    )

    df['mensaje']  = mensajes
    df['objetivo'] =  "1.10"
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]




