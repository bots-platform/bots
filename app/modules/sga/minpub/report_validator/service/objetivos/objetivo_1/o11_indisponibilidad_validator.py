
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime, timedelta

import re

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


@log_exceptions
def validate_indisponibilidad(
    merged_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Validate anexos indisponibilidad.
    Return a DataFrame with new Boolean match columns.
    """
    df = merged_df.copy()


    def split_indispo(paragraph: str):
    
        if not isinstance(paragraph, str) or not paragraph.strip():
            return pd.Series({
                'indisponibilidad_header': "",
                'indisponibilidad_periodos': "",
                'indisponibilidad_total': ""
            })
        lines = [l.strip() for l in paragraph.splitlines() if l.strip()]
        return pd.Series({
            'indisponibilidad_header': lines[0],
            'indisponibilidad_periodos': "\n".join(lines[1:-1]) if len(lines) > 2 else "",
            'indisponibilidad_total': lines[-1] if len(lines) > 1 else ""
        })

    df[['indisponibilidad_header',
        'indisponibilidad_periodos',
        'indisponibilidad_total']] = (
        df['INDISPONIBILIDAD']
          .apply(split_indispo)
    )


    df['indisponibilidad_header_match'] = (
        df['indisponibilidad_header'].astype(str).str.strip()
        == df['clock_stops_paragraph_header']
    )
    df['indisponibilidad_periodos_match'] = (
        df['indisponibilidad_periodos'].astype(str).str.strip()
        == df['clock_stops_paragraph_periodos']
    )
    df['indisponibilidad_total_match'] = (
        df['indisponibilidad_total'].astype(str).str.strip()
        == df['clock_stops_paragraph_footer']
    )

    df['Validation_OK'] = (
        df['indisponibilidad_header_match']
        & df['indisponibilidad_periodos_match']
        & df['indisponibilidad_total_match']
    )
    df['fail_count'] = (
        (~df['indisponibilidad_header_match']).astype(int)
        + (~df['indisponibilidad_periodos_match']).astype(int)
        + (~df['indisponibilidad_total_match']).astype(int)
    )

    return df



@log_exceptions
def build_failure_messages_indisponibilidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame of failures with columns:
    ['nro_incidencia','mensaje','TIPO REPORTE','objetivo']
    """
    # guard
    if df is None or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia','mensaje','TIPO REPORTE','objetivo'])

    header_err = np.where(
        ~df['indisponibilidad_header_match'],
        "Encabezado inválido EXCEL-CORTE: "
        + df['indisponibilidad_header'].astype(str)
        + " ↔ "
        + df['clock_stops_paragraph_header'].astype(str)
        + ". ",
        ""
    )

    periodos_err = np.where(
        ~df['indisponibilidad_periodos_match'],
        "Periodo(s) inválido(s): "
        + df['indisponibilidad_periodos'].astype(str)
        + " ↔ "
        + df['clock_stops_paragraph_periodos'].astype(str)
        + ". ",
        ""
    )

    total_err = np.where(
        ~df['indisponibilidad_total_match'],
        "Total inválido EXCEL-CORTE: "
        + df['indisponibilidad_total'].astype(str)
        + " ↔ "
        + df['clock_stops_paragraph_footer'].astype(str)
        + ". ",
        ""
    )


    df['mensaje'] = np.where(
        df['Validation_OK'],
        "Validación exitosa: INDISPONIBILIDAD coincide con las paradas de reloj",
        header_err + periodos_err + total_err
    )

   
    df['objetivo'] = "1.11"


    df_failures = df[df['fail_count'] > 0]

    return df_failures[
        ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo']
    ]




