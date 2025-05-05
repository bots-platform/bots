
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

    _header_pat   = re.compile(r"^Se tuvo indisponibilidad por parte del cliente.*", re.IGNORECASE)
    _periodo_pat  = re.compile(
        r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*hasta el día\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*$",
        re.IGNORECASE
    )

    _hour_pad = re.compile(r"\b(\d)(?=:\d{2}(?::\d{2})?)")
    _day_pad_start = re.compile(r"^(\d)(?=/)")
    _day_pad_end = re.compile(r"(?<=hasta el día\s)(\d)(?=/)")

    def pad_periodo(line: str) -> str:

        line = _day_pad_start.sub(lambda m: m.group(1).zfill(2), line)
        line = _day_pad_end.sub(lambda m: m.group(1).zfill(2), line)
        line = _hour_pad.sub(lambda m: m.group(1).zfill(2), line)
        return line
    
    _total_pat    = re.compile(
        r"^\(Total de horas sin acceso a la sede:\s*(\d{1,3}:\d{2})\s*horas\)",
        re.IGNORECASE
    )

    def split_indispo(text: str) -> pd.Series:
        """
        Dado un bloque de texto que contiene:
          - 1 línea header (Se tuvo indisponibilidad...)
          - varias líneas periodo (DD/MM/YYYY hh:mm ... hasta el día ...)
          - 1 línea total (Total de horas… HH:MM horas)
        Devuelve una Series con tres campos:
          ['indisponibilidad_header', 'indisponibilidad_periodos', 'indisponibilidad_total']
        """

        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        header = ""
        for ln in lines:
            if _header_pat.match(ln):
                header = ln
                break
        
        periodos = [ln for ln in lines if _periodo_pat.match(ln)]
        periodos_fill_ceros = [pad_periodo(l) for l in periodos]
        
        total = ""
        for ln in lines:
            m = _total_pat.match(ln)
            if m:
                line = _hour_pad.sub(lambda m: m.group(1).zfill(2), ln)
                m = _total_pat.match(line)
                total = m.group(1)
                break

        return pd.Series({
            "indisponibilidad_header":   header,
            "indisponibilidad_periodos": "\n".join(periodos_fill_ceros),
            "indisponibilidad_total":    total,
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
        df['indisponibilidad_total'].astype(str).str.strip() == df['clock_stops_paragraph_footer']
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

    if df is None or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia','mensaje','TIPO REPORTE','objetivo'])

    header_err = np.where(
        ~df['indisponibilidad_header_match'],
        "\n Encabezado inválido en EXCEL-CORTE columna INDISPONIBILIDAD: \n"
        + df['indisponibilidad_header'].astype(str)
        + "\n es diferente a formato de Encabezado Indisponibilidad debe ser: \n"
        + df['clock_stops_paragraph_header'].astype(str),
        ""
    )

    periodos_err = np.where(
        ~df['indisponibilidad_periodos_match'],
        "\n Parada(s) de clientes en  CORTE - EXCEL columna INDISPONIBILIDAD : \n"
        + df['indisponibilidad_periodos'].astype(str)
        + "\n  ES DIFERENTE A SGA PAUSA CLIENTE SIN OVERLAP: \n"
        + df['clock_stops_paragraph_periodos'].astype(str),
        ""
    )

    total_err = np.where(
        ~df['indisponibilidad_total_match'],
        "\n Total inválido CORTE - EXCEL columna INDISPONIBILIDAD: \n"
        + df['indisponibilidad_total'].astype(str)
        + "\n  ES DIFERENTE a total SGA PAUSA CLIENTE SIN OVERLAP: \n"
        + df['clock_stops_paragraph_footer'].astype(str),
        ""
    )


    df['mensaje'] = np.where(
        df['Validation_OK'],
        "\n\n Validación exitosa: INDISPONIBILIDAD coincide con las paradas de reloj",
        header_err + periodos_err + total_err
    )

   
    df['objetivo'] = "1.11"


    df_failures = df[df['fail_count'] > 0]

    return df_failures[
        ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo']
    ]




