import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_fin_inicio_HHMM(merged_df: pd.DataFrame) -> pd.DataFrame:

    df = merged_df.copy()
    
    df['duration_diff_335'] = df['interrupcion_fin_truncated'] - df['Expected_Inicio_truncated']

    df['duration_diff_335_sec']  = df['duration_diff_335'].dt.total_seconds().astype(int)
    df['diff_335_sec_hhmm'] = (
        df['duration_diff_335_sec'] // 3600
    ).astype(str).str.zfill(2) + ":" + (
        (df['duration_diff_335_sec'] % 3600) // 60
    ).astype(str).str.zfill(2)

    df['duration_diff_corte_sec'] = (df['FECHA Y HORA FIN'] - df['FECHA Y HORA INICIO'])
    df['diff_corte_sec_hhmm'] = df['duration_diff_corte_sec'].apply(lambda x: f"{int(x.total_seconds() // 3600):02}:{int(x.total_seconds() % 3600 // 60):02d}")

    def parse_hhmm_to_minutes(hhmm_str):
        if pd.isna(hhmm_str):
            return np.nan
        try:
            h,m = str(hhmm_str).split(':')
            total_minutes = float(h) * 60 + float(m)
            print(f"Converted {hhmm_str} to {total_minutes} seconds")
            return total_minutes
        except Exception as e: 
            print(f"Error with {hhmm_str}: {e}")
            return np.nan
    
    df['fin_inicio_hhmm_column_corte_to_minutes'] = df['FIN-INICIO (HH:MM)_trimed'].apply(parse_hhmm_to_minutes)

    df['non_negative_335'] = df['duration_diff_335_sec'] >= 0
    df['non_negative_corte'] = df['duration_diff_corte_sec'].dt.total_seconds() >= 0

    df['non_negative_fin_inicio_column_corte_hhmm_to_minutes'] = df['fin_inicio_hhmm_column_corte_to_minutes'] >= 0


    def hhmm_to_minutes(hhmm_str):
        hh, mm = hhmm_str.split(":")
        return int(hh) * 60 + int (mm)

    df['duration_diff_335_min'] = df['diff_335_sec_hhmm'].apply(hhmm_to_minutes) 
    df['duration_diff_corte_min'] = df['diff_corte_sec_hhmm'].apply(hhmm_to_minutes) 

    df['match_335_corte'] = abs(df['duration_diff_335_min'] - df['duration_diff_corte_min']) <= 1


    df['match_corte_fin_inicio_hhmm_column'] = df['diff_corte_sec_hhmm'] == df['FIN-INICIO (HH:MM)_trimed']

    df['Validation_OK'] = (
        df['non_negative_335'] &
        df['non_negative_corte'] &
        df['non_negative_fin_inicio_column_corte_hhmm_to_minutes'] &
        df['match_335_corte'] &
        df['match_corte_fin_inicio_hhmm_column']
    )

    df['fail_count'] = (
        (~df['non_negative_335']).astype(int) +
        (~df['non_negative_corte']).astype(int) +
        (~df['non_negative_fin_inicio_column_corte_hhmm_to_minutes']).astype(int)+
        (~df['match_335_corte']).astype(int)+
        (~df['match_corte_fin_inicio_hhmm_column']).astype(int)
    )
    return df

@log_exceptions
def build_failure_messages_diff_fin_inicio_HHMM(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'FIN-INICIO' validation.
    Returns rows that fail any check (fail_count > 0 ) with columns:
    -'nummero de incidencia'
    - 'mensaje'
    - 'objetivo'
    """

    message = np.where(
        df['Validation_OK'],
        "FIN-INICIO validation successful",
        (
            np.where(~df['non_negative_335'],
                     "\n Diferencia en fechas interrupcion fin - inicio Esperado (depende si es masivo ) en SGA - 335  es negativo. ", "") +
            np.where(~df['non_negative_corte'],
                     "\n Diferencia Fecha Hora Fin - Fecha Hora Inicio en CORTE-EXCEL  es negativo. ", "") +
            np.where(~df['non_negative_fin_inicio_column_corte_hhmm_to_minutes'],
                     "\n\n FIN-INICIO (HH:MM) is negativo. ", "")+
            np.where(~df['match_335_corte'],
                    "\n Diferencia en Fecha Fin y Inicio esperado (depende si es masivo) en SGA 335: \n" + df["diff_335_sec_hhmm"].astype(str)+
                      "\n no coincide con diferencia en Fecha Inicio y Fin en CORTE-EXCEL (FIN-INICIO (HH:MM)): \n" +
                        df["diff_corte_sec_hhmm"].astype(str), "")+
             np.where(~df['match_corte_fin_inicio_hhmm_column'],
                    " \n Diferencia en Fecha Inicio y Fin en CORTE-EXCEL: \n" +
                      df["diff_corte_sec_hhmm"].astype(str)+
                    "\n no coincide con column FIN-INICIO (HH:MM) en CORTE-EXCEL:  \n" +
                        df['FIN-INICIO (HH:MM)_trimed'].astype(str), "")
        )
    )

    df['mensaje'] = message
    df['objetivo'] = "1.3"

    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]




