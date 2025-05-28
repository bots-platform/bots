import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)
 

@log_exceptions
def validation_tiempo_HHMM_paradas_cliente(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Validatess  the 'TIEMPO (HH:MM)' column CORTE-EXCEL by comparing:
    - (interruppcion_fin - interrupcion) - sum(paradas)
    vs.
    - The parsed minutes of 'TIEMPO(HH:MM)'.
    Returns a Dataframe with boolean flags and 'fail_count'. 
    """

    df = df_merged.copy()

    df['diff_335_min'] = (
        (df['interrupcion_fin_truncated'] - df['Expected_Inicio_truncated'])
        .dt.total_seconds()/60
    )

    def parse_hhmm_to_minutes(hhmm_str):
        if pd.isna(hhmm_str):
            return np.nan
        try:
            h,m = str(hhmm_str).split(':')
            total_minutes = float(h)*60 + float(m)
            return total_minutes
        except Exception as e:
            return np.nan
        

    df['tiempo_corte_min'] = df['TIEMPO (HH:MM)_trimed'].apply(parse_hhmm_to_minutes)

    df['effective_time_335'] = df['diff_335_min'] - df['sum_paradas']

    def convert_minutes_to_hhmm(minutes):
        if pd.isna(minutes):
            return np.nan
        try:
            hours = int(minutes//60)
            mins = int(minutes % 60)

            time_str = f"{hours:02d}:{mins:02d}"
            print(f"Converted {minutes} minutes to {time_str}")
            return time_str
        except Exception as e:
            print(f"Error converting {minutes} to HH:MM {e}")
            return np.nan

    df['effective_time_335_to_HHMM_str'] = df['effective_time_335'].apply(convert_minutes_to_hhmm)

    df['non_negative_335'] = df['diff_335_min'] >= 0
    df['non_negative_paradas'] = df['sum_paradas'] >= 0
    df['non_negative_effective'] = df['effective_time_335'] >= 0

    tolerance = 1

    df['match_corte'] = (
        (df['tiempo_corte_min'] - df['effective_time_335'])
        .abs() <= tolerance
    )

    df['Validation_OK'] = (
        df['non_negative_335'] &
        df['non_negative_paradas'] &
        df['non_negative_effective'] &
        df['match_corte']
    )

    df['fail_count'] = (
        (~df['non_negative_335']).astype(int) + 
        (~df['non_negative_paradas']).astype(int) + 
        (~df['non_negative_effective']).astype(int) +
        (~df['match_corte']).astype(int)
    
    )

    return df

@log_exceptions
def buid_failure_messages_tiempo_HHMM_paradas_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'TIEMPO (HH:MM)' validation.
    Returns rows that fail any check (fall_count > 0) with columns:
    -'numero de incidencia'
    -'mensaje'
    -'objetivo'
    
    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de TIEMPO (HH:MM) exitosa",
        (
            np.where(~df['non_negative_335'],
                     "\n  INTERRUPCION_FIN - INTERRUPCION_INICIO es negativo. \n",  "")+
            np.where(~df['non_negative_paradas'],
                     "\n  Suma de paradas de reloj es negativa. \n", "")+
            np.where(~df['non_negative_effective'],
                     "\n Tiempo efectivo (INTERRUPCION - paradas) es negativo.", "")+
           np.where(~df['match_corte'],
                     "\n EL TIEMPO (HH:MM) en CORTE-EXCEL: \n" + df['TIEMPO (HH:MM)_trimed'].astype(str) +
                       "\n no coincide con el tiempo efectivo calculado SGA: \n" +df['effective_time_335_to_HHMM_str'].astype(str)  , "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.4"

    df_failures = df[df['fail_count'] > 0 ]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]

