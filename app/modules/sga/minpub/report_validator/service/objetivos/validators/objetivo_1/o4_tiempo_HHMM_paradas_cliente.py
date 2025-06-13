from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime
import pandas as pd

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_tiempo_HHMM_paradas_cliente(df_merged: DataFrame) -> pd.DataFrame:
    """
    Validates the 'TIEMPO (HH:MM)' column in CORTE-EXCEL by comparing:
    - (interruppcion_fin - interrupcion) - sum(paradas)
    vs.
    - The parsed minutes of 'TIEMPO(HH:MM)'.
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - interrupcion_fin_truncated (timestamp)
        - Expected_Inicio_truncated (timestamp)
        - TIEMPO (HH:MM)_trimed (string)
        - sum_paradas (double)

    Returns
    -------
    pandas.DataFrame
        DataFrame with these additional columns:
        - diff_335_min (double): Time difference in minutes
        - tiempo_corte_min (double): Parsed time from HH:MM format
        - effective_time_335 (double): Effective time after subtracting stops
        - effective_time_335_to_HHMM_str (string): Formatted time string
        - non_negative_335 (boolean): True if time difference is non-negative
        - non_negative_paradas (boolean): True if sum of stops is non-negative
        - non_negative_effective (boolean): True if effective time is non-negative
        - match_corte (boolean): True if times match within tolerance
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-4)
    """
    with spark_manager.get_session_context() as spark:
        df = df_merged.cache()
        
        df = df.withColumn(
            'diff_335_min',
            F.expr("(unix_timestamp(interrupcion_fin_truncated) - unix_timestamp(Expected_Inicio_truncated)) / 60")
        )

        def parse_hhmm_to_minutes(hhmm_str):
            if hhmm_str is None:
                return None
            try:
                h, m = str(hhmm_str).split(':')
                return float(h) * 60 + float(m)
            except Exception:
                return None

        parse_hhmm_udf = F.udf(parse_hhmm_to_minutes, 'double')
        df = df.withColumn('tiempo_corte_min', parse_hhmm_udf(F.col('TIEMPO (HH:MM)_trimed')))

        df = df.withColumn('effective_time_335', F.col('diff_335_min') - F.col('sum_paradas'))

        def convert_minutes_to_hhmm(minutes):
            if minutes is None:
                return None
            try:
                hours = int(minutes // 60)
                mins = int(minutes % 60)
                return f"{hours:02d}:{mins:02d}"
            except Exception:
                return None

        convert_hhmm_udf = F.udf(convert_minutes_to_hhmm, 'string')
        df = df.withColumn('effective_time_335_to_HHMM_str', convert_hhmm_udf(F.col('effective_time_335')))

        df = df.withColumn('non_negative_335', F.col('diff_335_min') >= 0) \
              .withColumn('non_negative_paradas', F.col('sum_paradas') >= 0) \
              .withColumn('non_negative_effective', F.col('effective_time_335') >= 0)

        tolerance = 1
        df = df.withColumn(
            'match_corte',
            F.abs(F.col('tiempo_corte_min') - F.col('effective_time_335')) <= tolerance
        )

        df = df.withColumn(
            'Validation_OK',
            F.col('non_negative_335') &
            F.col('non_negative_paradas') &
            F.col('non_negative_effective') &
            F.col('match_corte')
        )

        df = df.withColumn(
            'fail_count',
            F.when(~F.col('non_negative_335'), 1).otherwise(0) +
            F.when(~F.col('non_negative_paradas'), 1).otherwise(0) +
            F.when(~F.col('non_negative_effective'), 1).otherwise(0) +
            F.when(~F.col('match_corte'), 1).otherwise(0)
        )

        pdf = df.toPandas()
        return pdf

@log_exceptions
def buid_failure_messages_tiempo_HHMM_paradas_cliente(df: DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'TIEMPO (HH:MM)' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    with spark_manager.get_session_context() as spark:
        df = df.withColumn(
            'mensaje',
            F.when(
                F.col('Validation_OK'),
                F.lit("Validation de TIEMPO (HH:MM) exitosa")
            ).otherwise(
                F.concat(
                    F.when(
                        ~F.col('non_negative_335'),
                        F.lit("\n  INTERRUPCION_FIN - INTERRUPCION_INICIO es negativo. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('non_negative_paradas'),
                        F.lit("\n  Suma de paradas de reloj es negativa. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('non_negative_effective'),
                        F.lit("\n Tiempo efectivo (INTERRUPCION - paradas) es negativo.")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('match_corte'),
                        F.concat(
                            F.lit("\n EL TIEMPO (HH:MM) en CORTE-EXCEL: \n"),
                            F.col('TIEMPO (HH:MM)_trimed'),
                            F.lit("\n no coincide con el tiempo efectivo calculado SGA: \n"),
                            F.col('effective_time_335_to_HHMM_str')
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("1.4")
        )
        df_failures = df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )
        pdf = df_failures.toPandas()
        return pdf
