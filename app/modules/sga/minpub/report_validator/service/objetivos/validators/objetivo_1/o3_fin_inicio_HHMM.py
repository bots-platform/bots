from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_fin_inicio_HHMM(merged_df: DataFrame) -> DataFrame:
    """
    Validates the time difference between start and end dates in both SGA-335 and Excel report.
    
    Parameters
    ----------
    merged_df : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - duration_diff_335_sec (double)
        - duration_diff_corte_sec (interval)
        - fin_inicio_hhmm_column_corte_to_minutes (double)
        - duration_diff_335_min (double)
        - duration_diff_corte_min (double)
        - diff_corte_sec_hhmm (string)
        - FIN-INICIO (HH:MM)_trimed (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - non_negative_335 (boolean): True if SGA-335 duration is non-negative
        - non_negative_corte (boolean): True if Excel duration is non-negative
        - non_negative_fin_inicio_column_corte_hhmm_to_minutes (boolean): True if HH:MM duration is non-negative
        - match_335_corte (boolean): True if durations match within 1 minute
        - match_corte_fin_inicio_hhmm_column (boolean): True if durations match exactly
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-5)
    """
    # Cache the DataFrame since it will be used multiple times
    df = merged_df.cache()
    
    # Check if durations are non-negative
    df = df.withColumn(
        'non_negative_335',
        F.col('duration_diff_335_sec') >= 0
    ).withColumn(
        'non_negative_corte',
        F.expr("duration_diff_corte_sec.total_seconds() >= 0")
    ).withColumn(
        'non_negative_fin_inicio_column_corte_hhmm_to_minutes',
        F.col('fin_inicio_hhmm_column_corte_to_minutes') >= 0
    )

    # Check if durations match within tolerance
    df = df.withColumn(
        'match_335_corte',
        F.abs(F.col('duration_diff_335_min') - F.col('duration_diff_corte_min')) <= 1
    ).withColumn(
        'match_corte_fin_inicio_hhmm_column',
        F.col('diff_corte_sec_hhmm') == F.col('FIN-INICIO (HH:MM)_trimed')
    )

    # Calculate overall validation status
    df = df.withColumn(
        'Validation_OK',
        F.col('non_negative_335') &
        F.col('non_negative_corte') &
        F.col('non_negative_fin_inicio_column_corte_hhmm_to_minutes') &
        F.col('match_335_corte') &
        F.col('match_corte_fin_inicio_hhmm_column')
    )

    # Count failures
    df = df.withColumn(
        'fail_count',
        F.when(~F.col('non_negative_335'), 1).otherwise(0) +
        F.when(~F.col('non_negative_corte'), 1).otherwise(0) +
        F.when(~F.col('non_negative_fin_inicio_column_corte_hhmm_to_minutes'), 1).otherwise(0) +
        F.when(~F.col('match_335_corte'), 1).otherwise(0) +
        F.when(~F.col('match_corte_fin_inicio_hhmm_column'), 1).otherwise(0)
    )

    return df

@log_exceptions
def build_failure_messages_diff_fin_inicio_HHMM(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'FIN-INICIO' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    # Build error message using PySpark's concat and when functions
    df = df.withColumn(
        'mensaje',
        F.when(
            F.col('Validation_OK'),
            F.lit("FIN-INICIO validation successful")
        ).otherwise(
            F.concat(
                F.when(
                    ~F.col('non_negative_335'),
                    F.lit("\n Diferencia en fechas interrupcion fin - inicio Esperado (depende si es masivo ) en SGA - 335  es negativo. ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('non_negative_corte'),
                    F.lit("\n Diferencia Fecha Hora Fin - Fecha Hora Inicio en CORTE-EXCEL  es negativo. ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('non_negative_fin_inicio_column_corte_hhmm_to_minutes'),
                    F.lit("\n\n FIN-INICIO (HH:MM) is negativo. ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('match_335_corte'),
                    F.concat(
                        F.lit("\n Diferencia en Fecha Fin y Inicio esperado (depende si es masivo) en SGA 335: \n"),
                        F.col("diff_335_sec_hhmm"),
                        F.lit("\n no coincide con diferencia en Fecha Inicio y Fin en CORTE-EXCEL (FIN-INICIO (HH:MM)): \n"),
                        F.col("diff_corte_sec_hhmm")
                    )
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('match_corte_fin_inicio_hhmm_column'),
                    F.concat(
                        F.lit(" \n Diferencia en Fecha Inicio y Fin en CORTE-EXCEL: \n"),
                        F.col("diff_corte_sec_hhmm"),
                        F.lit("\n no coincide con column FIN-INICIO (HH:MM) en CORTE-EXCEL:  \n"),
                        F.col('FIN-INICIO (HH:MM)_trimed')
                    )
                ).otherwise(F.lit(""))
            )
        )
    ).withColumn(
        'objetivo',
        F.lit("1.3")
    )

    # Filter failures and select required columns
    return df.filter(F.col('fail_count') > 0).select(
        'nro_incidencia',
        'mensaje',
        'TIPO REPORTE',
        'objetivo'
    )
