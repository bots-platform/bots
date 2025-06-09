from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_medidas_correctivas(df_merged: DataFrame) -> DataFrame:
    """
    Validates the 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS' column in CORTE-EXCEL by checking:
    - If the value is non-empty
    - If the value matches the expected format based on TIPO REPORTE
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed (string)
        - TIPO REPORTE_trimed (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - non_empty_medidas (boolean): True if MEDIDAS CORRECTIVAS is not null
        - match_medidas (boolean): True if MEDIDAS CORRECTIVAS matches expected format
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-2)
    """
    # Cache the DataFrame since it will be used multiple times
    df = df_merged.cache()
    
    # Check if column is non-empty
    df = df.withColumn(
        'non_empty_medidas',
        F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed').isNotNull()
    )

    # Check if MEDIDAS CORRECTIVAS matches expected format based on TIPO REPORTE
    df = df.withColumn(
        'match_medidas',
        (
            (F.col('TIPO REPORTE_trimed') == F.lit('INCIDENTE')) & 
            F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed').startswith('Se realizó la revisión del servicio') |
            (F.col('TIPO REPORTE_trimed') == F.lit('INTERRUPCION')) & 
            F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed').startswith('Se realizó la revisión del servicio')
        )
    )

    # Calculate overall validation status
    df = df.withColumn(
        'Validation_OK',
        F.col('non_empty_medidas') &
        F.col('match_medidas')
    )

    # Count failures
    df = df.withColumn(
        'fail_count',
        F.when(~F.col('non_empty_medidas'), 1).otherwise(0) +
        F.when(~F.col('match_medidas'), 1).otherwise(0)
    )

    return df

@log_exceptions
def build_failure_messages_medidas_correctivas(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS' validation.
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
            F.lit("Validation de MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS exitosa")
        ).otherwise(
            F.concat(
                F.when(
                    ~F.col('non_empty_medidas'),
                    F.lit("\n  MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS en CORTE-EXCEL es vacio. \n")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('match_medidas'),
                    F.concat(
                        F.lit("\n  MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS en CORTE-EXCEL: \n"),
                        F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_trimed'),
                        F.lit("\n no coincide con el formato esperado para el TIPO REPORTE: \n"),
                        F.col('TIPO REPORTE_trimed')
                    )
                ).otherwise(F.lit(""))
            )
        )
    ).withColumn(
        'objetivo',
        F.lit("1.8")
    )

    # Filter failures and select required columns
    return df.filter(F.col('fail_count') > 0).select(
        'nro_incidencia',
        'mensaje',
        'TIPO REPORTE',
        'objetivo'
    )