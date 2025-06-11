from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_fecha_inicio_fin(merged_df: DataFrame) -> DataFrame:
    """
    Validates start and end dates between Excel and Dynamic Report 335.

    Checks that Excel columns are not empty and that dates match the expected
    values according to business logic. Adds validation indicators and counts
    the number of failures.

    Parameters
    ----------
    merged_df : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - FECHA Y HORA INICIO (timestamp)
        - FECHA Y HORA FIN (timestamp)
        - Expected_Inicio_truncated (timestamp)
        - interrupcion_fin_truncated (timestamp)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - NotEmpty (boolean): True if both Excel dates are not null
        - Fecha_Inicio_match (boolean): True if start date matches
        - Fecha_Fin_match (boolean): True if end date matches
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-3)
    """
    # Cache the DataFrame since it will be used multiple times
    df = merged_df.cache()

    # Check if dates are not null
    df = df.withColumn(
        'NotEmpty',
        F.col('FECHA Y HORA INICIO').isNotNull() & F.col('FECHA Y HORA FIN').isNotNull()
    )

    # Check if dates match
    df = df.withColumn(
        'Fecha_Inicio_match',
        F.col('Expected_Inicio_truncated') == F.col('FECHA Y HORA INICIO')
    ).withColumn(
        'Fecha_Fin_match',
        F.col('interrupcion_fin_truncated') == F.col('FECHA Y HORA FIN')
    )

    # Calculate overall validation status
    df = df.withColumn(
        'Validation_OK',
        F.col('NotEmpty') & F.col('Fecha_Inicio_match') & F.col('Fecha_Fin_match')
    )

    # Count failures
    df = df.withColumn(
        'fail_count',
        F.when(~F.col('NotEmpty'), 1).otherwise(0) +
        F.when(~F.col('Fecha_Inicio_match'), 1).otherwise(0) +
        F.when(~F.col('Fecha_Fin_match'), 1).otherwise(0)
    )

    return df

@log_exceptions
def build_failure_messages_fechas_fin_inicio(df: DataFrame) -> DataFrame:
    """
    Generates validation error messages and filters failures.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Must contain:
        - Validation flags: Validation_OK, NotEmpty,
          Fecha_Inicio_match, Fecha_Fin_match, fail_count
        - Formatted dates: Expected_Inicio_truncated_fm,
          FECHA_Y_HORA_INICIO_fmt, interrupcion_fin_truncated_fm,
          FECHA_Y_HORA_FIN_fmt
        - Identifiers: nro_incidencia, TIPO REPORTE

    Returns
    -------
    pyspark.sql.DataFrame
        Filters rows with fail_count > 0 and columns:
        - nro_incidencia
        - mensaje (failure description)
        - TIPO REPORTE
        - objetivo (constant "1.2")
    """
    # Build error message using PySpark's concat and when functions
    df = df.withColumn(
        'mensaje',
        F.when(
            F.col('Validation_OK'),
            F.lit("Validación de fechas exitosa")
        ).otherwise(
            F.concat(
                F.when(
                    ~F.col('NotEmpty'),
                    F.lit("\n Las columnas 'FECHA Y HORA INICIO' y/o 'FECHA Y HORA FIN' están vacías. ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('Fecha_Inicio_match'),
                    F.concat(
                        F.lit("\n (interrupcion_inicio|fecha generacion) en SGA: \n"),
                        F.col('Expected_Inicio_truncated_fm'),
                        F.lit("\n no coincide con FECHA Y HORA INICIO CORTE-EXCEL: \n"),
                        F.col('FECHA_Y_HORA_INICIO_fmt')
                    )
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('Fecha_Fin_match'),
                    F.concat(
                        F.lit("\n (interrupcion_fin) en SGA: \n"),
                        F.col('interrupcion_fin_truncated_fm'),
                        F.lit("\n no coincide con FECHA Y HORA FIN CORTE-EXCEL: \n"),
                        F.col('FECHA_Y_HORA_FIN_fmt')
                    )
                ).otherwise(F.lit(""))
            )
        )
    ).withColumn(
        'objetivo',
        F.lit("1.2")
    )

    # Filter failures and select required columns
    return df.filter(F.col('fail_count') > 0).select(
        'nro_incidencia',
        'mensaje',
        'TIPO REPORTE',
        'objetivo'
    )

