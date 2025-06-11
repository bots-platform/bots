from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_duracion_entero(df_merged: DataFrame) -> DataFrame:
    """
    Validates the 'DURACIÓN ENTERO' column in CORTE-EXCEL by checking:
    - If the value matches the extracted hour from TIEMPO (HH:MM)
    - If the value's grouping matches the expected grouping based on duration ranges
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - Duracion entero (string/int)
        - extracted_hour (string/int)
        - Agrupación entero (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - duracion_entero_ok (boolean): True if Duracion entero matches extracted_hour
        - agrupacion_expected (string): Expected grouping based on duration
        - agrupacion_entero_ok (boolean): True if Agrupación entero matches expected
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations
    """
    # Cache the DataFrame since it will be used multiple times
    df = df_merged.cache()
    
    # Check if Duracion entero matches extracted_hour
    df = df.withColumn(
        'duracion_entero_ok',
        F.col('extracted_hour') == F.col('Duracion entero')
    )

    # Define the expected grouping based on duration ranges
    df = df.withColumn(
        'agrupacion_expected',
        F.when(F.col('Duracion entero') == 0, F.lit('Menor a 1h'))
        .when(F.col('Duracion entero').isin([1, 2, 3]), F.lit('Entre 1h a 4h'))
        .when(F.col('Duracion entero').isin([4, 5, 6, 7]), F.lit('Entre 4h a 8h'))
        .when(F.col('Duracion entero').between(8, 23), F.lit('Entre 8h a 24h'))
        .otherwise(F.lit('Mayor a 24h'))
    )

    # Check if Agrupación entero matches the expected grouping
    df = df.withColumn(
        'agrupacion_entero_ok',
        F.trim(F.col('Agrupación entero')) == F.col('agrupacion_expected')
    )

    # Calculate overall validation status
    df = df.withColumn(
        'Validation_OK',
        F.col('duracion_entero_ok') & F.col('agrupacion_entero_ok')
    )

    # Count failures
    df = df.withColumn(
        'fail_count',
        F.when(~F.col('Validation_OK'), 1).otherwise(0)
    )

    return df

@log_exceptions
def build_failure_messages_duracion_entero(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'DURACIÓN ENTERO' validation.
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
            F.lit("Validación exitosa: DURACION ENTERO y Agrupacion entero")
        ).otherwise(
            F.concat(
                F.when(
                    ~F.col('duracion_entero_ok'),
                    F.concat(
                        F.lit("\n Duración entero EXCEL-CORTE: \n"),
                        F.col('Duracion entero').cast('string'),
                        F.lit("\n es diferente a hora extraída de TIEMPO (HH:MM) en EXCEL-CORTE: \n"),
                        F.col('extracted_hour').cast('string')
                    )
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('agrupacion_entero_ok'),
                    F.concat(
                        F.lit("\n Es incorrecto Agrupación entero en CORTE-EXCEL: \n"),
                        F.col('Agrupación entero'),
                        F.lit("\n debe ser: \n"),
                        F.col('agrupacion_expected')
                    )
                ).otherwise(F.lit(""))
            )
        )
    ).withColumn(
        'objetivo',
        F.lit("1.10")
    )

    # Filter failures and select required columns
    return df.filter(F.col('fail_count') > 0).select(
        'nro_incidencia',
        'mensaje',
        'TIPO REPORTE',
        'objetivo'
    )
